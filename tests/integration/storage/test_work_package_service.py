import asyncio
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from vuzol.discussion import DomainError, PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.discussion.domain import (
    CapabilityProvisioning,
    CapabilityRequirementDraft,
    ComponentKind,
    EnvironmentComponentDraft,
    EnvironmentDeltaDraft,
)
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    PlanRevision,
    ProjectEnvironmentRevision,
    Task,
    WorkPackage,
    WorkPackageOpenDetail,
)
from vuzol.storage.types import (
    EditSessionStatus,
    PlanRevisionCreatedBy,
    PlanRevisionState,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def plan(title: str = "Discussion MVP", *, item_id: uuid.UUID | None = None) -> PlanDraft:
    return PlanDraft(
        title=title,
        items=(
            PlanItemDraft(
                item_id=item_id,
                local_id="domain",
                summary="Implement lifecycle",
                goal="Persist fenced work-package changes",
                expected_outcome="A deterministic revision exists",
                completion_criteria=("Domain tests pass",),
                allowed_scope="src/vuzol/discussion/**",
                out_of_scope=("Telegram ingress",),
                trusted_checks=("pytest",),
            ),
        ),
    )


async def create_package(factory: object) -> tuple[uuid.UUID, uuid.UUID, str]:
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=91
        )
        result = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
    return session_id, result.package_id, result.content_hash


async def test_create_approve_revise_invalidates_approval_without_tasks(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, first_hash = await create_package(factory)

    async with UnitOfWork(factory) as uow:
        generation = await WorkPackageService(uow).approve(
            package_id=package_id,
            revision_number=1,
            h8=first_hash[:8],
            expected_status_generation=1,
            user_id=42,
        )
    assert generation == 2

    async with UnitOfWork(factory) as uow:
        service = WorkPackageService(uow)
        approved_id = await service.validate_startable(
            package_id=package_id,
            revision_number=1,
            h8=first_hash[:8],
            expected_status_generation=2,
        )
        first_revision = await uow.work_packages.get_revision(approved_id)
        first_item_id = uuid.UUID(str(first_revision.immutable_body["items"][0]["item_id"]))
        revised = await service.revise_draft(
            package_id=package_id,
            expected_status_generation=2,
            plan=plan("Discussion MVP revised", item_id=first_item_id),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
        )
    assert revised.revision_number == 2
    assert revised.status_generation == 3

    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        revisions = (
            await session.scalars(
                select(PlanRevision)
                .where(PlanRevision.work_package_id == package_id)
                .order_by(PlanRevision.revision_number)
            )
        ).all()
        task_count = await session.scalar(select(func.count()).select_from(Task))
        link_count = await session.scalar(select(func.count()).select_from(MaterializationLink))
    assert package is not None
    assert package.status is WorkPackageStatus.DRAFT
    assert package.approved_revision_id is None
    assert [revision.state for revision in revisions] == [
        PlanRevisionState.SUPERSEDED,
        PlanRevisionState.DRAFT,
    ]
    assert task_count == 0 and link_count == 0
    await engine.dispose()


async def test_plan_approval_versions_environment_delta_atomically(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=92
        )
        drafted = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=PlanDraft(
                title="Add runtime",
                items=plan().items,
                environment_delta=EnvironmentDeltaDraft(
                    upsert_components=(
                        EnvironmentComponentDraft(
                            key="api",
                            label="API",
                            kind=ComponentKind.WEB_SERVICE,
                            technology="Flask",
                            version="3",
                            run_command=("python", "-m", "app"),
                            port=8000,
                            healthcheck_path="/health",
                        ),
                    ),
                    required_capabilities=(
                        CapabilityRequirementDraft(
                            key="python-runtime",
                            label="Python runtime",
                            provisioning=CapabilityProvisioning.AUTOMATIC,
                        ),
                    ),
                ),
            ),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
    async with UnitOfWork(factory) as uow:
        await WorkPackageService(uow).approve(
            package_id=drafted.package_id,
            revision_number=1,
            h8=drafted.content_hash[:8],
            expected_status_generation=1,
            user_id=42,
        )
    async with factory() as session:
        environment = await session.scalar(select(ProjectEnvironmentRevision))
        revision = await session.get(PlanRevision, drafted.revision_id)
        assert environment is not None and revision is not None
        assert environment.source_plan_revision_id == revision.id
        assert environment.approved_by_user_id == 42
        assert environment.contract["components"]["api"]["technology"] == "Flask"
        assert "python-runtime" in environment.contract["capabilities"]
        assert revision.immutable_body["environment_delta"]["upsert_components"][0][
            "run_command"
        ] == ["python", "-m", "app"]
    await engine.dispose()


async def test_create_rejects_duplicate_outline_of_terminal_plan(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id, package_id, _ = await create_package(factory)
    async with factory.begin() as session:
        package = await session.get(WorkPackage, package_id, with_for_update=True)
        assert package is not None
        package.status = WorkPackageStatus.DISCARDED

    with pytest.raises(DomainError) as duplicate:
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).create_draft(
                session_id=session_id,
                project_id="vuzol",
                plan=PlanDraft(
                    title="  DISCUSSION   MVP ",
                    items=(
                        PlanItemDraft(
                            local_id="different-id",
                            summary="  IMPLEMENT   LIFECYCLE ",
                            goal="A slightly rephrased goal",
                            expected_outcome="Different wording",
                            completion_criteria=("A different criterion",),
                            allowed_scope="different/**",
                        ),
                    ),
                ),
                created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
                actor_type="planner_model",
            )
    assert duplicate.value.code == "duplicate_terminal_plan"

    async with UnitOfWork(factory) as uow:
        created = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=plan("A genuinely new plan"),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
    assert created.package_id != package_id
    await engine.dispose()


async def test_fences_edit_session_and_detail_events_fail_closed(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, content_hash = await create_package(factory)

    with pytest.raises(DomainError) as stale:
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).approve(
                package_id=package_id,
                revision_number=1,
                h8=content_hash[:8],
                expected_status_generation=9,
                user_id=42,
            )
    assert stale.value.code == "stale_generation"

    async with UnitOfWork(factory) as uow:
        service = WorkPackageService(uow)
        await service.set_open_detail(
            package_id=package_id, revision_number=1, h8=content_hash[:8], ordinal=1
        )
        edit = await service.open_edit_session(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            ordinal=1,
            user_id=42,
            expires_in=timedelta(minutes=5),
        )

    with pytest.raises(DomainError) as owner:
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).close_edit_session(edit_session_id=edit.id, user_id=7)
    assert owner.value.code == "edit_session_owner_mismatch"

    async with UnitOfWork(factory) as uow:
        service = WorkPackageService(uow)
        await service.clear_open_detail(package_id=package_id)
        await service.close_edit_session(edit_session_id=edit.id, user_id=42)

    async with factory() as session:
        persisted_edit = await session.get(type(edit), edit.id)
        detail = await session.get(WorkPackageOpenDetail, package_id)
        events = (
            await session.scalars(
                select(Event.event_type).where(
                    Event.event_type.in_(
                        {"work_package.detail_pointer_changed", "edit_session.closed"}
                    )
                )
            )
        ).all()
    assert persisted_edit is not None and persisted_edit.status is EditSessionStatus.CLOSED
    assert detail is None
    assert events.count("work_package.detail_pointer_changed") == 2
    assert "edit_session.closed" in events
    await engine.dispose()


async def test_concurrent_approve_accepts_only_button_epoch_once(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, content_hash = await create_package(factory)

    async def approve() -> str:
        try:
            async with UnitOfWork(factory) as uow:
                await WorkPackageService(uow).approve(
                    package_id=package_id,
                    revision_number=1,
                    h8=content_hash[:8],
                    expected_status_generation=1,
                    user_id=42,
                )
        except DomainError as error:
            return error.code
        return "approved"

    outcomes = await asyncio.gather(approve(), approve())
    assert sorted(outcomes) == ["approved", "stale_generation"]

    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        approval_events = await session.scalar(
            select(func.count())
            .select_from(Event)
            .where(Event.event_type == "plan_revision.approved")
        )
    assert package is not None and package.version == 2
    assert approval_events == 1
    await engine.dispose()


async def test_item_edit_creates_new_revision_and_consumes_session(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, content_hash = await create_package(factory)

    async with UnitOfWork(factory) as uow:
        service = WorkPackageService(uow)
        edit = await service.open_edit_session(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            ordinal=1,
            user_id=42,
        )
        head = await uow.work_packages.get_revision(edit.plan_revision_id)
        item_id = uuid.UUID(str(head.immutable_body["items"][0]["item_id"]))

    async with UnitOfWork(factory) as uow:
        revised = await WorkPackageService(uow).apply_item_edit(
            edit_session_id=edit.id,
            expected_session_generation=1,
            replacement=plan("Edited package", item_id=item_id),
            user_id=42,
        )

    async with factory() as session:
        persisted_edit = await session.get(type(edit), edit.id)
        tasks = await session.scalar(select(func.count()).select_from(Task))
    assert revised.revision_number == 2 and revised.status_generation == 2
    assert persisted_edit is not None and persisted_edit.status is EditSessionStatus.ACCEPTED
    assert tasks == 0
    await engine.dispose()


async def test_discard_is_terminal_and_expires_edit_session_explicitly(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, content_hash = await create_package(factory)

    async with UnitOfWork(factory) as uow:
        edit = await WorkPackageService(uow).open_edit_session(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            ordinal=1,
            user_id=42,
            expires_in=timedelta(seconds=-1),
        )

    with pytest.raises(DomainError) as expired:
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).apply_item_edit(
                edit_session_id=edit.id,
                expected_session_generation=1,
                replacement=plan("will not persist"),
                user_id=42,
            )
    assert expired.value.code == "edit_session_expired"

    async with UnitOfWork(factory) as uow:
        service = WorkPackageService(uow)
        await service.close_edit_session(edit_session_id=edit.id, user_id=None, expired=True)
        generation = await service.discard(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            expected_status_generation=1,
            user_id=42,
        )
    assert generation == 2

    with pytest.raises(DomainError) as terminal:
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).revise_draft(
                package_id=package_id,
                expected_status_generation=2,
                plan=plan("forbidden"),
                created_by=PlanRevisionCreatedBy.USER,
                actor_type="user",
            )
    assert terminal.value.code == "terminal_package"

    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        persisted_edit = await session.get(type(edit), edit.id)
        task_count = await session.scalar(select(func.count()).select_from(Task))
    assert package is not None and package.status is WorkPackageStatus.DISCARDED
    assert persisted_edit is not None and persisted_edit.status is EditSessionStatus.EXPIRED
    assert task_count == 0
    await engine.dispose()
