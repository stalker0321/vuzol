import asyncio
import uuid

import pytest
from sqlalchemy import func, select

from vuzol.discussion import (
    DomainError,
    PackageControlAction,
    PlanDraft,
    PlanItemDraft,
    WorkPackageService,
)
from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    DiscussionPlanApplicationService,
    PackageControlIngress,
    PackageControlResultCode,
    PackageControlSource,
)
from vuzol.interpretation.discussion import (
    DiscussionInterpretation,
    DiscussionInterpretRequest,
    EditSessionContext,
    ItemEditPayload,
    PlanSnapshot,
)
from vuzol.storage.models import PlanRevision, Task, TelegramControlAction, WorkPackage
from vuzol.storage.types import PlanRevisionCreatedBy, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def plan(
    title: str = "Discussion plan",
    *,
    item_id: uuid.UUID | None = None,
    summary: str = "Implement discussion plan",
) -> PlanDraft:
    return PlanDraft(
        title=title,
        items=(
            PlanItemDraft(
                item_id=item_id,
                local_id="discussion",
                summary=summary,
                goal="Apply a fenced plan change",
                expected_outcome="A new immutable revision exists",
                completion_criteria=("Application tests pass",),
                allowed_scope="src/vuzol/discussion/**",
                out_of_scope=("Task materialization",),
                trusted_checks=("pytest",),
            ),
        ),
    )


def plan_interpretation(
    *,
    intent: str,
    title: str,
    base_revision_id: uuid.UUID | None = None,
    base_revision_hash: str | None = None,
) -> DiscussionInterpretation:
    return DiscussionInterpretation(
        interaction_mode="plan_request",
        confidence=0.95,
        should_mutate_plan=True,
        user_visible_summary="План подготовлен.",
        plan_request={
            "intent": intent,
            "base_revision_id": base_revision_id,
            "base_revision_hash": base_revision_hash,
            "title": title,
            "items": [
                {
                    "local_id": "discussion",
                    "summary": "Implement discussion plan",
                    "goal": "Apply a fenced plan change",
                    "expected_outcome": "A new immutable revision exists",
                    "completion_criteria": ["Application tests pass"],
                    "allowed_scope": "src/vuzol/discussion/**",
                    "out_of_scope": ["Task materialization"],
                    "trusted_checks": ["pytest"],
                    "suggested_risk": "low",
                    "needs_approval": False,
                    "estimated_complexity": "medium",
                }
            ],
        },
    )


async def create_package(factory: object) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, str]:
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
        created = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
    return session_id, created.package_id, created.revision_id, created.content_hash


async def test_plan_application_creates_and_revises_only_with_trusted_snapshot(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
    application = DiscussionPlanApplicationService(factory, enabled=True)
    create_request = DiscussionInterpretRequest(
        original_input="create a plan", project_id="vuzol", user_id=42
    )
    created = await application.apply_plan_request(
        session_id=session_id,
        request=create_request,
        interpretation=plan_interpretation(intent="create_draft", title="Discussion plan"),
        planner_profile="fake-planner",
    )
    snapshot = PlanSnapshot(
        package_id=created.package_id,
        revision_id=created.revision_id,
        revision_number=created.revision_number,
        revision_hash=created.content_hash,
        status_generation=created.status_generation,
        title="Discussion plan",
    )
    revise_request = DiscussionInterpretRequest(
        original_input="revise the plan",
        project_id="vuzol",
        user_id=42,
        plan_snapshot=snapshot,
    )
    revised = await application.apply_plan_request(
        session_id=session_id,
        request=revise_request,
        interpretation=plan_interpretation(
            intent="revise_draft",
            title="Discussion plan revised",
            base_revision_id=uuid.uuid4(),
            base_revision_hash="f" * 64,
        ),
    )
    assert revised.revision_number == 2
    assert revised.status_generation == 2
    async with factory() as session:
        revisions = (
            await session.scalars(
                select(PlanRevision)
                .where(PlanRevision.work_package_id == created.package_id)
                .order_by(PlanRevision.revision_number)
            )
        ).all()
        assert len(revisions) == 2
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
    await engine.dispose()


async def test_item_edit_application_rejects_changes_outside_fenced_item(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, revision_id, content_hash = await create_package(factory)
    async with UnitOfWork(factory) as uow:
        service = WorkPackageService(uow)
        edit = await service.open_edit_session(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            ordinal=1,
            user_id=42,
        )
        revision = await uow.work_packages.get_revision(revision_id)
        item_id = uuid.UUID(str(revision.immutable_body["items"][0]["item_id"]))
    context = EditSessionContext(
        edit_session_id=edit.id,
        package_id=package_id,
        revision_id=revision_id,
        revision_number=1,
        revision_hash=content_hash,
        session_generation=1,
        item_id=item_id,
        opened_by_user_id=42,
    )
    request = DiscussionInterpretRequest(
        original_input="shorten the item",
        project_id="vuzol",
        user_id=42,
        edit_session=context,
    )
    interpretation = DiscussionInterpretation(
        interaction_mode="item_edit",
        confidence=0.9,
        should_mutate_plan=True,
        user_visible_summary="Изменение подготовлено.",
        item_edit=ItemEditPayload(
            edit_session_id=edit.id,
            package_id=package_id,
            revision_id=revision_id,
            revision_number=1,
            revision_hash=content_hash,
            item_id=item_id,
            refinement_text="shorten",
        ),
    )
    application = DiscussionPlanApplicationService(factory, enabled=True)
    with pytest.raises(DomainError) as scope:
        await application.apply_item_edit(
            request=request,
            interpretation=interpretation,
            replacement=plan("Changed package title", item_id=item_id, summary="Short item"),
        )
    assert scope.value.code == "item_edit_scope_violation"
    revised = await application.apply_item_edit(
        request=request,
        interpretation=interpretation,
        replacement=plan(item_id=item_id, summary="Short item"),
    )
    assert revised.revision_number == 2
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
    await engine.dispose()


def command(
    *,
    action: PackageControlAction,
    package_id: uuid.UUID,
    content_hash: str,
    generation: int,
    key: str,
    user_id: int = 42,
) -> AuthoritativeControlCommand:
    return AuthoritativeControlCommand(
        action=action,
        package_id=package_id,
        plan_revision_number=1,
        h8=content_hash[:8],
        expected_status_generation=generation,
        user_id=user_id,
        source=PackageControlSource.TELEGRAM_CALLBACK,
        external_idempotency_key=key,
    )


async def test_control_ingress_is_authorized_idempotent_and_never_starts(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, _, content_hash = await create_package(factory)
    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))
    approve = command(
        action=PackageControlAction.APPROVE,
        package_id=package_id,
        content_hash=content_hash,
        generation=1,
        key="approve-1",
    )
    applied = await ingress.apply(approve)
    duplicate = await ingress.apply(approve)
    assert applied.code is PackageControlResultCode.APPLIED
    assert applied.status_generation == 2
    assert duplicate.duplicate and duplicate.action_id == applied.action_id

    start = await ingress.apply(
        command(
            action=PackageControlAction.START,
            package_id=package_id,
            content_hash=content_hash,
            generation=2,
            key="start-1",
        )
    )
    assert start.code is PackageControlResultCode.START_NOT_WIRED
    assert start.status_generation == 2
    with pytest.raises(DomainError) as conflict:
        await ingress.apply(
            command(
                action=PackageControlAction.DISCARD,
                package_id=package_id,
                content_hash=content_hash,
                generation=2,
                key="approve-1",
            )
        )
    assert conflict.value.code == "idempotency_conflict"
    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        assert package is not None and package.status is WorkPackageStatus.APPROVED
        assert package.version == 2
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
        assert await session.scalar(select(func.count()).select_from(TelegramControlAction)) == 2

    with pytest.raises(DomainError) as unauthorized:
        await ingress.apply(
            command(
                action=PackageControlAction.DISCARD,
                package_id=package_id,
                content_hash=content_hash,
                generation=2,
                key="discard-foreign",
                user_id=7,
            )
        )
    assert unauthorized.value.code == "control_unauthorized"

    discarded = await ingress.apply(
        command(
            action=PackageControlAction.DISCARD,
            package_id=package_id,
            content_hash=content_hash,
            generation=2,
            key="discard-1",
        )
    )
    assert discarded.code is PackageControlResultCode.APPLIED
    assert discarded.status_generation == 3
    with pytest.raises(DomainError) as terminal:
        await ingress.apply(
            command(
                action=PackageControlAction.RETRY_ITEM,
                package_id=package_id,
                content_hash=content_hash,
                generation=3,
                key="retry-terminal",
            )
        )
    assert terminal.value.code == "terminal_package"
    await engine.dispose()


async def test_control_ingress_concurrent_button_epoch_has_one_effect(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, _, content_hash = await create_package(factory)
    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))

    async def approve(key: str) -> str:
        try:
            result = await ingress.apply(
                command(
                    action=PackageControlAction.APPROVE,
                    package_id=package_id,
                    content_hash=content_hash,
                    generation=1,
                    key=key,
                )
            )
        except DomainError as error:
            return error.code
        return result.code.value

    outcomes = await asyncio.gather(approve("race-a"), approve("race-b"))
    assert sorted(outcomes) == ["applied", "stale_generation"]
    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        assert package is not None and package.version == 2
        assert package.status is WorkPackageStatus.APPROVED
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
    await engine.dispose()


async def test_control_ingress_rejects_stale_fences_before_persisting_action(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, _, content_hash = await create_package(factory)
    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))
    with pytest.raises(DomainError) as stale_hash:
        await ingress.apply(
            AuthoritativeControlCommand(
                action=PackageControlAction.APPROVE,
                package_id=package_id,
                plan_revision_number=1,
                h8="deadbeef",
                expected_status_generation=1,
                user_id=42,
                source=PackageControlSource.EXPLICIT_COMMAND,
                external_idempotency_key="stale-hash",
            )
        )
    assert stale_hash.value.code == "stale_revision"
    with pytest.raises(DomainError) as stale_generation:
        await ingress.apply(
            command(
                action=PackageControlAction.APPROVE,
                package_id=package_id,
                content_hash=content_hash,
                generation=99,
                key="stale-generation",
            )
        )
    assert stale_generation.value.code == "stale_generation"
    with pytest.raises(DomainError) as disabled:
        await PackageControlIngress(
            factory, enabled=False, authorized_user_ids=frozenset({42})
        ).apply(
            command(
                action=PackageControlAction.APPROVE,
                package_id=package_id,
                content_hash=content_hash,
                generation=1,
                key="disabled",
            )
        )
    assert disabled.value.code == "project_discussion_disabled"
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(TelegramControlAction)) == 0
        package = await session.get(WorkPackage, package_id)
        assert package is not None and package.version == 1
    await engine.dispose()


async def test_item_edit_vs_approve_race_has_one_revision_epoch_winner(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, revision_id, content_hash = await create_package(factory)
    async with UnitOfWork(factory) as uow:
        edit = await WorkPackageService(uow).open_edit_session(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            ordinal=1,
            user_id=42,
        )
        revision = await uow.work_packages.get_revision(revision_id)
        item_id = uuid.UUID(str(revision.immutable_body["items"][0]["item_id"]))
    context = EditSessionContext(
        edit_session_id=edit.id,
        package_id=package_id,
        revision_id=revision_id,
        revision_number=1,
        revision_hash=content_hash,
        session_generation=1,
        item_id=item_id,
        opened_by_user_id=42,
    )
    request = DiscussionInterpretRequest(
        original_input="edit item",
        project_id="vuzol",
        user_id=42,
        edit_session=context,
    )
    interpretation = DiscussionInterpretation(
        interaction_mode="item_edit",
        confidence=0.9,
        should_mutate_plan=True,
        user_visible_summary="Edit ready.",
        item_edit={
            "edit_session_id": edit.id,
            "package_id": package_id,
            "revision_id": revision_id,
            "revision_number": 1,
            "revision_hash": content_hash,
            "item_id": item_id,
            "refinement_text": "shorten",
        },
    )
    application = DiscussionPlanApplicationService(factory, enabled=True)
    controls = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))

    async def apply_edit() -> str:
        try:
            await application.apply_item_edit(
                request=request,
                interpretation=interpretation,
                replacement=plan(item_id=item_id, summary="Short item"),
            )
        except DomainError as error:
            return error.code
        return "edited"

    async def approve() -> str:
        try:
            await controls.apply(
                command(
                    action=PackageControlAction.APPROVE,
                    package_id=package_id,
                    content_hash=content_hash,
                    generation=1,
                    key="approve-racing-edit",
                )
            )
        except DomainError as error:
            return error.code
        return "approved"

    outcomes = await asyncio.gather(apply_edit(), approve())
    assert len({"edited", "approved"}.intersection(outcomes)) == 1
    assert any(outcome in {"stale_generation", "stale_revision"} for outcome in outcomes)
    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        assert package is not None and package.version == 2
        assert package.status in {WorkPackageStatus.DRAFT, WorkPackageStatus.APPROVED}
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
    await engine.dispose()


async def test_model_plan_control_has_no_application_mutation_edge(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id, package_id, _, _ = await create_package(factory)
    application = DiscussionPlanApplicationService(factory, enabled=True)
    interpretation = DiscussionInterpretation(
        interaction_mode="plan_control",
        confidence=0.99,
        user_visible_summary="Approve requested.",
        plan_control={"action": "approve", "authoritative": True},
    )
    request = DiscussionInterpretRequest(original_input="approve", project_id="vuzol", user_id=42)
    with pytest.raises(DomainError) as rejected:
        await application.apply_plan_request(
            session_id=session_id,
            request=request,
            interpretation=interpretation,
        )
    assert rejected.value.code == "plan_mutation_not_authorized"
    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        assert package is not None and package.status is WorkPackageStatus.DRAFT
        assert package.version == 1
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
    await engine.dispose()
