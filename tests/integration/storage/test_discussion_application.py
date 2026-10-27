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
    PackageControlIngress,
    PackageControlResultCode,
    PackageControlSource,
)
from vuzol.storage.models import (
    Interpretation,
    MaterializationLink,
    Task,
    TelegramControlAction,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import (
    PlanRevisionCreatedBy,
    TaskStatus,
    WorkPackageStatus,
)
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


async def test_control_ingress_is_authorized_idempotent_and_starts_atomically(
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
    assert applied.status_generation == 3
    assert duplicate.duplicate and duplicate.action_id == applied.action_id
    with pytest.raises(DomainError) as conflict:
        await ingress.apply(
            command(
                action=PackageControlAction.DISCARD,
                package_id=package_id,
                content_hash=content_hash,
                generation=3,
                key="approve-1",
            )
        )
    assert conflict.value.code == "idempotency_conflict"
    async with factory() as session:
        package = await session.get(WorkPackage, package_id)
        assert package is not None and package.status is WorkPackageStatus.RUNNING
        assert package.version == 3 and package.start_generation == 3
        assert await session.scalar(select(func.count()).select_from(Task)) == 1
        task = await session.scalar(select(Task))
        assert task is not None and task.status is TaskStatus.INTERPRETED
        assert task.interpreter_profile == "work_package_materializer"
        assert await session.scalar(select(func.count()).select_from(Interpretation)) == 1
        assert await session.scalar(select(func.count()).select_from(MaterializationLink)) == 1
        assert (
            await session.scalar(
                select(func.count())
                .select_from(TransactionalOutbox)
                .where(TransactionalOutbox.destination == "workflow_dispatch")
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(TelegramControlAction)) == 1

    with pytest.raises(DomainError) as unauthorized:
        await ingress.apply(
            command(
                action=PackageControlAction.DISCARD,
                package_id=package_id,
                content_hash=content_hash,
                generation=3,
                key="discard-foreign",
                user_id=7,
            )
        )
    assert unauthorized.value.code == "control_unauthorized"

    with pytest.raises(DomainError) as running:
        await ingress.apply(
            command(
                action=PackageControlAction.DISCARD,
                package_id=package_id,
                content_hash=content_hash,
                generation=3,
                key="discard-running",
            )
        )
    assert running.value.code == "invalid_transition"
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
        assert package is not None and package.version == 3
        assert package.status is WorkPackageStatus.RUNNING
        assert await session.scalar(select(func.count()).select_from(Task)) == 1
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
