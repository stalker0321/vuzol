from __future__ import annotations

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
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
from vuzol.discussion.service import RevisionResult
from vuzol.storage.models import Event, PlanRevision, PlanRevisionItem, Task, WorkPackage
from vuzol.storage.types import (
    PlanRevisionCreatedBy,
    PlanRevisionState,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.work_package_projections import build_work_package_plan_card

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _plan() -> PlanDraft:
    return PlanDraft(
        title="Queue plan",
        items=tuple(
            PlanItemDraft(
                local_id=f"item-{ordinal}",
                summary=f"Step {ordinal}",
                goal=f"Goal {ordinal}",
                expected_outcome=f"Outcome {ordinal}",
                completion_criteria=(f"Check {ordinal}",),
                allowed_scope="src/**",
            )
            for ordinal in (1, 2)
        ),
    )


async def _running_package(
    factory: async_sessionmaker[AsyncSession],
) -> tuple[RevisionResult, object]:
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
        created = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=_plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
        await WorkPackageService(uow).approve(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=1,
            user_id=42,
        )
    async with factory.begin() as session:
        package = await session.get(WorkPackage, created.package_id, with_for_update=True)
        assert package is not None
        package.status = WorkPackageStatus.RUNNING
        package.running_revision_id = created.revision_id
        package.cursor_ordinal = 1
        package.version = 3
    return created, session_id


def _command(
    action: PackageControlAction, created: RevisionResult, generation: int, key: str
) -> AuthoritativeControlCommand:
    return AuthoritativeControlCommand(
        action=action,
        package_id=created.package_id,
        plan_revision_number=1,
        h8=created.content_hash[:8],
        expected_status_generation=generation,
        user_id=42,
        source=PackageControlSource.TELEGRAM_CALLBACK,
        external_idempotency_key=key,
    )


async def test_failure_pause_retry_skip_and_replan_preserve_revision_evidence(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created, _ = await _running_package(factory)
    async with UnitOfWork(factory) as uow:
        paused_generation = await WorkPackageService(uow).pause_for_item_outcome(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=3,
            ordinal=1,
            blocked=False,
        )
    assert paused_generation == 4
    with pytest.raises(DomainError) as duplicate_failure:
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).pause_for_item_outcome(
                package_id=created.package_id,
                revision_number=1,
                h8=created.content_hash[:8],
                expected_status_generation=3,
                ordinal=1,
                blocked=False,
            )
    assert duplicate_failure.value.code == "stale_generation"

    async with factory() as session:
        card = await build_work_package_plan_card(session, created.package_id)
    assert "Автоматического перехода дальше не будет" in card.html
    labels = {label for row in card.callback_buttons for label, _ in row}
    assert "Повторить" not in labels
    assert labels >= {
        "Пропустить",
        "Перепланировать",
        "Остановить",
    }

    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))
    with pytest.raises(DomainError) as unsafe_retry:
        await ingress.apply(_command(PackageControlAction.RETRY_ITEM, created, 4, "retry-1"))
    assert unsafe_retry.value.code == "item_not_safely_retryable"
    skipped = await ingress.apply(_command(PackageControlAction.SKIP_ITEM, created, 4, "skip-1"))
    assert skipped.code is PackageControlResultCode.APPLIED and skipped.status_generation == 5
    replanned = await ingress.apply(
        _command(PackageControlAction.REQUEST_REPLAN, created, 5, "replan-1")
    )
    assert replanned.code is PackageControlResultCode.APPLIED
    assert replanned.status_generation == 6 and replanned.revision_id is not None

    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        revisions = tuple(
            (
                await session.scalars(
                    select(PlanRevision)
                    .where(PlanRevision.work_package_id == created.package_id)
                    .order_by(PlanRevision.revision_number)
                )
            ).all()
        )
        revision_items = tuple(
            (
                await session.scalars(
                    select(PlanRevisionItem)
                    .where(PlanRevisionItem.work_package_id == created.package_id)
                    .order_by(PlanRevisionItem.plan_revision_id, PlanRevisionItem.ordinal)
                )
            ).all()
        )
        events = tuple(
            (
                await session.scalars(
                    select(Event.event_type).where(Event.entity_id == created.package_id)
                )
            ).all()
        )
        task_count = await session.scalar(select(func.count()).select_from(Task))
    assert package is not None
    assert package.status is WorkPackageStatus.DRAFT and package.version == 6
    assert package.cursor_ordinal == 2
    assert [revision.state for revision in revisions] == [
        PlanRevisionState.SUPERSEDED,
        PlanRevisionState.DRAFT,
    ]
    assert len(revision_items) == 4
    assert {item.item_id for item in revision_items[:2]} == {
        item.item_id for item in revision_items[2:]
    }
    assert {
        "work_package.paused",
        "work_package.item_skipped",
        "work_package.replan_requested",
    }.issubset(events)
    assert task_count == 1
    await engine.dispose()


async def test_stop_is_terminal_and_queue_controls_require_failure_pause(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created, _ = await _running_package(factory)
    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))
    with pytest.raises(DomainError) as not_paused:
        await ingress.apply(_command(PackageControlAction.RETRY_ITEM, created, 3, "retry-running"))
    assert not_paused.value.code == "invalid_transition"
    stopped = await ingress.apply(_command(PackageControlAction.STOP_PACKAGE, created, 3, "stop-1"))
    assert stopped.code is PackageControlResultCode.APPLIED and stopped.status_generation == 4
    with pytest.raises(DomainError) as terminal:
        await ingress.apply(
            _command(PackageControlAction.REQUEST_REPLAN, created, 4, "replan-stopped")
        )
    assert terminal.value.code == "terminal_package"
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
    assert package is not None and package.status is WorkPackageStatus.STOPPED
    assert package.pause_reason is None
    await engine.dispose()
