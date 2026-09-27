from __future__ import annotations

import pytest
from sqlalchemy import select
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
from vuzol.storage.models import PlanRevision, Task, WorkPackage
from vuzol.storage.types import PlanRevisionCreatedBy, PlanRevisionState, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _plan() -> PlanDraft:
    return PlanDraft(
        title="Horizon plan",
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


async def _running_horizon_package(
    factory: async_sessionmaker[AsyncSession],
) -> RevisionResult:
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
        package.goal = "ship the horizon"
        package.status = WorkPackageStatus.RUNNING
        package.running_revision_id = created.revision_id
        package.cursor_ordinal = 1
        package.version = 3
    return created


async def test_restart_continues_approved_horizon_without_reapprove(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _running_horizon_package(factory)
    ingress = PackageControlIngress(
        factory, enabled=True, authorized_user_ids=frozenset({42}), horizon_enabled=True
    )
    stopped = await ingress.apply(_command(PackageControlAction.STOP_PACKAGE, created, 3, "stop-1"))
    assert stopped.code is PackageControlResultCode.APPLIED
    assert stopped.status_generation == 4
    restarted = await ingress.apply(
        _command(PackageControlAction.RESTART_PACKAGE, created, 4, "restart-1")
    )
    assert restarted.code is PackageControlResultCode.APPLIED
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
        tasks = tuple((await session.scalars(select(Task))).all())
    assert package is not None and package.status is WorkPackageStatus.RUNNING
    assert package.approved_revision_id == created.revision_id
    assert package.head_revision_id == created.revision_id
    assert len(revisions) == 1 and revisions[0].state is PlanRevisionState.APPROVED
    assert len(tasks) == 1
    await engine.dispose()


async def test_restart_without_flag_keeps_legacy_reapprove_path(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _running_horizon_package(factory)
    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))
    await ingress.apply(_command(PackageControlAction.STOP_PACKAGE, created, 3, "stop-1"))
    restarted = await ingress.apply(
        _command(PackageControlAction.RESTART_PACKAGE, created, 4, "restart-1")
    )
    assert restarted.code is PackageControlResultCode.APPLIED
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
    # Legacy path: cloned DRAFT revision + re-approve, package resumes.
    assert package is not None and package.status is WorkPackageStatus.RUNNING
    assert len(revisions) == 2 and revisions[-1].state is PlanRevisionState.APPROVED
    await engine.dispose()


async def _running_legacy_package(
    factory: async_sessionmaker[AsyncSession],
) -> RevisionResult:
    """Pre-horizon package: no goal/contract, as created before the deploy."""

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
        assert package.goal is None
        package.status = WorkPackageStatus.RUNNING
        package.running_revision_id = created.revision_id
        package.cursor_ordinal = 1
        package.version = 3
    return created


async def test_old_runs_and_approvals_survive_deploy(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _running_legacy_package(factory)
    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))
    await ingress.apply(_command(PackageControlAction.STOP_PACKAGE, created, 3, "stop-1"))
    restarted = await ingress.apply(
        _command(PackageControlAction.RESTART_PACKAGE, created, 4, "restart-1")
    )
    assert restarted.code is PackageControlResultCode.APPLIED
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
        tasks = tuple((await session.scalars(select(Task))).all())
    # Old approval chain intact: superseded head + fresh approved revision.
    assert package is not None and package.status is WorkPackageStatus.RUNNING
    assert package.goal is None
    assert package.approved_revision_id == revisions[-1].id
    assert [revision.state for revision in revisions] == [
        PlanRevisionState.SUPERSEDED,
        PlanRevisionState.APPROVED,
    ]
    assert len(tasks) == 1
    await engine.dispose()


async def test_revision_conflict_on_rewritten_past_item(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _running_horizon_package(factory)
    async with factory.begin() as session:
        package = await session.get(WorkPackage, created.package_id, with_for_update=True)
        assert package is not None
        package.cursor_ordinal = 2
    rewritten = PlanDraft(
        title="Horizon plan",
        items=(
            PlanItemDraft(
                local_id="item-1",
                summary="Rewritten history",
                goal="Goal 1",
                expected_outcome="Outcome 1",
                completion_criteria=("Check 1",),
                allowed_scope="src/**",
            ),
            PlanItemDraft(
                local_id="item-2",
                summary="Step 2",
                goal="Goal 2",
                expected_outcome="Outcome 2",
                completion_criteria=("Check 2",),
                allowed_scope="src/**",
            ),
        ),
    )
    with pytest.raises(DomainError, match="revision_conflict"):
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).revise_draft(
                package_id=created.package_id,
                expected_status_generation=3,
                plan=rewritten,
                created_by=PlanRevisionCreatedBy.USER,
                actor_type="user",
                horizon_enabled=True,
            )
    await engine.dispose()
