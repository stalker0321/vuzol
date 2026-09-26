from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from vuzol.discussion import (
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
