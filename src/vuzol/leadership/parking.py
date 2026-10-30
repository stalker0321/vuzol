"""Park a goal to run another one, then return to it.

Owner 2026-10-30: set a goal aside without cancelling it. A parked goal is not
live, so the project accepts a new goal; returning restores where it stood.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.leadership.persistence import LIVE_GOAL_LIFECYCLES, ManagedGoal
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    ProjectDiscussionSession,
    Task,
    WorkPackage,
)
from vuzol.storage.types import USER_TERMINAL_TASK_STATUSES, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

PARKED = "parked"
_LIVE_PACKAGE = {
    WorkPackageStatus.DRAFT,
    WorkPackageStatus.APPROVED,
    WorkPackageStatus.RUNNING,
    WorkPackageStatus.PAUSED,
}


async def park_goal(uow: UnitOfWork, goal: ManagedGoal, *, actor_id: str | None) -> dict[str, Any]:
    """Caller holds project admission and the goal lock."""
    from vuzol.discussion.sequencer import WorkPackageSequencer
    from vuzol.leadership.jobs import cancel_goal_jobs
    from vuzol.workflows.controls import cancel_task

    session = uow.session
    assert session is not None
    if goal.lifecycle not in LIVE_GOAL_LIFECYCLES:
        raise DomainError("goal_not_parkable")
    package = await _package(session, goal)
    interrupted = None
    saved_package: dict[str, Any] | None = None
    if package is not None and package.status in _LIVE_PACKAGE:
        task = await _current_task(session, package)
        if task is not None and task.status not in USER_TERMINAL_TASK_STATUSES:
            # The step stops now; on return the lead retries or replans it.
            await cancel_task(session, task.id, actor_id=actor_id)
            await WorkPackageSequencer(uow).observe_terminal(task_id=task.id)
            interrupted = str(task.id)
        discussion = await session.get(
            ProjectDiscussionSession, package.session_id, with_for_update=True
        )
        saved_package = {
            "id": str(package.id),
            "status": package.status.value,
            "active_in_topic": discussion is not None
            and discussion.active_work_package_id == package.id,
        }
        # A stopped package is not live: the topic accepts the next goal's plan.
        package.status = WorkPackageStatus.STOPPED
        package.version += 1
        if discussion is not None and discussion.active_work_package_id == package.id:
            discussion.active_work_package_id = None
    await cancel_goal_jobs(session, goal_id=goal.id)
    goal.constraints = {
        **goal.constraints,
        "parked": {
            "lifecycle": goal.lifecycle,
            "waiting_reason": goal.waiting_reason,
            "wakeup_condition": goal.wakeup_condition,
            "package": saved_package,
            "interrupted_task_id": interrupted,
            "at": datetime.now(UTC).isoformat(),
        },
    }
    previous = goal.lifecycle
    goal.lifecycle = PARKED
    goal.waiting_reason = None
    goal.wakeup_condition = None
    goal.lease_owner = None
    goal.lease_until = None
    goal.version += 1
    goal.fence_epoch += 1
    _event(session, goal, "managed_goal.parked", previous, {"interrupted_task_id": interrupted})
    await session.flush()
    return {"parked": True, "interrupted_task_id": interrupted}


async def unpark_goal(uow: UnitOfWork, goal: ManagedGoal) -> dict[str, Any]:
    """Caller holds project admission and the goal lock."""
    from vuzol.leadership.wakeups import enqueue_goal_wakeup

    session = uow.session
    assert session is not None
    if goal.lifecycle != PARKED:
        raise DomainError("goal_not_parked")
    other = await session.scalar(
        select(ManagedGoal.id)
        .where(
            ManagedGoal.project_id == goal.project_id,
            ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
        )
        .limit(1)
    )
    if other is not None:
        raise DomainError("goal_live_blocks_return")
    saved = goal.constraints.get("parked") or {}
    stored = saved.get("package") or None
    package = await _package(session, goal)
    if stored and package is not None and str(package.id) == stored.get("id"):
        live = await session.scalar(
            select(WorkPackage.id)
            .where(
                WorkPackage.session_id == package.session_id,
                WorkPackage.status.in_(tuple(_LIVE_PACKAGE)),
            )
            .limit(1)
        )
        if live is not None:
            raise DomainError("goal_live_blocks_return")
        package.status = WorkPackageStatus(stored["status"])
        package.version += 1
        if stored.get("active_in_topic"):
            discussion = await session.get(
                ProjectDiscussionSession, package.session_id, with_for_update=True
            )
            if discussion is not None and discussion.active_work_package_id is None:
                discussion.active_work_package_id = package.id
    goal.lifecycle = saved.get("lifecycle") or "active"
    goal.waiting_reason = saved.get("waiting_reason")
    goal.wakeup_condition = saved.get("wakeup_condition")
    goal.constraints = {key: value for key, value in goal.constraints.items() if key != "parked"}
    goal.version += 1
    goal.fence_epoch += 1
    _event(session, goal, "managed_goal.unparked", PARKED, {})
    await enqueue_goal_wakeup(
        session,
        goal_id=goal.id,
        idempotency_key=f"lead:unparked:{goal.id}:{goal.version}",
        package_id=goal.current_package_id,
        kind="unparked",
    )
    await session.flush()
    return {"unparked": True, "lifecycle": goal.lifecycle}


async def parked_goals(session: AsyncSession, project_id: str) -> list[ManagedGoal]:
    return list(
        await session.scalars(
            select(ManagedGoal)
            .where(ManagedGoal.project_id == project_id, ManagedGoal.lifecycle == PARKED)
            .order_by(ManagedGoal.updated_at.desc())
        )
    )


async def _package(session: AsyncSession, goal: ManagedGoal) -> WorkPackage | None:
    if goal.current_package_id is None:
        return None
    package = await session.get(WorkPackage, goal.current_package_id, with_for_update=True)
    return package if package is not None and package.managed_goal_id == goal.id else None


async def _current_task(session: AsyncSession, package: WorkPackage) -> Task | None:
    if package.cursor_ordinal is None or package.running_revision_id is None:
        return None
    task_id = await session.scalar(
        select(MaterializationLink.task_id).where(
            MaterializationLink.work_package_id == package.id,
            MaterializationLink.plan_revision_id == package.running_revision_id,
            MaterializationLink.ordinal == package.cursor_ordinal,
        )
    )
    return await session.get(Task, task_id) if task_id is not None else None


def _event(
    session: AsyncSession,
    goal: ManagedGoal,
    event_type: str,
    previous: str,
    payload: dict[str, Any],
) -> None:
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type=event_type,
            actor_type="user",
            previous_state=previous,
            new_state=goal.lifecycle,
            payload={**payload, "version": goal.version},
        )
    )
