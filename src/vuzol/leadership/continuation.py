"""One durable next turn when an accepted decision leaves no executing package."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import ManagedGoal
from vuzol.leadership.service import request_owner_recovery
from vuzol.leadership.wakeups import continuation_wakeup_key, enqueue_goal_wakeup
from vuzol.storage.models import Event, TransactionalOutbox, WorkPackage
from vuzol.storage.types import DeliveryStatus, WorkPackageStatus


async def continue_if_idle(session: AsyncSession, goal: ManagedGoal, *, repeat_limit: int) -> None:
    """Caller holds the goal lock; ack and continuation commit or roll back together."""
    if goal.lifecycle != "active" or goal.wakeup_condition is not None:
        return
    package = (
        await session.get(WorkPackage, goal.current_package_id) if goal.current_package_id else None
    )
    if (
        package is not None
        and package.managed_goal_id == goal.id
        and package.status is WorkPackageStatus.RUNNING
    ):
        from vuzol.leadership.tasks import task_turn_ready

        if not await task_turn_ready(session, package):
            return
    outstanding = await session.scalar(
        select(TransactionalOutbox.id)
        .where(
            TransactionalOutbox.destination == "project_lead",
            TransactionalOutbox.linked_entity_id == goal.id,
            TransactionalOutbox.status.in_((DeliveryStatus.PENDING, DeliveryStatus.LEASED)),
        )
        .limit(1)
    )
    if outstanding is not None:
        return
    key = continuation_wakeup_key(goal.id, goal.version)
    existing = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == "project_lead",
            TransactionalOutbox.idempotency_key == key,
        )
    )
    answer = goal.constraints.get("owner_answer") or {}
    progress = {
        "scope_revision": goal.scope_revision,
        "package_id": str(package.id) if package else None,
        "package_version": package.version if package else None,
        "head": package.integration_head_commit if package else None,
        "owner_answer_version": answer.get("version"),
    }
    previous = goal.constraints.get("continuation") or {}
    count = previous.get("count", 0) + 1 if previous.get("progress") == progress else 1
    goal.constraints = {**goal.constraints, "continuation": {"progress": progress, "count": count}}
    # Changing wording does not buy another unbounded inference loop. Only new
    # result/scope/owner information resets the consecutive no-job-turn budget.
    if existing is not None or count > repeat_limit:
        goal.lifecycle = "waiting"
        goal.waiting_reason = "continuation_without_progress"
        goal.version += 1
        goal.fence_epoch += 1
        goal.lease_owner = None
        goal.lease_until = None
        await request_owner_recovery(session, goal, goal.waiting_reason)
        session.add(
            Event(
                entity_type="managed_goal",
                entity_id=goal.id,
                event_type="managed_goal.continuation_blocked",
                actor_type="system",
                previous_state="active",
                new_state="waiting",
                payload={"reason": goal.waiting_reason, "version": goal.version},
            )
        )
        return
    await enqueue_goal_wakeup(
        session,
        goal_id=goal.id,
        idempotency_key=key,
        package_id=goal.current_package_id,
        kind="continuation",
    )
