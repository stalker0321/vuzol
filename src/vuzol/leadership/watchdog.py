"""Watchdog for waiting goals whose wakeup can never arrive.

Live pilot 2026-10-30: goals sat for hours waiting on a package with no task, or
on a plan that no longer needed approval; each time an operator noticed by hand.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import ManagedGoal, has_owner_mandate
from vuzol.storage.models import Event, MaterializationLink, Task, TransactionalOutbox, WorkPackage
from vuzol.storage.types import USER_TERMINAL_TASK_STATUSES, DeliveryStatus

GRACE = timedelta(minutes=2)
STALLED_REASON = "goal_stalled"


async def watch_waiting_goal(session: AsyncSession, goal: ManagedGoal, *, now: datetime) -> bool:
    """Caller holds the goal lock and has checked that no lead lease is live."""
    if goal.lifecycle != "waiting" or goal.updated_at > now - GRACE:
        return False
    if await _outstanding_wakeup(session, goal):
        return False
    from vuzol.leadership.quota_wait import quota_retry_at, resume_planning

    retry_at = quota_retry_at(goal.wakeup_condition)
    if retry_at is not None:
        # A planner limit is a live wait until it resets; then planning repeats itself.
        if retry_at > now:
            return False
        return await resume_planning(
            session, goal, key=f"lead:planner-quota-retry:{goal.id}:{goal.version}"
        )
    stuck = await _stuck_reason(session, goal)
    if stuck is None:
        return False
    fingerprint = await _fingerprint(session, goal)
    previous = goal.constraints.get("watchdog") or {}
    goal.version += 1
    goal.fence_epoch += 1
    if previous.get("fingerprint") == fingerprint or not has_owner_mandate(goal):
        # The lead already had one chance at this exact state: the owner decides.
        from vuzol.leadership.service import request_owner_recovery

        goal.waiting_reason = STALLED_REASON
        await request_owner_recovery(session, goal, STALLED_REASON)
        _event(session, goal, "managed_goal.watchdog_escalated", stuck)
        return True
    from vuzol.leadership.wakeups import enqueue_goal_wakeup
    from vuzol.telegram.attention import sync_goal_attention

    goal.constraints = {
        **goal.constraints,
        "watchdog": {"fingerprint": fingerprint, "reason": stuck, "at": now.isoformat()},
    }
    goal.lifecycle = "active"
    goal.waiting_reason = None
    goal.wakeup_condition = None
    await sync_goal_attention(session, goal)
    await enqueue_goal_wakeup(
        session,
        goal_id=goal.id,
        idempotency_key=f"lead:watchdog:{goal.id}:{goal.version}",
        package_id=goal.current_package_id,
        kind="watchdog",
    )
    _event(session, goal, "managed_goal.watchdog_woke", stuck)
    return True


async def _stuck_reason(session: AsyncSession, goal: ManagedGoal) -> str | None:
    condition = goal.wakeup_condition if isinstance(goal.wakeup_condition, dict) else {}
    kind = condition.get("kind")
    if kind == "owner":
        return None
    if kind == "plan_approval":
        from vuzol.telegram.attention import pending_owner_plan

        return None if await pending_owner_plan(session, goal) is not None else "no_pending_plan"
    if kind == "package":
        package = await _package(session, condition.get("package_id"))
        if package is None:
            return "package_missing"
        if await package_has_live_task(session, package.id):
            return None
        from vuzol.leadership.worker import _waiting_exit_ready

        # A ready package whose wakeup was lost is stuck just the same.
        return "package_ready" if await _waiting_exit_ready(session, goal) else "package_idle"
    return "no_wakeup_condition"


async def _fingerprint(session: AsyncSession, goal: ManagedGoal) -> str:
    condition = goal.wakeup_condition if isinstance(goal.wakeup_condition, dict) else {}
    package = await _package(session, condition.get("package_id") or goal.current_package_id)
    state: dict[str, Any] = {
        "kind": condition.get("kind"),
        "scope_revision": goal.scope_revision,
        "package": str(package.id) if package else None,
        "status": package.status.value if package else None,
        "package_version": package.version if package else None,
    }
    return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()


async def _package(session: AsyncSession, package_id: object) -> WorkPackage | None:
    try:
        return await session.get(WorkPackage, uuid.UUID(str(package_id)))
    except ValueError:
        return None


async def package_has_live_task(session: AsyncSession, package_id: object) -> bool:
    try:
        package = uuid.UUID(str(package_id))
    except ValueError:
        return False
    live = await session.scalar(
        select(Task.id)
        .join(MaterializationLink, MaterializationLink.task_id == Task.id)
        .where(
            MaterializationLink.work_package_id == package,
            Task.status.not_in(USER_TERMINAL_TASK_STATUSES),
        )
        .limit(1)
    )
    return live is not None


async def _outstanding_wakeup(session: AsyncSession, goal: ManagedGoal) -> bool:
    item = await session.scalar(
        select(TransactionalOutbox.id)
        .where(
            TransactionalOutbox.destination == "project_lead",
            TransactionalOutbox.linked_entity_id == goal.id,
            TransactionalOutbox.status.in_((DeliveryStatus.PENDING, DeliveryStatus.LEASED)),
        )
        .limit(1)
    )
    return item is not None


def _event(session: AsyncSession, goal: ManagedGoal, event_type: str, reason: str) -> None:
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type=event_type,
            actor_type="system",
            actor_id="watchdog",
            previous_state="waiting",
            new_state=goal.lifecycle,
            payload={"reason": reason, "version": goal.version},
        )
    )
