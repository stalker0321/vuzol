"""Skip the current plan item of a lead goal on the owner's word.

Owner 2026-10-30: a stuck or no longer needed step could only be retried or the
whole plan replanned. Skipping moves on; the lead starts the next item itself.
"""

from __future__ import annotations

from typing import Any

from vuzol.discussion.domain import DomainError
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import WorkPackage
from vuzol.storage.unit_of_work import UnitOfWork


async def skip_goal_item(
    uow: UnitOfWork, goal: ManagedGoal, *, actor_type: str, actor_id: str | None
) -> dict[str, Any]:
    """Caller holds the goal lock (or the project admission lock)."""
    from vuzol.discussion.sequencer import WorkPackageSequencer
    from vuzol.leadership.wakeups import enqueue_goal_wakeup

    assert uow.session is not None
    if goal.lifecycle in {"completed", "cancelled"}:
        raise DomainError("goal_terminal")
    if goal.lifecycle == "paused":
        raise DomainError("goal_paused")
    package = (
        await uow.session.get(WorkPackage, goal.current_package_id)
        if goal.current_package_id
        else None
    )
    if package is None or package.managed_goal_id != goal.id:
        raise DomainError("nothing_to_skip")
    skipped_ordinal = package.cursor_ordinal
    result = await WorkPackageSequencer(uow).skip_current(
        package_id=package.id, actor_type=actor_type, actor_id=actor_id
    )
    if goal.lifecycle == "waiting":
        condition = goal.wakeup_condition if isinstance(goal.wakeup_condition, dict) else {}
        # The skip is the owner's answer to a stuck item; a plan approval still waits.
        if condition.get("kind") in {"owner", "package", None}:
            goal.lifecycle = "active"
            goal.waiting_reason = None
            goal.wakeup_condition = None
            goal.version += 1
            goal.fence_epoch += 1
    await enqueue_goal_wakeup(
        uow.session,
        goal_id=goal.id,
        idempotency_key=f"lead:item-skipped:{package.id}:{result.status_generation}",
        package_id=package.id,
        kind="item_skipped",
    )
    return {
        "skipped_ordinal": skipped_ordinal,
        "next_ordinal": None if result.completed else result.ordinal,
        "package_completed": result.completed,
    }
