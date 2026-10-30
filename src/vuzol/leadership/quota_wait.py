"""Planner quota waits: park the goal until the limit resets, or switch on request.

Owner 2026-10-30: when Codex hits its subscription limit, say when it resets and
continue by itself then; one button switches to the fallback planner now.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import Event, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus

QUOTA_REASON = "planner_quota_exhausted"
WAITING_REASON = "planner_quota"
# Without a reset time from the provider, try again after this long.
DEFAULT_WAIT = timedelta(hours=1)
# Owner-facing clock, as on the project dashboard.
OWNER_ZONE = ZoneInfo("Europe/Amsterdam")
_REQUEST_KEYS = ("command_name", "request", "owner_corrections")


def quota_retry_at(condition: object) -> datetime | None:
    if not isinstance(condition, dict) or condition.get("kind") != "quota":
        return None
    try:
        return datetime.fromisoformat(str(condition["retry_at"]))
    except (KeyError, ValueError):
        return None


async def park_planner_for_quota(
    session: AsyncSession,
    goal: ManagedGoal,
    *,
    request: TransactionalOutbox,
    outcome: dict[str, Any],
) -> None:
    """Hold the planning request until the limit resets; the lead is not woken."""
    now = datetime.now(UTC)
    try:
        retry_at = datetime.fromisoformat(str(outcome["retry_at"]))
    except (KeyError, ValueError):
        retry_at = now + DEFAULT_WAIT
    retry_at = max(retry_at, now + timedelta(minutes=1))
    previous = goal.wakeup_condition if isinstance(goal.wakeup_condition, dict) else None
    resume = (
        previous["resume"]
        if previous is not None and previous.get("kind") == "quota"
        else {
            "lifecycle": goal.lifecycle,
            "waiting_reason": goal.waiting_reason,
            "wakeup_condition": previous,
        }
    )
    goal.lifecycle = "waiting"
    goal.waiting_reason = WAITING_REASON
    goal.wakeup_condition = {
        "kind": "quota",
        "retry_at": retry_at.isoformat(),
        "profile_id": outcome.get("profile_id"),
        "fallback_profile_id": outcome.get("fallback_profile_id"),
        "request": {key: request.payload[key] for key in _REQUEST_KEYS if key in request.payload},
        "resume": resume,
    }
    from vuzol.telegram.projections import enqueue_chat_notice

    fallback_label = outcome.get("fallback_label") if outcome.get("fallback_profile_id") else None

    await enqueue_chat_notice(
        session,
        chat_id=goal.source_chat_id,
        thread_id=goal.source_thread_id,
        key=f"lead:planner-quota:{request.id}",
        text=(
            f"⏳ У Codex закончился лимит, план пока не собран. "  # noqa: RUF001
            f"Лимит обновится {_owner_time(retry_at, now)}, тогда продолжу сам."
        ),
        projection_role=f"lw:{goal.id.hex}",
        revision=1,
        callback_buttons=(
            ((f"Переключиться на {fallback_label}", f"v1:lg:{goal.version}:fallback"),),
        )
        if fallback_label
        else (),
    )
    _event(session, goal, "managed_goal.planner_quota_wait", {"retry_at": retry_at.isoformat()})


async def resume_planning(
    session: AsyncSession,
    goal: ManagedGoal,
    *,
    key: str,
    planner_profile_id: str | None = None,
) -> bool:
    """Restore the goal and repeat the held planning request (on a fallback if given)."""
    condition = goal.wakeup_condition
    if goal.lifecycle != "waiting" or quota_retry_at(condition) is None:
        return False
    assert isinstance(condition, dict)
    resume = condition.get("resume") or {}
    goal.lifecycle = resume.get("lifecycle") or "active"
    goal.waiting_reason = resume.get("waiting_reason")
    goal.wakeup_condition = resume.get("wakeup_condition")
    goal.version += 1
    goal.fence_epoch += 1
    session.add(
        TransactionalOutbox(
            destination="project_lead",
            operation_type="lead_wakeup",
            linked_entity_type="managed_goal",
            linked_entity_id=goal.id,
            idempotency_key=key,
            payload={
                "goal_id": str(goal.id),
                "wakeup_kind": "plan_requested",
                "scope_revision": goal.scope_revision,
                **condition.get("request", {}),
                **({"planner_profile_id": planner_profile_id} if planner_profile_id else {}),
            },
            status=DeliveryStatus.PENDING,
        )
    )
    from vuzol.telegram.attention import sync_attention

    await sync_attention(session, goal, None, scope="planner")
    _event(
        session,
        goal,
        "managed_goal.planner_quota_resumed",
        {"planner_profile_id": planner_profile_id},
    )
    await session.flush()
    return True


def _owner_time(moment: datetime, now: datetime) -> str:
    local = moment.astimezone(OWNER_ZONE)
    if local.date() == now.astimezone(OWNER_ZONE).date():
        return f"в {local:%H:%M}"
    return f"{local:%d.%m} в {local:%H:%M}"


def _event(
    session: AsyncSession, goal: ManagedGoal, event_type: str, payload: dict[str, Any]
) -> None:
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type=event_type,
            actor_type="system",
            actor_id="planner-quota",
            previous_state=None,
            new_state=goal.lifecycle,
            payload={**payload, "version": goal.version},
        )
    )
