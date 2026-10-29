"""Bind a new request to an explicit owner answer before closing its old goal."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import ProjectDiscussionSession, TransactionalOutbox
from vuzol.storage.types import DiscussionSessionStatus

RESTART_CHOICE = "Новая задача, текущую закрыть"


def _source_matches(source: TransactionalOutbox, goal: ManagedGoal) -> bool:
    body = source.payload
    return (
        source.destination == "project_lead"
        and source.linked_entity_type == "managed_goal"
        and source.linked_entity_id == goal.id
        and body.get("goal_id") == str(goal.id)
        and body.get("project_id", goal.project_id) == goal.project_id
        and body.get("wakeup_kind") == "owner_message"
        and body.get("owner_user_id") == goal.owner_user_id
        and body.get("session_id") == str(goal.discussion_session_id)
        and body.get("delegation_id") == str(goal.delegation_id)
        and body.get("scope_revision") == goal.scope_revision
    )


async def routing_question_source(
    session: AsyncSession,
    goal: ManagedGoal,
    options: list[str],
    *,
    wakeup_id: uuid.UUID | None,
) -> str | None:
    """Bind server-side, never accept an event id supplied in an owner's answer."""
    if RESTART_CHOICE not in options:
        return None
    source = await session.get(TransactionalOutbox, wakeup_id) if wakeup_id else None
    if source is None or source.payload.get("wakeup_kind") != "owner_message":
        source = await session.scalar(
            select(TransactionalOutbox)
            .where(
                TransactionalOutbox.destination == "project_lead",
                TransactionalOutbox.linked_entity_id == goal.id,
                TransactionalOutbox.payload["wakeup_kind"].astext == "owner_message",
            )
            .order_by(
                TransactionalOutbox.payload["received_at"].astext.desc(),
                TransactionalOutbox.created_at.desc(),
                TransactionalOutbox.id.desc(),
            )
            .limit(1)
        )
    return str(source.id) if source is not None and _source_matches(source, goal) else None


async def confirmed_restart_source(
    session: AsyncSession, goal: ManagedGoal, source_id: object
) -> TransactionalOutbox:
    try:
        key = uuid.UUID(str(source_id))
    except ValueError:
        raise DomainError("restart_source_invalid") from None
    source = await session.get(TransactionalOutbox, key)
    if source is None or not _source_matches(source, goal):
        raise DomainError("restart_source_mismatch")
    if not isinstance(source.payload.get("text"), str) or not source.payload["text"].strip():
        raise DomainError("restart_source_invalid")
    # answer_permission is owner-only and already validates the question's version
    # and scope. Goal versions prove ordering, unlike transaction-start timestamps.
    answer = goal.constraints.get("owner_answer", {})
    source_version = source.payload.get("goal_version")
    question_version = answer.get("question_version")
    if (
        answer.get("answer") != RESTART_CHOICE
        or answer.get("restart_from_event_id") != str(source.id)
        or answer.get("scope_revision") != goal.scope_revision
        or type(source_version) is not int
        or type(question_version) is not int
        or not source_version < question_version <= answer.get("version", 0)
        or goal.wakeup_condition is not None
    ):
        raise DomainError("restart_confirmation_required")
    return source


async def restart_draft(
    session: AsyncSession,
    goal: ManagedGoal,
    source: TransactionalOutbox,
    *,
    branch_prefix: str,
    default_goal_max_cost: float | None = None,
) -> dict[str, Any]:
    """Caller cancelled under project admission and a savepoint; failures roll it all back."""
    from vuzol.leadership.events import live_or_draft

    discussion = await session.get(
        ProjectDiscussionSession, goal.discussion_session_id, with_for_update=True
    )
    if (
        discussion is None
        or discussion.project_id != goal.project_id
        or discussion.status is not DiscussionSessionStatus.ACTIVE
    ):
        raise DomainError("restart_discussion_unavailable")
    body = source.payload
    new = await live_or_draft(
        session,
        project_id=goal.project_id,
        owner_user_id=goal.owner_user_id,
        discussion=discussion,
        text=body["text"],
        scope=body.get("request_scope", body["text"]),
        criteria=body.get("request_criteria", []),
        constraints={
            "work_limits": body.get("request_work_limits", {}),
            "request_origin": body.get("request_origin"),
            "project_default_branch": goal.constraints.get("project_default_branch", "main"),
            "restart_from_event_id": str(source.id),
        },
        branch_prefix=branch_prefix,
        default_goal_max_cost=default_goal_max_cost,
    )
    wakeup_id = uuid.uuid5(source.id, "restart")
    session.add(
        TransactionalOutbox(
            id=wakeup_id,
            destination="project_lead",
            operation_type="lead_wakeup",
            linked_entity_type="managed_goal",
            linked_entity_id=new.id,
            idempotency_key=f"lead:restart:{source.id}",
            payload={
                **body,
                "goal_id": str(new.id),
                "source_event_id": str(wakeup_id),
                "restart_from_event_id": str(source.id),
                "previous_goal_id": str(goal.id),
                "goal_version": new.version,
                "scope_revision": new.scope_revision,
                "delegation_id": str(new.delegation_id),
            },
        )
    )
    handoff = {"source_event_id": str(source.id), "new_goal_id": str(new.id)}
    goal.constraints = {**goal.constraints, "restart_handoff": handoff}
    await session.flush()
    return handoff
