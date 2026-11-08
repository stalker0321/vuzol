"""Owner tasks queued behind a project's current goal, started one by one as goals end.

Owner 2026-11-03/11-09: an unrelated task sent while a goal was busy got glued into
it, or the lead asked "addition or new task?" and told the owner to resend later.
Now the lead queues it, keeping the owner's message as sent, and the next goal
starts from it on its own once the project is free.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.leadership.persistence import LIVE_GOAL_LIFECYCLES, ManagedGoal
from vuzol.storage.models import (
    ProjectDiscussionSession,
    ProjectTaskQueueItem,
    TransactionalOutbox,
)
from vuzol.storage.types import DiscussionSessionStatus


def _owner_message(source: TransactionalOutbox | None, goal: ManagedGoal) -> bool:
    return (
        source is not None
        and source.destination == "project_lead"
        and source.linked_entity_id == goal.id
        and source.payload.get("wakeup_kind") == "owner_message"
        and source.payload.get("owner_user_id") == goal.owner_user_id
        and isinstance(source.payload.get("text"), str)
        and bool(source.payload["text"].strip())
    )


async def queue_task(
    session: AsyncSession, goal: ManagedGoal, source: TransactionalOutbox | None, summary: str
) -> tuple[ProjectTaskQueueItem, int]:
    """Queue the owner message that woke this turn; repeating it is harmless."""
    if not _owner_message(source, goal):
        raise DomainError("owner_request_required")
    assert source is not None
    item = await session.scalar(
        select(ProjectTaskQueueItem).where(ProjectTaskQueueItem.source_event_id == source.id)
    )
    if item is None:
        item = ProjectTaskQueueItem(
            id=uuid.uuid4(),
            project_id=goal.project_id,
            owner_user_id=goal.owner_user_id,
            discussion_session_id=goal.discussion_session_id,
            source_event_id=source.id,
            body=dict(source.payload),
            summary=summary.strip()[:200],
            state="queued",
            queued_by_goal_id=goal.id,
        )
        session.add(item)
        await session.flush()
    place = [row.id for row in await waiting(session, goal.project_id)].index(item.id) + 1
    return item, place


async def drop_queued_task(
    session: AsyncSession, goal: ManagedGoal, queue_id: str
) -> ProjectTaskQueueItem:
    try:
        key = uuid.UUID(queue_id)
    except ValueError:
        raise DomainError("queue_item_unknown") from None
    item = await session.get(ProjectTaskQueueItem, key, with_for_update=True)
    if item is None or item.project_id != goal.project_id or item.state != "queued":
        raise DomainError("queue_item_unknown")
    item.state = "dropped"
    await session.flush()
    return item


async def waiting(session: AsyncSession, project_id: str) -> list[ProjectTaskQueueItem]:
    return list(
        await session.scalars(
            select(ProjectTaskQueueItem)
            .where(
                ProjectTaskQueueItem.project_id == project_id,
                ProjectTaskQueueItem.state == "queued",
            )
            .order_by(ProjectTaskQueueItem.created_at, ProjectTaskQueueItem.id)
        )
    )


async def queue_view(session: AsyncSession, project_id: str) -> list[dict[str, Any]]:
    """What the lead sees: the queue in order, with the ids drop_queued_task takes."""
    return [
        {"queue_id": str(item.id), "place": place, "summary": item.summary}
        for place, item in enumerate(await waiting(session, project_id), start=1)
    ]


async def start_next(
    session: AsyncSession,
    project_id: str,
    *,
    branch_prefix: str,
    default_goal_max_cost: float | None = None,
) -> ManagedGoal | None:
    """Start the first queued task as a new goal when the project is free.

    Caller holds project admission. A finished goal still waiting for the owner's
    acceptance holds the queue: the next goal would build on unmerged work.
    """
    from vuzol.leadership.events import grant_preapproval, live_or_draft

    if await session.scalar(
        select(ManagedGoal.id).where(
            ManagedGoal.project_id == project_id,
            ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
        )
    ):
        return None
    last = await session.scalar(
        select(ManagedGoal)
        .where(ManagedGoal.project_id == project_id)
        .order_by(ManagedGoal.created_at.desc())
        .limit(1)
    )
    if (
        last is not None
        and last.lifecycle == "completed"
        and not last.constraints.get("owner_acceptance")
    ):
        return None
    item = await session.scalar(
        select(ProjectTaskQueueItem)
        .where(
            ProjectTaskQueueItem.project_id == project_id,
            ProjectTaskQueueItem.state == "queued",
        )
        .order_by(ProjectTaskQueueItem.created_at, ProjectTaskQueueItem.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if item is None:
        return None
    discussion = await session.get(ProjectDiscussionSession, item.discussion_session_id)
    if discussion is None or discussion.status is not DiscussionSessionStatus.ACTIVE:
        discussion = await session.scalar(
            select(ProjectDiscussionSession).where(
                ProjectDiscussionSession.project_id == project_id,
                ProjectDiscussionSession.status == DiscussionSessionStatus.ACTIVE,
            )
        )
    if discussion is None:
        return None
    body = item.body
    previous = await session.get(ManagedGoal, item.queued_by_goal_id)
    goal = await live_or_draft(
        session,
        project_id=project_id,
        owner_user_id=item.owner_user_id,
        discussion=discussion,
        text=body["text"],
        scope=body.get("request_scope", body["text"]),
        criteria=body.get("request_criteria", []),
        constraints={
            "work_limits": body.get("request_work_limits", {}),
            "request_origin": body.get("request_origin"),
            "project_default_branch": (
                previous.constraints.get("project_default_branch", "main")
                if previous is not None
                else "main"
            ),
            "queued_from_event_id": str(item.source_event_id),
        },
        branch_prefix=branch_prefix,
        default_goal_max_cost=default_goal_max_cost,
    )
    if body.get("request_preapproved"):
        await grant_preapproval(session, goal, str(item.source_event_id))
    wakeup_id = uuid.uuid5(item.id, "start")
    session.add(
        TransactionalOutbox(
            id=wakeup_id,
            destination="project_lead",
            operation_type="lead_wakeup",
            linked_entity_type="managed_goal",
            linked_entity_id=goal.id,
            idempotency_key=f"lead:queued:{item.id}",
            payload={
                **body,
                "goal_id": str(goal.id),
                "session_id": str(discussion.id),
                "source_event_id": str(wakeup_id),
                "queued_from_event_id": str(item.source_event_id),
                "goal_version": goal.version,
                "scope_revision": goal.scope_revision,
                "delegation_id": str(goal.delegation_id),
            },
        )
    )
    item.state = "started"
    item.started_goal_id = goal.id
    await session.flush()
    return goal
