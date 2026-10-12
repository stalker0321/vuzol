"""Publish optional model-derived summaries only after full reply confirmation."""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory import ensure_memory_safe
from vuzol.storage.models import ConversationTurn, Event, TransactionalOutbox
from vuzol.storage.repositories.discussion import DiscussionRepository
from vuzol.storage.types import ConversationSummaryGenerator


def safe_summary_candidate(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        body = ensure_memory_safe(value)
    except DomainError:
        return None
    return body if len(body) <= 2_000 else None


async def publish_reply_summary(session: AsyncSession, item: TransactionalOutbox) -> None:
    candidate = item.payload.get("memory_summary")
    revision = item.payload.get("memory_summary_revision")
    if (
        not isinstance(candidate, str)
        or not item.payload.get("memory_summary_eligible")
        or type(revision) is not int
    ):
        return
    try:
        body = ensure_memory_safe(candidate)
        source_id = uuid.UUID(str(item.payload["source_turn_id"]))
    except (DomainError, KeyError, ValueError):
        return
    if len(body) > 2_000:
        return
    source = await session.get(ConversationTurn, source_id)
    reply = await session.get(ConversationTurn, item.linked_entity_id)
    if source is None or reply is None or source.session_id != reply.session_id:
        return
    repository = DiscussionRepository(session)
    discussion = await repository.get_session(source.session_id, for_update=True)
    if discussion.summary_revision != revision:
        return  # A delayed delivery cannot overwrite a newer summary.
    summary_id, new_revision = await repository.add_summary(
        session_id=source.session_id,
        body=body,
        covered_through_turn_ordinal=source.ordinal + 1,
        generator=ConversationSummaryGenerator.MODEL,
        expected_summary_revision=revision,
    )
    session.add(
        Event(
            entity_type="discussion_session",
            entity_id=source.session_id,
            event_type="conversation_summary.updated",
            actor_type="model",
            payload={
                "summary_id": str(summary_id),
                "revision": new_revision,
                "covered_through_turn_ordinal": source.ordinal + 1,
                "source_turn_id": str(source.id),
                "reply_turn_id": str(reply.id),
            },
        )
    )
