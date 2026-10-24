"""Owner-authorized transfer of a persisted discussion brief to the project lead."""

from sqlalchemy import select

from vuzol.config import RuntimeConfiguration
from vuzol.discussion.agent import validated_handoff
from vuzol.leadership.events import ProjectEventService
from vuzol.leadership.fence import lock_project_admission
from vuzol.leadership.provenance import RequestOrigin
from vuzol.storage.models import (
    ConversationTurn,
    ProjectDiscussionSession,
    Task,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.types import ConversationTurnRole, DiscussionSessionStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import ControlUpdate, IngressResult, IngressStatus
from vuzol.telegram.policy import TelegramPolicyError
from vuzol.workflows.application import Principal


async def accept_think_handoff(
    uow: UnitOfWork, runtime: RuntimeConfiguration, update: ControlUpdate
) -> IngressResult:
    assert uow.session is not None
    session = uow.session
    if (
        not runtime.settings.project_lead.enabled
        or update.assistant_turn_id is None
        or update.message_thread_id is None
        or update.message_id is None
    ):
        raise TelegramPolicyError("think_handoff_unavailable")
    topic = runtime.registries.topics.resolve(update.chat_id, update.message_thread_id)
    if not topic.enabled or topic.project_id is None:
        raise TelegramPolicyError("think_handoff_unavailable")
    # Serialize older/newer presses. Let submit_event lock the goal before the
    # discussion row, matching the lead worker's order when it records a reply.
    await lock_project_admission(session, topic.project_id)
    turn = await session.get(ConversationTurn, update.assistant_turn_id)
    if turn is None or turn.role is not ConversationTurnRole.ASSISTANT:
        raise TelegramPolicyError("think_handoff_unavailable")
    discussion = await session.get(ProjectDiscussionSession, turn.session_id)
    if (
        discussion is None
        or discussion.status is not DiscussionSessionStatus.ACTIVE
        or discussion.project_id != topic.project_id
        or (discussion.chat_id, discussion.message_thread_id)
        != (update.chat_id, update.message_thread_id)
    ):
        raise TelegramPolicyError("think_handoff_unavailable")
    reply = await session.scalar(
        select(TransactionalOutbox).where(
            TransactionalOutbox.destination == "telegram",
            TransactionalOutbox.idempotency_key == f"discussion_reply:{turn.id}",
            TransactionalOutbox.linked_entity_id == turn.id,
            TransactionalOutbox.payload["role"].as_string() == "discussion_reply",
        )
    )
    brief = validated_handoff(reply.payload.get("handoff")) if reply else None
    if reply is None or brief is None:
        raise TelegramPolicyError("think_handoff_unavailable")
    task = await session.scalar(select(Task).where(Task.id == reply.payload.get("worker_task_id")))
    route = task.task_draft.get("conversation_route") if task else None
    if (
        task is None
        or task.user_id != update.user_id
        or not task.task_draft.get("handoff_enabled")
        or not isinstance(route, dict)
        or route.get("conversation_mode") != "think"
        or task.task_draft.get("discussion_session_id") != str(discussion.id)
    ):
        raise TelegramPolicyError("think_handoff_unavailable")
    link = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.chat_id == update.chat_id,
            TelegramMessageLink.message_thread_id == update.message_thread_id,
            TelegramMessageLink.message_id == update.message_id,
            TelegramMessageLink.message_role == "discussion_reply",
            TelegramMessageLink.task_id == task.id,
        )
    )
    if link is None:
        raise TelegramPolicyError("think_handoff_unavailable")
    newer = await session.scalar(
        select(TransactionalOutbox.id)
        .join(ConversationTurn, ConversationTurn.id == TransactionalOutbox.linked_entity_id)
        .where(
            TransactionalOutbox.destination == "telegram",
            TransactionalOutbox.payload["role"].as_string() == "discussion_reply",
            TransactionalOutbox.payload["handoff_submitted"].as_boolean().is_(True),
            ConversationTurn.session_id == discussion.id,
            ConversationTurn.ordinal > turn.ordinal,
        )
        .limit(1)
    )
    duplicate = newer is not None
    if not duplicate:
        try:
            receipt = await ProjectEventService(runtime).submit_event(
                uow,
                Principal(user_id=update.user_id, ingress_source="telegram"),
                project_id=discussion.project_id,
                session_id=discussion.id,
                idempotency_key=f"think-handoff:{turn.id}",
                text=brief.request,
                scope=brief.scope,
                criteria=[
                    {"id": f"c{i}", "description": criterion}
                    for i, criterion in enumerate(brief.criteria, 1)
                ],
                origin=RequestOrigin(channel="think", display_name="Обсуждение"),
            )
        except PermissionError as error:
            raise TelegramPolicyError("think_handoff_unavailable") from error
        if receipt.status not in {"accepted", "duplicate"}:
            raise TelegramPolicyError("think_handoff_unavailable")
        duplicate = receipt.status == "duplicate"
        if not duplicate:
            mapping_id = await uow.topics.upsert(
                TopicMapping(
                    chat_id=update.chat_id,
                    message_thread_id=update.message_thread_id,
                    topic_kind=topic.kind.value,
                    project_id=topic.project_id,
                    accepts_new_tasks=topic.accepts_new_tasks,
                    default_workflow=topic.default_workflow,
                    enabled=topic.enabled,
                )
            )
            mapping = await session.get(TopicMapping, mapping_id, with_for_update=True)
            assert mapping is not None
            mapping.conversation_mode = "lead"
            mapping.conversation_mode_message_id = max(
                mapping.conversation_mode_message_id or 0, update.message_id
            )
            reply.payload = {**reply.payload, "handoff_submitted": True}
    confirmation_key = f"think-handoff:confirmation:{turn.id}:{update.message_id}"
    confirmation = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == "telegram",
            TransactionalOutbox.idempotency_key == confirmation_key,
        )
    )
    if confirmation is None:
        await uow.outbox.enqueue(
            destination="telegram",
            operation_type="edit_reply_markup",
            entity_type="conversation_turn",
            entity_id=turn.id,
            idempotency_key=confirmation_key,
            payload={
                "role": "think_handoff_confirmation",
                "chat_id": update.chat_id,
                "message_thread_id": update.message_thread_id,
                "message_id": update.message_id,
                "callback_buttons": [[["✅ Передано лиду", f"v2:th:{turn.id.hex}"]]],
            },
        )
    return IngressResult(
        status=IngressStatus.DUPLICATE if duplicate else IngressStatus.HANDLED,
        reason="think_handoff_duplicate" if duplicate else "think_handoff_sent",
    )
