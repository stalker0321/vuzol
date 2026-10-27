"""Same-turn read-only chat recovery using existing tasks and durable delivery."""

import uuid
from html import escape

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import RuntimeConfiguration, TopicKind
from vuzol.config.registries import RegistryError
from vuzol.discussion.agent import (
    DISCUSSION_INTERNAL_TASK_TYPE,
    DiscussionAgentReply,
    persist_discussion_reply,
    schedule_discussion_agent,
)
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.storage.models import (
    ConversationTurn,
    Event,
    Run,
    Step,
    SupervisedProcess,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TransactionalOutbox,
)
from vuzol.storage.repositories.delivery import OutboxRepository
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    ConversationTurnRole,
    DeliveryStatus,
    DiscussionSessionStatus,
    InteractionMode,
    ProcessStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import ControlUpdate, IngressResult, IngressStatus
from vuzol.telegram.policy import TelegramPolicyError


def failure_role(task_id: uuid.UUID) -> str:
    return f"chat_failure:{task_id.hex}"


async def enqueue_chat_failure(
    session: AsyncSession,
    task: Task,
    *,
    key: str,
    delivery: bool = False,
    ambiguous: bool = False,
) -> None:
    if task.source_chat_id is None or task.source_thread_id is None:
        return
    text = (
        "Неясно, доставлен ли ответ. Автоматический повтор заблокирован, чтобы не отправить дубль."
        if ambiguous
        else (
            "Ответ получен, но не удалось его доставить. Можно повторить доставку."  # noqa: RUF001
            if delivery
            else "Не удалось получить ответ собеседника. Ничего не изменено."  # noqa: RUF001
        )
    )
    await OutboxRepository(session).enqueue(
        destination="telegram",
        operation_type="send_message",
        entity_type="task",
        entity_id=task.id,
        idempotency_key=key,
        payload={
            "role": "chat_failure",
            "chat_id": task.source_chat_id,
            "message_thread_id": task.source_thread_id,
            "html": escape(text),
            "retry_allowed": not ambiguous,
        },
    )


async def retry_chat_turn(
    uow: UnitOfWork,
    runtime: RuntimeConfiguration,
    update: ControlUpdate,
) -> IngressResult:
    """Explicit retry; never reset a terminal task or duplicate a raw turn."""
    assert uow.session is not None
    session = uow.session
    task = await session.get(Task, update.task_id)
    if (
        not runtime.settings.chat.enabled
        or task is None
        or task.task_type != DISCUSSION_INTERNAL_TASK_TYPE
        or (task.user_id, task.source_chat_id, task.source_thread_id)
        != (update.user_id, update.chat_id, update.message_thread_id)
        or task.source_turn_id is None
    ):
        raise TelegramPolicyError("chat_retry_unavailable")
    try:
        topic = runtime.registries.topics.resolve(update.chat_id, update.message_thread_id or 0)
    except RegistryError:
        raise TelegramPolicyError("chat_retry_unavailable") from None
    if (
        not topic.enabled
        or topic.kind is not TopicKind.PROJECT
        or topic.project_id != task.project_id
    ):
        raise TelegramPolicyError("chat_retry_unavailable")
    source = await session.get(ConversationTurn, task.source_turn_id)
    if source is None or source.role is not ConversationTurnRole.USER:
        raise TelegramPolicyError("chat_retry_unavailable")
    raw = source.content
    if source.intake_message_id is not None:
        intake = await session.get(TelegramIntakeMessage, source.intake_message_id)
        if intake is None or (intake.chat_id, intake.message_thread_id, intake.user_id) != (
            update.chat_id,
            update.message_thread_id,
            update.user_id,
        ):
            raise TelegramPolicyError("chat_retry_unavailable")
        if intake.original_text is not None and not intake.attachments:
            raw = intake.original_text
    discussion = await uow.discussions.get_session(source.session_id, for_update=True)
    if (
        discussion.status is not DiscussionSessionStatus.ACTIVE
        or discussion.project_id != task.project_id
        or (discussion.chat_id, discussion.message_thread_id)
        != (update.chat_id, update.message_thread_id)
    ):
        raise TelegramPolicyError("chat_retry_unavailable")
    # A forged UUID is insufficient: bind to a confirmed bot error message.
    link = await session.scalar(
        select(TelegramMessageLink.id).where(
            TelegramMessageLink.chat_id == update.chat_id,
            TelegramMessageLink.message_thread_id == update.message_thread_id,
            TelegramMessageLink.message_id == update.message_id,
            TelegramMessageLink.message_role == failure_role(task.id),
            TelegramMessageLink.task_id == task.id,
        )
    )
    if link is None:
        raise TelegramPolicyError("chat_retry_unavailable")
    latest_user = await session.scalar(
        select(ConversationTurn.id)
        .where(
            ConversationTurn.session_id == source.session_id,
            ConversationTurn.role == ConversationTurnRole.USER,
        )
        .order_by(ConversationTurn.ordinal.desc())
        .limit(1)
    )
    if latest_user != source.id:
        raise TelegramPolicyError("chat_retry_stale")
    active = await session.scalar(
        select(Task.id)
        .where(
            Task.task_type == DISCUSSION_INTERNAL_TASK_TYPE,
            Task.task_draft["discussion_session_id"].as_string() == str(source.session_id),
            Task.status.not_in((*USER_TERMINAL_TASK_STATUSES, TaskStatus.QUOTA_EXHAUSTED)),
        )
        .limit(1)
    )
    if active is not None:
        return IngressResult(status=IngressStatus.DUPLICATE, reason="chat_retry_pending")
    latest_attempt = await session.scalar(
        select(Task.id)
        .where(
            Task.task_type == DISCUSSION_INTERNAL_TASK_TYPE,
            Task.source_turn_id == source.id,
        )
        .order_by(Task.created_at.desc(), Task.id.desc())
        .limit(1)
    )
    if latest_attempt != task.id:
        raise TelegramPolicyError("chat_retry_stale")
    steps = tuple(
        (
            await session.scalars(
                select(Step)
                .join(Run)
                .where(
                    Run.task_id == task.id,
                )
            )
        ).all()
    )
    if any(step.unknown_effects for step in steps):
        raise TelegramPolicyError("chat_retry_ambiguous")
    live_process = await session.scalar(
        select(SupervisedProcess.id)
        .where(
            SupervisedProcess.step_id.in_([step.id for step in steps]),
            SupervisedProcess.status != ProcessStatus.EXITED,
        )
        .limit(1)
    )
    if live_process is not None:
        raise TelegramPolicyError("chat_retry_ambiguous")
    # Serialize callbacks with the delivery lease/complete transaction as well.
    replies = tuple(
        (
            await session.scalars(
                select(TransactionalOutbox)
                .where(
                    TransactionalOutbox.destination == "telegram",
                    TransactionalOutbox.payload["role"].as_string() == "discussion_reply",
                    TransactionalOutbox.payload["worker_task_id"].as_string() == str(task.id),
                )
                .order_by(TransactionalOutbox.created_at)
                .with_for_update()
            )
        ).all()
    )
    if any(
        item.status is DeliveryStatus.AMBIGUOUS or item.last_error_ambiguous for item in replies
    ):
        raise TelegramPolicyError("chat_retry_ambiguous")
    if replies:
        if any(item.status in {DeliveryStatus.PENDING, DeliveryStatus.LEASED} for item in replies):
            return IngressResult(status=IngressStatus.DUPLICATE, reason="chat_retry_pending")
        failed = [item for item in replies if item.status is DeliveryStatus.DEAD_LETTER]
        if not failed:
            return IngressResult(status=IngressStatus.HANDLED, reason="chat_already_delivered")
        # Only the unconfirmed suffix is retried, never the confirmed prefix.
        for item in failed:
            previous_attempts = item.attempt_count
            item.status = DeliveryStatus.PENDING
            item.attempt_count = 0
            item.available_at = func.now()
            item.lease_owner = None
            item.lease_expires_at = None
            item.lease_generation += 1
            session.add(
                Event(
                    entity_type="transactional_outbox",
                    entity_id=item.id,
                    event_type="chat.delivery_retry_requested",
                    actor_type="user",
                    actor_id=str(update.user_id),
                    payload={
                        "task_id": str(task.id),
                        "failure_category": item.last_error_category,
                        "previous_attempt_count": previous_attempts,
                    },
                )
            )
        return IngressResult(status=IngressStatus.HANDLED, reason="chat_delivery_retry")
    if task.status not in {TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.QUOTA_EXHAUSTED}:
        raise TelegramPolicyError("chat_retry_unavailable")
    completed = next(
        (
            step
            for step in steps
            if step.step_type == "execute_agent" and step.status is StepStatus.COMPLETED
        ),
        None,
    )
    if completed is not None and completed.result is not None:
        structured = completed.result.get("structured_output")
        if isinstance(structured, dict):
            try:
                parsed = DiscussionAgentReply.model_validate(structured)
            except ValueError:
                raise TelegramPolicyError("chat_retry_unavailable") from None
            await persist_discussion_reply(
                uow,
                task,
                parsed,
                literal_reply=(
                    completed.result.get("output_schema_version") == "discussion-text-reply.v2"
                ),
            )
            session.add(
                Event(
                    entity_type="task",
                    entity_id=task.id,
                    event_type="chat.answer_delivery_recovered",
                    actor_type="user",
                    actor_id=str(update.user_id),
                    payload={"source_turn_id": str(source.id)},
                )
            )
            return IngressResult(status=IngressStatus.HANDLED, reason="chat_delivery_retry")
    # Keep the task-referenced read context available on a fresh attempt.
    work = tuple(
        (
            await session.scalars(
                select(Task)
                .where(
                    Task.project_id == task.project_id,
                    Task.user_id == task.user_id,
                    Task.source_chat_id == task.source_chat_id,
                    Task.source_thread_id == task.source_thread_id,
                    Task.task_type != DISCUSSION_INTERNAL_TASK_TYPE,
                )
                .order_by(Task.created_at.desc())
                .limit(20)
            )
        ).all()
    )
    context = "\n".join(
        f"Task {t.id}, number {t.public_task_number}, status {t.status.value}: "
        f"{t.original_text[:1000]}"
        for t in work
    )
    for work_task in work:
        result = await session.scalar(
            select(Step.result)
            .join(Run)
            .where(
                Run.task_id == work_task.id,
                Step.step_type.in_(["execute_agent", "execute_code"]),
                Step.result.is_not(None),
            )
            .order_by(Step.created_at.desc())
            .limit(1)
        )
        if result is not None:
            context += "\nRecorded result (bounded): " + str(result)[:2000]
    new_id = await schedule_discussion_agent(
        session,
        runtime=runtime,
        session_id=source.session_id,
        source_turn_id=source.id,
        project_id=discussion.project_id,
        chat_id=discussion.chat_id,
        thread_id=discussion.message_thread_id,
        user_id=update.user_id,
        interaction_mode=InteractionMode(str(task.task_draft["interaction_mode"])),
        classifier_confidence=float(task.task_draft["classifier_confidence"]),
        memory_pack=await DiscussionMemoryService(uow).load_context(
            session_id=source.session_id,
            chat_memory=runtime.settings.chat.memory_enabled,
            chat_context=runtime.settings.chat.enabled,
            query=raw,
        ),
        current_turn=raw,
        task_context=context,
    )
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="chat.reply_retry_requested",
            actor_type="user",
            actor_id=str(update.user_id),
            payload={"new_task_id": str(new_id), "source_turn_id": str(source.id)},
        )
    )
    return IngressResult(status=IngressStatus.HANDLED, task_id=new_id, reason="chat_reply_retry")
