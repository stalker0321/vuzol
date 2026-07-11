"""Authorized Telegram ingress with persisted inbox and task affinity."""

import hashlib
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.storage.models import TelegramIntakeMessage, TelegramMessageLink, TopicMapping
from vuzol.storage.types import IntakeStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import IngressResult, IngressStatus, MessageUpdate
from vuzol.telegram.policy import TelegramPolicyError, authorize, validate_message


def update_hash(update: MessageUpdate) -> str:
    payload = update.model_dump_json(exclude_none=False)
    return hashlib.sha256(payload.encode()).hexdigest()


class TelegramIngressService:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._runtime = runtime
        self._session_factory = session_factory

    async def accept_message(self, update: MessageUpdate) -> IngressResult:
        settings = self._runtime.settings
        try:
            authorize(settings, chat_id=update.chat_id, user_id=update.user_id)
            validate_message(settings, update)
            topic = self._runtime.registries.topics.resolve(
                update.chat_id, update.message_thread_id
            )
            if not topic.enabled or not topic.accepts_new_tasks:
                raise TelegramPolicyError("topic does not accept new tasks")
        except (TelegramPolicyError, RegistryError) as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        async with UnitOfWork(self._session_factory) as uow:
            inbox_id, created = await uow.inbox.receive_once(
                source="telegram",
                consumer=f"bot:{update.bot_id}",
                external_event_id=str(update.update_id),
                payload_hash=update_hash(update),
            )
            if not created:
                return IngressResult(status=IngressStatus.DUPLICATE)

            await uow.topics.upsert(
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

            task_id: uuid.UUID | None = None
            affinity_kind: str | None = None
            candidates: tuple[uuid.UUID, ...] = ()
            if update.reply_to_message_id is not None:
                task_id = await uow.telegram_links.resolve_task(
                    update.chat_id, update.reply_to_message_id
                )
                if task_id is not None:
                    affinity_kind = "reply"
            if task_id is None:
                active = await uow.tasks.active_in_topic(update.chat_id, update.message_thread_id)
                if len(active) == 1:
                    task_id = active[0].id
                    affinity_kind = "single_active_task"
                elif len(active) > 1:
                    candidates = tuple(task.id for task in active)

            if task_id is None and not candidates:
                task = await uow.tasks.create(
                    user_id=update.user_id,
                    chat_id=update.chat_id,
                    thread_id=update.message_thread_id,
                    project_id=topic.project_id,
                    original_text=update.text or "[attachment request]",
                    task_type="general",
                )
                task_id = task.id
                affinity_kind = "new_task"

            intake_status = (
                IntakeStatus.NEEDS_CLARIFICATION
                if candidates
                else IntakeStatus.AWAITING_INTERPRETATION
            )
            intake = TelegramIntakeMessage(
                inbox_id=inbox_id,
                chat_id=update.chat_id,
                message_thread_id=update.message_thread_id,
                message_id=update.message_id,
                user_id=update.user_id,
                task_id=task_id,
                original_text=update.text,
                attachments=[
                    attachment.model_dump(mode="json") for attachment in update.attachments
                ],
                affinity_kind=affinity_kind,
                ambiguous_task_ids=[str(candidate) for candidate in candidates],
                status=intake_status,
            )
            intake_id = await uow.telegram_intake.add(intake)

            if task_id is not None:
                await uow.telegram_links.add(
                    TelegramMessageLink(
                        chat_id=update.chat_id,
                        message_thread_id=update.message_thread_id,
                        message_id=update.message_id,
                        task_id=task_id,
                        message_role="source_request",
                    )
                )

            await uow.inbox.mark_processed(
                inbox_id, entity_type="telegram_intake", entity_id=intake_id
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="telegram_intake",
                entity_id=intake_id,
                idempotency_key=f"telegram:intake:{update.chat_id}:{update.message_id}",
                payload={
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "role": "intake_ack" if not candidates else "clarification",
                    "task_id": str(task_id) if task_id is not None else None,
                    "candidate_task_ids": [str(candidate) for candidate in candidates],
                },
            )
            for attachment in update.attachments:
                await uow.outbox.enqueue(
                    destination="telegram_file",
                    operation_type="download_attachment",
                    entity_type="telegram_intake",
                    entity_id=intake_id,
                    idempotency_key=f"telegram:file:{attachment.file_unique_id}:{intake_id}",
                    payload={
                        "file_id": attachment.file_id,
                        "file_unique_id": attachment.file_unique_id,
                        "declared_size": attachment.file_size,
                        "media_type": attachment.media_type,
                        "filename": attachment.filename,
                    },
                )

        if candidates:
            return IngressResult(status=IngressStatus.NEEDS_CLARIFICATION, intake_id=intake_id)
        return IngressResult(
            status=(
                IngressStatus.CREATED if affinity_kind == "new_task" else IngressStatus.CONTINUATION
            ),
            task_id=task_id,
            intake_id=intake_id,
        )
