"""Authorized Telegram ingress with persisted inbox and task affinity."""

import hashlib
import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.config.models import TopicConfig, TopicKind
from vuzol.interpretation.discussion import DISCUSSION_CLASSIFY_DESTINATION
from vuzol.providers.subscription_limits import SUBSCRIPTION_LIMITS_DESTINATION
from vuzol.storage.models import TelegramIntakeMessage, TelegramMessageLink, TopicMapping
from vuzol.storage.types import IntakeStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import IngressResult, IngressStatus, MessageUpdate
from vuzol.telegram.layout import (
    HELP_CARD_ROLE,
    is_help_command,
    is_model_command,
    is_status_dashboard_topic,
    is_update_command,
)
from vuzol.telegram.model_command import enqueue_worker_picker
from vuzol.telegram.policy import TelegramPolicyError, authorize, validate_message
from vuzol.telegram.projections import enqueue_project_status_dashboard
from vuzol.telegram.work_packages import ContinueDiscussionOverrides


def update_hash(update: MessageUpdate) -> str:
    payload = update.model_dump_json(exclude_none=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _discussion_lock_key(chat_id: int, message_thread_id: int) -> int:
    digest = hashlib.blake2b(
        f"{chat_id}:{message_thread_id}".encode(), digest_size=8, person=b"vuzol-p4"
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


class TelegramIngressService:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
        continue_discussion_overrides: ContinueDiscussionOverrides | None = None,
    ) -> None:
        self._runtime = runtime
        self._session_factory = session_factory
        self._continue_discussion_overrides = continue_discussion_overrides

    async def accept_message(self, update: MessageUpdate) -> IngressResult:
        settings = self._runtime.settings
        try:
            authorize(settings, chat_id=update.chat_id, user_id=update.user_id)
            validate_message(settings, update)
            topic = self._runtime.registries.topics.resolve(
                update.chat_id, update.message_thread_id
            )
            if is_help_command(update.text):
                if not topic.enabled:
                    raise TelegramPolicyError("topic is disabled")
                return await self._handle_help_command(update, topic)
            if is_status_dashboard_topic(topic.kind) and is_update_command(update.text):
                if not topic.enabled:
                    raise TelegramPolicyError("status dashboard topic is disabled")
                return await self._handle_dashboard_update(update, topic)
            if is_model_command(update.text):
                if topic.kind is not TopicKind.PROJECT or topic.project_id is None:
                    raise TelegramPolicyError("/model is only available in a project topic")
                if not topic.enabled:
                    raise TelegramPolicyError("project topic is disabled")
                return await self._handle_model_command(update, topic)
            if not topic.enabled or not topic.accepts_new_tasks:
                raise TelegramPolicyError("topic does not accept new tasks")
        except (TelegramPolicyError, RegistryError) as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        if (
            settings.project_discussion_enabled
            and topic.kind is TopicKind.PROJECT
            and topic.project_id is not None
            and update.text is not None
            and not update.attachments
            and not await self._reply_targets_task(update)
        ):
            return await self._accept_discussion_message(update, topic)

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
                awaiting_clarification = tuple(
                    task for task in active if task.status is TaskStatus.AWAITING_USER
                )
                if len(awaiting_clarification) == 1:
                    task_id = awaiting_clarification[0].id
                    affinity_kind = "clarification_answer"
            if task_id is None:
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

            intake_status = IntakeStatus.AWAITING_INTERPRETATION
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
            if task_id is not None:
                assert uow.session is not None
                await enqueue_project_status_dashboard(uow.session, update.chat_id)
            has_voice = any(
                attachment.kind.value in {"voice", "audio"} for attachment in update.attachments
            )
            if not has_voice:
                await uow.outbox.enqueue(
                    destination="interpretation",
                    operation_type="interpret_intake",
                    entity_type="telegram_intake",
                    entity_id=intake_id,
                    idempotency_key=f"interpretation:intake:{intake_id}",
                    payload={},
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
                        "kind": attachment.kind.value,
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

    async def _reply_targets_task(self, update: MessageUpdate) -> bool:
        """Keep only replies to canonical Task-linked messages on the legacy task path."""

        if update.reply_to_message_id is None:
            return False
        async with UnitOfWork(self._session_factory) as uow:
            return (
                await uow.telegram_links.resolve_task(update.chat_id, update.reply_to_message_id)
                is not None
            )

    async def _accept_discussion_message(
        self, update: MessageUpdate, topic: TopicConfig
    ) -> IngressResult:
        """Persist default-off discussion intake without materializing a Task."""

        assert topic.project_id is not None
        force_discussion = (
            False
            if self._continue_discussion_overrides is None
            else await self._continue_discussion_overrides.consume(
                chat_id=update.chat_id,
                thread_id=update.message_thread_id,
                user_id=update.user_id,
            )
        )
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
            assert uow.session is not None
            await uow.session.execute(
                select(
                    func.pg_advisory_xact_lock(
                        _discussion_lock_key(update.chat_id, update.message_thread_id)
                    )
                )
            )
            session_id = await uow.discussions.active_session_id(
                chat_id=update.chat_id,
                message_thread_id=update.message_thread_id,
            )
            if session_id is None:
                session_id = await uow.discussions.create_session(
                    project_id=topic.project_id,
                    chat_id=update.chat_id,
                    message_thread_id=update.message_thread_id,
                )
            intake = TelegramIntakeMessage(
                inbox_id=inbox_id,
                chat_id=update.chat_id,
                message_thread_id=update.message_thread_id,
                message_id=update.message_id,
                user_id=update.user_id,
                task_id=None,
                original_text=update.text,
                attachments=[],
                affinity_kind="discussion",
                ambiguous_task_ids=[],
                status=IntakeStatus.AWAITING_INTERPRETATION,
            )
            intake_id = await uow.telegram_intake.add(intake)
            await uow.inbox.mark_processed(
                inbox_id, entity_type="telegram_intake", entity_id=intake_id
            )
            await uow.outbox.enqueue(
                destination=DISCUSSION_CLASSIFY_DESTINATION,
                operation_type="classify_intake",
                entity_type="telegram_intake",
                entity_id=intake_id,
                idempotency_key=f"discussion:classify:{intake_id}",
                payload={
                    "discussion_session_id": str(session_id),
                    "project_id": topic.project_id,
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "user_id": update.user_id,
                    "control_override": "continue_discussion" if force_discussion else None,
                },
            )
        return IngressResult(status=IngressStatus.HANDLED, intake_id=intake_id)

    async def _handle_help_command(
        self, update: MessageUpdate, topic: TopicConfig
    ) -> IngressResult:
        """Show topic-specific help without materializing a Task."""

        async with UnitOfWork(self._session_factory) as uow:
            inbox_id, created = await uow.inbox.receive_once(
                source="telegram",
                consumer=f"bot:{update.bot_id}",
                external_event_id=str(update.update_id),
                payload_hash=update_hash(update),
            )
            if not created:
                return IngressResult(status=IngressStatus.DUPLICATE)

            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="telegram_inbox",
                entity_id=inbox_id,
                idempotency_key=f"telegram:help:{update.chat_id}:{update.update_id}",
                payload={
                    "role": HELP_CARD_ROLE,
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "topic_kind": topic.kind.value,
                },
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="delete_message",
                entity_type="telegram_inbox",
                entity_id=inbox_id,
                idempotency_key=f"telegram:delete_command:{update.chat_id}:{update.message_id}",
                payload={
                    "role": "user_command_delete",
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "message_id": update.message_id,
                },
            )
            await uow.inbox.mark_processed(
                inbox_id, entity_type="telegram_command", entity_id=inbox_id
            )
        return IngressResult(status=IngressStatus.HANDLED)

    async def _handle_dashboard_update(
        self, update: MessageUpdate, topic: TopicConfig
    ) -> IngressResult:
        """Handle ``/update`` in «Статус проектов»: refresh limits and delete the command.

        Limit collection requires provider-state ACL, so the ingress only enqueues a
        refresh for the executor and a delete for Telegram delivery.
        """

        del topic
        async with UnitOfWork(self._session_factory) as uow:
            inbox_id, created = await uow.inbox.receive_once(
                source="telegram",
                consumer=f"bot:{update.bot_id}",
                external_event_id=str(update.update_id),
                payload_hash=update_hash(update),
            )
            if not created:
                return IngressResult(status=IngressStatus.DUPLICATE)

            await uow.inbox.mark_processed(
                inbox_id, entity_type="telegram_command", entity_id=inbox_id
            )
            await uow.outbox.enqueue(
                destination=SUBSCRIPTION_LIMITS_DESTINATION,
                operation_type="refresh",
                entity_type="telegram_inbox",
                entity_id=inbox_id,
                idempotency_key=(
                    f"subscription_limits:refresh:{update.chat_id}:{update.update_id}"
                ),
                payload={"chat_id": update.chat_id, "reason": "user_update_command"},
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="delete_message",
                entity_type="telegram_inbox",
                entity_id=inbox_id,
                idempotency_key=(f"telegram:delete_command:{update.chat_id}:{update.message_id}"),
                payload={
                    "role": "user_command_delete",
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "message_id": update.message_id,
                },
            )
        return IngressResult(status=IngressStatus.HANDLED)

    async def _handle_model_command(
        self, update: MessageUpdate, topic: TopicConfig
    ) -> IngressResult:
        """Handle ``/model`` in a project topic: show the project executor picker."""

        assert topic.project_id is not None
        async with UnitOfWork(self._session_factory) as uow:
            inbox_id, created = await uow.inbox.receive_once(
                source="telegram",
                consumer=f"bot:{update.bot_id}",
                external_event_id=str(update.update_id),
                payload_hash=update_hash(update),
            )
            if not created:
                return IngressResult(status=IngressStatus.DUPLICATE)

            assert uow.session is not None
            await enqueue_worker_picker(
                uow.session,
                runtime=self._runtime,
                project_id=topic.project_id,
                chat_id=update.chat_id,
                message_thread_id=update.message_thread_id,
                inbox_id=inbox_id,
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="delete_message",
                entity_type="telegram_inbox",
                entity_id=inbox_id,
                idempotency_key=(f"telegram:delete_command:{update.chat_id}:{update.message_id}"),
                payload={
                    "role": "user_command_delete",
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "message_id": update.message_id,
                },
            )
            await uow.inbox.mark_processed(
                inbox_id, entity_type="telegram_command", entity_id=inbox_id
            )
        return IngressResult(status=IngressStatus.HANDLED)
