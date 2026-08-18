"""Authorized Telegram ingress with persisted inbox and task affinity."""

import hashlib
import html
import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.config.models import TopicConfig, TopicKind
from vuzol.interpretation.discussion import DISCUSSION_CLASSIFY_DESTINATION
from vuzol.projects.importing import ProjectImportError, parse_github_repository_url
from vuzol.providers.subscription_limits import SUBSCRIPTION_LIMITS_DESTINATION
from vuzol.security.secret_ingress import create_request, parse_secret_command
from vuzol.storage.models import (
    ProjectDiscussionSession,
    ProjectProvisioning,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
)
from vuzol.storage.types import IntakeStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import IngressResult, IngressStatus, MessageUpdate
from vuzol.telegram.layout import (
    HELP_CARD_ROLE,
    is_help_command,
    is_import_command,
    is_model_command,
    is_plan_command,
    is_status_dashboard_topic,
    is_update_command,
)
from vuzol.telegram.model_command import enqueue_worker_picker
from vuzol.telegram.policy import TelegramPolicyError, authorize, validate_message
from vuzol.telegram.projections import enqueue_project_status_dashboard
from vuzol.telegram.work_package_projections import enqueue_project_topic_status
from vuzol.telegram.work_packages import ContinueDiscussionOverrides
from vuzol.workflows.transitions import transition_task


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
            if (
                update.text is not None
                and update.text.split(maxsplit=1)[0].split("@")[0] == "/secret"
            ):
                if topic.kind is not TopicKind.SYSTEM:
                    raise TelegramPolicyError("/secret is only available in the System topic")
                secret_name = parse_secret_command(update.text)
                if secret_name is None:
                    raise TelegramPolicyError("usage: /secret NAME")
                if (
                    not settings.secret_ingress.enabled
                    or secret_name not in settings.secret_ingress.allowed_names
                ):
                    raise TelegramPolicyError("secret name is not enabled")
                return await self._handle_secret_command(update, secret_name)
            if is_help_command(update.text):
                if not topic.enabled:
                    raise TelegramPolicyError("topic is disabled")
                return await self._handle_help_command(update, topic)
            if is_import_command(update.text):
                if topic.kind is not TopicKind.INBOX:
                    raise TelegramPolicyError("/import is only available in the New Project topic")
                if not topic.enabled:
                    raise TelegramPolicyError("new project topic is disabled")
                return await self._handle_import_command(update)
            if topic.kind is TopicKind.INBOX and update.text is not None:
                pending_import = await self._pending_import_task(update)
                if pending_import is not None:
                    return await self._handle_import_url(update, pending_import)
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
            if is_plan_command(update.text):
                if topic.kind is not TopicKind.PROJECT or topic.project_id is None:
                    raise TelegramPolicyError("/plan is only available in a project topic")
                if not topic.enabled:
                    raise TelegramPolicyError("project topic is disabled")
                return await self._handle_plan_command(update, topic)
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

    async def _pending_import_task(self, update: MessageUpdate) -> uuid.UUID | None:
        async with self._session_factory() as session:
            task_id = await session.scalar(
                select(Task.id)
                .where(
                    Task.source_chat_id == update.chat_id,
                    Task.source_thread_id == update.message_thread_id,
                    Task.user_id == update.user_id,
                    Task.task_type == "project_import",
                    Task.status == TaskStatus.AWAITING_USER,
                )
                .order_by(Task.created_at.desc())
                .limit(1)
            )
            return task_id if isinstance(task_id, uuid.UUID) else None

    async def _handle_import_command(self, update: MessageUpdate) -> IngressResult:
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
            existing = await uow.session.scalar(
                select(Task.id)
                .where(
                    Task.source_chat_id == update.chat_id,
                    Task.source_thread_id == update.message_thread_id,
                    Task.user_id == update.user_id,
                    Task.task_type == "project_import",
                    Task.status == TaskStatus.AWAITING_USER,
                )
                .order_by(Task.created_at.desc())
                .limit(1)
            )
            if existing is not None:
                await uow.inbox.mark_processed(inbox_id, entity_type="task", entity_id=existing)
                return IngressResult(status=IngressStatus.HANDLED, task_id=existing)
            record = await uow.tasks.create(
                user_id=update.user_id,
                chat_id=update.chat_id,
                thread_id=update.message_thread_id,
                original_text="/import",
                task_type="project_import",
            )
            task = await uow.tasks.get(record.id, for_update=True)
            await transition_task(
                uow.session,
                task,
                TaskStatus.AWAITING_USER,
                actor_type="telegram_import",
                actor_id=str(update.user_id),
                payload={"waiting_for": "repository_url"},
            )
            await uow.inbox.mark_processed(inbox_id, entity_type="task", entity_id=task.id)
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="task",
                entity_id=task.id,
                idempotency_key=f"telegram:project-import:{task.id}:prompt",
                payload={
                    "role": "project_import_prompt",
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "html": (
                        "<b>Подключить существующий проект</b>\n"
                        "Пришлите ссылку вида <code>https://github.com/owner/repository</code>."
                    ),
                },
            )
        return IngressResult(status=IngressStatus.HANDLED, task_id=task.id)

    async def _handle_import_url(self, update: MessageUpdate, task_id: uuid.UUID) -> IngressResult:
        try:
            imported = parse_github_repository_url(update.text or "")
        except ProjectImportError as error:
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
            assert uow.session is not None
            lock_key = int.from_bytes(
                hashlib.sha256(f"project:{imported.project_id}".encode()).digest()[:8],
                signed=True,
            )
            await uow.session.execute(select(func.pg_advisory_xact_lock(lock_key)))
            task = await uow.tasks.get(task_id, for_update=True)
            if task.status is not TaskStatus.AWAITING_USER:
                return IngressResult(
                    status=IngressStatus.REJECTED, reason="import is no longer active"
                )
            known_ids = {project.id for project in self._runtime.registries.projects.items()}
            conflict = await uow.session.scalar(
                select(ProjectProvisioning.id).where(
                    ProjectProvisioning.project_id == imported.project_id
                )
            )
            if imported.project_id in known_ids or conflict is not None:
                await uow.inbox.mark_processed(inbox_id, entity_type="task", entity_id=task.id)
                return IngressResult(
                    status=IngressStatus.REJECTED,
                    reason="a project with this repository name already exists",
                )
            provisioning = ProjectProvisioning(
                task_id=task.id,
                requested_by_user_id=update.user_id,
                chat_id=update.chat_id,
                source_thread_id=update.message_thread_id,
                project_id=imported.project_id,
                display_name=imported.display_name,
                description=f"Imported from {imported.url}",
                repository_path=imported.project_id,
                source_repository_url=imported.url,
            )
            uow.session.add(provisioning)
            await uow.session.flush()
            task.project_id = imported.project_id
            await transition_task(
                uow.session,
                task,
                TaskStatus.EXECUTING,
                actor_type="telegram_import",
                actor_id=str(update.user_id),
                payload={"project_id": imported.project_id},
            )
            await uow.inbox.mark_processed(
                inbox_id, entity_type="project_provisioning", entity_id=provisioning.id
            )
            await uow.outbox.enqueue(
                destination="project_provisioning",
                operation_type="import_project",
                entity_type="project_provisioning",
                entity_id=provisioning.id,
                idempotency_key=f"project:provision:{provisioning.id}",
                payload={"project_id": provisioning.project_id},
            )
        return IngressResult(status=IngressStatus.HANDLED, task_id=task.id)

    async def _handle_secret_command(
        self, update: MessageUpdate, secret_name: str
    ) -> IngressResult:
        configured = self._runtime.settings.secret_ingress
        async with UnitOfWork(self._session_factory) as uow:
            assert uow.session is not None
            request, token = await create_request(
                uow.session,
                configured,
                secret_name=secret_name,
                user_id=update.user_id,
                chat_id=update.chat_id,
                thread_id=update.message_thread_id,
            )
            base = str(configured.public_base_url).rstrip("/")
            url = f"{base}/secret/{token}"
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="delete_message",
                entity_type="secret_ingress_request",
                entity_id=request.id,
                idempotency_key=f"telegram:secret_command_delete:{update.chat_id}:{update.message_id}",
                payload={
                    "role": "user_command_delete",
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "message_id": update.message_id,
                },
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="secret_ingress_request",
                entity_id=request.id,
                idempotency_key=f"telegram:secret_ingress:{request.id}",
                payload={
                    "role": "secret_ingress",
                    "chat_id": update.chat_id,
                    "message_thread_id": update.message_thread_id,
                    "html": (
                        f"🔐 <b>{html.escape(secret_name)}</b>\n"
                        f'<a href="{html.escape(url, quote=True)}">Открыть защищённую форму</a>\n'
                        "Ссылка одноразовая и действует несколько минут."
                    ),
                    "callback_buttons": (("Отменить", f"v1:secret_cancel:{request.id}"),),
                },
            )
        return IngressResult(status=IngressStatus.HANDLED)

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
        control_override = (
            None
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
                    "control_override": (
                        None if control_override is None else control_override.value
                    ),
                },
            )
            await enqueue_project_topic_status(
                uow.session,
                chat_id=update.chat_id,
                thread_id=update.message_thread_id,
                project_id=topic.project_id,
                revision=update.message_id,
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

    async def _handle_plan_command(
        self, update: MessageUpdate, topic: TopicConfig
    ) -> IngressResult:
        """Render the current plan card without mixing it with runtime progress."""

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
            discussion = await uow.session.scalar(
                select(ProjectDiscussionSession).where(
                    ProjectDiscussionSession.chat_id == update.chat_id,
                    ProjectDiscussionSession.message_thread_id == update.message_thread_id,
                )
            )
            if discussion is None or discussion.active_work_package_id is None:
                raise TelegramPolicyError("this project topic has no current plan")
            await uow.outbox.enqueue(
                destination="work_package_projection",
                operation_type="clear_plan",
                entity_type="work_package",
                entity_id=discussion.active_work_package_id,
                idempotency_key=f"wp:plan:clear-command:{inbox_id}",
                payload={"package_id": str(discussion.active_work_package_id)},
            )
            await uow.outbox.enqueue(
                destination="work_package_projection",
                operation_type="render_plan",
                entity_type="work_package",
                entity_id=discussion.active_work_package_id,
                idempotency_key=f"wp:plan:command:{inbox_id}",
                payload={"package_id": str(discussion.active_work_package_id)},
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
