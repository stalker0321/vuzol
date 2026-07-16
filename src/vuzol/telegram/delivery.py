"""Fenced Telegram outbox dispatch from canonical PostgreSQL state."""

import asyncio
import uuid
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut

from vuzol.config import TopicKind, TopicRegistry
from vuzol.observability import get_logger
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    mark_outbox_ambiguous,
    retry_outbox_item,
)
from vuzol.storage.models import (
    Interpretation,
    ProjectNamingRequest,
    ProjectProvisioning,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TransactionalOutbox,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.telegram.projections import (
    LostTelegramResponse,
    TelegramClient,
    build_approval_card,
    build_status_card,
    telegram_html,
)

TELEGRAM_DESTINATIONS = frozenset({"telegram"})


class DeliveryAction(StrEnum):
    SEND_STATUS = "send_status"
    EDIT_STATUS = "edit_status"
    SEND_CLARIFICATION = "send_clarification"
    SEND_PROJECT_WELCOME = "send_project_welcome"
    SEND_PROJECT_NAMES = "send_project_names"
    DELETE_MESSAGE = "delete_message"
    NOOP = "noop"


@dataclass(frozen=True, slots=True)
class PreparedDelivery:
    action: DeliveryAction
    chat_id: int
    thread_id: int | None
    html: str = ""
    task_id: uuid.UUID | None = None
    revision: int | None = None
    link_id: uuid.UUID | None = None
    message_id: int | None = None
    buttons: tuple[str, ...] = ()
    approval_id: uuid.UUID | None = None
    message_role: str | None = None
    callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = ()


class PermanentDeliveryError(RuntimeError):
    """A safe, categorized delivery failure that must not be retried."""

    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


class DeliveryRunner(Protocol):
    async def deliver_one(self) -> bool: ...


async def prepare_delivery(
    session: AsyncSession,
    item: TransactionalOutbox,
    topics: TopicRegistry | None = None,
) -> PreparedDelivery:
    if item.operation_type not in {"send_message", "delete_message"}:
        raise PermanentDeliveryError("unsupported_telegram_operation")
    if item.linked_entity_type == "project_naming":
        naming = await session.get(ProjectNamingRequest, item.linked_entity_id)
        if naming is None:
            raise PermanentDeliveryError("project_naming_missing")
        link = await session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.task_id == naming.task_id,
                TelegramMessageLink.message_role == "project_naming",
            )
        )
        if item.operation_type == "delete_message":
            if link is None:
                return PreparedDelivery(
                    DeliveryAction.NOOP,
                    chat_id=naming.chat_id,
                    thread_id=naming.source_thread_id,
                )
            return PreparedDelivery(
                DeliveryAction.DELETE_MESSAGE,
                chat_id=link.chat_id,
                thread_id=link.message_thread_id,
                link_id=link.id,
                message_id=link.message_id,
            )
        revision = item.payload.get("revision")
        if (
            item.payload.get("role") != "project_name_options"
            or revision != naming.revision
            or naming.status.value != "pending"
            or len(naming.options) != 9
        ):
            raise PermanentDeliveryError("invalid_project_naming_delivery")
        if link is not None and link.projection_revision >= naming.revision:
            return PreparedDelivery(
                DeliveryAction.NOOP,
                chat_id=naming.chat_id,
                thread_id=naming.source_thread_id,
            )
        rows: list[tuple[tuple[str, str], ...]] = []
        for offset in range(0, 9, 3):
            rows.append(
                tuple(
                    (
                        str(option["display_name"]),
                        f"v1:pn:{naming.id.hex}:{naming.revision}:{index}",
                    )
                    for index, option in enumerate(
                        naming.options[offset : offset + 3], start=offset
                    )
                )
            )
        rows.append((("Другие варианты", f"v1:pn:{naming.id.hex}:{naming.revision}:r"),))
        html = (
            "<b>Выберите название проекта</b>\n"
            "Каждый вариант включает безопасное имя репозитория.\n\n"
            f"{telegram_html(naming.description)}"
        )
        return PreparedDelivery(
            DeliveryAction.SEND_PROJECT_NAMES,
            chat_id=naming.chat_id,
            thread_id=naming.source_thread_id,
            html=html,
            task_id=naming.task_id,
            revision=naming.revision,
            message_role="project_naming",
            callback_buttons=tuple(rows),
        )
    if item.linked_entity_type == "project_provisioning":
        if item.payload.get("role") != "project_created":
            raise PermanentDeliveryError("invalid_project_delivery_payload")
        provisioning = await session.get(ProjectProvisioning, item.linked_entity_id)
        if provisioning is None or provisioning.topic_thread_id is None:
            raise PermanentDeliveryError("project_provisioning_missing")
        html = (
            f"<b>{telegram_html(provisioning.display_name)}</b>\n"
            f"Проект создан: <code>{telegram_html(provisioning.project_id)}</code>\n\n"
            f"{telegram_html(provisioning.description)}"
        )
        return PreparedDelivery(
            DeliveryAction.SEND_PROJECT_WELCOME,
            chat_id=provisioning.chat_id,
            thread_id=provisioning.topic_thread_id,
            html=html,
            task_id=provisioning.task_id,
            message_role="project_welcome",
        )
    if item.linked_entity_type != "telegram_intake":
        raise PermanentDeliveryError("unsupported_telegram_operation")
    intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
    if intake is None:
        raise PermanentDeliveryError("telegram_intake_missing")
    role = item.payload.get("role")
    if role == "semantic_clarification":
        raw_id = item.payload.get("interpretation_id")
        try:
            interpretation_id = uuid.UUID(str(raw_id))
        except ValueError as error:
            raise PermanentDeliveryError("invalid_interpretation_id") from error
        interpretation = await session.get(Interpretation, interpretation_id)
        if interpretation is None or interpretation.task_id != intake.task_id:
            raise PermanentDeliveryError("interpretation_missing")
        question = interpretation.task_draft.get("clarification_question")
        title = interpretation.task_draft.get("normalized_title")
        if not isinstance(question, str) or not question:
            raise PermanentDeliveryError("clarification_question_missing")
        html = f"<b>{telegram_html(title or 'Clarification required')}</b>\n"
        html += telegram_html(question)
        return PreparedDelivery(
            DeliveryAction.SEND_CLARIFICATION,
            chat_id=intake.chat_id,
            thread_id=intake.message_thread_id,
            html=html,
            task_id=intake.task_id,
        )
    if role == "clarification":
        try:
            candidate_ids = [uuid.UUID(value) for value in intake.ambiguous_task_ids]
        except ValueError as error:
            raise PermanentDeliveryError("invalid_candidate_task_id") from error
        candidates = (await session.scalars(select(Task).where(Task.id.in_(candidate_ids)))).all()
        summaries = [
            f"• <code>{task.id}</code> — {telegram_html(task.original_text.strip()[:80])}"
            for task in candidates
        ]
        html = "Multiple active tasks matched. Reply to the intended task status card:\n"
        html += "\n".join(summaries) if summaries else "No active candidates remain."
        return PreparedDelivery(
            DeliveryAction.SEND_CLARIFICATION,
            chat_id=intake.chat_id,
            thread_id=intake.message_thread_id,
            html=html,
        )
    if role not in {"intake_ack", "approval_card"} or intake.task_id is None:
        raise PermanentDeliveryError("invalid_telegram_payload")
    approval_projection = role == "approval_card"
    card = (
        await build_approval_card(session, intake.task_id)
        if approval_projection
        else await build_status_card(session, intake.task_id)
    )
    message_role = "approval_card" if approval_projection else "task_status"
    chat_id = intake.chat_id
    thread_id = intake.message_thread_id
    if approval_projection:
        destination = topics.system_topic(chat_id, TopicKind.APPROVALS) if topics else None
        if destination is None or not destination.enabled:
            raise PermanentDeliveryError("approval_topic_missing")
        thread_id = destination.message_thread_id
    link = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.task_id == card.task_id,
            TelegramMessageLink.message_role == message_role,
            *(
                (TelegramMessageLink.approval_id == card.approval_id,)
                if approval_projection
                else ()
            ),
        )
    )
    if link is not None and card.revision <= link.projection_revision:
        return PreparedDelivery(
            DeliveryAction.NOOP,
            chat_id=chat_id,
            thread_id=thread_id,
        )
    if link is None:
        return PreparedDelivery(
            DeliveryAction.SEND_STATUS,
            chat_id=chat_id,
            thread_id=thread_id,
            html=card.html,
            task_id=card.task_id,
            revision=card.revision,
            buttons=card.buttons,
            approval_id=card.approval_id,
            message_role=message_role,
        )
    return PreparedDelivery(
        DeliveryAction.EDIT_STATUS,
        chat_id=link.chat_id,
        thread_id=link.message_thread_id,
        html=card.html,
        task_id=card.task_id,
        revision=card.revision,
        link_id=link.id,
        message_id=link.message_id,
        buttons=card.buttons,
        approval_id=card.approval_id,
        message_role=message_role,
    )


class TelegramDeliveryService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        client: TelegramClient,
        *,
        owner: str,
        lease_seconds: int,
        max_attempts: int,
        retry_min_seconds: float,
        retry_max_seconds: float,
        topics: TopicRegistry | None = None,
    ) -> None:
        self._factory = session_factory
        self._client = client
        self._owner = owner
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._retry_min = retry_min_seconds
        self._retry_max = retry_max_seconds
        self._topics = topics
        self._logger = get_logger(__name__)

    async def deliver_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._lease_seconds,
                allowed_destinations=TELEGRAM_DESTINATIONS,
            )
        if token is None:
            return False
        try:
            async with self._factory() as session:
                item = await session.get(TransactionalOutbox, token.item_id)
                assert item is not None
                attempt_count = item.attempt_count
                prepared = await prepare_delivery(session, item, self._topics)
            if prepared.action == DeliveryAction.NOOP:
                await self._complete(token, prepared, None)
                return True
            confirmed_message_id = await self._call_telegram(prepared)
            await self._complete(token, prepared, confirmed_message_id)
            self._logger.info(
                "Telegram outbox item delivered",
                extra={"event": "telegram.delivery.delivered", "outbox_id": str(token.item_id)},
            )
        except LostTelegramResponse:
            await self._mark_ambiguous(token)
        except (TimedOut, RetryAfter, NetworkError) as error:
            await self._handle_transient(token, attempt_count, type(error).__name__.lower())
        except (BadRequest, Forbidden) as error:
            await self._dead_letter(token, f"telegram_{type(error).__name__.lower()}")
        except TelegramError as error:
            await self._dead_letter(token, f"telegram_{type(error).__name__.lower()}")
        except PermanentDeliveryError as error:
            await self._dead_letter(token, error.category)
        except LeaseLost:
            self._logger.warning(
                "Telegram delivery lease was lost",
                extra={"event": "telegram.delivery.lease_lost", "outbox_id": str(token.item_id)},
            )
        return True

    async def _call_telegram(self, prepared: PreparedDelivery) -> int | None:
        if prepared.action in {
            DeliveryAction.SEND_STATUS,
            DeliveryAction.SEND_CLARIFICATION,
            DeliveryAction.SEND_PROJECT_WELCOME,
            DeliveryAction.SEND_PROJECT_NAMES,
        }:
            message_id = await self._client.send_message(
                chat_id=prepared.chat_id,
                thread_id=prepared.thread_id,
                html=prepared.html,
                buttons=prepared.buttons,
                task_id=prepared.task_id,
                approval_id=prepared.approval_id,
                callback_buttons=prepared.callback_buttons,
            )
            if not message_id:
                raise LostTelegramResponse("Telegram returned no confirmed message ID")
            return message_id
        if prepared.action == DeliveryAction.DELETE_MESSAGE:
            assert prepared.message_id is not None
            await self._client.delete_message(
                chat_id=prepared.chat_id,
                message_id=prepared.message_id,
            )
            return None
        assert prepared.message_id is not None
        await self._client.edit_message(
            chat_id=prepared.chat_id,
            message_id=prepared.message_id,
            html=prepared.html,
            buttons=prepared.buttons,
            task_id=prepared.task_id,
            approval_id=prepared.approval_id,
            callback_buttons=prepared.callback_buttons,
        )
        return None

    async def _complete(
        self,
        token: OutboxLeaseToken,
        prepared: PreparedDelivery,
        confirmed_message_id: int | None,
    ) -> None:
        async with self._factory.begin() as session:
            if prepared.action == DeliveryAction.SEND_STATUS:
                assert prepared.task_id is not None and prepared.revision is not None
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        approval_id=prepared.approval_id,
                        message_role=prepared.message_role or "task_status",
                        projection_revision=prepared.revision,
                    )
                )
            elif prepared.action == DeliveryAction.EDIT_STATUS:
                assert prepared.link_id is not None and prepared.revision is not None
                link = await session.get(TelegramMessageLink, prepared.link_id)
                if link is None:
                    raise LeaseLost(f"Telegram projection disappeared: {prepared.link_id}")
                link.projection_revision = prepared.revision
            elif prepared.action == DeliveryAction.SEND_CLARIFICATION:
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        message_role="clarification",
                    )
                )
            elif prepared.action == DeliveryAction.SEND_PROJECT_WELCOME:
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        message_role=prepared.message_role or "project_welcome",
                    )
                )
            elif prepared.action == DeliveryAction.SEND_PROJECT_NAMES:
                assert prepared.task_id is not None and prepared.revision is not None
                assert confirmed_message_id is not None
                session.add(
                    TelegramMessageLink(
                        chat_id=prepared.chat_id,
                        message_thread_id=prepared.thread_id,
                        message_id=confirmed_message_id,
                        task_id=prepared.task_id,
                        message_role="project_naming",
                        projection_revision=prepared.revision,
                    )
                )
            elif prepared.action == DeliveryAction.DELETE_MESSAGE:
                assert prepared.link_id is not None
                link = await session.get(TelegramMessageLink, prepared.link_id)
                if link is not None:
                    await session.delete(link)
            await complete_outbox_item(session, token)

    async def _mark_ambiguous(self, token: OutboxLeaseToken) -> None:
        async with self._factory.begin() as session:
            await mark_outbox_ambiguous(session, token)

    async def _handle_transient(
        self, token: OutboxLeaseToken, attempt_count: int, category: str
    ) -> None:
        if attempt_count >= self._max_attempts:
            await self._dead_letter(token, category)
            return
        delay = min(self._retry_max, self._retry_min * (2 ** (attempt_count - 1)))
        async with self._factory.begin() as session:
            await retry_outbox_item(session, token, delay_seconds=delay, error_category=category)

    async def _dead_letter(self, token: OutboxLeaseToken, category: str) -> None:
        async with self._factory.begin() as session:
            await dead_letter_outbox_item(session, token, error_category=category[:100])


async def run_delivery_loop(
    service: DeliveryRunner,
    *,
    poll_interval_seconds: float,
    stop_event: asyncio.Event,
) -> None:
    logger = get_logger(__name__)
    logger.info("Telegram delivery ready", extra={"event": "telegram.delivery.ready"})
    while not stop_event.is_set():
        delivered = await service.deliver_one()
        if not delivered:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
    logger.info("Telegram delivery stopped", extra={"event": "telegram.delivery.stopped"})
