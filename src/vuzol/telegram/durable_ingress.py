"""Raw, commit-before-offset Telegram admission and bounded durable processing."""

import asyncio
import hashlib
import json
import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import Bot, Update

from vuzol.config import Settings
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import ExternalInbox, TransactionalOutbox
from vuzol.storage.repositories.delivery import InboxRepository, OutboxRepository
from vuzol.storage.types import InboxStatus
from vuzol.telegram.adapter import ControlHandlerFn, MessageHandlerFn, dispatch_update
from vuzol.telegram.policy import TelegramPolicyError, authorize

_LOG = logging.getLogger(__name__)
_MAX_UPDATE_BYTES = 1_000_000


class DurableTelegramIngress:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        settings: Settings,
        bot_id: str,
        on_message: MessageHandlerFn,
        on_control: ControlHandlerFn,
        max_attempts: int = 3,
        retry_seconds: float = 1,
        processing_seconds: float = 30,
    ) -> None:
        if not 1 <= len(bot_id) <= 80:
            raise ValueError("invalid bot identity")
        if max_attempts < 1 or retry_seconds < 0 or not 0 < processing_seconds <= 30:
            raise ValueError("invalid ingress retry budget")
        self._factory = factory
        self._settings = settings
        self._bot_id = bot_id
        self.destination = f"telegram_ingress:{bot_id}"
        self._on_message = on_message
        self._on_control = on_control
        self._max_attempts = max_attempts
        self._retry_seconds = retry_seconds
        self._processing_seconds = processing_seconds
        self._owner = f"telegram-ingress:{uuid.uuid4()}"
        self._runner: asyncio.Task[None] | None = None

    async def admit(self, update: Update) -> None:
        # Do not parse domain fields here: malformed input must also be durable.
        encoded = update.to_json().encode()
        digest = hashlib.sha256(encoded).hexdigest()
        authorized = False
        if update.effective_chat is not None and update.effective_user is not None:
            try:
                authorize(
                    self._settings,
                    chat_id=update.effective_chat.id,
                    user_id=update.effective_user.id,
                )
                authorized = True
            except TelegramPolicyError:
                pass
        payload = (
            {"rejected": "update_too_large", "payload_hash": digest}
            if len(encoded) > _MAX_UPDATE_BYTES
            # A JSON string, not a nested JSONB object: PostgreSQL rejects NUL
            # and unpaired surrogates in decoded JSON strings. Keep the escaped
            # wire representation so such input cannot poison raw admission.
            else {"update_json": encoded.decode("ascii")}
        )
        payload["bot_id"] = self._bot_id
        async with self._factory.begin() as session:
            inbox_id, created = await InboxRepository(session).receive_once(
                source="telegram_transport",
                consumer=f"bot:{self._bot_id}",
                external_event_id=str(update.update_id),
                payload_hash=digest,
            )
            if created and not authorized:
                inbox = await session.get(ExternalInbox, inbox_id)
                assert inbox is not None
                inbox.status = InboxStatus.REJECTED
                inbox.outcome = {"reason": "identity_not_authorized"}
                return  # No raw body or replayable command from an untrusted identity.
            if created:
                await OutboxRepository(session).enqueue(
                    destination=self.destination,
                    operation_type="process_update",
                    entity_type="external_inbox",
                    entity_id=inbox_id,
                    idempotency_key=f"update:{update.update_id}",
                    payload=payload,
                )

    async def process_one(self, bot: Bot) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=60,
                allowed_destinations=frozenset({self.destination}),
            )
            if token is None:
                return False
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            payload, inbox_id, attempts = item.payload, item.linked_entity_id, item.attempt_count
        category = None
        try:
            if payload.get("bot_id") != self._bot_id or "rejected" in payload:
                raise ValueError("invalid persisted update")
            update = Update.de_json(json.loads(payload["update_json"]), bot)
            if update is None:
                raise ValueError("empty persisted update")
            await asyncio.wait_for(
                dispatch_update(
                    update,
                    bot_id=self._bot_id,
                    on_message=self._on_message,
                    on_control=self._on_control,
                ),
                timeout=self._processing_seconds,
            )
        except asyncio.CancelledError:
            # Leave the lease recoverable after shutdown/crash.
            raise
        except Exception as error:
            category = f"telegram_ingress_{type(error).__name__}"[:100]
        try:
            async with self._factory.begin() as session:
                if category is None:
                    await complete_outbox_item(session, token)
                    await InboxRepository(session).mark_processed(
                        inbox_id, entity_type=None, entity_id=None
                    )
                    inbox = await session.get(ExternalInbox, inbox_id)
                    if inbox is not None:
                        inbox.outcome = {"status": "processed", "outbox_id": str(token.item_id)}
                elif attempts >= self._max_attempts or "rejected" in payload:
                    await dead_letter_outbox_item(session, token, error_category=category)
                    inbox = await session.get(ExternalInbox, inbox_id)
                    if inbox is not None:
                        inbox.status = InboxStatus.FAILED
                        inbox.outcome = {"reason": category, "outbox_id": str(token.item_id)}
                    _LOG.error(
                        "telegram ingress quarantined: item=%s category=%s", token.item_id, category
                    )
                else:
                    await retry_outbox_item(
                        session,
                        token,
                        delay_seconds=self._retry_seconds * attempts,
                        error_category=category,
                    )
        except LeaseLost:
            # Never acknowledge a successor's delivery.
            pass
        return True

    def start(self, bot: Bot) -> None:
        if self._runner is None:
            self._runner = asyncio.create_task(self._run(bot))

    async def _run(self, bot: Bot) -> None:
        while True:
            try:
                if await self.process_one(bot):
                    continue
            except Exception as error:
                _LOG.warning("telegram ingress worker unavailable: %s", type(error).__name__)
            await asyncio.sleep(0.25)

    async def close(self) -> None:
        if self._runner is not None:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)
            self._runner = None
