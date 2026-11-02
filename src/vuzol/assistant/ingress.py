"""Messages and buttons in the assistant topic become durable assistant work."""

from __future__ import annotations

import re
import uuid
from contextlib import suppress
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import Update
from telegram.error import TelegramError

from vuzol.assistant import ASSISTANT_DESTINATION
from vuzol.config import Settings
from vuzol.storage.models import AssistantEntry, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus

# A few seconds for a second message to join the first before the assistant answers.
SETTLE_SECONDS = 2.0
_BUTTON = re.compile(r"^as:([se]):([0-9a-f]{32})$")
_BUTTON_NOTES = {"s": "🚀 Отправляю…", "e": "✏️ Жду правку"}


def is_assistant_update(update: Update, settings: Settings) -> bool:
    config = settings.assistant
    message, user = update.effective_message, update.effective_user
    return (
        config.enabled
        and message is not None
        and user is not None
        and message.chat_id == config.chat_id
        and message.message_thread_id == config.message_thread_id
        and user.id in settings.allowed_user_ids
    )


async def accept_assistant_update(
    factory: async_sessionmaker[AsyncSession], settings: Settings, update: Update
) -> bool:
    message, user = update.effective_message, update.effective_user
    if message is None or user is None or not is_assistant_update(update, settings):
        return False
    if update.callback_query is not None:
        return await _accept_button(factory, update, user.id)
    text = message.text or message.caption or ""
    voice = message.voice or message.audio
    if not text.strip() and voice is None:
        return True  # Service messages, stickers: nothing to answer.
    entry_id = uuid.uuid4()
    async with factory.begin() as session:
        result = await session.execute(
            insert(AssistantEntry)
            .values(
                id=entry_id,
                chat_id=message.chat_id,
                message_thread_id=message.message_thread_id,
                role="owner",
                body=text,
                telegram_message_id=message.message_id,
                reply_to_message_id=(
                    message.reply_to_message.message_id
                    if message.reply_to_message is not None
                    and message.reply_to_message.message_id != message.message_thread_id
                    else None
                ),
                dispatches=[],
            )
            .on_conflict_do_nothing(constraint="uq_assistant_entry_message")
        )
        if not result.rowcount:  # type: ignore[attr-defined]
            return True  # A redelivered update.
        await schedule(
            session,
            operation="turn",
            key=f"assistant:turn:{message.chat_id}:{message.message_id}",
            payload={
                "entry_id": str(entry_id),
                "user_id": user.id,
                "voice": (
                    {"file_id": voice.file_id, "mime": voice.mime_type or "audio/ogg"}
                    if voice is not None
                    else None
                ),
            },
            delay=SETTLE_SECONDS,
        )
    return True


async def _accept_button(
    factory: async_sessionmaker[AsyncSession], update: Update, user_id: int
) -> bool:
    query = update.callback_query
    assert query is not None and query.message is not None
    match = _BUTTON.match(query.data or "")
    if match is None:
        return True
    async with factory.begin() as session:
        await schedule(
            session,
            operation="button",
            key=f"assistant:button:{query.id}",
            payload={
                "action": match.group(1),
                "entry_id": str(uuid.UUID(hex=match.group(2))),
                "user_id": user_id,
            },
        )
    with suppress(TelegramError):
        await query.answer(text=_BUTTON_NOTES[match.group(1)])
    return True


async def schedule(
    session: AsyncSession,
    *,
    operation: str,
    key: str,
    payload: dict[str, Any],
    delay: float = 0,
) -> None:
    if await session.scalar(
        select(TransactionalOutbox.id).where(TransactionalOutbox.idempotency_key == key)
    ):
        return
    session.add(
        TransactionalOutbox(
            destination=ASSISTANT_DESTINATION,
            operation_type=operation,
            linked_entity_type="assistant",
            linked_entity_id=uuid.uuid4(),
            idempotency_key=key,
            payload=payload,
            status=DeliveryStatus.PENDING,
            available_at=func.now() + timedelta(seconds=delay),
        )
    )
    await session.flush()
