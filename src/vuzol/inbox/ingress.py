"""Private messages to the bot: store each one, then settle a burst into one batch.

Telegram sends a forward's comment and the post itself as two messages, and an
album as several. Each message schedules a settle a few seconds later; only the
newest one finds the burst complete.
"""

from __future__ import annotations

import re
import uuid
from contextlib import suppress
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import (
    Message,
    MessageEntity,
    MessageOriginChannel,
    MessageOriginChat,
    MessageOriginHiddenUser,
    MessageOriginUser,
    Update,
)
from telegram.error import TelegramError

from vuzol.config import Settings
from vuzol.inbox import INBOX_DESTINATION
from vuzol.storage.models import InboxMessage, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus

_MAX_URLS = 5


def is_inbox_update(update: Update, settings: Settings) -> bool:
    chat, user = update.effective_chat, update.effective_user
    return (
        settings.inbox.enabled
        and chat is not None
        and user is not None
        and chat.type == "private"
        and chat.id == user.id
        and user.id in settings.allowed_user_ids
    )


def inbox_row(message: Message, user_id: int) -> dict[str, Any] | None:
    attachments = _attachments(message)
    text = message.text or message.caption
    if not text and not attachments:
        return None
    label, link = _forward(message)
    return {
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "user_id": user_id,
        "body": text,
        "forward_label": label,
        "forward_url": link,
        "urls": _urls(message),
        "attachments": attachments,
        "media_group_id": message.media_group_id,
        "reply_to_message_id": (
            message.reply_to_message.message_id if message.reply_to_message is not None else None
        ),
    }


async def accept_inbox_update(
    factory: async_sessionmaker[AsyncSession], settings: Settings, update: Update
) -> bool:
    message, user = update.effective_message, update.effective_user
    if message is None or user is None or not is_inbox_update(update, settings):
        return False
    if update.callback_query is not None:
        return await _accept_button(factory, update)
    row = inbox_row(message, user.id)
    async with factory.begin() as session:
        if row is None:
            if message.text is None:
                await _say(session, message.chat_id, "Такое пока не умею сохранять.")
            return True
        if (message.text or "").strip() in {"/start", "/help"}:
            await _say(session, message.chat_id, _HELLO)
            return True
        await session.execute(
            insert(InboxMessage)
            .values(id=uuid.uuid4(), **row)
            .on_conflict_do_nothing(constraint="uq_inbox_message")
        )
        await schedule(
            session,
            operation="settle",
            key=f"inbox:settle:{message.chat_id}:{message.message_id}",
            payload={"chat_id": message.chat_id},
            delay=settings.inbox.settle_seconds,
        )
    return True


_BUTTON = re.compile(r"^ib:([ad]):([0-9a-f]{32})$")
_BUTTON_NOTES = {"a": "🔍 Разбираю…", "d": "🗑 Удаляю"}


async def _accept_button(factory: async_sessionmaker[AsyncSession], update: Update) -> bool:
    query = update.callback_query
    assert query is not None and query.message is not None
    match = _BUTTON.match(query.data or "")
    if match is None:
        return True
    async with factory.begin() as session:
        await schedule(
            session,
            operation="button",
            key=f"inbox:button:{query.id}",
            payload={
                "chat_id": query.message.chat.id,
                "message_id": query.message.message_id,
                "callback_id": query.id,
                "action": match.group(1),
                "item_id": str(uuid.UUID(hex=match.group(2))),
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
            destination=INBOX_DESTINATION,
            operation_type=operation,
            linked_entity_type="inbox",
            linked_entity_id=uuid.uuid4(),
            idempotency_key=key,
            payload=payload,
            status=DeliveryStatus.PENDING,
            available_at=func.now() + timedelta(seconds=delay),
        )
    )
    await session.flush()


async def _say(session: AsyncSession, chat_id: int, text: str) -> None:
    await schedule(
        session,
        operation="reply",
        key=f"inbox:say:{uuid.uuid4()}",
        payload={"chat_id": chat_id, "text": text, "item_ids": []},
    )


_HELLO = (
    "Пересылай сюда посты, ссылки, скрины, афиши, названия книг и фильмов: я разберу и "
    "сохраню. Вопрос можно написать до или после пересылки, или ответом на мою карточку. "
    "Спросить по базе: «что у нас на ноябрь?», «что я сохранял про бэкапы?»."
)


def _attachments(message: Message) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if message.photo:
        photo = message.photo[-1]
        found.append(
            {
                "kind": "photo",
                "file_id": photo.file_id,
                "size": photo.file_size or 0,
                "mime": "image/jpeg",
            }
        )
    for kind, media in (
        ("voice", message.voice),
        ("audio", message.audio),
        ("document", message.document),
        ("video", message.video),
    ):
        if media is None:
            continue
        found.append(
            {
                "kind": kind,
                "file_id": media.file_id,
                "size": media.file_size or 0,
                "mime": getattr(media, "mime_type", None) or "application/octet-stream",
                "filename": getattr(media, "file_name", None),
            }
        )
    return found


def _urls(message: Message) -> list[str]:
    found: list[str] = []
    for entities in (
        message.parse_entities([MessageEntity.URL, MessageEntity.TEXT_LINK]),
        message.parse_caption_entities([MessageEntity.URL, MessageEntity.TEXT_LINK]),
    ):
        for entity, text in entities.items():
            url = entity.url if entity.type == MessageEntity.TEXT_LINK else text
            if url and not url.startswith(("http://", "https://")):
                url = "https://" + url
            if url and url not in found:
                found.append(url)
    return found[:_MAX_URLS]


def _forward(message: Message) -> tuple[str | None, str | None]:
    origin = message.forward_origin
    if isinstance(origin, MessageOriginChannel):
        username = origin.chat.username
        link = f"https://t.me/{username}/{origin.message_id}" if username else None
        return (origin.chat.title or username or "канал")[:300], link
    if isinstance(origin, MessageOriginChat):
        return (origin.sender_chat.title or "чат")[:300], None
    if isinstance(origin, MessageOriginUser):
        return origin.sender_user.full_name[:300], None
    if isinstance(origin, MessageOriginHiddenUser):
        return origin.sender_user_name[:300], None
    return None, None
