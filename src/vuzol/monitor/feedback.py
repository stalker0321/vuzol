"""Owner 2026-11-25: 👍 · 💾 · 👎 under each news item.

A like or a dislike teaches the classifier (it sees the owner's latest verdicts); 💾 puts
the news into the owner's knowledge base, where the assistant finds it.
"""

from __future__ import annotations

import re
import uuid
from contextlib import suppress
from urllib.parse import urlparse

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError

from vuzol.config import Settings
from vuzol.monitor.service import news_buttons
from vuzol.storage.models import InboxItem, MonitorEvent

_BUTTON = re.compile(r"^mn:([lsd]):([0-9a-f]{32})$")
_VERDICTS = {"l": "like", "d": "dislike"}


def is_monitor_update(update: Update, settings: Settings) -> bool:
    config, query, user = settings.monitor, update.callback_query, update.effective_user
    return (
        config.enabled
        and query is not None
        and user is not None
        and user.id in settings.allowed_user_ids
        and (query.data or "").startswith("mn:")
        and query.message is not None
        and query.message.chat.id == config.chat_id
    )


async def accept_monitor_update(
    factory: async_sessionmaker[AsyncSession], settings: Settings, update: Update
) -> bool:
    query, user = update.callback_query, update.effective_user
    if query is None or user is None or not is_monitor_update(update, settings):
        return False
    match = _BUTTON.match(query.data or "")
    if match is None:
        return True
    key = match.group(1)
    async with factory.begin() as session:
        event = await session.get(MonitorEvent, uuid.UUID(hex=match.group(2)), with_for_update=True)
        if event is None:
            return True
        if key == "s":
            note = "💾 Уже в базе знаний" if event.saved else "💾 Сохранил в базу знаний"
            if not event.saved:
                event.saved = True
                session.add(_knowledge(event, user.id))
        else:
            verdict = _VERDICTS[key]
            # A second press takes the verdict back.
            event.feedback = None if event.feedback == verdict else verdict
            note = "Учту" if event.feedback else "Отменил"
        buttons = news_buttons(event)
    with suppress(TelegramError):
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton(label, callback_data=data) for label, data in row]
                    for row in buttons
                ]
            )
        )
    with suppress(TelegramError):
        await query.answer(text=note)
    return True


def _knowledge(event: MonitorEvent, owner: int) -> InboxItem:
    parts = urlparse(event.url)
    path = [part for part in parts.path.split("/") if part]
    repo = parts.netloc == "github.com" and len(path) == 2
    summary = "\n\n".join(part for part in (event.summary, event.details) if part)
    return InboxItem(
        owner_user_id=owner,
        # The knowledge base lives in the owner's private chat, whose id is their user id.
        chat_id=owner,
        kind="repo" if repo else "article",
        title=event.title[:300],
        summary=summary[:2000],
        tags=["мониторинг", event.source_id],
        fields={"url": event.url, "source": "monitor"},
    )
