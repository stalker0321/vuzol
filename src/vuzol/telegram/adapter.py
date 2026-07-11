"""python-telegram-bot adapter; no Telegram types escape this boundary."""

import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from pydantic import SecretStr
from telegram import Bot, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from vuzol.config import ScopedSecretResolver, Settings
from vuzol.telegram.domain import AttachmentKind, ControlUpdate, MessageUpdate, TelegramAttachment

MessageHandlerFn = Callable[[MessageUpdate], Awaitable[None]]
ControlHandlerFn = Callable[[ControlUpdate], Awaitable[None]]


def resolve_bot_token(
    settings: Settings, *, environment: Mapping[str, str] | None = None
) -> SecretStr:
    reference = settings.telegram_bot_token_reference
    if reference is None:
        raise ValueError("telegram_bot_token_reference is required for the Telegram process")
    resolver = ScopedSecretResolver(
        access_policy={reference: frozenset({"system:telegram"})},
        secret_file_root=settings.secret_file_root,
        environment=environment,
    )
    return resolver.get(reference, "system:telegram")


class PythonTelegramClient:
    def __init__(self, bot: Bot) -> None:
        self._bot = bot

    async def send_message(self, *, chat_id: int, thread_id: int | None, html: str) -> int:
        message = await self._bot.send_message(
            chat_id=chat_id,
            message_thread_id=thread_id,
            text=html,
            parse_mode=ParseMode.HTML,
        )
        return message.message_id

    async def edit_message(self, *, chat_id: int, message_id: int, html: str) -> None:
        await self._bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=html, parse_mode=ParseMode.HTML
        )


def message_update(update: Update, bot_id: str) -> MessageUpdate | None:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if message is None or user is None or chat is None or message.message_thread_id is None:
        return None
    attachments: list[TelegramAttachment] = []
    if message.document is not None:
        document = message.document
        attachments.append(
            TelegramAttachment(
                file_id=document.file_id,
                file_unique_id=document.file_unique_id,
                filename=document.file_name,
                media_type=document.mime_type or "application/octet-stream",
                file_size=document.file_size or 0,
                kind=AttachmentKind.DOCUMENT,
            )
        )
    if message.voice is not None:
        voice = message.voice
        attachments.append(
            TelegramAttachment(
                file_id=voice.file_id,
                file_unique_id=voice.file_unique_id,
                media_type=voice.mime_type or "audio/ogg",
                file_size=voice.file_size or 0,
                kind=AttachmentKind.VOICE,
            )
        )
    return MessageUpdate(
        bot_id=bot_id,
        update_id=update.update_id,
        chat_id=chat.id,
        message_thread_id=message.message_thread_id,
        message_id=message.message_id,
        user_id=user.id,
        text=message.text or message.caption,
        reply_to_message_id=(
            message.reply_to_message.message_id if message.reply_to_message is not None else None
        ),
        attachments=tuple(attachments),
    )


def control_update(update: Update, bot_id: str) -> ControlUpdate | None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None or query.data is None:
        return None
    parts = query.data.split(":", maxsplit=2)
    allowed_actions = {"approve", "reject", "pause", "resume", "cancel", "retry"}
    if len(parts) != 3 or parts[0] != "v1" or parts[1] not in allowed_actions:
        return None
    try:
        target_id = uuid.UUID(parts[2])
    except ValueError:
        return None
    targets = (
        {"approval_id": target_id} if parts[1] in {"approve", "reject"} else {"task_id": target_id}
    )
    return ControlUpdate(
        bot_id=bot_id,
        update_id=update.update_id,
        callback_query_id=query.id,
        chat_id=query.message.chat.id,
        user_id=user.id,
        action_kind=parts[1],
        **targets,
    )


def build_long_polling_application(
    token: str,
    *,
    bot_id: str,
    on_message: MessageHandlerFn,
    on_control: ControlHandlerFn,
) -> Application[Any, Any, Any, Any, Any, Any]:
    application = ApplicationBuilder().token(token).build()

    async def handle_message(update: Update, _context: object) -> None:
        converted = message_update(update, bot_id)
        if converted is not None:
            await on_message(converted)

    async def handle_control(update: Update, _context: object) -> None:
        converted = control_update(update, bot_id)
        if converted is not None:
            await on_control(converted)

    application.add_handler(CallbackQueryHandler(handle_control, pattern=r"^v1:"))
    application.add_handler(MessageHandler(filters.ALL, handle_message))
    return application
