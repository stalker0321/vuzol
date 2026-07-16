"""python-telegram-bot adapter; no Telegram types escape this boundary."""

import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from pydantic import SecretStr
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from vuzol.config import ScopedSecretResolver, Settings
from vuzol.telegram.domain import AttachmentKind, ControlUpdate, MessageUpdate, TelegramAttachment
from vuzol.telegram.workspace import TopicCreationOutcomeUnknown, TopicSynchronizationError

MessageHandlerFn = Callable[[MessageUpdate], Awaitable[None]]
ControlHandlerFn = Callable[[ControlUpdate], Awaitable[None]]
StartupHandlerFn = Callable[[Bot], Awaitable[None]]


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

    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> int:
        message = await self._bot.send_message(
            chat_id=chat_id,
            message_thread_id=thread_id,
            text=html,
            parse_mode=ParseMode.HTML,
            reply_markup=_control_markup(
                buttons,
                task_id=task_id,
                approval_id=approval_id,
                callback_buttons=callback_buttons,
            ),
        )
        return message.message_id

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> None:
        await self._bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=html,
            parse_mode=ParseMode.HTML,
            reply_markup=_control_markup(
                buttons,
                task_id=task_id,
                approval_id=approval_id,
                callback_buttons=callback_buttons,
            ),
        )

    async def delete_message(self, *, chat_id: int, message_id: int) -> None:
        try:
            await self._bot.delete_message(chat_id=chat_id, message_id=message_id)
        except BadRequest:
            # Naming cards are ephemeral. Missing/expired cards must not block regeneration.
            return

    async def download(self, file_id: str) -> bytes:
        telegram_file = await self._bot.get_file(file_id)
        content = await telegram_file.download_as_bytearray()
        return bytes(content)

    async def rename_topic(self, *, chat_id: int, thread_id: int, name: str) -> None:
        try:
            await self._bot.edit_forum_topic(
                chat_id=chat_id,
                message_thread_id=thread_id,
                name=name,
            )
        except BadRequest as error:
            if "topic not modified" not in str(error).lower().replace("_", " "):
                raise TopicSynchronizationError(type(error).__name__) from error
        except TelegramError as error:
            raise TopicSynchronizationError(type(error).__name__) from error

    async def create_topic(self, *, chat_id: int, name: str) -> int:
        try:
            topic = await self._bot.create_forum_topic(chat_id=chat_id, name=name)
        except TelegramError as error:
            raise TopicCreationOutcomeUnknown(type(error).__name__) from error
        return topic.message_thread_id


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
    parts = query.data.split(":")
    if len(parts) == 5 and parts[:2] == ["v1", "pn"]:
        try:
            naming_request_id = uuid.UUID(hex=parts[2])
            revision = int(parts[3])
            option_index = None if parts[4] == "r" else int(parts[4])
        except (ValueError, TypeError):
            return None
        return ControlUpdate(
            bot_id=bot_id,
            update_id=update.update_id,
            callback_query_id=query.id,
            chat_id=query.message.chat.id,
            user_id=user.id,
            action_kind=(
                "project_name_regenerate" if option_index is None else "project_name_select"
            ),
            naming_request_id=naming_request_id,
            naming_revision=revision,
            naming_option_index=option_index,
        )
    allowed_actions = {
        "approve",
        "redo",
        "reject",
        "start",
        "pause",
        "resume",
        "cancel",
        "retry",
    }
    if len(parts) != 3 or parts[0] != "v1" or parts[1] not in allowed_actions:
        return None
    try:
        target_id = uuid.UUID(parts[2])
    except ValueError:
        return None
    approval_id = target_id if parts[1] in {"approve", "redo", "reject"} else None
    task_id = None if approval_id is not None else target_id
    return ControlUpdate(
        bot_id=bot_id,
        update_id=update.update_id,
        callback_query_id=query.id,
        chat_id=query.message.chat.id,
        user_id=user.id,
        action_kind=parts[1],
        task_id=task_id,
        approval_id=approval_id,
    )


def _control_markup(
    actions: tuple[str, ...],
    *,
    task_id: uuid.UUID | None,
    approval_id: uuid.UUID | None,
    callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
) -> InlineKeyboardMarkup | None:
    if callback_buttons:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(label, callback_data=data) for label, data in row]
                for row in callback_buttons
            ]
        )
    if not actions:
        return None
    labels = {
        "start": "Start",
        "pause": "Pause",
        "resume": "Resume",
        "cancel": "Cancel",
        "approve": "Approve",
        "redo": "Redo",
        "reject": "Reject",
    }
    approval_actions = {"approve", "redo", "reject"}
    targets = {action: approval_id if action in approval_actions else task_id for action in actions}
    if any(target is None for target in targets.values()):
        return None
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(labels[action], callback_data=f"v1:{action}:{targets[action]}")]
            for action in actions
        ]
    )


def build_long_polling_application(
    token: str,
    *,
    bot_id: str,
    on_message: MessageHandlerFn,
    on_control: ControlHandlerFn,
    on_startup: StartupHandlerFn | None = None,
) -> Application[Any, Any, Any, Any, Any, Any]:
    builder = ApplicationBuilder().token(token)

    if on_startup is not None:

        async def post_init(application: Application[Any, Any, Any, Any, Any, Any]) -> None:
            await on_startup(application.bot)

        builder = builder.post_init(post_init)

    application = builder.build()

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
