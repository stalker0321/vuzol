"""python-telegram-bot adapter; no Telegram types escape this boundary."""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from io import BytesIO
from typing import Any

from pydantic import SecretStr
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, EndPointNotFound, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from vuzol.config import ScopedSecretResolver, Settings
from vuzol.telegram.domain import (
    AttachmentKind,
    ControlUpdate,
    IngressResult,
    IngressStatus,
    MessageUpdate,
    TelegramAttachment,
    WorkPackageControlUpdate,
)
from vuzol.telegram.projections import LostTelegramResponse, RichMessageRejected
from vuzol.telegram.workspace import (
    TopicCreationOutcomeUnknown,
    TopicPinUnsupported,
    TopicSynchronizationError,
)

MessageHandlerFn = Callable[[MessageUpdate], Awaitable[None]]
ControlHandlerFn = Callable[
    [ControlUpdate | WorkPackageControlUpdate], Awaitable[IngressResult | None]
]
StartupHandlerFn = Callable[[Bot], Awaitable[None]]
_LOG = logging.getLogger(__name__)


class CommittingUpdateQueue(asyncio.Queue[object]):
    """PTB advances getUpdates offset only after put returns.

    Business updates go straight to the durable ingress, not to Application's
    volatile consumer. Non-update sentinels still use its normal lifecycle queue.
    A failed transaction must keep polling at the old offset.
    """

    def __init__(self, dispatch: Callable[[Update], Awaitable[None]]) -> None:
        super().__init__()
        self._dispatch = dispatch

    async def put(self, item: object) -> None:
        if not isinstance(item, Update):
            await super().put(item)
            return
        delay = 0.1
        while True:
            try:
                await self._dispatch(item)
                return
            except Exception as error:
                # Never log the update (possibly a secret) or exception payload.
                _LOG.warning("telegram ingress not committed: %s", type(error).__name__)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)


def _callback_answer(result: IngressResult | None) -> tuple[str | None, bool]:
    chat_messages = {
        "task_retry_stale": "Состояние задачи изменилось. Отправляю актуальную карточку.",
        "task_retry_scope_mismatch": "Эта кнопка не относится к твоей задаче в этом топике.",
        "task_retry_version_required": (
            "Старая кнопка. Напиши «продолжи», чтобы получить актуальную карточку."
        ),
        "task_retry_message_mismatch": (
            "Кнопка устарела. Напиши «продолжи» для актуальной карточки."
        ),
        "chat_retry_stale": "Это старый запрос. Используй кнопку под последней ошибкой.",
        "chat_retry_ambiguous": (
            "Результат доставки неизвестен. Повтор заблокирован, чтобы не отправить дубль."
        ),
        "chat_retry_unavailable": "Повтор этого сообщения недоступен.",
        "chat_retry_pending": "Попытка уже выполняется.",
        "chat_already_delivered": "Ответ уже доставлен.",
        "chat_delivery_retry": "Повторяю доставку готового ответа.",
        "chat_reply_retry": "Пробую ответить ещё раз.",
        "document_context_too_large": (
            "Не удалось вместить полный запрос с документами. "  # noqa: RUF001
            "Пришли нужные разделы меньшим файлом."
        ),
        "chat_context_too_large": (
            "Полный запрос не помещается: 40k для переписки, отдельно 50k для документов. "
            "История сохранена, ничего не обрезалось. Уточни более узкий вопрос."
        ),
        "chat_context_source_mismatch": (
            "Не удалось подтвердить полный контекст документов. История сохранена; "  # noqa: RUF001
            "ничего не запущено."
        ),
    }
    if result is not None and result.reason in chat_messages:
        return chat_messages[result.reason], result.status is IngressStatus.REJECTED
    if result is not None and result.status is IngressStatus.REJECTED:
        if result.reason == "item_not_safely_retryable":
            return "Повтор недоступен для текущего состояния задачи.", True
        return "Действие уже недоступно. Обновите карточку.", True
    if result is not None and result.reason == "continue_discussion":
        return "Напишите следующим сообщением, что хотите обсудить.", False
    if result is not None and result.reason == "request_replan":
        return "Напишите следующим сообщением, что изменить в оставшейся части плана.", False
    if result is not None and result.reason == "secret_cancelled":
        return "Ссылка отменена.", False
    return None, False


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

    async def send_document(
        self, *, chat_id: int, thread_id: int | None, content: bytes, filename: str, caption: str
    ) -> int:
        message = await self._bot.send_document(
            chat_id=chat_id,
            message_thread_id=thread_id,
            document=BytesIO(content),
            filename=filename,
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
        if type(message.message_id) is not int or message.message_id <= 0:
            raise LostTelegramResponse("document send returned no confirmed message ID")
        return message.message_id

    async def send_rich_message(self, *, chat_id: int, thread_id: int | None, html: str) -> int:
        try:
            result = await self._bot.do_api_request(
                "sendRichMessage",
                api_kwargs={
                    "chat_id": chat_id,
                    "message_thread_id": thread_id,
                    "rich_message": {"html": html},
                },
            )
        except (BadRequest, EndPointNotFound):
            raise RichMessageRejected("rich message rejected before creation") from None
        message_id = result.get("message_id") if isinstance(result, dict) else None
        if type(message_id) is not int or message_id <= 0:
            raise LostTelegramResponse("rich send returned no confirmed message ID")
        return message_id

    async def edit_rich_message(self, *, chat_id: int, message_id: int, html: str) -> None:
        try:
            result = await self._bot.do_api_request(
                "editMessageText",
                api_kwargs={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "rich_message": {"html": html},
                },
            )
        except BadRequest as error:
            if "message is not modified" in str(error).lower().replace("_", " "):
                return
            raise RichMessageRejected("rich edit rejected before application") from None
        except EndPointNotFound:
            raise RichMessageRejected("rich edit endpoint unavailable") from None
        if result is not True and (
            not isinstance(result, dict) or result.get("message_id") != message_id
        ):
            raise LostTelegramResponse("rich edit returned no confirmation")

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
        try:
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
        except BadRequest as error:
            # Delivery can lose the database acknowledgement after Telegram has already
            # applied an edit. The fenced retry then receives this response even though
            # the desired projection is visible, so treat it as an idempotent success.
            if "message is not modified" not in str(error).lower().replace("_", " "):
                raise

    async def delete_message(self, *, chat_id: int, message_id: int) -> None:
        try:
            await self._bot.delete_message(chat_id=chat_id, message_id=message_id)
        except BadRequest:
            # Naming cards are ephemeral. Missing/expired cards must not block regeneration.
            return

    async def pin_message(self, *, chat_id: int, message_id: int) -> bool:
        """Best-effort pin for the reconstructable package status message."""

        try:
            await self._bot.pin_chat_message(
                chat_id=chat_id,
                message_id=message_id,
                disable_notification=True,
            )
        except TelegramError:
            return False
        return True

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

    async def set_topic_pinned(self, *, chat_id: int, thread_id: int, pinned: bool) -> None:
        """Apply desired forum-topic pin state.

        Telegram exposes topic pin/reorder only on the MTProto client API today
        (``messages.updatePinnedForumTopic`` / ``messages.reorderPinnedForumTopics``).
        The Bot API used by this adapter has no equivalent method, so pin intent is
        product policy until Telegram adds bot support. Callers treat
        :class:`TopicPinUnsupported` as non-fatal layout documentation.
        """

        del chat_id, thread_id, pinned
        raise TopicPinUnsupported("forum_topic_pin_unsupported_by_bot_api")


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
        media_group_id=message.media_group_id,
    )


def control_update(update: Update, bot_id: str) -> ControlUpdate | WorkPackageControlUpdate | None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or query.message is None or query.data is None:
        return None
    parts = query.data.split(":")
    message_thread_id = getattr(query.message, "message_thread_id", None)
    message_id = getattr(query.message, "message_id", None)
    if len(parts) == 5 and parts[:2] == ["v2", "task"]:
        if parts[2] not in {"start", "pause", "resume", "cancel"}:
            return None
        try:
            control_task_id, version = uuid.UUID(parts[3]), int(parts[4])
            if version < 1:
                return None
        except ValueError:
            return None
        return ControlUpdate(
            bot_id=bot_id,
            update_id=update.update_id,
            callback_query_id=query.id,
            chat_id=query.message.chat.id,
            user_id=user.id,
            message_thread_id=message_thread_id,
            message_id=message_id,
            action_kind=parts[2],
            task_id=control_task_id,
            expected_task_version=version,
        )
    if len(parts) == 6 and parts[0] == "v2" and parts[1] == "c":
        try:
            criterion_task_id = uuid.UUID(parts[2])
            version, ordinal = int(parts[3]), int(parts[4])
            token = parts[5]
            if (
                version < 1
                or ordinal < 0
                or len(token) != 8
                or any(char not in "0123456789abcdef" for char in token)
            ):
                return None
        except ValueError:
            return None
        return ControlUpdate(
            bot_id=bot_id,
            update_id=update.update_id,
            callback_query_id=query.id,
            chat_id=query.message.chat.id,
            user_id=user.id,
            message_thread_id=message_thread_id,
            message_id=message_id,
            action_kind="criterion",
            task_id=criterion_task_id,
            expected_task_version=version,
            retry_step_ordinal=ordinal,
            criterion_token=token,
        )
    if len(parts) == 5 and parts[0] == "v2" and parts[1] == "cp":
        try:
            page_task_id = uuid.UUID(parts[2])
            version, page = int(parts[3]), int(parts[4])
            if version < 1 or page < 1:
                return None
        except ValueError:
            return None
        return ControlUpdate(
            bot_id=bot_id,
            update_id=update.update_id,
            callback_query_id=query.id,
            chat_id=query.message.chat.id,
            user_id=user.id,
            message_thread_id=message_thread_id,
            message_id=message_id,
            action_kind="cpage",
            task_id=page_task_id,
            expected_task_version=version,
            criterion_page=page,
        )
    if (
        len(parts) == 5
        and parts[0] == "v2"
        and parts[1]
        in {
            "retry",
            "admit",
            "uncertain",
            "criterion",
        }
    ):
        try:
            retry_task_id = uuid.UUID(parts[2])
            version, ordinal = int(parts[3]), int(parts[4])
            if version < 1 or ordinal < 0:
                return None
        except ValueError:
            return None
        return ControlUpdate(
            bot_id=bot_id,
            update_id=update.update_id,
            callback_query_id=query.id,
            chat_id=query.message.chat.id,
            user_id=user.id,
            message_thread_id=message_thread_id,
            message_id=message_id,
            action_kind=parts[1],
            task_id=retry_task_id,
            expected_task_version=version,
            retry_step_ordinal=ordinal,
        )
    if query.data.startswith("v1:wp:"):
        return WorkPackageControlUpdate(
            bot_id=bot_id,
            update_id=update.update_id,
            callback_query_id=query.id,
            callback_data=query.data,
            chat_id=query.message.chat.id,
            message_id=query.message.message_id,
            user_id=user.id,
            message_thread_id=message_thread_id,
        )
    if len(parts) == 3 and parts[:2] == ["v1", "secret_cancel"]:
        try:
            request_id = uuid.UUID(parts[2])
        except ValueError:
            return None
        return ControlUpdate(
            bot_id=bot_id,
            update_id=update.update_id,
            callback_query_id=query.id,
            chat_id=query.message.chat.id,
            user_id=user.id,
            message_thread_id=message_thread_id,
            message_id=query.message.message_id,
            action_kind="secret_cancel",
            secret_request_id=request_id,
        )
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
            message_thread_id=message_thread_id,
            action_kind=(
                "project_name_regenerate" if option_index is None else "project_name_select"
            ),
            naming_request_id=naming_request_id,
            naming_revision=revision,
            naming_option_index=option_index,
        )
    if len(parts) >= 4 and parts[:2] == ["v1", "pm"]:
        try:
            kind = parts[2]
            revision = int(parts[3])
        except (ValueError, TypeError, IndexError):
            return None
        if kind == "a" and len(parts) == 4:
            return ControlUpdate(
                bot_id=bot_id,
                update_id=update.update_id,
                callback_query_id=query.id,
                chat_id=query.message.chat.id,
                user_id=user.id,
                message_thread_id=message_thread_id,
                message_id=message_id,
                action_kind="project_model_select_auto",
                preference_revision=revision,
            )
        if kind == "w" and len(parts) == 5:
            return ControlUpdate(
                bot_id=bot_id,
                update_id=update.update_id,
                callback_query_id=query.id,
                chat_id=query.message.chat.id,
                user_id=user.id,
                message_thread_id=message_thread_id,
                message_id=message_id,
                action_kind="project_model_select_worker",
                preference_revision=revision,
                preference_worker=parts[4],
            )
        if kind == "e" and len(parts) == 6:
            return ControlUpdate(
                bot_id=bot_id,
                update_id=update.update_id,
                callback_query_id=query.id,
                chat_id=query.message.chat.id,
                user_id=user.id,
                message_thread_id=message_thread_id,
                message_id=message_id,
                action_kind="project_model_select_effort",
                preference_revision=revision,
                preference_worker=parts[4],
                preference_effort=parts[5],
            )
        return None
    if len(parts) >= 5 and parts[:2] == ["v2", "pm"]:
        try:
            kind = parts[2]
            revision = int(parts[3])
        except (ValueError, TypeError, IndexError):
            return None
        if kind == "c" and len(parts) == 5:
            return ControlUpdate(
                bot_id=bot_id,
                update_id=update.update_id,
                callback_query_id=query.id,
                chat_id=query.message.chat.id,
                user_id=user.id,
                message_thread_id=message_thread_id,
                message_id=message_id,
                action_kind="project_model_select_connection",
                preference_revision=revision,
                preference_profile_id=parts[4],
            )
        if kind == "m" and len(parts) == 6:
            return ControlUpdate(
                bot_id=bot_id,
                update_id=update.update_id,
                callback_query_id=query.id,
                chat_id=query.message.chat.id,
                user_id=user.id,
                message_thread_id=message_thread_id,
                message_id=message_id,
                action_kind="project_model_select_worker",
                preference_revision=revision,
                preference_profile_id=parts[4],
                preference_worker=parts[5],
            )
        if kind == "e" and len(parts) == 7:
            return ControlUpdate(
                bot_id=bot_id,
                update_id=update.update_id,
                callback_query_id=query.id,
                chat_id=query.message.chat.id,
                user_id=user.id,
                message_thread_id=message_thread_id,
                message_id=message_id,
                action_kind="project_model_select_effort",
                preference_revision=revision,
                preference_profile_id=parts[4],
                preference_worker=parts[5],
                preference_effort=parts[6],
            )
        return None
    allowed_actions = {
        "approve",
        "redo",
        "reject",
        "start",
        "pause",
        "resume",
        "cancel",
        "retry",
        "chat_retry",
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
        message_thread_id=message_thread_id,
        action_kind=parts[1],
        message_id=query.message.message_id if parts[1] == "chat_retry" else None,
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
    if not actions and not callback_buttons:
        return None
    # Fresh versioned task callbacks replace their legacy UUID-only counterpart.
    overridden = {
        data.split(":")[2]
        for row in callback_buttons
        for _, data in row
        if data.startswith("v2:task:") and len(data.split(":")) == 5
    }
    actions = tuple(action for action in actions if action not in overridden)
    labels = {
        "start": "Старт",
        "pause": "Пауза",
        "resume": "Продолжить",
        "cancel": "Отмена",
        "approve": "Принять",
        "redo": "Переделать",
        "reject": "Отклонить",
        "retry": "Повторить",
    }
    approval_actions = {"approve", "redo", "reject"}
    targets = {action: approval_id if action in approval_actions else task_id for action in actions}
    if any(target is None for target in targets.values()):
        return None
    rows = [
        [InlineKeyboardButton(labels[action], callback_data=f"v1:{action}:{targets[action]}")]
        for action in actions
    ]
    rows.extend(
        [InlineKeyboardButton(label, callback_data=data) for label, data in row]
        for row in callback_buttons
    )
    return InlineKeyboardMarkup(rows)


def build_long_polling_application(
    token: str,
    *,
    bot_id: str,
    on_message: MessageHandlerFn,
    on_control: ControlHandlerFn,
    on_startup: StartupHandlerFn | None = None,
) -> Application[Any, Any, Any, Any, Any, Any]:
    async def handle_message(update: Update, _context: object) -> None:
        converted = message_update(update, bot_id)
        if converted is not None:
            await on_message(converted)

    async def handle_control(update: Update, _context: object) -> None:
        converted = control_update(update, bot_id)
        if converted is not None:
            result = await on_control(converted)
            query = update.callback_query
            if query is not None:
                text, show_alert = _callback_answer(result)
                try:
                    await query.answer(text=text, show_alert=show_alert)
                except TelegramError:
                    # The control is already committed. Toast delivery is not
                    # part of admission and must not stall subsequent updates.
                    _LOG.warning("telegram callback acknowledgement unavailable")

    async def dispatch(update: Update) -> None:
        query = update.callback_query
        if (
            query is not None
            and isinstance(query.data, str)
            and query.data.startswith(("v1:", "v2:"))
        ):
            await handle_control(update, None)
        else:
            await handle_message(update, None)

    builder = ApplicationBuilder().token(token).update_queue(CommittingUpdateQueue(dispatch))
    if on_startup is not None:

        async def post_init(application: Application[Any, Any, Any, Any, Any, Any]) -> None:
            await on_startup(application.bot)

        builder = builder.post_init(post_init)
    application = builder.build()

    application.add_handler(CallbackQueryHandler(handle_control, pattern=r"^v[12]:"))
    application.add_handler(MessageHandler(filters.ALL, handle_message))
    return application
