import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from telegram import Update
from telegram.error import BadRequest, TimedOut

from vuzol.config import Settings
from vuzol.telegram.adapter import (
    PythonTelegramClient,
    _callback_answer,
    _control_markup,
    build_long_polling_application,
    control_update,
    message_update,
    resolve_bot_token,
)
from vuzol.telegram.domain import (
    ControlUpdate,
    IngressResult,
    IngressStatus,
    MessageUpdate,
    WorkPackageControlUpdate,
)
from vuzol.telegram.workspace import (
    TopicCreationOutcomeUnknown,
    TopicPinUnsupported,
    TopicSynchronizationError,
)


def test_bot_token_is_resolved_only_from_telegram_scope(tmp_path: Path) -> None:
    settings = Settings(
        environment="test",
        secret_file_root=tmp_path,
        telegram_bot_token_reference="env:BOT_TOKEN",  # noqa: S106
    )
    token = resolve_bot_token(settings, environment={"BOT_TOKEN": "123:test"})
    assert token.get_secret_value() == "123:test"
    with pytest.raises(ValueError):
        resolve_bot_token(Settings(environment="test"), environment={})


def test_long_polling_application_registers_boundary_handlers() -> None:
    async def message_handler(_update: MessageUpdate) -> None:
        return None

    async def control_handler(_update: ControlUpdate | WorkPackageControlUpdate) -> None:
        return None

    application = build_long_polling_application(
        "123:test",  # pragma: allowlist secret
        bot_id="main",
        admit_update=AsyncMock(),
        on_message=message_handler,
        on_control=control_handler,
    )
    assert len(application.handlers[0]) == 2
    callback_handler = application.handlers[0][0]
    assert cast(Any, callback_handler).pattern.pattern == r"^v[12]:"


def test_callback_answer_explains_discussion_mode_and_rejections() -> None:
    assert _callback_answer(
        IngressResult(status=IngressStatus.HANDLED, reason="continue_discussion")
    ) == ("Напишите следующим сообщением, что хотите обсудить.", False)
    assert _callback_answer(IngressResult(status=IngressStatus.REJECTED)) == (
        "Эта кнопка устарела: состояние уже изменилось.",
        True,
    )
    assert _callback_answer(
        IngressResult(status=IngressStatus.REJECTED, reason="stale_card_reposted")
    ) == ("Эта карточка устарела. Актуальная карточка — внизу чата.", True)
    assert _callback_answer(
        IngressResult(
            status=IngressStatus.REJECTED,
            reason="item_not_safely_retryable",
        )
    ) == ("Повтор недоступен для текущего состояния задачи.", True)
    assert _callback_answer(
        IngressResult(status=IngressStatus.HANDLED, reason="secret_cancelled")
    ) == ("Ссылка отменена.", False)
    assert _callback_answer(
        IngressResult(status=IngressStatus.HANDLED, reason="request_replan")
    ) == ("Запрос на пересмотр плана передан лиду.", False)


@pytest.mark.parametrize("reason", ["chat_context_too_large", "chat_context_source_mismatch"])
def test_context_retry_failure_explains_problem_without_task_card(reason):
    text, alert = _callback_answer(IngressResult(status=IngressStatus.REJECTED, reason=reason))
    assert alert and "История сохранена" in text
    assert "карточк" not in text and len(text) <= 200


def test_python_telegram_client_delegates_send_and_edit() -> None:
    async def scenario() -> None:
        bot = AsyncMock()
        bot.send_message.return_value = SimpleNamespace(message_id=17)
        client = PythonTelegramClient(bot)
        task_id = uuid.uuid4()
        assert (
            await client.send_message(
                chat_id=-100,
                thread_id=10,
                html="<b>ok</b>",
                buttons=("start",),
                task_id=task_id,
            )
            == 17
        )
        await client.edit_message(
            chat_id=-100,
            message_id=17,
            html="updated",
            buttons=("pause", "cancel"),
            task_id=task_id,
        )
        bot.send_message.assert_awaited_once()
        bot.edit_message_text.assert_awaited_once()
        telegram_file = AsyncMock()
        telegram_file.download_as_bytearray.return_value = bytearray(b"payload")
        bot.get_file.return_value = telegram_file
        assert await client.download("file-id") == b"payload"
        send_markup = bot.send_message.await_args.kwargs["reply_markup"]
        edit_markup = bot.edit_message_text.await_args.kwargs["reply_markup"]
        assert send_markup.inline_keyboard[0][0].callback_data == f"v1:start:{task_id}"
        assert [row[0].callback_data for row in edit_markup.inline_keyboard] == [
            f"v1:pause:{task_id}",
            f"v1:cancel:{task_id}",
        ]
        assert await client.pin_message(chat_id=-100, message_id=17)
        bot.pin_chat_message.assert_awaited_once_with(
            chat_id=-100, message_id=17, disable_notification=True
        )
        bot.pin_chat_message.side_effect = BadRequest("not enough rights")
        assert not await client.pin_message(chat_id=-100, message_id=17)

    asyncio.run(scenario())


def test_python_telegram_client_treats_an_already_applied_edit_as_success() -> None:
    async def scenario() -> None:
        bot = AsyncMock()
        client = PythonTelegramClient(bot)
        bot.edit_message_text.side_effect = BadRequest("Message_is_not_modified")

        await client.edit_message(chat_id=-100, message_id=17, html="current")

        bot.edit_message_text.side_effect = BadRequest("message to edit not found")
        with pytest.raises(BadRequest, match="message to edit not found"):
            await client.edit_message(chat_id=-100, message_id=17, html="new")

    asyncio.run(scenario())


def test_python_telegram_client_creates_topic_and_fails_closed_on_unknown_outcome() -> None:
    async def scenario() -> None:
        bot = AsyncMock()
        bot.create_forum_topic.return_value = SimpleNamespace(message_thread_id=41)
        client = PythonTelegramClient(bot)
        assert await client.create_topic(chat_id=-100, name="Notes") == 41
        bot.create_forum_topic.side_effect = TimedOut("unknown")
        with pytest.raises(TopicCreationOutcomeUnknown):
            await client.create_topic(chat_id=-100, name="Notes")

    asyncio.run(scenario())


def test_python_telegram_client_renames_topic_idempotently_and_categorizes_errors() -> None:
    async def scenario() -> None:
        bot = AsyncMock()
        client = PythonTelegramClient(bot)
        await client.rename_topic(chat_id=-100, thread_id=41, name="Notes")
        bot.edit_forum_topic.side_effect = BadRequest("Topic_not_modified")
        await client.rename_topic(chat_id=-100, thread_id=41, name="Notes")
        bot.edit_forum_topic.side_effect = BadRequest("topic closed")
        with pytest.raises(TopicSynchronizationError):
            await client.rename_topic(chat_id=-100, thread_id=41, name="Notes")
        bot.edit_forum_topic.side_effect = TimedOut("network")
        with pytest.raises(TopicSynchronizationError):
            await client.rename_topic(chat_id=-100, thread_id=41, name="Notes")

    asyncio.run(scenario())


def test_python_telegram_client_reports_forum_topic_pin_unsupported() -> None:
    async def scenario() -> None:
        client = PythonTelegramClient(AsyncMock())
        with pytest.raises(TopicPinUnsupported, match="unsupported_by_bot_api"):
            await client.set_topic_pinned(chat_id=-100, thread_id=41, pinned=True)

    asyncio.run(scenario())


def test_message_update_collects_document_and_voice() -> None:
    update = Update.de_json(
        {
            "update_id": 7,
            "message": {
                "message_id": 11,
                "date": 0,
                "message_thread_id": 3,
                "from": {"id": 9, "is_bot": False, "first_name": "User"},
                "chat": {"id": -100, "type": "supergroup"},
                "caption": "files",
                "media_group_id": "document-album",
                "document": {
                    "file_id": "doc",
                    "file_unique_id": "doc-u",
                    "file_name": "a.txt",
                    "mime_type": "text/plain",
                    "file_size": 4,
                },
                "voice": {
                    "file_id": "voice",
                    "file_unique_id": "voice-u",
                    "duration": 1,
                    "file_size": 5,
                },
            },
        },
        None,
    )
    converted = message_update(update, "main")
    assert isinstance(converted, MessageUpdate)
    assert converted.text == "files"
    assert converted.media_group_id == "document-album"
    assert [item.file_id for item in converted.attachments] == ["doc", "voice"]


def test_message_update_rejects_non_topic_update() -> None:
    update = Update.de_json({"update_id": 8}, None)
    assert message_update(update, "main") is None


def test_start_callback_crosses_the_provider_boundary() -> None:
    task_id = uuid.uuid4()
    update = Update.de_json(
        {
            "update_id": 1,
            "callback_query": {
                "id": "callback",
                "from": {"id": 7, "is_bot": False, "first_name": "User"},
                "chat_instance": "instance",
                "data": f"v1:start:{task_id}",
                "message": {
                    "message_id": 9,
                    "date": 0,
                    "chat": {"id": -100, "type": "supergroup"},
                },
            },
        },
        None,
    )
    converted = control_update(update, "main")
    assert isinstance(converted, ControlUpdate)
    assert converted.action_kind == "start"
    assert converted.task_id == task_id


def test_secret_cancel_callback_targets_the_exact_request() -> None:
    request_id = uuid.uuid4()

    def update(data: str) -> Update:
        return Update.de_json(
            {
                "update_id": 3,
                "callback_query": {
                    "id": "secret-cancel",
                    "from": {"id": 7, "is_bot": False, "first_name": "User"},
                    "chat_instance": "instance",
                    "data": data,
                    "message": {
                        "message_id": 11,
                        "message_thread_id": 12,
                        "date": 0,
                        "chat": {"id": -100, "type": "supergroup"},
                    },
                },
            },
            None,
        )

    converted = control_update(update(f"v1:secret_cancel:{request_id}"), "main")

    assert isinstance(converted, ControlUpdate)
    assert converted.action_kind == "secret_cancel"
    assert converted.secret_request_id == request_id
    assert converted.message_id == 11
    assert control_update(update("v1:secret_cancel:not-a-uuid"), "main") is None


def test_result_decision_callback_targets_the_exact_approval() -> None:
    approval_id = uuid.uuid4()
    update = Update.de_json(
        {
            "update_id": 2,
            "callback_query": {
                "id": "decision",
                "from": {"id": 7, "is_bot": False, "first_name": "User"},
                "chat_instance": "instance",
                "data": f"v1:redo:{approval_id}",
                "message": {
                    "message_id": 10,
                    "date": 0,
                    "chat": {"id": -100, "type": "supergroup"},
                },
            },
        },
        None,
    )
    converted = control_update(update, "main")
    assert isinstance(converted, ControlUpdate)
    assert converted.action_kind == "redo"
    assert converted.approval_id == approval_id
    assert converted.task_id is None


def test_project_naming_callbacks_include_revision_and_option() -> None:
    request_id = uuid.uuid4()
    for suffix, action, option in (
        ("4", "project_name_select", 4),
        ("r", "project_name_regenerate", None),
    ):
        update = Update.de_json(
            {
                "update_id": 3 if option is not None else 4,
                "callback_query": {
                    "id": f"naming-{suffix}",
                    "from": {"id": 7, "is_bot": False, "first_name": "User"},
                    "chat_instance": "instance",
                    "data": f"v1:pn:{request_id.hex}:2:{suffix}",
                    "message": {
                        "message_id": 11,
                        "date": 0,
                        "chat": {"id": -100, "type": "supergroup"},
                    },
                },
            },
            None,
        )
        converted = control_update(update, "main")
        assert isinstance(converted, ControlUpdate)
        assert converted.action_kind == action
        assert converted.naming_request_id == request_id
        assert converted.naming_revision == 2
        assert converted.naming_option_index == option


def test_project_naming_callback_rejects_malformed_payload() -> None:
    update = Update.de_json(
        {
            "update_id": 5,
            "callback_query": {
                "id": "malformed-naming",
                "from": {"id": 7, "is_bot": False, "first_name": "User"},
                "chat_instance": "instance",
                "data": "v1:pn:not-a-uuid:revision:choice",
                "message": {
                    "message_id": 12,
                    "date": 0,
                    "chat": {"id": -100, "type": "supergroup"},
                },
            },
        },
        None,
    )
    assert control_update(update, "main") is None
    invalid_target = Update.de_json(
        {
            "update_id": 6,
            "callback_query": {
                "id": "malformed-target",
                "from": {"id": 7, "is_bot": False, "first_name": "User"},
                "chat_instance": "instance",
                "data": "v1:start:not-a-uuid",
                "message": {
                    "message_id": 13,
                    "date": 0,
                    "chat": {"id": -100, "type": "supergroup"},
                },
            },
        },
        None,
    )
    assert control_update(invalid_target, "main") is None


def _callback(data: str, update_id: int = 11) -> Update:
    return Update.de_json(
        {
            "update_id": update_id,
            "callback_query": {
                "id": f"callback-{update_id}",
                "from": {"id": 42, "is_bot": False, "first_name": "Owner"},
                "chat_instance": "test",
                "data": data,
                "message": {
                    "message_id": 77,
                    "message_thread_id": 10,
                    "date": 0,
                    "chat": {"id": -100, "type": "supergroup"},
                },
            },
        },
        None,
    )


def test_owner_choice_callback_keeps_the_composite_revision() -> None:
    task_id = uuid.uuid4()
    admit = control_update(_callback(f"v2:admit:{task_id.hex}:40004:2"), "main")
    assert isinstance(admit, ControlUpdate)
    assert admit.action_kind == "admit"
    assert admit.expected_task_version == 40004
    assert admit.retry_step_ordinal == 2
    assert control_update(_callback(f"v2:c:{task_id.hex}:40004:2:ab12cd34"), "main") is None


def test_python_telegram_client_builds_custom_markup_and_best_effort_delete() -> None:
    async def scenario() -> None:
        bot = AsyncMock()
        bot.send_message.return_value = SimpleNamespace(message_id=19)
        client = PythonTelegramClient(bot)
        message_id = await client.send_message(
            chat_id=-100,
            thread_id=10,
            html="names",
            callback_buttons=((("First", "v1:first"), ("Second", "v1:second")),),
        )
        assert message_id == 19
        markup = bot.send_message.await_args.kwargs["reply_markup"]
        assert [button.callback_data for button in markup.inline_keyboard[0]] == [
            "v1:first",
            "v1:second",
        ]
        await client.send_message(chat_id=-100, thread_id=10, html="without controls")
        assert bot.send_message.await_args.kwargs["reply_markup"] is None
        await client.delete_message(chat_id=-100, message_id=19)
        bot.delete_message.side_effect = BadRequest("message not found")
        await client.delete_message(chat_id=-100, message_id=19)
        assert bot.delete_message.await_count == 2

    asyncio.run(scenario())


def test_python_telegram_client_builds_result_decision_markup() -> None:
    async def scenario() -> None:
        bot = AsyncMock()
        bot.send_message.return_value = SimpleNamespace(message_id=18)
        approval_id = uuid.uuid4()
        await PythonTelegramClient(bot).send_message(
            chat_id=-100,
            thread_id=10,
            html="result",
            buttons=("approve", "redo", "reject"),
            approval_id=approval_id,
        )
        markup = bot.send_message.await_args.kwargs["reply_markup"]
        assert [row[0].text for row in markup.inline_keyboard] == [
            "Принять",
            "Переделать",
            "Отклонить",
        ]
        assert [row[0].callback_data for row in markup.inline_keyboard] == [
            f"v1:approve:{approval_id}",
            f"v1:redo:{approval_id}",
            f"v1:reject:{approval_id}",
        ]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("actions", "labels"),
    [
        (("start",), ("Старт",)),
        (("pause", "cancel"), ("Пауза", "Отмена")),
        (("resume", "cancel"), ("Продолжить", "Отмена")),
    ],
)
def test_control_labels_are_russian_and_wire_actions_stay_stable(
    actions: tuple[str, ...], labels: tuple[str, ...]
) -> None:
    task_id = uuid.uuid4()
    markup = _control_markup(
        actions,
        task_id=task_id,
        approval_id=None,
    )
    assert markup is not None
    assert tuple(row[0].text for row in markup.inline_keyboard) == labels
    assert tuple(row[0].callback_data for row in markup.inline_keyboard) == tuple(
        f"v1:{action}:{task_id}" for action in actions
    )


def test_task_controls_and_package_controls_share_one_card() -> None:
    task_id = uuid.uuid4()
    markup = _control_markup(
        ("pause",),
        task_id=task_id,
        approval_id=None,
        callback_buttons=((("Изменить", "v1:wp:U:payload"),),),
    )
    assert markup is not None
    assert tuple(row[0].text for row in markup.inline_keyboard) == ("Пауза", "Изменить")


@pytest.mark.parametrize(
    "data,action",
    [
        ("v1:lq:36:1", "lead_answer"),
        ("v1:lg:36:resume", "lead_resume"),
        ("v1:lg:36:cancel", "lead_cancel"),
        ("v1:lg:36:fallback", "lead_fallback"),
    ],
)
def test_lead_buttons_keep_message_and_version_binding(data, action):
    converted = control_update(_callback(data), "main")
    assert isinstance(converted, ControlUpdate)
    assert converted.action_kind == action
    assert converted.message_id is not None
    if action == "lead_answer":
        assert converted.question_version == 36 and converted.answer_option == 1
    else:
        assert converted.goal_version == 36
    assert len(data.encode()) <= 64


@pytest.mark.parametrize(
    "data",
    [
        "v1:lq:0:1",
        "v1:lq:36:6",
        "v1:lq:x:0",
        "v1:lg:0:resume",
        "v1:lg:36:approve",
        "v1:lq:" + "1" * 65 + ":0",
    ],
)
def test_lead_buttons_reject_malformed_payloads(data):
    assert control_update(_callback(data), "main") is None


def test_attention_url_button_and_silent_project_send():
    async def scenario():
        bot = AsyncMock()
        bot.send_message.return_value = SimpleNamespace(message_id=17)
        client = PythonTelegramClient(bot)
        url = "https://t.me/c/1234567890/10/99"
        await client.send_message(
            chat_id=-1001234567890,
            thread_id=7,
            html="Нужно решение",
            callback_buttons=((("Открыть", url),),),
            disable_notification=False,
        )
        button = bot.send_message.await_args.kwargs["reply_markup"].inline_keyboard[0][0]
        assert button.url == url and button.callback_data is None
        assert bot.send_message.await_args.kwargs["disable_notification"] is False
        await client.send_message(
            chat_id=-1001234567890,
            thread_id=10,
            html="Статус",
            disable_notification=True,
        )
        assert bot.send_message.await_args.kwargs["disable_notification"] is True
        bot.do_api_request.return_value = {"message_id": 18}
        await client.send_rich_message(
            chat_id=-1001234567890,
            thread_id=10,
            html="Ответ",
            disable_notification=True,
        )
        assert bot.do_api_request.await_args.kwargs["api_kwargs"]["disable_notification"] is True

    asyncio.run(scenario())


def test_handoff_keyboard_edit_is_idempotent_and_keeps_answer_text() -> None:
    async def scenario() -> None:
        bot = AsyncMock()
        client = PythonTelegramClient(bot)
        callback = f"v2:th:{uuid.uuid4().hex}"
        buttons = ((("✅ Передано лиду", callback),),)
        await client.edit_reply_markup(chat_id=-100, message_id=17, callback_buttons=buttons)
        kwargs = bot.edit_message_reply_markup.call_args.kwargs
        assert kwargs["chat_id"] == -100 and kwargs["message_id"] == 17
        assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == callback
        bot.edit_message_text.assert_not_called()
        bot.edit_message_reply_markup.side_effect = BadRequest("Message_is_not_modified")
        await client.edit_reply_markup(chat_id=-100, message_id=17, callback_buttons=buttons)
        bot.edit_message_reply_markup.side_effect = BadRequest("message to edit not found")
        with pytest.raises(BadRequest, match="message to edit not found"):
            await client.edit_reply_markup(chat_id=-100, message_id=17, callback_buttons=buttons)

    asyncio.run(scenario())


def test_every_accepted_press_says_what_it_did() -> None:
    """Live pilot 2026-10-25: a successful press only stopped the spinner."""
    from vuzol.telegram.adapter import _done_answer

    package = WorkPackageControlUpdate(
        bot_id="main",
        update_id=1,
        callback_query_id="q",
        callback_data="v1:wp:R:0123456789abcdef0123456789abcdef:3:abcdef01:3",
        chat_id=-100,
        message_id=7,
        user_id=42,
        message_thread_id=9,
    )
    assert _done_answer(package) == (
        "🔁 Повторяю пункт. Карточка обновится, когда он начнёт работу."
    )
    task = ControlUpdate(
        bot_id="main",
        update_id=2,
        callback_query_id="q2",
        chat_id=-100,
        user_id=42,
        action_kind="pause",
    )
    assert _done_answer(task) == "⏸ Ставлю на паузу."
