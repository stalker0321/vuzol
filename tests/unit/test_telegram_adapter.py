import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram import Update
from telegram.error import BadRequest, TimedOut

from vuzol.config import Settings
from vuzol.telegram.adapter import (
    PythonTelegramClient,
    build_long_polling_application,
    control_update,
    message_update,
    resolve_bot_token,
)
from vuzol.telegram.domain import ControlUpdate, MessageUpdate
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

    async def control_handler(_update: ControlUpdate) -> None:
        return None

    application = build_long_polling_application(
        "123:test",  # pragma: allowlist secret
        bot_id="main",
        on_message=message_handler,
        on_control=control_handler,
    )
    assert len(application.handlers[0]) == 2


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
    assert converted is not None
    assert converted.text == "files"
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
    assert converted is not None
    assert converted.action_kind == "start"
    assert converted.task_id == task_id


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
    assert converted is not None
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
        assert converted is not None
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
        assert [row[0].callback_data for row in markup.inline_keyboard] == [
            f"v1:approve:{approval_id}",
            f"v1:redo:{approval_id}",
            f"v1:reject:{approval_id}",
        ]

    asyncio.run(scenario())
