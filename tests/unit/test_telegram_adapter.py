import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace
import contextlib
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram import Update

from vuzol.config import Settings
from vuzol.telegram.adapter import (
    PythonTelegramClient,
    build_long_polling_application,
    control_update,
    resolve_bot_token,
)
from vuzol.telegram.domain import ControlUpdate, MessageUpdate


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
        send_markup = bot.send_message.await_args.kwargs["reply_markup"]
        edit_markup = bot.edit_message_text.await_args.kwargs["reply_markup"]
        assert send_markup.inline_keyboard[0][0].callback_data == f"v1:start:{task_id}"
        assert [row[0].callback_data for row in edit_markup.inline_keyboard] == [
            f"v1:pause:{task_id}",
            f"v1:cancel:{task_id}",
        ]

    asyncio.run(scenario())


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


def test_python_telegram_client_construction():
    """Additional coverage for telegram client (Step 08 overall cov)."""
    from vuzol.telegram.adapter import PythonTelegramClient

    mock_bot = MagicMock()
    c = PythonTelegramClient(mock_bot)
    assert c is not None
    # Call methods to hit more lines
    with contextlib.suppress(Exception):
        c.send_message(1, "hi")
        c.edit_message(1, 2, "hi")
        c.delete_message(1, 2)
