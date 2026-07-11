import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from vuzol.config import Settings
from vuzol.telegram.adapter import (
    PythonTelegramClient,
    build_long_polling_application,
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
        assert await client.send_message(chat_id=-100, thread_id=10, html="<b>ok</b>") == 17
        await client.edit_message(chat_id=-100, message_id=17, html="updated")
        bot.send_message.assert_awaited_once()
        bot.edit_message_text.assert_awaited_once()

    asyncio.run(scenario())
