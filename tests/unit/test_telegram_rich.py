"""Safe rich rendering and raw Bot API bridge, without live requests."""

import uuid
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, EndPointNotFound, NetworkError, RetryAfter

from vuzol.config import Settings
from vuzol.discussion.agent import _discussion_prompt
from vuzol.discussion.memory import MemoryPack
from vuzol.telegram.adapter import PythonTelegramClient
from vuzol.telegram.formatting import telegram_rich_html
from vuzol.telegram.projections import LostTelegramResponse, RichMessageRejected


def test_rich_formatting_preserves_actual_blocks_and_tables() -> None:
    rendered = telegram_rich_html(
        "# Выбор\n\n**Текст** и ~~старый~~\n\n- Один\n- Два\n\n"
        "| Имя | Значение |\n|---|---:|\n| Sol | `high` |\n\n"
        "> Цитата\n\n```python\nx = 1 < 2\n```"
    )
    assert rendered is not None
    assert "<h1>Выбор</h1>" in rendered and "<ul><li>" in rendered
    assert "<table><tr><th>Имя</th>" in rendered
    assert '<td align="right"><code>high</code></td>' in rendered
    assert "<blockquote>" in rendered and "<s>старый</s>" in rendered
    assert '<pre><code class="language-python">x = 1 &lt; 2;\n' not in rendered
    assert "x = 1 &lt; 2" in rendered
    assert "<thead>" not in rendered and "<tbody>" not in rendered


def test_rich_formatting_does_not_allow_model_html_media_or_buttons() -> None:
    rendered = telegram_rich_html(
        '<tg-button type="callback_data" data="start">Start</tg-button>\n\n'
        "![image](https://example.com/private.png)\n\n<script>bad</script>\n\n"
        "[bad](javascript:alert(1)) [safe](https://example.com?a=1&b=2)"
    )
    assert rendered is not None
    assert "<img" not in rendered and "<tg-button" not in rendered and "<script" not in rendered
    assert "&lt;tg-button" in rendered
    assert '<a href="javascript' not in rendered
    assert '<a href="https://example.com?a=1&amp;b=2">safe</a>' in rendered


def test_rich_limits_choose_lossless_ordinary_path_not_schema_rejection() -> None:
    assert telegram_rich_html("x" * 40_000) is None
    assert telegram_rich_html("- item\n" * 501) is None
    assert telegram_rich_html(">" * 17 + " text") is None
    assert telegram_rich_html("") is None


def test_rich_setting_is_default_off_and_prompt_switches_presentation_only() -> None:
    assert not Settings(environment="test").chat.rich_messages
    pack = MemoryPack(uuid.uuid4(), None, (), (), False, False, False)
    ordinary = _discussion_prompt(project_id="test", memory_pack=pack)
    rich = _discussion_prompt(project_id="test", memory_pack=pack, rich_messages=True)
    assert "Do not use Markdown tables" in ordinary
    assert "compact Markdown table" in rich
    assert (
        "starting work and applying results are" in ordinary
        and "starting work and applying results are" in rich
    )


@pytest.mark.anyio
async def test_api_bridge_send_and_edit_have_rich_payload_without_text_or_token() -> None:
    bot = AsyncMock()
    bot.do_api_request.return_value = {"message_id": 17}
    client = PythonTelegramClient(bot)
    assert await client.send_rich_message(chat_id=-100, thread_id=10, html="<h1>Ответ</h1>") == 17
    await client.edit_rich_message(chat_id=-100, message_id=17, html="<p>Другой ответ</p>")
    calls = bot.do_api_request.await_args_list
    assert calls[0].args == ("sendRichMessage",)
    assert calls[0].kwargs["api_kwargs"] == {
        "chat_id": -100,
        "message_thread_id": 10,
        "rich_message": {"html": "<h1>Ответ</h1>"},
    }
    assert calls[1].args == ("editMessageText",)
    assert calls[1].kwargs["api_kwargs"] == {
        "chat_id": -100,
        "message_id": 17,
        "rich_message": {"html": "<p>Другой ответ</p>"},
    }


@pytest.mark.anyio
@pytest.mark.parametrize("error", [BadRequest("format rejected"), EndPointNotFound("not found")])
async def test_definite_rich_rejection_is_safe_to_fallback(error) -> None:
    bot = AsyncMock()
    bot.do_api_request.side_effect = error
    with pytest.raises(RichMessageRejected):
        await PythonTelegramClient(bot).send_rich_message(chat_id=-100, thread_id=10, html="text")
    with pytest.raises(RichMessageRejected):
        await PythonTelegramClient(bot).edit_rich_message(chat_id=-100, message_id=17, html="text")


@pytest.mark.anyio
@pytest.mark.parametrize("error", [NetworkError("unknown"), RetryAfter(1)])
async def test_unknown_or_rate_limited_rich_send_never_uses_fallback(error) -> None:
    bot = AsyncMock()
    bot.do_api_request.side_effect = error
    with pytest.raises(type(error)):
        await PythonTelegramClient(bot).send_rich_message(chat_id=-100, thread_id=10, html="text")
    bot.send_message.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("response", [{}, False, True, {"message_id": 0}, {"message_id": True}])
async def test_unknown_rich_send_response_is_quarantined(response) -> None:
    bot = AsyncMock()
    bot.do_api_request.return_value = response
    with pytest.raises(LostTelegramResponse):
        await PythonTelegramClient(bot).send_rich_message(chat_id=-100, thread_id=10, html="text")


@pytest.mark.anyio
async def test_rich_edit_idempotency_and_unknown_confirmation() -> None:
    bot = AsyncMock()
    client = PythonTelegramClient(bot)
    bot.do_api_request.side_effect = BadRequest("Message_is_not_modified")
    await client.edit_rich_message(chat_id=-100, message_id=17, html="text")
    bot.do_api_request.side_effect = None
    bot.do_api_request.return_value = {}
    with pytest.raises(LostTelegramResponse):
        await client.edit_rich_message(chat_id=-100, message_id=17, html="text")
