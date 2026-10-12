"""Long replies are not malformed output or truncated Telegram messages."""

import asyncio
import html
from html.parser import HTMLParser
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from telegram.error import BadRequest

from vuzol.discussion.agent import DiscussionAgentReply
from vuzol.telegram.delivery import DeliveryAction, PreparedDelivery, TelegramDeliveryService
from vuzol.telegram.formatting import telegram_markdown_html, telegram_markdown_parts


class ParsedHtml(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[str] = []
        self.text: list[str] = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)

    def handle_endtag(self, tag):
        assert self.tags.pop() == tag

    def handle_data(self, data):
        self.text.append(data)


def visible(markup: str) -> str:
    parser = ParsedHtml()
    parser.feed(markup)
    parser.close()
    assert not parser.tags
    return "".join(parser.text)


@pytest.mark.parametrize(
    "markdown",
    [
        "я" * 4337,
        "слово " * 2000,
        "😀" * 5000,
        "**" + "<>&😀 " * 1400 + "**",
        "> " + "важно " * 1500,
        "```python\n" + "print('😀 & <tag>')\n" * 1000 + "```",
        "[" + "ссылка " * 1200 + "](https://example.com?a=1&b=2)",
        "# Заголовок\n\n" + "- **пункт** и `код`\n" * 600,
        "**" + "a" * 4096 + "**&<script>x</script>",
    ],
)
def test_lossless_balanced_unicode_safe_chunks(markdown: str) -> None:
    parts = telegram_markdown_parts(markdown)
    assert len(parts) > 1
    assert "".join(visible(part) for part, _ in parts) == visible(telegram_markdown_html(markdown))
    for formatted, fallback in parts:
        text = visible(formatted)
        assert 0 < len(text.encode("utf-16-le")) // 2 <= 4096
        assert html.unescape(fallback) == text
        assert visible(fallback) == text


def test_short_reply_markup_is_unchanged() -> None:
    assert telegram_markdown_parts("**Ответ** и `код` & <опасно>") == (
        (
            "<b>Ответ</b> и <code>код</code> &amp; &lt;опасно&gt;",
            "Ответ и код &amp; &lt;опасно&gt;",
        ),
    )


def test_reply_schema_has_no_telegram_length_cap() -> None:
    reply = "я" * 100_000
    assert DiscussionAgentReply(reply=reply).reply == reply
    assert "maxLength" not in DiscussionAgentReply.model_json_schema()["properties"]["reply"]
    with pytest.raises(ValidationError):
        DiscussionAgentReply(reply="")
    with pytest.raises(ValidationError):
        DiscussionAgentReply.model_validate({"reply": "ok", "start": True})


def test_small_limits_entities_and_empty_rendering() -> None:
    assert telegram_markdown_parts("😀😀&<", limit=2) == (
        ("😀", "😀"),
        ("😀", "😀"),
        ("&amp;&lt;", "&amp;&lt;"),
    )
    assert telegram_markdown_parts("") == (("", ""),)
    with pytest.raises(ValueError):
        telegram_markdown_parts("text", limit=1)


def test_first_chunk_edit_has_plain_fallback() -> None:
    async def scenario() -> None:
        client = AsyncMock()
        client.edit_message.side_effect = [BadRequest("can't parse entities"), None]
        service = TelegramDeliveryService(
            AsyncMock(),
            client,
            owner="test",
            lease_seconds=30,
            max_attempts=3,
            retry_min_seconds=1,
            retry_max_seconds=5,
        )
        prepared = PreparedDelivery(
            DeliveryAction.EDIT_MESSAGE,
            chat_id=-100,
            thread_id=7,
            message_id=42,
            html="<b>Ответ</b>",
            fallback_html="Ответ",
        )
        assert await service._call_telegram(prepared) is None
        assert [c.kwargs["html"] for c in client.edit_message.await_args_list] == [
            "<b>Ответ</b>",
            "Ответ",
        ]

    asyncio.run(scenario())
