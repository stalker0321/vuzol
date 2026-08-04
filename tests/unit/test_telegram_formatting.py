"""Telegram-safe Markdown rendering."""

import asyncio
from unittest.mock import AsyncMock

from telegram.error import BadRequest

from vuzol.telegram.delivery import DeliveryAction, PreparedDelivery, TelegramDeliveryService
from vuzol.telegram.formatting import telegram_markdown_html


def test_renders_common_discussion_markdown() -> None:
    rendered = telegram_markdown_html(
        "# Выбор\n\n**Лучший:** первый.\n\n- Просто\n- Быстро\n\n`myQty`"
    )
    assert rendered == (
        "<b>Выбор</b>\n<b>Лучший:</b> первый.\n\n• Просто\n• Быстро\n\n<code>myQty</code>"
    )


def test_escapes_raw_html_and_unsafe_links() -> None:
    rendered = telegram_markdown_html("<script>x</script> [bad](javascript:alert(1))")
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert "javascript" in rendered
    assert '<a href="javascript' not in rendered


def test_renders_safe_links_quotes_and_fenced_code() -> None:
    rendered = telegram_markdown_html(
        "> Важно\n\n[Документация](https://example.com?a=1&b=2)\n\n```js\nconst x = 1 < 2;\n```"
    )
    assert "<blockquote>Важно\n\n</blockquote>" in rendered
    assert '<a href="https://example.com?a=1&amp;b=2">Документация</a>' in rendered
    assert '<pre><code class="language-js">const x = 1 &lt; 2;\n</code></pre>' in rendered


def test_discussion_delivery_falls_back_when_telegram_rejects_markup() -> None:
    async def scenario() -> None:
        client = AsyncMock()
        client.send_message.side_effect = [BadRequest("can't parse entities"), 42]
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
            DeliveryAction.SEND_DISCUSSION_REPLY,
            chat_id=-100,
            thread_id=7,
            html="<b>Ответ</b>",
            fallback_html="**Ответ**",
        )
        assert await service._call_telegram(prepared) == 42
        assert [call.kwargs["html"] for call in client.send_message.await_args_list] == [
            "<b>Ответ</b>",
            "**Ответ**",
        ]

    asyncio.run(scenario())
