import asyncio
import uuid
from datetime import UTC, datetime

import pytest

from vuzol.telegram.projections import EditRateLimiter, split_message, telegram_html


def test_html_is_escaped_and_messages_are_bounded() -> None:
    assert telegram_html('<b x="1">&') == "&lt;b x=&quot;1&quot;&gt;&amp;"
    assert split_message("abcdef", limit=2) == ("ab", "cd", "ef")
    assert split_message("") == ("",)
    with pytest.raises(ValueError):
        split_message("x", limit=0)


def test_edit_rate_limiter_serializes_one_projection() -> None:
    async def scenario() -> None:
        limiter = EditRateLimiter(2)
        task_id = uuid.uuid4()
        now = datetime.now(UTC)
        assert await limiter.reserve(task_id, now) == now
        assert (await limiter.reserve(task_id, now) - now).total_seconds() == 2
        assert await limiter.reserve(uuid.uuid4(), now) == now

    asyncio.run(scenario())
