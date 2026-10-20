"""Exercise the production builder and PTB's actual polling/offset loop."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from telegram import Update
from telegram.ext import Updater

from vuzol.telegram.adapter import build_long_polling_application

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize("fail_first", [False, True])
async def test_offset_waits_for_committed_ingress(monkeypatch, fail_first):
    entered, release, acknowledged = (asyncio.Event() for _ in range(3))
    committed = []
    calls = 0

    async def ingress(update):
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        if fail_first and calls == 1:
            raise ConnectionError("fake database unavailable")
        committed.append(update.update_id)

    app = build_long_polling_application(
        "123:FAKE_TOKEN_NOT_A_CREDENTIAL",
        bot_id="fixture",
        admit_update=ingress,
        on_message=AsyncMock(),
        on_control=AsyncMock(),
    )
    updater = app.updater
    assert updater is not None
    monkeypatch.setattr(Updater, "_bootstrap", AsyncMock())
    offsets = []

    async def get_updates(_bot, *, offset, **kwargs):
        offsets.append(offset)
        if len(offsets) == 1:
            return [
                Update.de_json(
                    {
                        "update_id": 12345,
                        "message": {
                            "message_id": 10,
                            "date": 1792000000,
                            "from": {"id": 77, "is_bot": False, "first_name": "Fixture"},
                            "chat": {"id": -10077, "type": "supergroup", "title": "Fixture"},
                            "message_thread_id": 20,
                            "text": "continue",
                        },
                    },
                    app.bot,
                )
            ]
        assert committed == [12345], "offset acknowledged before commit"
        acknowledged.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(type(app.bot), "get_updates", get_updates)
    updater._running = True
    try:
        await updater._start_polling(
            poll_interval=0,
            timeout=1,
            bootstrap_retries=0,
            drop_pending_updates=False,
            allowed_updates=["message"],
            ready=asyncio.Event(),
            error_callback=None,
        )
        await asyncio.wait_for(entered.wait(), 2)
        assert offsets == [0]
        assert not committed
        assert updater._last_update_id == 0
        release.set()
        await asyncio.wait_for(acknowledged.wait(), 2)
        assert offsets == [0, 12346]
        assert app.update_queue.empty()  # Never deliver the same action a second time.
        assert calls == (2 if fail_first else 1)
    finally:
        updater._running = False
        task = updater._Updater__polling_task
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_cancellation_before_commit_does_not_finish_admission():
    started = asyncio.Event()
    completed = []

    async def ingress(_update):
        started.set()
        await asyncio.Event().wait()
        completed.append(True)

    app = build_long_polling_application(
        "123:FAKE_TOKEN_NOT_A_CREDENTIAL",
        bot_id="fixture",
        admit_update=ingress,
        on_message=AsyncMock(),
        on_control=AsyncMock(),
    )
    update = Update.de_json(
        {
            "update_id": 1,
            "message": {
                "message_id": 1,
                "message_thread_id": 20,
                "date": 1792000000,
                "text": "hello",
                "from": {"id": 77, "is_bot": False, "first_name": "Fixture"},
                "chat": {"id": -10077, "type": "supergroup", "title": "Fixture"},
            },
        },
        app.bot,
    )
    pending = asyncio.create_task(app.update_queue.put(update))
    await asyncio.wait_for(started.wait(), 2)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not completed
    assert app.updater._last_update_id == 0
