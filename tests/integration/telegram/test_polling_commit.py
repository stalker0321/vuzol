"""Actual Telegram composition, fake getUpdates, real committed PostgreSQL inbox."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select
from telegram import Update
from telegram.ext import Updater

from tests.integration.leadership.test_managed_goal_flow import (
    CHAT,
    OWNER,
    THREAD,
    _factory,
    _runtime,
)
from vuzol.cli.telegram import ingress_handlers
from vuzol.storage.models import ExternalInbox, Task
from vuzol.telegram.adapter import build_long_polling_application

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize("failure", ["none", "before_commit", "after_commit"])
async def test_polling_receipt_precedes_offset_and_replay_is_idempotent(
    postgres_dsn,
    tmp_path,
    monkeypatch,
    failure,
):
    async with _factory(postgres_dsn) as factory:
        repository = tmp_path / "repositories" / "repo"
        repository.mkdir(parents=True)
        runtime = _runtime(tmp_path, repository, lead=False, discussion=False)
        on_message, on_control = ingress_handlers(runtime, factory)
        admitted = asyncio.Event()
        calls = 0

        async def faulted(update):
            nonlocal calls
            calls += 1
            if failure == "before_commit" and calls == 1:
                raise ConnectionError("fault before transaction")
            await on_message(update)
            if failure == "after_commit" and calls == 1:
                raise ConnectionError("fault after commit before ack")

        app = build_long_polling_application(
            "123:FAKE_TOKEN_NOT_A_CREDENTIAL",
            bot_id="fixture",
            on_message=faulted,
            on_control=on_control,
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
                            "update_id": 99,
                            "message": {
                                "message_id": 99,
                                "message_thread_id": THREAD,
                                "date": 1792000000,
                                "from": {"id": OWNER, "is_bot": False, "first_name": "Owner"},
                                "chat": {"id": CHAT, "type": "supergroup", "title": "Fixture"},
                                "text": "Implement a marker",
                            },
                        },
                        app.bot,
                    )
                ]
            # Independent connection: no uncommitted in-memory state can satisfy this.
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 1
                assert await session.scalar(select(func.count()).select_from(Task)) == 1
            admitted.set()
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
            await asyncio.wait_for(admitted.wait(), 5)
            assert offsets == [0, 100]
            assert calls == (1 if failure == "none" else 2)
        finally:
            updater._running = False
            pending = updater._Updater__polling_task
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
