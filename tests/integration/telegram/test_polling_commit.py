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
from vuzol.storage.models import ExternalInbox, Task, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.adapter import build_long_polling_application
from vuzol.telegram.durable_ingress import DurableTelegramIngress

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
        durable = DurableTelegramIngress(
            factory,
            settings=runtime.settings,
            bot_id="fixture",
            on_message=on_message,
            on_control=on_control,
        )
        admitted = asyncio.Event()
        calls = 0

        async def faulted(update):
            nonlocal calls
            calls += 1
            if failure == "before_commit" and calls == 1:
                raise ConnectionError("fault before transaction")
            await durable.admit(update)
            if failure == "after_commit" and calls == 1:
                raise ConnectionError("fault after commit before ack")

        app = build_long_polling_application(
            "123:FAKE_TOKEN_NOT_A_CREDENTIAL",
            bot_id="fixture",
            admit_update=faulted,
            on_message=on_message,
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
                assert (
                    await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 1
                )
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
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
            # Restart after receipt/offset but before any business processing.
            restarted = DurableTelegramIngress(
                factory,
                settings=runtime.settings,
                bot_id="fixture",
                on_message=on_message,
                on_control=on_control,
            )
            assert await restarted.process_one(app.bot)
            assert not await restarted.process_one(app.bot)
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Task)) == 1
                assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 2
        finally:
            updater._running = False
            pending = updater._Updater__polling_task
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


async def test_actual_polling_acknowledges_later_updates_despite_poison_handler(
    postgres_dsn,
    monkeypatch,
):
    from tests.integration.telegram.test_durable_ingress import INGRESS_SETTINGS, update

    attempted = []
    acknowledged = asyncio.Event()

    async def business(message):
        attempted.append(message.update_id)
        if message.update_id == 101:
            raise ValueError("permanent domain failure")

    async with _factory(postgres_dsn) as factory:
        durable = DurableTelegramIngress(
            factory,
            settings=INGRESS_SETTINGS,
            bot_id="fixture",
            on_message=business,
            on_control=AsyncMock(),
            retry_seconds=0,
        )
        app = build_long_polling_application(
            "123:FAKE_TOKEN_NOT_A_CREDENTIAL",
            bot_id="fixture",
            admit_update=durable.admit,
            on_message=business,
            on_control=AsyncMock(),
        )
        updater = app.updater
        monkeypatch.setattr(Updater, "_bootstrap", AsyncMock())
        offsets = []

        async def get_updates(_bot, *, offset, **kwargs):
            offsets.append(offset)
            if len(offsets) == 1:
                return [update(app.bot, 101), update(app.bot, 102, chat=CHAT - 1)]
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 2
            acknowledged.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(type(app.bot), "get_updates", get_updates)
        updater._running = True
        durable.start(app.bot)
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
            await asyncio.wait_for(acknowledged.wait(), 3)
            assert offsets == [0, 103]

            async def drained():
                while True:
                    async with factory() as session:
                        rows = (await session.scalars(select(TransactionalOutbox))).all()
                        if {row.status for row in rows} == {
                            DeliveryStatus.DELIVERED,
                            DeliveryStatus.DEAD_LETTER,
                        }:
                            return
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(drained(), 3)
            assert attempted.count(101) == 3
            assert attempted.count(102) == 1
        finally:
            updater._running = False
            pending = updater._Updater__polling_task
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            await durable.close()
