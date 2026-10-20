"""Poison updates, restart/replay and fencing through the production ingress."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr
from sqlalchemy import func, select
from telegram import Bot, Update

from tests.integration.leadership.test_managed_goal_flow import (
    CHAT,
    OWNER,
    THREAD,
    _factory,
    _runtime,
)
from vuzol.cli import outbox_replay
from vuzol.cli.telegram import ingress_handlers
from vuzol.config import Settings
from vuzol.storage.models import ExternalInbox, Task, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus, InboxStatus
from vuzol.telegram.durable_ingress import DurableTelegramIngress

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
INGRESS_SETTINGS = Settings(
    environment="test", allowed_user_ids=(OWNER,), allowed_chat_ids=(CHAT, CHAT - 1)
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def update(bot, number, *, chat=CHAT):
    return Update.de_json(
        {
            "update_id": number,
            "message": {
                "message_id": number,
                "message_thread_id": THREAD,
                "date": 1792000000,
                "from": {"id": OWNER, "is_bot": False, "first_name": "Owner"},
                "chat": {"id": chat, "type": "supergroup", "title": "Fixture"},
                "text": "Implement a marker",
            },
        },
        bot,
    )


async def test_poison_update_does_not_block_later_chats_and_cli_replay_works(
    postgres_dsn,
    tmp_path,
    monkeypatch,
):
    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    attempts = []
    poison = True

    async def handle(message):
        attempts.append(message.update_id)
        if message.update_id == 101 and poison:
            raise ValueError("private exception must not be persisted")

    async with _factory(postgres_dsn) as factory:
        ingress = DurableTelegramIngress(
            factory,
            settings=INGRESS_SETTINGS,
            bot_id="fixture",
            on_message=handle,
            on_control=AsyncMock(),
            retry_seconds=0,
        )
        await ingress.admit(update(bot, 101))
        await ingress.admit(update(bot, 102, chat=CHAT - 1))
        for _ in range(4):
            assert await ingress.process_one(bot)
        assert attempts == [101, 102, 101, 101]
        assert not await ingress.process_one(bot)
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            failed = next(row for row in items if row.status is DeliveryStatus.DEAD_LETTER)
            assert failed.attempt_count == 3
            assert failed.last_error_category == "telegram_ingress_ValueError"
            assert (
                await session.get(ExternalInbox, failed.linked_entity_id)
            ).status is InboxStatus.FAILED
            assert sum(row.status is DeliveryStatus.DELIVERED for row in items) == 1
        repository = tmp_path / "repositories/repo"
        repository.mkdir(parents=True)
        runtime = _runtime(tmp_path, repository, lead=False, discussion=False)
        monkeypatch.setattr(outbox_replay, "get_runtime_configuration", lambda **_: runtime)
        monkeypatch.setattr(
            outbox_replay, "resolve_database_dsn", lambda _: SecretStr(postgres_dsn)
        )
        # Real CLI transaction: dry-run does not mutate; apply resets the retry budget.
        args = ["--destination", ingress.destination, "--item-id", str(failed.id), "--json"]
        assert await outbox_replay._run(outbox_replay._parse_args(args)) == 0
        assert not await ingress.process_one(bot)
        assert await outbox_replay._run(outbox_replay._parse_args([*args, "--apply"])) == 0
        poison = False
        assert await ingress.process_one(bot)
        async with factory() as session:
            item = await session.get(TransactionalOutbox, failed.id)
            assert item.status is DeliveryStatus.DELIVERED
            assert item.attempt_count == 1
            assert (
                await session.get(ExternalInbox, item.linked_entity_id)
            ).status is InboxStatus.PROCESSED


async def test_crash_after_business_commit_before_ack_replays_without_duplicate_task(
    postgres_dsn,
    tmp_path,
    monkeypatch,
):
    from vuzol.telegram import durable_ingress

    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    repository = tmp_path / "repositories/repo"
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=False, discussion=False)
    async with _factory(postgres_dsn) as factory:
        on_message, on_control = ingress_handlers(runtime, factory)
        ingress = DurableTelegramIngress(
            factory,
            settings=runtime.settings,
            bot_id="fixture",
            on_message=on_message,
            on_control=on_control,
        )
        await ingress.admit(update(bot, 101))
        real_ack = durable_ingress.complete_outbox_item
        monkeypatch.setattr(
            durable_ingress, "complete_outbox_item", AsyncMock(side_effect=ConnectionError)
        )
        with pytest.raises(ConnectionError):
            await ingress.process_one(bot)
        async with factory.begin() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == ingress.destination,
                )
            )
            assert item.status is DeliveryStatus.LEASED
            item.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        monkeypatch.setattr(durable_ingress, "complete_outbox_item", real_ack)
        restarted = DurableTelegramIngress(
            factory,
            settings=runtime.settings,
            bot_id="fixture",
            on_message=on_message,
            on_control=on_control,
        )
        assert await restarted.process_one(bot)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert (
                await session.get(TransactionalOutbox, item.id)
            ).status is DeliveryStatus.DELIVERED


async def test_late_worker_cannot_ack_successor_lease(postgres_dsn):
    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    entered, release = asyncio.Event(), asyncio.Event()

    async def slow(_message):
        entered.set()
        await release.wait()

    async with _factory(postgres_dsn) as factory:
        old = DurableTelegramIngress(
            factory,
            settings=INGRESS_SETTINGS,
            bot_id="fixture",
            on_message=slow,
            on_control=AsyncMock(),
        )
        await old.admit(update(bot, 101))
        pending = asyncio.create_task(old.process_one(bot))
        await asyncio.wait_for(entered.wait(), 2)
        try:
            async with factory.begin() as session:
                item = await session.scalar(select(TransactionalOutbox))
                item.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            new = DurableTelegramIngress(
                factory,
                settings=INGRESS_SETTINGS,
                bot_id="fixture",
                on_message=AsyncMock(),
                on_control=AsyncMock(),
            )
            assert await new.process_one(bot)
            release.set()
            await pending
            async with factory() as session:
                row = await session.get(TransactionalOutbox, item.id)
                assert row.status is DeliveryStatus.DELIVERED
                assert row.lease_generation == 2
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


async def test_processing_timeout_quarantines_without_blocking_admission(postgres_dsn):
    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    handled = []

    async def handler(message):
        if message.update_id == 101:
            await asyncio.Event().wait()
        handled.append(message.update_id)

    async with _factory(postgres_dsn) as factory:
        ingress = DurableTelegramIngress(
            factory,
            settings=INGRESS_SETTINGS,
            bot_id="fixture",
            on_message=handler,
            on_control=AsyncMock(),
            processing_seconds=0.05,
            max_attempts=1,
        )
        await ingress.admit(update(bot, 101))
        await ingress.admit(update(bot, 102))
        assert await ingress.process_one(bot)
        assert await ingress.process_one(bot)
        assert handled == [102]
        async with factory() as session:
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.status == DeliveryStatus.DEAD_LETTER,
                )
            )
            assert item.last_error_category == "telegram_ingress_TimeoutError"


@pytest.mark.parametrize("invalid", ["domain", "oversized"])
async def test_invalid_update_has_durable_failure_receipt(postgres_dsn, monkeypatch, invalid):
    from vuzol.telegram import adapter, durable_ingress

    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    handler = AsyncMock()
    if invalid == "domain":

        def reject_domain(*args):
            raise ValueError("invalid domain fields")

        monkeypatch.setattr(adapter, "message_update", reject_domain)
    else:
        monkeypatch.setattr(durable_ingress, "_MAX_UPDATE_BYTES", 10)
    async with _factory(postgres_dsn) as factory:
        ingress = DurableTelegramIngress(
            factory,
            settings=INGRESS_SETTINGS,
            bot_id="fixture",
            on_message=handler,
            on_control=AsyncMock(),
            max_attempts=1,
        )
        raw = update(bot, 101)
        await ingress.admit(raw)
        await ingress.admit(raw)
        assert await ingress.process_one(bot)
        handler.assert_not_awaited()
        async with factory() as session:
            rows = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(rows) == 1
            assert rows[0].status is DeliveryStatus.DEAD_LETTER
            inbox = await session.get(ExternalInbox, rows[0].linked_entity_id)
            assert inbox.status is InboxStatus.FAILED
            assert inbox.outcome["outbox_id"] == str(rows[0].id)
            if invalid == "oversized":
                assert "update_json" not in rows[0].payload
                assert rows[0].payload["rejected"] == "update_too_large"


@pytest.mark.parametrize("forbidden", ["user", "chat", "callback_user"])
async def test_unauthorized_update_never_persists_body_or_dispatches(postgres_dsn, forbidden):
    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    body = update(bot, 101).to_dict()
    if forbidden == "user":
        body["message"]["from"]["id"] = OWNER + 999
    elif forbidden == "chat":
        body["message"]["chat"]["id"] = CHAT - 999
    else:
        message = body.pop("message")
        body["callback_query"] = {
            "id": "forbidden",
            "from": {"id": OWNER + 999, "is_bot": False, "first_name": "X"},
            "message": message,
            "chat_instance": "fixture",
            "data": "v1:approve:private",
        }
    raw = Update.de_json(body, bot)
    async with _factory(postgres_dsn) as factory:
        handler = AsyncMock()
        ingress = DurableTelegramIngress(
            factory,
            settings=INGRESS_SETTINGS,
            bot_id="fixture",
            on_message=handler,
            on_control=handler,
        )
        await ingress.admit(raw)
        assert not await ingress.process_one(bot)
        handler.assert_not_awaited()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 0
            inbox = await session.scalar(select(ExternalInbox))
            assert inbox.status is InboxStatus.REJECTED
            assert inbox.outcome == {"reason": "identity_not_authorized"}
            assert inbox.raw_payload_reference is None


async def test_null_byte_message_is_durable_and_cannot_block_next_task(postgres_dsn, tmp_path):
    bot = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
    repository = tmp_path / "repositories/repo"
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=False, discussion=False)
    body = update(bot, 101).to_dict()
    body["message"]["text"] = "Invalid PostgreSQL text: \x00"
    raw = Update.de_json(body, bot)
    async with _factory(postgres_dsn) as factory:
        on_message, on_control = ingress_handlers(runtime, factory)
        ingress = DurableTelegramIngress(
            factory,
            settings=runtime.settings,
            bot_id="fixture",
            on_message=on_message,
            on_control=on_control,
            retry_seconds=0,
        )
        await ingress.admit(raw)
        await ingress.admit(update(bot, 102))
        for _ in range(4):
            assert await ingress.process_one(bot)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            jobs = (
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == ingress.destination,
                    )
                )
            ).all()
            assert {job.status for job in jobs} == {
                DeliveryStatus.DEAD_LETTER,
                DeliveryStatus.DELIVERED,
            }
            failed = next(job for job in jobs if job.status is DeliveryStatus.DEAD_LETTER)
            assert failed.attempt_count == 3
            assert "\\u0000" in failed.payload["update_json"]
