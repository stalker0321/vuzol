"""Rich send/edit is one fenced outcome; fallback never duplicates an unknown send."""

import pytest
from sqlalchemy import select
from telegram.error import NetworkError, RetryAfter

from tests.integration.storage.helpers import storage
from tests.integration.telegram.test_delivery import service
from tests.integration.telegram.test_discussion_chunks import seed_reply
from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.formatting import telegram_markdown_parts, telegram_rich_html
from vuzol.telegram.projections import FakeTelegramClient, RichMessageRejected

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.mark.parametrize("thinking", [False, True])
async def test_short_markdown_stays_ordinary_even_with_rich_capability(postgres_dsn, thinking):
    engine, factory = storage(postgres_dsn)
    text = "# Итог\n\n**Жирный** и *курсив*.\n\n- Пункт\n\n> Цитата\n\n`код`"
    await seed_reply(factory, text, thinking=thinking)
    client = FakeTelegramClient()
    try:
        assert await service(factory, client, rich_messages=True).deliver_one()
        messages = client.edited if thinking else client.sent
        assert len(messages) == 1
        assert messages[0][2] == telegram_markdown_parts(text)[0][0]
        assert "<b>Жирный</b>" in messages[0][2] and "<i>курсив</i>" in messages[0][2]
        assert "<code>код</code>" in messages[0][2] and "<blockquote>" in messages[0][2]
        assert not client.rich_sent and not client.rich_edited
    finally:
        await engine.dispose()


async def test_short_table_selects_rich_but_literal_table_tag_does_not(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    client = FakeTelegramClient()
    try:
        await seed_reply(factory, "| Имя | Значение |\n|---|---|\n| Sol | high |")
        assert await service(factory, client, rich_messages=True).deliver_one()
        assert len(client.rich_sent) == 1 and not client.sent
        await seed_reply(factory, "`<table>` — просто код, не таблица.")
        assert await service(factory, client, rich_messages=True).deliver_one()
        assert len(client.rich_sent) == 1 and len(client.sent) == 1
    finally:
        await engine.dispose()


@pytest.mark.parametrize("thinking", [False, True])
async def test_whole_rich_reply_does_not_enqueue_plain_suffix(postgres_dsn, thinking):
    engine, factory = storage(postgres_dsn)
    text = "# Итог\n\n" + "длинный ответ " * 600 + "\n\n| A | B |\n|---|---|\n| 1 | 2 |"
    await seed_reply(factory, text, thinking=thinking)
    client = FakeTelegramClient()
    try:
        assert await service(factory, client, rich_messages=True).deliver_one()
        messages = client.rich_edited if thinking else client.rich_sent
        assert len(messages) == 1 and messages[0][2] == telegram_rich_html(text)
        assert not client.sent and not client.edited
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(items) == 1 and items[0].status is DeliveryStatus.DELIVERED
        assert not await service(factory, client, rich_messages=True).deliver_one()
    finally:
        await engine.dispose()


class RejectedRichClient(FakeTelegramClient):
    async def send_rich_message(self, **kwargs):
        raise RichMessageRejected("unsupported rich content")

    async def edit_rich_message(self, **kwargs):
        raise RichMessageRejected("unsupported rich content")


@pytest.mark.parametrize("thinking", [False, True])
async def test_definite_rejection_falls_back_to_complete_ordered_chunks(postgres_dsn, thinking):
    engine, factory = storage(postgres_dsn)
    text = "я" * 9000
    await seed_reply(factory, text, thinking=thinking)
    client = RejectedRichClient()
    try:
        parts = telegram_markdown_parts(text)
        for _ in parts:
            assert await service(factory, client, rich_messages=True).deliver_one()
        delivered = ([m[2] for m in client.edited] if thinking else []) + [
            m[2] for m in client.sent
        ]
        assert delivered == [p[0] for p in parts]
        assert not client.rich_sent and not client.rich_edited
    finally:
        await engine.dispose()


@pytest.mark.parametrize("missing_id", [False, True])
async def test_unknown_rich_send_does_not_send_ordinary_fallback(postgres_dsn, missing_id):
    class UnknownClient(FakeTelegramClient):
        async def send_rich_message(self, **kwargs):
            if missing_id:
                return 0
            raise NetworkError("send outcome unknown")

    engine, factory = storage(postgres_dsn)
    await seed_reply(factory, "я" * 9000)
    client = UnknownClient()
    try:
        assert await service(factory, client, rich_messages=True).deliver_one()
        assert not client.sent and not client.edited
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(items) == 1 and items[0].status is DeliveryStatus.AMBIGUOUS
        assert not await service(factory, FakeTelegramClient(), rich_messages=True).deliver_one()
    finally:
        await engine.dispose()


async def test_rate_limited_rich_reply_defers_without_fallback_or_suffix(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    await seed_reply(factory, "я" * 9000)
    client = FakeTelegramClient(fail=RetryAfter(1))
    try:
        assert await service(factory, client, rich_messages=True).deliver_one()
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(items) == 1 and items[0].status is DeliveryStatus.PENDING
            assert items[0].attempt_count == 0
        assert not client.sent and not client.edited
    finally:
        await engine.dispose()


async def test_reply_exceeding_rich_wire_limit_is_still_complete(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    text = "я" * 40_000
    await seed_reply(factory, text)
    client = FakeTelegramClient()
    try:
        parts = telegram_markdown_parts(text)
        for _ in parts:
            assert await service(factory, client, rich_messages=True).deliver_one()
        assert [m[2] for m in client.sent] == [p[0] for p in parts]
        assert not client.rich_sent
    finally:
        await engine.dispose()
