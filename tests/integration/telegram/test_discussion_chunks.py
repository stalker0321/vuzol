"""Durable ordered multi-message replies, with real TEST DB and fake Telegram."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from telegram.error import NetworkError, RetryAfter

from tests.integration.storage.helpers import storage
from tests.integration.telegram.test_delivery import service
from tests.unit.test_telegram_reply_chunks import visible
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import claim_outbox_item
from vuzol.storage.models import TelegramMessageLink, TransactionalOutbox
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    DeliveryStatus,
    InteractionMode,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import PermanentDeliveryError, prepare_delivery
from vuzol.telegram.formatting import telegram_markdown_html, telegram_markdown_parts
from vuzol.telegram.projections import FakeTelegramClient

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def seed_reply(factory, text: str, *, thinking: bool = False):
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="test",
            chat_id=-100,
            message_thread_id=10,
        )
        source_id, _ = await uow.discussions.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content="Обсудим.",
            classifier_mode=InteractionMode.DISCUSSION,
        )
        turn_id, _ = await uow.discussions.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.ASSISTANT,
            source=ConversationTurnSource.MODEL,
            content=text,
            classifier_mode=InteractionMode.DISCUSSION,
        )
        if thinking:
            await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=777,
                    message_role=f"thinking:{source_id.hex}",
                )
            )
        item_id = await uow.outbox.enqueue(
            destination="telegram",
            operation_type="send_message",
            entity_type="conversation_turn",
            entity_id=turn_id,
            idempotency_key=f"discussion_reply:{turn_id}",
            payload={
                "role": "discussion_reply",
                "session_id": str(session_id),
                "source_turn_id": str(source_id),
            },
        )
    return item_id


@pytest.mark.parametrize("thinking", [False, True])
async def test_ordered_parts_restart_and_no_duplicate_head(postgres_dsn, thinking):
    engine, factory = storage(postgres_dsn)
    text = "**" + "длинный ответ 😀 & <значение> " * 450 + "**"
    item_id = await seed_reply(factory, text, thinking=thinking)
    client = FakeTelegramClient()
    parts = telegram_markdown_parts(text)
    try:
        assert await service(factory, client).deliver_one()
        async with factory() as session:
            head = await session.get(TransactionalOutbox, item_id)
            assert head.status is DeliveryStatus.DELIVERED
            items = (
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["role"].astext == "discussion_reply"
                    )
                )
            ).all()
            assert len(items) == 2
            assert sorted(i.payload.get("reply_part_index", 0) for i in items) == [0, 1]
        if thinking:
            assert client.edited == [(-100, 777, parts[0][0])]
            assert not client.sent
        else:
            assert len(client.sent) == 1
        # A new delivery instance resumes only the queued continuation.
        for _ in parts[1:]:
            assert await service(factory, client, owner="restarted").deliver_one()
        sent = ([client.edited[0][2]] if thinking else []) + [m[2] for m in client.sent]
        assert sent == [p[0] for p in parts]
        assert "".join(visible(p) for p in sent) == visible(telegram_markdown_html(text))
        assert all(len(visible(p).encode("utf-16-le")) // 2 <= 4096 for p in sent)
        async with factory() as session:
            items = (
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["role"].astext == "discussion_reply"
                    )
                )
            ).all()
            assert len(items) == len(parts)
            assert all(i.status is DeliveryStatus.DELIVERED for i in items)
            assert len({i.idempotency_key for i in items}) == len(parts)
            links = (await session.scalars(select(TelegramMessageLink))).all()
            assert len(links) == len(parts)
            assert all(link.task_id is None for link in links)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("thinking", [False, True])
async def test_retry_after_holds_continuation_until_confirmation(postgres_dsn, thinking):
    engine, factory = storage(postgres_dsn)
    item_id = await seed_reply(factory, "я" * 9000, thinking=thinking)
    client = FakeTelegramClient(fail=RetryAfter(1))
    try:
        assert await service(factory, client).deliver_one()
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(items) == 1 and items[0].status is DeliveryStatus.PENDING
            assert items[0].attempt_count == 0
        assert not await service(factory, client, owner="other").deliver_one()
        client.fail = None
        async with factory.begin() as session:
            item = await session.get(TransactionalOutbox, item_id)
            item.available_at = datetime.now(UTC) - timedelta(seconds=1)
        assert await service(factory, client).deliver_one()
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(items) == 2
    finally:
        await engine.dispose()


async def test_lost_send_response_does_not_send_later_parts(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    item_id = await seed_reply(factory, "я" * 9000)
    try:
        client = FakeTelegramClient(fail=NetworkError("outcome unknown"))
        assert await service(factory, client).deliver_one()
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(items) == 1 and items[0].id == item_id
            assert items[0].status is DeliveryStatus.AMBIGUOUS
        assert not await service(factory, FakeTelegramClient()).deliver_one()
    finally:
        await engine.dispose()


async def test_stale_completion_rolls_back_continuation(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    await seed_reply(factory, "я" * 9000)
    try:
        async with factory.begin() as session:
            token = await claim_outbox_item(
                session, owner="old", lease_seconds=30, allowed_destinations=frozenset({"telegram"})
            )
        assert token is not None
        async with factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            prepared = await prepare_delivery(session, item)
        async with factory.begin() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            item.lease_generation += 1
        with pytest.raises(LeaseLost):
            await service(factory, FakeTelegramClient())._complete(token, prepared, 42)
        async with factory() as session:
            items = (await session.scalars(select(TransactionalOutbox))).all()
            assert len(items) == 1
            assert not (await session.scalars(select(TelegramMessageLink))).all()
    finally:
        await engine.dispose()


@pytest.mark.parametrize("index", [True, -1, "1", 100])
async def test_invalid_part_metadata_fails_closed(postgres_dsn, index):
    engine, factory = storage(postgres_dsn)
    item_id = await seed_reply(factory, "text")
    try:
        async with factory.begin() as session:
            item = await session.get(TransactionalOutbox, item_id)
            item.payload = {**item.payload, "reply_part_index": index}
        async with factory() as session:
            item = await session.get(TransactionalOutbox, item_id)
            with pytest.raises(PermanentDeliveryError, match="discussion_reply_part_invalid"):
                await prepare_delivery(session, item)
    finally:
        await engine.dispose()
