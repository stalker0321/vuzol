import asyncio
import uuid

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import storage
from vuzol.storage.models import TelegramMessageLink
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import (
    FakeTelegramClient,
    LostTelegramResponse,
    StatusCard,
    apply_status_projection,
    build_status_card,
)

pytestmark = pytest.mark.postgresql


def test_status_card_rebuild_and_revision_guard(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text='<script>alert("x")</script>',
                task_type="coding",
            )
        client = FakeTelegramClient()
        async with factory() as session, session.begin():
            card = await build_status_card(session, task.id)
            assert "<b>Задача №100001</b>" in card.html
            assert "&lt;script&gt;" not in card.html
            assert await apply_status_projection(
                session, client, card=card, chat_id=-100, thread_id=10
            )
        async with factory() as session, session.begin():
            stale = StatusCard(task.id, card.revision - 1, "stale")
            assert not await apply_status_projection(
                session, client, card=stale, chat_id=-100, thread_id=10
            )
            newer = StatusCard(task.id, card.revision + 1, "new")
            assert await apply_status_projection(
                session, client, card=newer, chat_id=-100, thread_id=10
            )
        assert len(client.sent) == 1
        assert client.edited == [(-100, 1, "new")]
        await engine.dispose()

    asyncio.run(scenario())


def test_failed_or_lost_send_does_not_create_projection_link(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = uuid.uuid4()
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                original_text="request",
                task_type="coding",
            )
            task_id = task.id
        client = FakeTelegramClient(fail=LostTelegramResponse("unknown outcome"))
        async with factory() as session:
            with pytest.raises(LostTelegramResponse):
                await apply_status_projection(
                    session,
                    client,
                    card=StatusCard(task_id, 1, "status"),
                    chat_id=-100,
                    thread_id=10,
                )
            await session.rollback()
        async with factory() as session:
            assert await session.scalar(select(TelegramMessageLink.id)) is None
        await engine.dispose()

    asyncio.run(scenario())
