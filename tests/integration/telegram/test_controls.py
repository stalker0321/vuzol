import asyncio
import uuid
from pathlib import Path

import pytest
from sqlalchemy import func, select

from vuzol.storage.models import TelegramControlAction, TransactionalOutbox
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, IngressStatus

from ..storage.helpers import storage
from .helpers import telegram_runtime


@pytest.mark.postgresql
def test_callback_is_persisted_idempotently_without_executing_action(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="task",
                task_type="general",
            )
        update = ControlUpdate(
            bot_id="main",
            update_id=20,
            callback_query_id="callback-1",
            chat_id=-100,
            user_id=42,
            action_kind="cancel",
            task_id=task.id,
        )
        service = TelegramControlService(telegram_runtime(tmp_path), factory)
        first = await service.accept(update)
        duplicate = await service.accept(update)
        assert first.status is IngressStatus.CREATED
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            assert (
                await session.scalar(select(func.count()).select_from(TelegramControlAction)) == 1
            )
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_control_rejects_unauthorized_or_missing_target(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramControlService(telegram_runtime(tmp_path), factory)
        unauthorized = await service.accept(
            ControlUpdate(
                bot_id="main",
                update_id=21,
                callback_query_id="callback-2",
                chat_id=-100,
                user_id=999,
                action_kind="cancel",
                task_id=uuid.uuid4(),
            )
        )
        missing = await service.accept(
            ControlUpdate(
                bot_id="main",
                update_id=22,
                callback_query_id="callback-3",
                chat_id=-100,
                user_id=42,
                action_kind="cancel",
                task_id=uuid.uuid4(),
            )
        )
        assert unauthorized.status is IngressStatus.REJECTED
        assert missing.status is IngressStatus.REJECTED
        await engine.dispose()

    asyncio.run(scenario())
