import asyncio
from pathlib import Path

import pytest
from sqlalchemy import func, select

from vuzol.storage.models import (
    ExternalInbox,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram import TelegramIngressService
from vuzol.telegram.domain import IngressStatus, MessageUpdate

from ..storage.helpers import storage
from .helpers import telegram_runtime


def message(update_id: int, message_id: int, **changes: object) -> MessageUpdate:
    values: dict[str, object] = {
        "bot_id": "main",
        "update_id": update_id,
        "chat_id": -100,
        "message_thread_id": 10,
        "message_id": message_id,
        "user_id": 42,
        "text": "create a task",
    }
    values.update(changes)
    return MessageUpdate.model_validate(values)


@pytest.mark.postgresql
def test_authorized_project_intake_is_atomic_and_duplicate_safe(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(telegram_runtime(tmp_path), factory)
        first = await service.accept_message(message(1, 100))
        duplicate = await service.accept_message(message(1, 100))

        assert first.status is IngressStatus.CREATED and first.task_id is not None
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 1
            assert await session.scalar(select(func.count()).select_from(TopicMapping)) == 1
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_unauthorized_input_creates_no_persisted_content(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(telegram_runtime(tmp_path), factory)
        result = await service.accept_message(message(1, 100, user_id=999))
        assert result.status is IngressStatus.REJECTED
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 0
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_reply_has_affinity_and_multiple_active_tasks_force_clarification(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        async with UnitOfWork(factory) as uow:
            task_a = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="task A",
                task_type="general",
            )
            await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="task B",
                task_type="general",
            )
            await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=50,
                    task_id=task_a.id,
                    message_role="status_card",
                )
            )
        service = TelegramIngressService(runtime, factory)
        reply = await service.accept_message(message(2, 101, reply_to_message_id=50))
        ambiguous = await service.accept_message(message(3, 102))

        assert reply.status is IngressStatus.CONTINUATION and reply.task_id == task_a.id
        assert ambiguous.status is IngressStatus.NEEDS_CLARIFICATION
        async with factory() as session:
            intake = await session.scalar(
                select(TelegramIntakeMessage).where(TelegramIntakeMessage.id == ambiguous.intake_id)
            )
            assert intake is not None and len(intake.ambiguous_task_ids) == 2
        await engine.dispose()

    asyncio.run(scenario())
