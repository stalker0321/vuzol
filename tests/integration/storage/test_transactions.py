import asyncio

import pytest
from sqlalchemy import func, select

from vuzol.storage.errors import IllegalTransition
from vuzol.storage.models import Event, Task, TransactionalOutbox
from vuzol.storage.transitions import transition_task
from vuzol.storage.types import TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage


@pytest.mark.postgresql
def test_task_transition_and_event_are_atomic(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=1,
                chat_id=-100,
                original_text="original input retained",
                task_type="general",
            )
            await uow.events.append(
                entity_type="task",
                entity_id=task.id,
                event_type="task.received",
                actor_type="telegram_user",
            )
        async with UnitOfWork(factory) as uow:
            transitioned = await transition_task(
                uow, task_id=task.id, target=TaskStatus.INTERPRETED, actor_type="system"
            )
            assert transitioned.version == 2
        async with factory() as session:
            stored = await session.get(Task, task.id)
            assert stored is not None
            assert stored.original_text == "original input retained"
            assert stored.status is TaskStatus.INTERPRETED
            event_count = await session.scalar(
                select(func.count()).select_from(Event).where(Event.entity_id == task.id)
            )
            assert event_count == 2
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_illegal_transition_and_transaction_failure_roll_back(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=1,
                chat_id=-100,
                original_text="request",
                task_type="general",
            )
        with pytest.raises(IllegalTransition):
            async with UnitOfWork(factory) as uow:
                await transition_task(
                    uow, task_id=task.id, target=TaskStatus.COMPLETED, actor_type="system"
                )
        with pytest.raises(RuntimeError):
            async with UnitOfWork(factory) as uow:
                await uow.outbox.enqueue(
                    destination="telegram",
                    operation_type="send",
                    entity_type="task",
                    entity_id=task.id,
                    idempotency_key="rollback-test",
                    payload={},
                )
                raise RuntimeError("force rollback")
        async with factory() as session:
            record = await session.get(Task, task.id)
            assert record is not None and record.status is TaskStatus.RECEIVED
            outbox_count = await session.scalar(
                select(func.count()).select_from(TransactionalOutbox)
            )
            assert outbox_count == 0
        await engine.dispose()

    asyncio.run(scenario())
