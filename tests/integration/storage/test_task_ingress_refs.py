"""WP09: ingress refs, explicit guards and backfill (PG)."""

from __future__ import annotations

import asyncio
import uuid

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from pytest import MonkeyPatch
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import get_settings
from vuzol.storage.models import Task, TransactionalOutbox
from vuzol.storage.types import TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import (
    build_task_history_report,
    enqueue_task_status_projection,
)

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def _create_task(
    factory: async_sessionmaker[AsyncSession], *, chat_id: int | None, ingress_source: str | None
) -> uuid.UUID:
    async with UnitOfWork(factory) as uow:
        record = await uow.tasks.create(
            user_id=7,
            chat_id=chat_id,
            original_text="ingress probe",
            task_type="coding",
            ingress_source=ingress_source,
        )
        return record.id


@pytest.mark.postgresql
def test_chatless_task_created_and_read_with_ingress_label(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = await _create_task(factory, chat_id=None, ingress_source="cli")
        async with factory() as session:
            task = await session.get(Task, task_id)
        assert task is not None
        assert task.source_chat_id is None
        assert task.ingress_source == "cli"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_projections_skip_chatless_and_zero_chat_explicitly(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        null_id = await _create_task(factory, chat_id=None, ingress_source="cli")
        zero_id = await _create_task(factory, chat_id=0, ingress_source="cli")
        async with factory.begin() as session:
            for task_id in (null_id, zero_id):
                task = await session.get(Task, task_id, with_for_update=True)
                assert task is not None
                task.status = TaskStatus.COMPLETED
        async with factory() as session:
            before = await session.scalar(select(func.count()).select_from(TransactionalOutbox))
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            for task_id in (null_id, zero_id):
                assert await build_task_history_report(uow.session, task_id) is None
                task = await uow.session.get(Task, task_id)
                assert task is not None
                await enqueue_task_status_projection(uow.session, task, None)
        async with factory() as session:
            after = await session.scalar(select(func.count()).select_from(TransactionalOutbox))
        # Explicit skip: zero new outbox rows for chatless/zero-chat tasks.
        assert after == before
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_legacy_rows_backfilled_on_upgrade(postgres_dsn: str, monkeypatch: MonkeyPatch) -> None:
    async def check_backfill() -> None:
        engine, factory = storage(postgres_dsn)
        async with factory() as session:
            task = await session.scalar(select(Task).where(Task.original_text == "legacy"))
        assert task is not None
        assert task.source_chat_id == -100
        assert task.ingress_source == "legacy"
        await engine.dispose()

    async_dsn = postgres_dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    sync_dsn = postgres_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    monkeypatch.setenv("VUZOL_DATABASE_DSN_REFERENCE", "env:VUZOL_DATABASE_DSN")
    monkeypatch.setenv("VUZOL_DATABASE_DSN", async_dsn)
    alembic = Config("alembic.ini")
    get_settings.cache_clear()
    try:
        command.downgrade(alembic, "c4e8f1a92b70")  # pragma: allowlist secret
        with psycopg.connect(sync_dsn, autocommit=True) as connection:
            nullable = connection.execute(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_name='tasks' AND column_name='source_chat_id'"
            ).fetchone()
            assert nullable == ("NO",)
            # Legacy row under the old schema: no ingress_source column.
            connection.execute(
                "INSERT INTO tasks (id, user_id, source_chat_id, original_text, task_type,"
                " status, risk, budget_epoch, version)"
                " VALUES (gen_random_uuid(), 7, -100, 'legacy', 'coding', 'received', 'low', 0, 1)"
            )
        command.upgrade(alembic, "head")
        asyncio.run(check_backfill())
    finally:
        get_settings.cache_clear()
        command.upgrade(alembic, "head")
        get_settings.cache_clear()


@pytest.mark.postgresql
def test_orchestration_trace_without_chat_fails_explicitly(postgres_dsn: str) -> None:
    from vuzol.telegram.delivery import PermanentDeliveryError, prepare_delivery
    from vuzol.telegram.tracing import ORCHESTRATION_TRACE_ROLE

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = await _create_task(factory, chat_id=None, ingress_source="cli")
        async with factory.begin() as session:
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="task",
                    linked_entity_id=task_id,
                    idempotency_key="trace:chatless-1",
                    payload={"role": ORCHESTRATION_TRACE_ROLE, "task_id": str(task_id)},
                )
            )
            await session.flush()
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == "trace:chatless-1"
                )
            )
            assert item is not None
            with pytest.raises(PermanentDeliveryError) as rejected:
                await prepare_delivery(session, item)
            assert "orchestration_trace_chat_missing" in str(rejected.value)
        await engine.dispose()

    asyncio.run(scenario())
