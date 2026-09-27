"""WP09: real Telegram control path and CLI drive identical transitions (PG)."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.workflows._test_runtime_helpers import (
    Settings,
    Task,
    TransactionalOutbox,
    WorkflowControlConsumer,
    asyncio,
    compile_workflow,
    materialize_run,
    seed_interpreted,
    simple_draft,
    storage,
)
from vuzol.storage.models import Event, TelegramControlAction
from vuzol.workflows.application import Principal, TaskControlService

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def _live_task_id(
    factory: async_sessionmaker[AsyncSession],
) -> tuple[uuid.UUID, int]:
    task_id, interpretation_id = await seed_interpreted(factory)
    async with factory.begin() as session:
        await materialize_run(
            session,
            task_id=task_id,
            workflow=compile_workflow(simple_draft(), interpretation_id=interpretation_id),
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            prompt_revision=None,
            automatic_start=True,
        )
    async with factory() as session:
        task = await session.get(Task, task_id)
        assert task is not None
        return task_id, task.version


async def _pause_via_telegram(
    factory: async_sessionmaker[AsyncSession], task_id: uuid.UUID, key: str
) -> None:
    async with factory.begin() as session:
        session.add(
            TelegramControlAction(
                external_action_id=key,
                action_kind="pause",
                requested_by_user_id=1,
                task_id=task_id,
                payload={},
            )
        )
        await session.flush()
        action = await session.scalar(
            select(TelegramControlAction).where(TelegramControlAction.external_action_id == key)
        )
        assert action is not None
        session.add(
            TransactionalOutbox(
                destination="workflow_control",
                operation_type="pause",
                linked_entity_type="telegram_control_action",
                linked_entity_id=action.id,
                idempotency_key=f"workflow-control:{key}",
                payload={},
            )
        )
    consumer = WorkflowControlConsumer(Settings(environment="test"), factory, owner="control")
    assert await consumer.process_one()


@pytest.mark.postgresql
def test_telegram_path_and_cli_reach_identical_state(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        telegram_task_id, telegram_generation = await _live_task_id(factory)
        await _pause_via_telegram(factory, telegram_task_id, "parity-tg-1")
        cli_task_id, cli_generation = await _live_task_id(factory)
        service = TaskControlService(factory)
        result = await service.execute(
            task_id=cli_task_id,
            command="pause",
            principal=Principal(user_id=7, ingress_source="cli"),
            expected_task_version=cli_generation,
        )
        async with factory() as session:
            telegram_task = await session.get(Task, telegram_task_id)
            cli_task = await session.get(Task, cli_task_id)
            assert telegram_task is not None and cli_task is not None
        assert telegram_task.status.value == cli_task.status.value == "paused"
        assert telegram_task.version - telegram_generation == cli_task.version - cli_generation == 1
        assert result.applied is True
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_idempotent_command_retry_applies_single_transition(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, generation = await _live_task_id(factory)
        service = TaskControlService(factory)
        principal = Principal(user_id=7, ingress_source="cli")
        first = await service.execute(
            task_id=task_id,
            command="pause",
            principal=principal,
            expected_task_version=generation,
        )
        assert first.applied is True
        with pytest.raises(ValueError, match="stale task version"):
            await service.execute(
                task_id=task_id,
                command="pause",
                principal=principal,
                expected_task_version=generation,
            )
        async with factory() as session:
            pauses = (
                await session.scalars(
                    select(Event).where(
                        Event.entity_id == task_id, Event.event_type == "task.pause_effective"
                    )
                )
            ).all()
        assert len(pauses) == 1
        await engine.dispose()

    asyncio.run(scenario())
