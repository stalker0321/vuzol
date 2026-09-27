"""WP09: Telegram and CLI transports drive identical domain transitions (PG)."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.workflows._test_runtime_helpers import (
    Task,
    asyncio,
    compile_workflow,
    materialize_run,
    seed_interpreted,
    simple_draft,
    storage,
)
from vuzol.storage.models import Event
from vuzol.workflows.application import Principal, TaskControlService

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def _paused_task_id(factory: async_sessionmaker[AsyncSession]) -> uuid.UUID:
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
    return task_id


@pytest.mark.postgresql
def test_telegram_and_cli_pause_reaches_identical_state(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        trajectories = []
        for ingress in ("telegram", "cli"):
            task_id = await _paused_task_id(factory)
            service = TaskControlService(factory)
            async with factory() as session:
                task = await session.get(Task, task_id)
                assert task is not None
                generation = task.version
            result = await service.execute(
                task_id=task_id,
                command="pause",
                principal=Principal(user_id=7, ingress_source=ingress),
                expected_task_version=generation,
            )
            trajectories.append((result.status, result.version - generation, result.applied))
        assert trajectories[0] == trajectories[1] == ("paused", 1, True)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_idempotent_command_retry_applies_single_transition(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = await _paused_task_id(factory)
        service = TaskControlService(factory)
        principal = Principal(user_id=7, ingress_source="cli")
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            generation = task.version
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
