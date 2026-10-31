"""Owner 2026-10-31 (memory step 4a): the executor of a goal step sees the lead's notes."""

import json
import uuid

import pytest
from sqlalchemy import select
from tests.integration.leadership.test_managed_goal_flow import PROJECT, _enable, _factory, _session
from tests.integration.leadership.test_plan_revisions import approve_plan, plan_items, start

from vuzol.leadership.executor_notes import executor_notes_context
from vuzol.storage.models import Task
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import MemoryScope

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_goal_step_carries_project_and_owner_notes(postgres_dsn):
    from tests.integration.leadership.planning_helpers import seed_plan

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": plan_items(1)})
        await approve_plan(factory, goal_id)
        assert (await start(factory, goal_id)).status == "applied"
        async with factory() as session:
            task = await session.scalar(select(Task))
            # No notes yet: nothing extra in the executor's context.
            assert await executor_notes_context(session, task) is None
        async with factory.begin() as session:
            units = MemoryUnitRepository(session)
            await units.create_note(
                scope=MemoryScope.PROJECT,
                project_id=PROJECT,
                goal_id=goal_id,
                text="Тесты гоняем через make test",
                extraction_identity="lead-note:exec-1",
            )
            await units.create_note(
                scope=MemoryScope.GLOBAL,
                project_id=None,
                goal_id=goal_id,
                text="Коммиты пишем по-английски",
                extraction_identity="lead-note:exec-2",
            )
            await units.create_note(
                scope=MemoryScope.PROJECT,
                project_id="other-project",
                goal_id=goal_id,
                text="Чужая заметка",
                extraction_identity="lead-note:exec-3",
            )
        async with factory() as session:
            task = await session.scalar(select(Task))
            item = await executor_notes_context(session, task)
            body = json.loads(item.content)
            assert item.source == "lead_notebook"
            assert body["project_notes"] == ["Тесты гоняем через make test"]
            assert body["owner_notes"] == ["Коммиты пишем по-английски"]
            # A task outside any goal gets no notes.
            loose = Task(id=uuid.uuid4(), project_id=PROJECT)
            assert await executor_notes_context(session, loose) is None
