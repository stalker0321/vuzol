"""Ranked lead recall uses persisted ownership and never recent-row fallback."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    PROJECT,
    _enable,
    _factory,
    _session,
)
from tests.integration.leadership.test_notebooks_pg import other_goal

from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import MemoryUnit
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import MemoryScope, MemoryUnitStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_recall_ranking_scope_lifecycle_and_details(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        sibling = await other_goal(factory)
        async with factory.begin() as session:
            repo = MemoryUnitRepository(session)

            async def note(text, *, project=PROJECT, goal=goal_id, status="observation"):
                return await repo.create_unit(
                    text=text,
                    unit_type="notebook_note",
                    project_id=project,
                    scope=MemoryScope.PROJECT,
                    goal_id=goal,
                    status=MemoryUnitStatus(status),
                    extraction_identity=str(uuid.uuid4()),
                    extractor_version="test",
                    effective_at=datetime.now(UTC),
                )

            high = await note("orchard orchard orchard")
            high.created_at = datetime.now(UTC) - timedelta(days=1)
            low = await note("orchard")
            other = await note("orchard sibling", project="other", goal=sibling)
            for status in ("retracted", "superseded", "tombstoned", "hypothesis"):
                await note("orchard hidden", status=status)
            decision = await repo.create_decision(
                project_id=PROJECT,
                goal_id=goal_id,
                text="A durable choice",
                details={"options": ["kumquat"], "links": {"commits": ["citruscommit"]}},
                extraction_identity=str(uuid.uuid4()),
            )
            # Neither unrelated derived memory nor an orphan project is recallable.
            await note("orchard orphan", project="unowned", goal=None)
            await repo.create_unit(
                text="orchard derived",
                unit_type="lesson",
                project_id=PROJECT,
                status=MemoryUnitStatus.OBSERVATION,
                extraction_identity=str(uuid.uuid4()),
                extractor_version="test",
                effective_at=datetime.now(UTC),
            )
            ids = high.id, low.id, other.id, decision.id
        async with factory() as session:
            repo = MemoryUnitRepository(session)

            async def recall(query, project="this", limit=8):
                return await repo.recall_lead(
                    goal_id=goal_id, query=query, project=project, limit=limit
                )

            assert [u.id for u in await recall("orchard")] == list(ids[:2])
            assert {u.id for u in await recall("orchard", "all")} == set(ids[:3])
            assert len(await recall("orchard", "all", 1)) == 1
            assert [u.id for u in await recall("kumquat")] == [ids[3]]
            assert [u.id for u in await recall("citruscommit")] == [ids[3]]
            assert await recall("no-such-memory") == ()
            assert await recall("   ") == ()
            assert await repo.recall_lead(goal_id=uuid.uuid4(), query="orchard") == ()
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, sibling)
            goal.owner_user_id = OWNER + 1
        async with factory() as session:
            hits = await MemoryUnitRepository(session).recall_lead(
                goal_id=goal_id, query="orchard", project="all"
            )
            assert {u.id for u in hits} == set(ids[:2])
            assert all(isinstance(u, MemoryUnit) for u in hits)
