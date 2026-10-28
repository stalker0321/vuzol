"""The repository issues ranked, bounded FTS rather than discussion fallback."""

import uuid
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from vuzol.storage.repositories.memory_units import MemoryUnitRepository


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_recall_uses_one_ownership_scoped_ranked_query():
    session = AsyncMock()
    session.scalars.return_value = Mock(all=lambda: [])
    assert (
        await MemoryUnitRepository(session).recall_lead(
            goal_id=uuid.uuid4(), query="durable choice", project="all", limit=2
        )
        == ()
    )
    statement = session.scalars.call_args.args[0]
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "@@ plainto_tsquery(" in sql
    assert "simple" in compiled.params.values()
    assert "durable choice" in compiled.params.values()
    assert "ORDER BY ts_rank(" in sql
    assert "created_at DESC" in sql and "LIMIT" in sql
    assert 2 in compiled.params.values()
    assert "owner_user_id" in sql and "requested_by_user_id" in sql
    assert ["decision_entry", "notebook_note"] in compiled.params.values()
    assert "superseded_by IS NULL" in sql
    session.scalars.assert_awaited_once()


@pytest.mark.anyio
async def test_empty_query_never_falls_back_to_recent_rows():
    session = AsyncMock()
    assert await MemoryUnitRepository(session).recall_lead(goal_id=uuid.uuid4(), query=" ") == ()
    session.scalars.assert_not_called()
