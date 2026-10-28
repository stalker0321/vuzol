"""Read tools have no write transaction and fail closed on lost authority."""

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_tool_loop import snapshot

from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import TurnInvocationContext, current_turn
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.read_tools import execute_read, parse_tool_call


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Factory:
    def __init__(self, session):
        self.session = session

    @asynccontextmanager
    async def begin(self):
        yield self.session


@pytest.mark.anyio
@pytest.mark.parametrize("permitted", [True, False])
async def test_tools_recheck_authority_in_a_read_only_transaction(monkeypatch, permitted):
    source = snapshot()
    session = AsyncMock()
    session.get.side_effect = [
        SimpleNamespace(id=source.goal_id, delegation_id=uuid.uuid4()),
        object(),
    ]
    context = TurnInvocationContext(
        TurnClaim(source.goal_id, uuid.uuid4(), "worker", 1, 1),
        uuid.uuid4(),
        source.project_id,
        1,
        1,
        1,
    )
    guard = AsyncMock(return_value=permitted)
    projector = AsyncMock(return_value={"lifecycle": "active"})
    monkeypatch.setattr("vuzol.leadership.read_tools.invocation_current", guard)
    monkeypatch.setattr("vuzol.leadership.service.read_projection", projector, raising=False)
    token = current_turn.set(context)
    try:
        if permitted:
            result = await execute_read(
                Factory(session), source, parse_tool_call({"name": "read_status", "args": {}})
            )
            assert result == {"lifecycle": "active"}
            projector.assert_awaited_once()
        else:
            with pytest.raises(LeadModelUnavailable):
                await execute_read(
                    Factory(session), source, parse_tool_call({"name": "read_status", "args": {}})
                )
            projector.assert_not_called()
        assert str(session.execute.call_args_list[0].args[0]) == "SET TRANSACTION READ ONLY"
        guard.assert_awaited_once_with(session, context, lock=False)
        session.add.assert_not_called()
        session.flush.assert_not_called()
    finally:
        current_turn.reset(token)


@pytest.mark.anyio
async def test_reads_require_server_turn_context():
    with pytest.raises(LeadModelUnavailable):
        await execute_read(
            Factory(AsyncMock()), snapshot(), parse_tool_call({"name": "read_result", "args": {}})
        )
