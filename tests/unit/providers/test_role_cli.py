import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.model_calls.queue import ModelCallClaim, current_model_call
from vuzol.providers.role_cli import RoleBoundCliAdapter
from vuzol.workflows.ports import CancellationContext

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def adapters():
    return [MagicMock(execute=AsyncMock(return_value=name)) for name in ("w", "r", "t", "c")]


async def test_a_model_call_goes_only_to_the_model_call_transport() -> None:
    workflow, readonly, turn, call = adapters()
    token = current_model_call.set(ModelCallClaim(uuid.uuid4(), "executor", 1))
    try:
        result = await RoleBoundCliAdapter(workflow, readonly, turn, call).execute(
            MagicMock(), MagicMock(), CancellationContext()
        )
        assert result is call.execute.return_value
        with pytest.raises(ValueError, match="model calls"):
            await RoleBoundCliAdapter(workflow, readonly, turn).execute(
                MagicMock(), MagicMock(), CancellationContext()
            )
    finally:
        current_model_call.reset(token)
    workflow.execute.assert_not_called()
    turn.execute.assert_not_called()
