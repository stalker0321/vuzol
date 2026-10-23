"""Card identity follows recorded attempts, not mutable registry defaults."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vuzol.execution.identity import step_execution_identity


@pytest.mark.anyio
async def test_running_attempt_has_frozen_account_model_and_effort() -> None:
    step = SimpleNamespace(id=uuid4(), executor_profile_id="pi-c-muse", result={}, payload={})
    process = SimpleNamespace(
        runtime_metadata={
            "account_id": "opencode-c",
            "executor_model": "muse-1.3",
            "executor_reasoning_effort": "high",
        }
    )
    session = MagicMock(scalar=AsyncMock(return_value=process))
    identity = await step_execution_identity(session, step)
    assert (identity.profile_id, identity.account_id, identity.model, identity.effort) == (
        "pi-c-muse",
        "opencode-c",
        "muse-1.3",
        "high",
    )


@pytest.mark.anyio
async def test_recorded_result_wins_over_requested_model() -> None:
    step = SimpleNamespace(
        id=uuid4(), executor_profile_id="codex-a", result={"model": "actual-model"}, payload={}
    )
    session = MagicMock(
        scalar=AsyncMock(
            return_value=SimpleNamespace(runtime_metadata={"executor_model": "requested-model"})
        )
    )
    assert (await step_execution_identity(session, step)).model == "actual-model"


@pytest.mark.anyio
async def test_historical_usage_fallback_and_unknown_are_explicit() -> None:
    step = SimpleNamespace(id=uuid4(), executor_profile_id="codex-a", result={}, payload={})
    session = MagicMock(scalar=AsyncMock(side_effect=[None, "old-model", None, None]))
    assert (await step_execution_identity(session, step)).model == "old-model"
    assert (await step_execution_identity(session, step)).model is None
