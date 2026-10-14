"""Absent profile limits must not become a deny-all map on assigned retries."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config import Settings
from vuzol.storage.types import QueueClass
from vuzol.workflows.worker import CompleteHandler, WorkflowWorker


@pytest.mark.anyio
@pytest.mark.parametrize("profile_limits", [None, {}, {"grok-a": 2}])
async def test_worker_preserves_absent_vs_explicit_profile_filter(monkeypatch, profile_limits):
    expected = None if profile_limits is None else dict(profile_limits)
    claim = AsyncMock(return_value=None)
    monkeypatch.setattr("vuzol.workflows.worker.claim_step", claim)
    worker = WorkflowWorker(
        Settings(environment="test"),
        MagicMock(),
        owner="review-worker",
        handlers={"review": CompleteHandler()},
        queue_classes=frozenset({QueueClass.LIGHT}),
        profile_limits=profile_limits,
    )
    if profile_limits is not None:
        profile_limits["grok-a"] = 99
    assert await worker._claim(AsyncMock(), {QueueClass.LIGHT: 2}) is None
    assert claim.await_args.kwargs["profile_limits"] == expected
    assert claim.await_args.kwargs["step_types"] == frozenset({"review"})
    assert claim.await_args.kwargs["queue_classes"] == frozenset({QueueClass.LIGHT})
