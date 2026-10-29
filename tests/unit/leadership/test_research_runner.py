"""Exercise runner scheduling and lease loss without a database server."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from tests.unit.leadership.test_jobs import _running

from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.job_runner import ResearchJobRunner
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.persistence import POLICY_REVISION, GoalDelegation, LeadershipTurn, LeadJob


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def harness(monkeypatch):
    _, goal, job, claim = _running()
    goal.project_id = "p"
    goal.version = goal.fence_epoch = goal.scope_revision = 1
    grant = GoalDelegation(
        project_id="p",
        policy_revision=POLICY_REVISION,
        agent_principal_id=server_agent_principal("p"),
    )
    rows = {type(goal): goal, LeadJob: job, GoalDelegation: grant}
    session = AsyncMock()
    session.get.side_effect = lambda model, *_args, **_kwargs: rows.get(model)
    session.add = lambda row: rows.__setitem__(type(row), row)

    class Factory:
        @asynccontextmanager
        async def begin(self):
            yield session

        def __call__(self):
            return self.begin()

    claims = AsyncMock(side_effect=[[claim], []])
    complete = AsyncMock(return_value=True)
    monkeypatch.setattr("vuzol.leadership.job_runner.claim_jobs", claims)
    monkeypatch.setattr("vuzol.leadership.job_runner.complete_job", complete)
    return Factory(), rows, job, claims, complete


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [False, True])
async def test_runner_publishes_once_and_finishes_its_attempt(harness, failure):
    factory, rows, _, claims, complete = harness
    model = AsyncMock()
    if failure:
        model.research.side_effect = LeadModelUnavailable("rate_limited")
    else:
        model.research.return_value = ("Исследование", {"input_tokens": 4})
    runner = ResearchJobRunner(factory, owner="runner", model=model, enabled=lambda: True)
    assert await runner.tick()
    await runner.wait_idle()
    assert not await runner.tick()
    assert model.research.await_count == 1
    assert complete.await_count == 1
    assert complete.call_args.kwargs == {
        "result_text": None if failure else "Исследование",
        "failure_reason": "rate_limited" if failure else None,
        "usage": {} if failure else {"input_tokens": 4},
    }
    assert rows[LeadershipTurn].status == "applied"
    assert claims.call_args.kwargs["concurrency"] == 2


@pytest.mark.anyio
@pytest.mark.parametrize("lose", [False, True])
async def test_runner_renews_while_running_and_cancels_call_after_lease_loss(harness, lose):
    factory, rows, job, _, complete = harness
    started, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def research(_job):
        started.set()
        try:
            await release.wait()
            return "late report", {}
        finally:
            cancelled.set()

    model = AsyncMock()
    model.research.side_effect = research
    runner = ResearchJobRunner(
        factory,
        owner="runner",
        model=model,
        enabled=lambda: True,
        lease_seconds=1,
    )
    assert await runner.tick()
    await started.wait()
    if lose:
        job.lease_owner = "another-runner"
        async with asyncio.timeout(2):
            await runner.wait_idle()
        assert cancelled.is_set()
        complete.assert_not_awaited()
        assert rows[LeadershipTurn].status == "lost"
    else:
        # Initial helper lease is a minute; observe the first renewal replace it.
        job.lease_expires_at = datetime.now(UTC).replace(year=2099)
        async with asyncio.timeout(0.5):
            await asyncio.sleep(0.4)
        assert job.lease_expires_at.year < 2099
        release.set()
        await runner.wait_idle()
        complete.assert_awaited_once()
    await runner.aclose()
