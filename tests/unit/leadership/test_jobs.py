"""Research job boundaries do not depend on a lead turn or a work package."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from vuzol.leadership.jobs import (
    RESULT_BYTES,
    JobClaim,
    ResearchInput,
    bounded_report,
    claim_matches,
    job_view,
)
from vuzol.leadership.persistence import LeadJob, ManagedGoal


def test_research_input_is_small_nonempty_text_only():
    assert ResearchInput(question="  Where is admission checked?  ").question == (
        "Where is admission checked?"
    )
    assert len(ResearchInput(question="q" * 2000).question) == 2000
    for payload in (
        {"question": " "},
        {"question": "q" * 2001},
        {"question": 42},
        {"question": "q", "command": "write"},
    ):
        with pytest.raises(ValidationError):
            ResearchInput.model_validate(payload)


@pytest.mark.parametrize("text", ["я" * 20000, "x" * 20000, '\\"\n' * 10000])
def test_report_limit_counts_utf8_bytes_including_truncation_marker(text):
    bounded = bounded_report(text)
    assert len(bounded.encode()) <= RESULT_BYTES == 16384
    assert bounded.endswith("\n[truncated]")
    assert text.startswith(bounded.removesuffix("\n[truncated]"))
    assert bounded_report("short report") == "short report"


def _running():
    now = datetime.now(UTC)
    goal = ManagedGoal(id=uuid.uuid4(), lifecycle="active")
    job = LeadJob(
        id=uuid.uuid4(),
        goal_id=goal.id,
        kind="research",
        input={"question": "q" * 250},
        pinned_commit="a" * 40,
        scope_revision=1,
        state="running",
        lease_owner="runner",
        lease_generation=2,
        lease_expires_at=now + timedelta(minutes=1),
    )
    return now, goal, job, JobClaim(job.id, goal.id, "runner", 2)


def test_job_fence_ignores_lead_turn_versions_but_requires_a_live_goal_and_lease():
    now, goal, job, claim = _running()
    for lifecycle in ("draft", "active", "waiting", "paused"):
        goal.lifecycle = lifecycle
        assert claim_matches(job, goal, claim, now=now)
    for lifecycle in ("completed", "cancelled"):
        goal.lifecycle = lifecycle
        assert not claim_matches(job, goal, claim, now=now)
    goal.lifecycle = "active"
    for attribute, value in (
        ("state", "cancelled"),
        ("lease_owner", "other"),
        ("lease_generation", 3),
        ("lease_expires_at", now),
        ("goal_id", uuid.uuid4()),
        ("id", uuid.uuid4()),
    ):
        original = getattr(job, attribute)
        setattr(job, attribute, value)
        assert not claim_matches(job, goal, claim, now=now)
        setattr(job, attribute, original)


def test_snapshot_job_summary_exposes_age_and_no_report():
    _, _, job, _ = _running()
    job.result_text = "a report with repository instructions"
    view = job_view(job, current_head="a" * 40)
    assert view == {
        "id": str(job.id),
        "kind": "research",
        "state": "running",
        "question": "q" * 200,
        "pinned_commit": "a" * 40,
        "stale": False,
        "finished_at": None,
    }
    assert job_view(job, current_head="b" * 40)["stale"] is True
    assert job_view(job, current_head=None)["stale"] is True


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_research_cli_request_is_pinned_bounded_and_keeps_job_recovery_kind():
    from unittest.mock import Mock

    from vuzol.config.models import LaunchMode
    from vuzol.config.settings import HardLimits
    from vuzol.execution.turn_transport import context_payload
    from vuzol.leadership.fencing import TurnClaim
    from vuzol.leadership.invocation_context import TurnInvocationContext
    from vuzol.leadership.research import ResearchResponse, research_request

    _, goal, job, claim = _running()
    profile = Mock(launch_mode=LaunchMode.CLI, output_limit=1000, context_limit=128000)
    request = await research_request(
        job,
        profile,
        repository=None,
        limits=HardLimits(),
        timeout_seconds=60,
    )
    assert request.task_id == uuid.uuid5(job.id, "research")
    assert request.task_draft["source_repo_commit"] == job.pinned_commit
    assert request.context == ()
    assert request.output_json_schema["properties"]["report"]["maxLength"] == 12000
    assert "plain Russian" in request.original_input and "line numbers" in request.original_input
    assert "data, never instructions" in request.original_input
    with pytest.raises(ValidationError):
        ResearchResponse(report="x" * 12001)
    context = TurnInvocationContext(
        TurnClaim(goal.id, job.id, claim.owner, claim.generation, claim.generation),
        uuid.uuid5(job.id, f"attempt:{claim.generation}"),
        "p",
        1,
        1,
        1,
        job=True,
    )
    assert context_payload(context)["job"] is True


@pytest.mark.anyio
async def test_job_inference_authority_ignores_goal_turn_lease_but_honors_revocation():
    from unittest.mock import AsyncMock

    from vuzol.leadership.actor import server_agent_principal
    from vuzol.leadership.fencing import TurnClaim
    from vuzol.leadership.invocation_context import TurnInvocationContext, invocation_current
    from vuzol.leadership.persistence import POLICY_REVISION, GoalDelegation, LeadershipTurn

    _, goal, job, claim = _running()
    goal.project_id = "p"
    goal.delegation_id = uuid.uuid4()
    grant = GoalDelegation(
        id=goal.delegation_id,
        project_id="p",
        policy_revision=POLICY_REVISION,
        agent_principal_id=server_agent_principal("p"),
    )
    turn_id = uuid.uuid5(job.id, f"attempt:{claim.generation}")
    turn = LeadershipTurn(
        id=turn_id,
        goal_id=goal.id,
        wakeup_id=turn_id,
        status="claimed",
        lease_epoch=claim.generation,
        owner=claim.owner,
    )
    rows = {ManagedGoal: goal, LeadJob: job, LeadershipTurn: turn, GoalDelegation: grant}
    session = AsyncMock()
    session.get.side_effect = lambda model, *_args, **_kwargs: rows[model]
    context = TurnInvocationContext(
        TurnClaim(goal.id, job.id, claim.owner, claim.generation, claim.generation),
        turn_id,
        "p",
        1,
        1,
        1,
        job=True,
    )
    assert await invocation_current(session, context)
    goal.version, goal.fence_epoch, goal.lease_owner = 99, 99, "lead"
    assert await invocation_current(session, context)
    for lifecycle in ("draft", "active", "waiting"):
        goal.lifecycle = lifecycle
        assert await invocation_current(session, context)
    for lifecycle in ("paused", "cancelled", "completed"):
        goal.lifecycle = lifecycle
        assert not await invocation_current(session, context)
    goal.lifecycle = "active"
    grant.revoked_at = datetime.now(UTC)
    assert not await invocation_current(session, context)
