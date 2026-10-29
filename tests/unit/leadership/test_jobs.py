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
