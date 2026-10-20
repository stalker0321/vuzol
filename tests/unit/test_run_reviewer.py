"""Run-selected reviewers and default API reviewers stay pinned across retries."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.unit.projects.test_model_matrix import assignment, bundle, configured
from tests.unit.test_explicit_reviewer import explicit_review as explicit_review
from vuzol.projects.model_matrix import MATRIX_KEY
from vuzol.review.independent import IndependentReviewError
from vuzol.storage.models import Run, Step
from vuzol.storage.types import RiskLevel, RunStatus, StepStatus


@pytest.fixture
def run_review(explicit_review):
    c = explicit_review
    lease = c.kwargs["lease"]
    c.run = Run(
        id=lease.step.run_id,
        task_id=c.kwargs["task"].id,
        status=RunStatus.RUNNING,
        selected_route={MATRIX_KEY: {}},
    )
    c.step = Step(
        id=lease.step.id,
        run_id=c.run.id,
        step_type="review",
        status=StepStatus.RUNNING,
        lease_generation=lease.generation,
        lease_owner=lease.owner,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        payload={},
    )
    c.session = AsyncMock()
    c.session.get.side_effect = lambda cls, _id, **_kwargs: c.run if cls is Run else c.step
    c.session.scalars.return_value = ("actual-executor",)
    factory = MagicMock()
    factory.return_value.__aenter__.return_value = c.session
    factory.begin.return_value.__aenter__.return_value = c.session
    c.reviewer._factory = factory
    return c


@pytest.mark.anyio
@pytest.mark.parametrize("provider", ["codex", "grok", "kimi", "pi", "openai-compatible"])
@pytest.mark.parametrize("risk", [RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.PRIVILEGED])
async def test_run_assignment_overrides_default_and_selects_risk_tier(run_review, provider, risk):
    c = run_review
    profile = configured(provider, profile_id="run-selected")
    c.reviewer._registries = bundle(profile, c.profile)
    slot = "reviewer_strong" if risk is RiskLevel.PRIVILEGED else "reviewer_light"
    pin = assignment(profile).model_dump(mode="json")
    c.run.selected_route[MATRIX_KEY][slot] = pin
    c.kwargs["risk"] = risk
    assert (await c.reviewer.review(**c.kwargs)).allows_progress
    c.adapters.get.assert_called_once_with(profile.id)
    assert c.step.payload["independent_reviewer_pin"] == pin


@pytest.mark.anyio
async def test_api_default_pin_survives_changed_settings_and_rejects_profile_drift(run_review):
    c = run_review
    first = c.profile
    other = configured("openai-compatible", profile_id="other")
    c.reviewer._registries = bundle(first, other)
    await c.reviewer.review(**c.kwargs)
    saved = dict(c.step.payload["independent_reviewer_pin"])
    c.reviewer._profile_id = other.id
    await c.reviewer.review(**c.kwargs)
    assert [call.args[0] for call in c.adapters.get.call_args_list] == [first.id, first.id]
    assert c.step.payload["independent_reviewer_pin"] == saved
    c.reviewer._registries = bundle(first.model_copy(update={"model": "changed"}), other)
    with pytest.raises(IndependentReviewError, match="pinned reviewer"):
        await c.reviewer.review(**c.kwargs)
    assert c.adapter.execute.await_count == 2


@pytest.mark.anyio
async def test_actual_executor_cannot_review_itself_even_with_reviewer_authorization(run_review):
    c = run_review
    c.session.scalars.return_value = (c.profile.id,)
    with pytest.raises(IndependentReviewError, match="actual executor"):
        await c.reviewer.review(**c.kwargs)
    c.reviewer._accounting.reserve.assert_not_awaited()
    c.adapters.get.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("invalid", ["missing", "changed", "paused", "lease"])
async def test_unavailable_run_pin_never_uses_default(run_review, invalid):
    c = run_review
    c.kwargs["risk"] = RiskLevel.PRIVILEGED
    profile = configured("pi", profile_id="run-selected")
    c.run.selected_route[MATRIX_KEY]["reviewer_strong"] = assignment(profile).model_dump(
        mode="json"
    )
    c.reviewer._registries = bundle(profile, c.profile)
    if invalid == "missing":
        c.reviewer._registries = bundle(c.profile)
    elif invalid == "changed":
        c.reviewer._registries = bundle(profile.model_copy(update={"model": "changed"}), c.profile)
    elif invalid == "paused":
        c.run.status = RunStatus.PAUSED
    else:
        c.step.lease_generation += 1
    with pytest.raises(IndependentReviewError):
        await c.reviewer.review(**c.kwargs)
    c.adapters.get.assert_not_called()
    c.reviewer._accounting.reserve.assert_not_awaited()
