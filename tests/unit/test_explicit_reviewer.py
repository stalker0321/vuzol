"""Explicit reviewer selection preserves authorization and never falls back."""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.unit.test_independent_review import _api_profile, _lease, _reviewer
from vuzol.config.models import LaunchMode, ProviderRole
from vuzol.execution.domain import GitInspection
from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus
from vuzol.providers.kimi import KIMI_MODEL
from vuzol.review.independent import IndependentReviewError
from vuzol.storage.models import Task
from vuzol.storage.types import RiskLevel
from vuzol.workflows.ports import CancellationContext


@pytest.fixture
def explicit_review():
    profile = _api_profile(profile_id="selected", roles={ProviderRole.REVIEWER})
    registries = MagicMock()
    registries.profiles.get.return_value = profile
    registries.profiles.items.return_value = (
        _api_profile(profile_id="fallback", roles={ProviderRole.REVIEWER}),
    )
    adapter = MagicMock(
        execute=AsyncMock(
            return_value=ProviderResult(
                status=ProviderResultStatus.SUCCEEDED,
                structured_output={"verdict": "pass", "summary": "reviewed", "findings": []},
                usage=NormalizedUsage(duration_ms=1),
                adapter_version="test.v1",
            )
        )
    )
    adapters = MagicMock()
    adapters.get.return_value = adapter
    reviewer = _reviewer(registries, adapters)
    reviewer._profile_id = profile.id
    task = Task(id=uuid.uuid4(), task_draft={}, original_text="review this")
    lease = _lease()
    return SimpleNamespace(
        profile=profile,
        registries=registries,
        adapters=adapters,
        adapter=adapter,
        reviewer=reviewer,
        kwargs=dict(
            task=task,
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40, branch="task", changed_files=("x.py",), diff=b"+safe\n"
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[],
            mechanical_findings=(),
            request_ids=(task.id, lease.step.run_id, lease.step.id),
            timeout_seconds=30,
            cancellation=CancellationContext(),
            lease=lease,
        ),
    )


@pytest.mark.anyio
@pytest.mark.parametrize("provider", ["openai-compatible", "codex", "grok", "kimi", "pi"])
async def test_explicit_supported_reviewer_uses_only_selected_profile(explicit_review, provider):
    c = explicit_review
    profile = c.profile.model_copy(
        update={
            "provider": provider,
            "model": KIMI_MODEL if provider == "kimi" else "unrelated-vendor/model",
            "launch_mode": LaunchMode.API if provider == "openai-compatible" else LaunchMode.CLI,
            "roles": frozenset({ProviderRole.REVIEWER, ProviderRole.EXECUTOR}),
        }
    )
    c.registries.profiles.get.return_value = profile
    verdict = await c.reviewer.review(**c.kwargs)
    assert verdict.allows_progress
    c.registries.profiles.get.assert_called_once_with(profile.id)
    c.registries.profiles.items.assert_not_called()
    c.adapters.get.assert_called_once_with(profile.id)
    request, selected, cancellation = c.adapter.execute.call_args.args
    assert selected is profile and cancellation is c.kwargs["cancellation"]
    assert request.role is ProviderRole.REVIEWER
    assert (request.task_id, request.run_id, request.step_id) == c.kwargs["request_ids"]
    assert request.lease_generation == c.kwargs["lease"].generation
    c.reviewer._accounting.reserve.assert_awaited_once()
    c.reviewer._accounting.reconcile.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"roles": frozenset()},
        {"roles": frozenset({ProviderRole.PLANNER})},
        {"roles": frozenset({ProviderRole.EXECUTOR})},
        {"api_base_url": None},
        {"provider": "unsupported"},
        {"launch_mode": LaunchMode.TOOL},
        {"launch_mode": LaunchMode.CLI, "provider": "unsupported"},
        {"launch_mode": LaunchMode.CLI, "provider": "pi", "roles": frozenset({ProviderRole.LEAD})},
    ],
)
async def test_ineligible_explicit_reviewer_never_falls_back(explicit_review, changes):
    c = explicit_review
    c.registries.profiles.get.return_value = c.profile.model_copy(update=changes)
    with pytest.raises(IndependentReviewError, match="configured reviewer is unavailable"):
        await c.reviewer.review(**c.kwargs)
    c.registries.profiles.items.assert_not_called()
    c.adapters.get.assert_not_called()
    c.adapter.execute.assert_not_awaited()
    c.reviewer._accounting.reserve.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("missing", ["profile", "adapter"])
async def test_unavailable_explicit_reviewer_never_falls_back(explicit_review, missing):
    c = explicit_review
    if missing == "profile":
        c.registries.profiles.get.side_effect = ValueError("missing")
    else:
        c.adapters.get.side_effect = LookupError("missing")
    with pytest.raises(IndependentReviewError, match=r"missing|unavailable"):
        await c.reviewer.review(**c.kwargs)
    c.registries.profiles.items.assert_not_called()
    c.adapter.execute.assert_not_awaited()
    c.reviewer._accounting.reserve.assert_not_awaited()
