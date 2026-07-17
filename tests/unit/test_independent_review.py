"""Independent model reviewer selection and structured verdict parsing."""

from __future__ import annotations

import hashlib
import json
import uuid
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import HttpUrl

from vuzol.config.models import CostClass, LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.execution.domain import GitInspection
from vuzol.providers.domain import (
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.review.domain import FindingSeverity, ReviewFinding, ReviewVerdictKind
from vuzol.review.independent import (
    IndependentModelReviewer,
    IndependentReviewError,
    ReviewBudgetReservation,
    _build_request,
    _verdict_from_provider_result,
    select_reviewer_profile,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import RiskLevel, StepStatus
from vuzol.workflows.ports import CancellationContext


def _api_profile(
    *,
    profile_id: str,
    roles: set[ProviderRole],
    priority: int = 50,
) -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id=profile_id,
        provider="openai-compatible",
        model="gpt-test",
        api_base_url=HttpUrl("https://api.example.com/v1"),
        launch_mode=LaunchMode.API,
        credential_reference="env:VUZOL_OPENAI_PLANNER_API_KEY",
        credential_required=True,
        capabilities=frozenset(),
        concurrency_limit=2,
        context_limit=8_000,
        output_limit=1_000,
        cost_class=CostClass.CHEAP,
        roles=frozenset(roles),
        routing_priority=priority,
        supported_task_types=frozenset({"coding"}),
        sandbox_required=False,
        input_cost_units_per_million=0.1,
        output_cost_units_per_million=0.2,
        minimum_unknown_usage_cost=0.001,
        enabled=True,
    )


def _lease() -> LeaseToken:
    return LeaseToken(
        step=StepRecord(
            id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            status=StepStatus.RUNNING,
            lease_generation=1,
            lease_owner="reviewer",
            lease_expires_at=None,
        ),
        owner="reviewer",
        generation=1,
    )


def _reviewer(registries: MagicMock, adapters: MagicMock) -> IndependentModelReviewer:
    accounting = MagicMock()
    accounting.reserve = AsyncMock(
        return_value=ReviewBudgetReservation(
            id=uuid.uuid4(),
            cost_units=Decimal("0.001"),
            quota_units=Decimal("0"),
        )
    )
    accounting.reconcile = AsyncMock()
    accounting.release = AsyncMock()
    return IndependentModelReviewer(registries, adapters, accounting)


def test_select_reviewer_prefers_reviewer_role() -> None:
    planner = _api_profile(profile_id="planner", roles={ProviderRole.PLANNER}, priority=10)
    reviewer = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER}, priority=90)
    selected = select_reviewer_profile((planner, reviewer))
    assert selected is not None and selected.id == "reviewer"


def test_select_reviewer_falls_back_to_planner_api() -> None:
    planner = _api_profile(profile_id="planner", roles={ProviderRole.PLANNER}, priority=10)
    selected = select_reviewer_profile((planner,))
    assert selected is not None and selected.id == "planner"


def test_select_reviewer_returns_none_without_api_profiles() -> None:
    assert select_reviewer_profile(()) is None


def test_pass_verdict_with_blocking_finding_requires_changes() -> None:
    result = ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        structured_output={
            "verdict": "pass",
            "summary": "Looks fine.",
            "findings": [
                {
                    "severity": "blocker",
                    "classification": "unsafe_change",
                    "summary": "A blocking issue remains.",
                }
            ],
        },
        usage=NormalizedUsage(duration_ms=1),
        adapter_version="openai-compatible.v1",
    )

    verdict = _verdict_from_provider_result(
        result,
        risk=RiskLevel.HIGH,
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash="c" * 64,
        changed_files=("x.py",),
        profile_id="reviewer",
        mechanical_findings=(),
    )

    assert verdict.verdict is ReviewVerdictKind.CHANGES_REQUIRED
    assert not verdict.allows_progress


def test_large_review_bundle_is_complete_across_hashed_context_chunks() -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=("x.py",),
        diff=b"+" + b"x" * 25_000,
    )
    request = _build_request(
        task=SimpleNamespace(task_draft={"goal": "Large change"}, original_text="x"),  # type: ignore[arg-type]
        risk=RiskLevel.HIGH,
        inspection=inspection,
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash=inspection.diff_hash,
        gates=[{"name": "tests", "exit_code": 0}],
        mechanical_findings=(),
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        timeout_seconds=60,
        profile=profile,
        policy_revision="test-policy.v1",
        provider_attempt=1,
    )

    assert len(request.context) > 1
    encoded = "".join(item.content for item in request.context)
    assert json.loads(encoded)["diff"].endswith("x" * 25_000)
    assert all(
        item.content_hash == hashlib.sha256(item.content.encode()).hexdigest()
        for item in request.context
    )


@pytest.mark.anyio
async def test_independent_reviewer_builds_pass_verdict() -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    adapter = MagicMock()
    adapter.execute = AsyncMock(
        return_value=ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            structured_output={
                "verdict": "pass",
                "summary": "Looks safe for apply.",
                "findings": [
                    {
                        "severity": "info",
                        "classification": "no_issues",
                        "summary": "Diff is small and focused.",
                    }
                ],
            },
            usage=NormalizedUsage(duration_ms=12),
            adapter_version="openai-compatible.v1",
        )
    )
    adapters = MagicMock()
    adapters.get.return_value = adapter
    reviewer = _reviewer(registries, adapters)
    task = SimpleNamespace(
        task_draft={"goal": "Harden login"},
        original_text="please harden login",
    )
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=("login.py",),
        diff=b"+return True\n",
    )
    verdict = await reviewer.review(
        task=task,  # type: ignore[arg-type]
        risk=RiskLevel.HIGH,
        inspection=inspection,
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash="c" * 64,
        gates=[{"name": "git-facts", "exit_code": 0}],
        mechanical_findings=(
            ReviewFinding(
                severity=FindingSeverity.WARNING,
                classification="exception_swallowing",
                summary="possible swallow",
            ),
        ),
        request_ids=(uuid.uuid4(), uuid.uuid4(), uuid.uuid4()),
        timeout_seconds=60,
        cancellation=CancellationContext(),
        lease=_lease(),
    )
    assert verdict.review_kind == "independent"
    assert verdict.verdict is ReviewVerdictKind.PASSED_WITH_WARNINGS
    assert "reviewer" in verdict.summary
    assert any(item.classification == "exception_swallowing" for item in verdict.findings)
    assert any(item.classification == "no_issues" for item in verdict.findings)
    request = adapter.execute.await_args.args[0]
    assert request.role is ProviderRole.REVIEWER
    assert request.sandbox_reference is None
    assert request.output_json_schema is not None
    assert all(
        item.content_hash == hashlib.sha256(item.content.encode()).hexdigest()
        for item in request.context
    )


@pytest.mark.anyio
async def test_independent_reviewer_fails_closed_on_provider_error() -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    adapter = MagicMock()
    adapter.execute = AsyncMock(
        side_effect=ProviderFailure(
            ProviderErrorCategory.TIMEOUT,
            retryable=True,
            request_sent=True,
            safe_summary="provider request timed out",
        )
    )
    adapters = MagicMock()
    adapters.get.return_value = adapter
    reviewer = _reviewer(registries, adapters)
    with pytest.raises(IndependentReviewError, match="timed out"):
        await reviewer.review(
            task=SimpleNamespace(task_draft={}, original_text="x"),  # type: ignore[arg-type]
            risk=RiskLevel.PRIVILEGED,
            inspection=GitInspection(
                head="b" * 40,
                branch="task",
                changed_files=("x.py",),
                diff=b"+x\n",
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[{"exit_code": 0}],
            mechanical_findings=(),
            request_ids=(uuid.uuid4(), uuid.uuid4(), uuid.uuid4()),
            timeout_seconds=30,
            cancellation=CancellationContext(),
            lease=_lease(),
        )


@pytest.mark.anyio
async def test_independent_reviewer_requires_configured_profile() -> None:
    registries = MagicMock()
    registries.profiles.items.return_value = ()
    reviewer = _reviewer(registries, MagicMock())
    with pytest.raises(IndependentReviewError, match="no openai-compatible"):
        await reviewer.review(
            task=SimpleNamespace(task_draft={}, original_text="x"),  # type: ignore[arg-type]
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40,
                branch="task",
                changed_files=("x.py",),
                diff=b"+x\n",
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[{"exit_code": 0}],
            mechanical_findings=(),
            request_ids=(uuid.uuid4(), uuid.uuid4(), uuid.uuid4()),
            timeout_seconds=30,
            cancellation=CancellationContext(),
            lease=_lease(),
        )


@pytest.mark.anyio
async def test_independent_reviewer_blocks_when_model_blocks() -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    adapter = MagicMock()
    adapter.execute = AsyncMock(
        return_value=ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            structured_output={
                "verdict": "blocked",
                "summary": "Secret material may have been introduced.",
                "findings": [
                    {
                        "severity": "blocker",
                        "classification": "secret_risk",
                        "summary": "Looks like a hard-coded token.",
                        "path": "cfg.py",
                        "line": 12,
                    }
                ],
            },
            usage=NormalizedUsage(duration_ms=9),
            adapter_version="openai-compatible.v1",
        )
    )
    adapters = MagicMock()
    adapters.get.return_value = adapter
    reviewer = _reviewer(registries, adapters)
    verdict = await reviewer.review(
        task=SimpleNamespace(task_draft={}, original_text="x"),  # type: ignore[arg-type]
        risk=RiskLevel.PRIVILEGED,
        inspection=GitInspection(
            head="b" * 40,
            branch="task",
            changed_files=("cfg.py",),
            diff=b"+token = 'x'\n",
        ),
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash="d" * 64,
        gates=[{"exit_code": 0}],
        mechanical_findings=(),
        request_ids=(uuid.uuid4(), uuid.uuid4(), uuid.uuid4()),
        timeout_seconds=45,
        cancellation=CancellationContext(),
        lease=_lease(),
    )
    assert verdict.verdict is ReviewVerdictKind.BLOCKED
    assert not verdict.allows_progress
    assert verdict.findings[0].path == "cfg.py"


@pytest.mark.anyio
async def test_independent_reviewer_adapter_missing() -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    adapters = MagicMock()
    adapters.get.side_effect = LookupError("missing")
    reviewer = _reviewer(registries, adapters)
    with pytest.raises(IndependentReviewError, match="adapter is unavailable"):
        await reviewer.review(
            task=SimpleNamespace(task_draft={}, original_text="x"),  # type: ignore[arg-type]
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40,
                branch="task",
                changed_files=("x.py",),
                diff=b"+x\n",
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[{"exit_code": 0}],
            mechanical_findings=(),
            request_ids=(uuid.uuid4(), uuid.uuid4(), uuid.uuid4()),
            timeout_seconds=30,
            cancellation=CancellationContext(),
            lease=_lease(),
        )


@pytest.mark.anyio
async def test_independent_reviewer_rejects_oversized_bundle() -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    adapter = MagicMock()
    adapter.execute = AsyncMock(
        return_value=ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            structured_output={
                "verdict": "pass",
                "summary": "ok",
                "findings": [],
            },
            usage=NormalizedUsage(duration_ms=4),
            adapter_version="openai-compatible.v1",
        )
    )
    adapters = MagicMock()
    adapters.get.return_value = adapter
    reviewer = _reviewer(registries, adapters)
    with pytest.raises(IndependentReviewError, match="maximum is 80"):
        await reviewer.review(
            task=SimpleNamespace(task_draft={}, original_text=""),  # type: ignore[arg-type]
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40,
                branch="task",
                changed_files=tuple(f"f{i}.py" for i in range(100)),
                diff=b"+safe\n",
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[{"exit_code": 0}],
            mechanical_findings=(),
            request_ids=(uuid.uuid4(), uuid.uuid4(), uuid.uuid4()),
            timeout_seconds=30,
            cancellation=CancellationContext(),
            lease=_lease(),
        )
    adapter.execute.assert_not_awaited()


@pytest.mark.anyio
async def test_independent_reviewer_rejects_invalid_structured_output() -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    adapter = MagicMock()
    adapter.execute = AsyncMock(
        return_value=ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            structured_output={"verdict": "not-a-real-verdict", "summary": "x", "findings": []},
            usage=NormalizedUsage(duration_ms=3),
            adapter_version="openai-compatible.v1",
        )
    )
    adapters = MagicMock()
    adapters.get.return_value = adapter
    reviewer = _reviewer(registries, adapters)
    with pytest.raises(IndependentReviewError, match="schema interpretation"):
        await reviewer.review(
            task=SimpleNamespace(task_draft={}, original_text="x"),  # type: ignore[arg-type]
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40,
                branch="task",
                changed_files=("x.py",),
                diff=b"+x\n",
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[{"exit_code": 0}],
            mechanical_findings=(),
            request_ids=(uuid.uuid4(), uuid.uuid4(), uuid.uuid4()),
            timeout_seconds=30,
            cancellation=CancellationContext(),
            lease=_lease(),
        )
