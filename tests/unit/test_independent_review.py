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
from vuzol.config.settings import HardLimits
from vuzol.execution.domain import GitInspection
from vuzol.providers.budgets import BudgetExceeded
from vuzol.providers.domain import (
    ContextItem,
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.review import independent as independent_module
from vuzol.review.domain import FindingSeverity, ReviewFinding, ReviewVerdictKind
from vuzol.review.independent import (
    DatabaseReviewAccounting,
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
    **changes: object,
) -> ProviderProfileConfig:
    values: dict[str, object] = {
        "id": profile_id,
        "provider": "openai-compatible",
        "model": "gpt-test",
        "api_base_url": HttpUrl("https://api.example.com/v1"),
        "launch_mode": LaunchMode.API,
        "credential_reference": "env:VUZOL_OPENAI_PLANNER_API_KEY",
        "credential_required": True,
        "capabilities": frozenset(),
        "concurrency_limit": 2,
        "context_limit": 8_000,
        "output_limit": 1_000,
        "cost_class": CostClass.CHEAP,
        "roles": frozenset(roles),
        "routing_priority": priority,
        "supported_task_types": frozenset({"coding"}),
        "sandbox_required": False,
        "input_cost_units_per_million": 0.1,
        "output_cost_units_per_million": 0.2,
        "minimum_unknown_usage_cost": 0.001,
        "enabled": True,
    }
    values.update(changes)
    return ProviderProfileConfig.model_validate(values)


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


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


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

    fallback = _verdict_from_provider_result(
        result.model_copy(
            update={
                "structured_output": {
                    "verdict": "pass",
                    "summary": "",
                    "findings": ["ignored non-object finding"],
                }
            }
        ),
        risk=RiskLevel.HIGH,
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash="c" * 64,
        changed_files=("x.py",),
        profile_id="reviewer",
        mechanical_findings=(),
    )
    assert fallback.verdict is ReviewVerdictKind.PASSED
    assert "Independent review via reviewer" in fallback.summary


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
    assert request.max_output_tokens == 1_000
    assert request.reasoning_max_tokens == min(2_000, request.max_output_tokens)
    encoded = "".join(item.content for item in request.context)
    assert json.loads(encoded)["diff"].endswith("x" * 25_000)
    assert all(
        item.content_hash == hashlib.sha256(item.content.encode()).hexdigest()
        for item in request.context
    )


def _build_request_for_profile(
    profile: ProviderProfileConfig,
    work_context: tuple[ContextItem, ...] = (),
    *,
    input_token_limit: int = 32_000,
) -> ProviderRequest:
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=("x.py",),
        diff=b"+x",
    )
    return _build_request(
        task=SimpleNamespace(task_draft={"goal": "x"}, original_text="x"),  # type: ignore[arg-type]
        risk=RiskLevel.HIGH,
        inspection=inspection,
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash=inspection.diff_hash,
        gates=[],
        mechanical_findings=(),
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        timeout_seconds=60,
        profile=profile,
        policy_revision="test-policy.v1",
        provider_attempt=1,
        work_context=work_context,
        input_token_limit=input_token_limit,
    )


def _work_context():
    chunks = ("Selected prototype; no registration. " + "x" * 19900, "Full file tail; no deploy.")
    return tuple(
        ContextItem(
            source="chat_work_context",
            reference=f"task:spec:part-{n}-of-2",
            content=text,
            content_hash=hashlib.sha256(text.encode()).hexdigest(),
        )
        for n, text in enumerate(chunks, 1)
    )


def test_review_preserves_frozen_spec_and_respects_full_context_budget():
    context = _work_context()
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    request = _build_request_for_profile(profile, context)
    assert request.context[:2] == context
    assert any(c.source == "retained_result_review_bundle_chunk" for c in request.context)
    small = profile.model_copy(update={"context_limit": 1000})
    with pytest.raises(IndependentReviewError, match="context budget"):
        _build_request_for_profile(small, context)


@pytest.mark.parametrize(
    ("profile_limit", "configured_limit", "expected"),
    [
        (None, 100_000, 100_000),
        (200_000, 100_000, 100_000),
        (50_000, 100_000, 50_000),
        (None, 20_000, 20_000),
    ],
)
def test_review_uses_configured_input_cap_without_exceeding_profile(
    profile_limit: int | None, configured_limit: int, expected: int
) -> None:
    profile = _api_profile(
        profile_id="reviewer", roles={ProviderRole.REVIEWER}, context_limit=profile_limit
    )
    request = _build_request_for_profile(profile, input_token_limit=configured_limit)
    assert request.max_input_tokens == expected


def test_review_large_frozen_context_uses_configured_cap_without_truncation() -> None:
    texts = ("x" * 20_000,) * 7
    context = tuple(
        ContextItem(
            source="chat_work_context",
            reference=f"task:large-spec:part-{n}-of-{len(texts)}",
            content=value,
            content_hash=hashlib.sha256(value.encode()).hexdigest(),
        )
        for n, value in enumerate(texts, 1)
    )
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER}, context_limit=None)
    with pytest.raises(IndependentReviewError, match="context budget"):
        _build_request_for_profile(profile, context)
    request = _build_request_for_profile(profile, context, input_token_limit=100_000)
    assert request.context[: len(context)] == context
    assert request.max_input_tokens == 100_000
    for profile_limit, configured_limit in [(30_000, 100_000), (None, 30_000)]:
        with pytest.raises(IndependentReviewError, match="context budget"):
            _build_request_for_profile(
                profile.model_copy(update={"context_limit": profile_limit}),
                context,
                input_token_limit=configured_limit,
            )


@pytest.mark.parametrize("limit", [0, -1])
def test_review_rejects_nonpositive_input_cap(limit: int) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        IndependentModelReviewer(MagicMock(), MagicMock(), MagicMock(), input_token_limit=limit)


def test_build_request_uses_profile_role_limits() -> None:
    sized = _build_request_for_profile(
        _api_profile(
            profile_id="reviewer",
            roles={ProviderRole.REVIEWER},
            output_limit=6_000,
        )
    )
    assert sized.max_output_tokens == 6_000
    assert sized.reasoning_max_tokens == 2_000

    hinted = _build_request_for_profile(
        _api_profile(
            profile_id="reviewer",
            roles={ProviderRole.REVIEWER},
            output_limit=4_000,
            max_reasoning_tokens=1_500,
        )
    )
    assert hinted.max_output_tokens == 4_000
    assert hinted.reasoning_max_tokens == 1_500

    clamped = _build_request_for_profile(
        _api_profile(
            profile_id="reviewer",
            roles={ProviderRole.REVIEWER},
            output_limit=6_000,
            max_reasoning_tokens=8_000,
        )
    )
    assert clamped.reasoning_max_tokens == 6_000


@pytest.mark.anyio
async def test_database_review_accounting_reserves_reconciles_and_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=("x.py",),
        diff=b"+safe\n",
    )
    request = _build_request(
        task=SimpleNamespace(task_draft={"goal": "Review"}, original_text="x"),  # type: ignore[arg-type]
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
    )
    reservation_id = uuid.uuid4()
    reserve = AsyncMock(return_value=SimpleNamespace(id=reservation_id))
    reconcile = AsyncMock()
    release = AsyncMock()
    monkeypatch.setattr(independent_module, "reserve_budget", reserve)
    monkeypatch.setattr(independent_module, "reconcile_usage", reconcile)
    monkeypatch.setattr(independent_module, "release_reservation", release)
    factory = MagicMock()
    review_session = MagicMock()
    review_session.scalar = AsyncMock(return_value=None)
    factory.begin.return_value = AsyncContext(review_session)
    accounting = DatabaseReviewAccounting(factory, HardLimits())

    reservation = await accounting.reserve(request=request, profile=profile)
    result = ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        structured_output={"verdict": "pass", "summary": "ok", "findings": []},
        usage=NormalizedUsage(input_tokens=10, output_tokens=5, duration_ms=2),
        provider_request_id="provider-request",
        adapter_version="openai-compatible.v1",
    )
    lease = _lease()
    await accounting.reconcile(
        reservation=reservation,
        lease=lease,
        profile=profile,
        result=result,
        outcome="succeeded",
        conservative=False,
    )
    await accounting.reconcile(
        reservation=reservation,
        lease=lease,
        profile=profile,
        result=None,
        outcome="timeout",
        conservative=True,
    )
    await accounting.release(reservation=reservation, lease=lease)

    assert reservation.id == reservation_id
    reserve.assert_awaited_once()
    assert reserve.await_args is not None
    # D3 Q4: deductible allowance replaces the bypass flag.
    assert reserve.await_args.kwargs["review_allowance"] is True
    assert reserve.await_args.kwargs["accounting"].purpose == "review"
    assert reconcile.await_count == 2
    release.assert_awaited_once()
    reserve.side_effect = BudgetExceeded("limit")
    with pytest.raises(
        IndependentReviewError,
        match="budget is exhausted: limit",
    ):
        await accounting.reserve(request=request, profile=profile)


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
    finding_schema = request.output_json_schema["properties"]["findings"]["items"]
    assert set(finding_schema["required"]) == set(finding_schema["properties"])
    assert all(
        item.content_hash == hashlib.sha256(item.content.encode()).hexdigest()
        for item in request.context
    )


@pytest.mark.anyio
@pytest.mark.parametrize("has_receipt", [False, True])
async def test_independent_reviewer_fails_closed_on_provider_error(has_receipt: bool) -> None:
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
            usage=NormalizedUsage(input_tokens=42, output_tokens=7, duration_ms=1)
            if has_receipt
            else None,
            provider_request_id="review-receipt" if has_receipt else None,
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
    reconciled = reviewer._accounting.reconcile.call_args.kwargs
    assert reconciled["conservative"] is (not has_receipt)
    if has_receipt:
        assert reconciled["result"].usage.output_tokens == 7
        assert reconciled["result"].provider_request_id == "review-receipt"
    else:
        assert reconciled["result"] is None
    adapter.execute.side_effect = ProviderFailure(
        ProviderErrorCategory.PROVIDER_UNAVAILABLE,
        retryable=True,
        request_sent=False,
        safe_summary="provider unavailable before request",
    )
    with pytest.raises(IndependentReviewError, match="before request"):
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
@pytest.mark.parametrize("with_context", [False, True])
@pytest.mark.parametrize("cli", [False, True])
async def test_independent_reviewer_partitions_many_files_instead_of_rejecting(
    with_context,
    cli,
) -> None:
    profile = _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER})
    if cli:
        profile = profile.model_copy(
            update={
                "provider": "grok",
                "model": "grok-4.7",
                "model_reasoning_effort": "high",
                "launch_mode": LaunchMode.CLI,
            }
        )
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    registries.profiles.get.return_value = profile
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
    if cli:
        reviewer._profile_id = profile.id
    context = _work_context() if with_context else ()
    verdict = await reviewer.review(
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
        work_context=context,
    )
    # 100 files flow through 2 bounded partitions + cross-partition check.
    assert verdict.allows_progress
    assert verdict.partition_count == 2
    assert adapter.execute.await_count == 3
    attempts = [call.args[0].provider_attempt for call in adapter.execute.await_args_list]
    assert len(set(attempts)) == 3
    assert all(call.args[1].id == profile.id for call in adapter.execute.await_args_list)
    if with_context:
        assert all(call.args[0].context[:2] == context for call in adapter.execute.await_args_list)
    with pytest.raises(IndependentReviewError, match="maximum is 120000"):
        await reviewer.review(
            task=SimpleNamespace(task_draft={}, original_text=""),  # type: ignore[arg-type]
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40,
                branch="task",
                changed_files=("large.py",),
                diff=b"+" + b"x" * 120_001,
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


def test_report_review_uses_report_instructions_and_hash_checked_context() -> None:
    content = '{"summary":"Read-only findings","sources":["README.md:1"]}'
    context = (
        ContextItem(
            source="task_report",
            reference="report:test:part-1-of-1",
            content=content,
            content_hash=hashlib.sha256(content.encode()).hexdigest(),
        ),
    )
    request = _build_request_for_profile(
        _api_profile(profile_id="reviewer", roles={ProviderRole.REVIEWER}), context
    )
    bundle = json.loads("".join(c.content for c in request.context if c.source != "task_report"))
    assert "independent report reviewer" in bundle["instruction"]
    assert "Do not require a code diff" in bundle["instruction"]
    assert request.context[0] == context[0]
