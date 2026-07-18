"""Telemetry export tests (split for cohesion)."""

from __future__ import annotations

from ._test_experiments_helpers import *


def test_telemetry_aggregation_pricing_revision_and_secret_rejection() -> None:
    first = telemetry()
    repaired = telemetry(
        task_id="t2",
        worker_attempts=2,
        repair_count=1,
        repair_severity=RepairSeverity.MINOR,
        defect_categories=frozenset({DefectCategory.MISSING_EDGE_CASE}),
        final_outcome=ReviewOutcome.ACCEPTED_AFTER_MINOR_REPAIR,
    )
    summary = aggregate_trials((first, repaired))
    assert summary["task_count"] == 2
    assert summary["provider_input_tokens"] == 200
    assert summary["provider_output_tokens"] == 40
    assert summary["accepted_first_pass"] == 1
    assert first.pricing_revision is not None
    assert first.pricing_revision.revision == "trial-v1"
    with pytest.raises(ValidationError, match="sensitive environment"):
        telemetry(environment_variable_names=("DATABASE_URL",))
    with pytest.raises(ValidationError, match="lineage"):
        telemetry(worker_attempts=1, repair_count=1)


def test_machine_readable_csv_export_preserves_measured_fields(tmp_path: Path) -> None:
    trial = telemetry()
    target = tmp_path / "results.csv"
    _write_csv(target, (trial,))
    lines = target.read_text().splitlines()
    assert "repeated_context_ratio" in lines[0]
    assert "step09a-test" in lines[1]
    assert "accepted_first_pass" in lines[1]
    assert "0.01" in lines[1]


def test_provider_usage_aggregates_by_role_without_fabricating_missing_values() -> None:
    context = ContextManifest(role="worker")
    invocations = (
        InvocationTelemetry(
            role="planner",
            profile_id="deterministic",
            model="deterministic",
            context=ContextManifest(role="planner"),
            usage=ReportedUsage(unavailable_reason="not a provider invocation"),
            duration_ms=1,
        ),
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=context,
            usage=ReportedUsage(
                input_tokens=100,
                cached_input_tokens=40,
                output_tokens=20,
                reasoning_tokens=5,
            ),
            duration_ms=2,
        ),
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=context,
            usage=ReportedUsage(
                input_tokens=0,
                cached_input_tokens=0,
                output_tokens=0,
                reasoning_tokens=0,
            ),
            duration_ms=3,
        ),
        InvocationTelemetry(
            role="reviewer",
            profile_id="codex-subscription-prod",
            model="codex",
            context=ContextManifest(role="reviewer"),
            usage=ReportedUsage(input_tokens=7, output_tokens=3, reasoning_tokens=2),
            duration_ms=4,
        ),
    )
    trial = telemetry(invocations=invocations)

    summary = aggregate_trials((trial,))

    assert summary["provider_input_tokens"] == 107
    assert summary["provider_output_tokens"] == 23
    assert summary["provider_cached_input_tokens"] == 40
    assert summary["provider_reasoning_tokens"] == 7
    assert summary["provider_usage_unavailable_invocations"] == 1
    assert summary["usage_by_role"] == {
        "planner": {
            "invocation_count": 1,
            "reported_invocation_count": 0,
            "unavailable_invocation_count": 1,
            "input_tokens": None,
            "cached_input_tokens": None,
            "output_tokens": None,
            "reasoning_tokens": None,
        },
        "worker": {
            "invocation_count": 2,
            "reported_invocation_count": 2,
            "unavailable_invocation_count": 0,
            "input_tokens": 100,
            "cached_input_tokens": 40,
            "output_tokens": 20,
            "reasoning_tokens": 5,
        },
        "reviewer": {
            "invocation_count": 1,
            "reported_invocation_count": 1,
            "unavailable_invocation_count": 0,
            "input_tokens": 7,
            "cached_input_tokens": None,
            "output_tokens": 3,
            "reasoning_tokens": 2,
        },
    }
    assert summary["task_count"] == 1
    assert summary["context_bytes"] == trial.total_context_bytes
    assert summary["accepted_first_pass"] == 1


def test_csv_role_usage_preserves_columns_and_zero_vs_unavailable(tmp_path: Path) -> None:
    invocations = (
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=ContextManifest(role="worker"),
            usage=ReportedUsage(
                input_tokens=0,
                cached_input_tokens=0,
                output_tokens=0,
                reasoning_tokens=0,
            ),
            duration_ms=1,
        ),
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=ContextManifest(role="worker"),
            usage=ReportedUsage(input_tokens=4, cached_input_tokens=3, output_tokens=2),
            duration_ms=1,
        ),
        InvocationTelemetry(
            role="reviewer",
            profile_id="codex-session",
            model="codex",
            context=ContextManifest(role="reviewer"),
            usage=ReportedUsage(unavailable_reason="interactive usage unavailable"),
            duration_ms=1,
        ),
    )
    target = tmp_path / "role-usage.csv"

    _write_csv(target, (telemetry(invocations=invocations),))

    with target.open(newline="") as handle:
        reader = csv.DictReader(handle)
        row = next(reader)
        assert next(reader, None) is None
    existing_fields = [
        "experiment_id",
        "task_id",
        "task_class",
        "predicted_mode",
        "actual_mode",
        "worker_profile",
        "final_outcome",
        "worker_attempts",
        "repair_count",
        "repair_severity",
        "execution_duration_ms",
        "review_duration_ms",
        "total_wall_time_ms",
        "context_bytes",
        "repeated_context_bytes",
        "repeated_context_ratio",
        "shadow_would_accept",
        "shadow_decision_correct",
        "estimated_cost",
    ]
    assert reader.fieldnames is not None
    assert reader.fieldnames[: len(existing_fields)] == existing_fields
    assert reader.fieldnames[len(existing_fields) :] == [
        f"{role}_{field}"
        for role in ("planner", "worker", "reviewer")
        for field in (
            "invocation_count",
            "usage_unavailable_invocations",
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_tokens",
        )
    ]
    assert row["experiment_id"] == "step09a-test"
    assert row["final_outcome"] == "accepted_first_pass"
    assert row["planner_invocation_count"] == "0"
    assert row["planner_usage_unavailable_invocations"] == "0"
    assert row["planner_input_tokens"] == ""
    assert row["worker_invocation_count"] == "2"
    assert row["worker_usage_unavailable_invocations"] == "0"
    assert row["worker_input_tokens"] == "4"
    assert row["worker_cached_input_tokens"] == "3"
    assert row["worker_output_tokens"] == "2"
    assert row["worker_reasoning_tokens"] == "0"
    assert row["reviewer_invocation_count"] == "1"
    assert row["reviewer_usage_unavailable_invocations"] == "1"
    assert row["reviewer_input_tokens"] == ""
    assert row["reviewer_cached_input_tokens"] == ""
    assert row["reviewer_output_tokens"] == ""
    assert row["reviewer_reasoning_tokens"] == ""


def test_stable_experiment_identity_and_empty_context_ratio() -> None:
    trial = telemetry(invocations=())
    assert trial.repeated_context_ratio == 0.0
    assert stable_json_hash(trial) == stable_json_hash(trial)
    assert new_experiment_id().startswith("step09a-")
    with pytest.raises(ValidationError, match="unavailable cost"):
        telemetry(estimated_cost=None, cost_unavailable_reason=None)
    with pytest.raises(ValidationError, match="unavailable egress"):
        telemetry(egress_bytes=None, egress_unavailable_reason=None)
