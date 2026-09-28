"""WP13 controlled harness tests (fixture-only, no live benchmark)."""

from __future__ import annotations

import argparse
import json
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from vuzol.experiments.analysis import (
    TrialRecord,
    analyze_hypothesis,
    build_report,
    check_pricing_consistency,
    classify_telemetry_outcome,
    cluster_bootstrap_ci,
    compare_arms,
    paired_deltas,
    summarize_arm,
    trial_record_from_json,
)
from vuzol.experiments.arms import ExperimentArm, describe_execution_path, plan_cohort
from vuzol.experiments.corpus import (
    CorpusManifest,
    CorpusSplit,
    load_corpus_manifest,
)
from vuzol.experiments.export import joint_export, pricing_comparable
from vuzol.experiments.snapshot import PolicySnapshot

from ._test_experiments_helpers import (
    ContextManifest,
    InvocationTelemetry,
    ReportedUsage,
    telemetry,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "experiments"


def _record(**updates: object) -> TrialRecord:
    values: dict[str, object] = {
        "pair_id": "t:seed-1",
        "task_id": "t",
        "family": "isolated",
        "arm": "current",
        "status": "verified_success",
        "verified": True,
        "assisted": False,
        "cost": Decimal("0.010"),
        "cost_unknown": False,
        "pricing_revision": "trial-v1",
        "duration_ms": 1000,
        "deadline_ms": 60000,
    }
    values.update(updates)
    return TrialRecord(**values)  # type: ignore[arg-type]


def _smoke_records() -> tuple[TrialRecord, ...]:
    raw = json.loads((FIXTURES / "smoke-outcomes.v1.json").read_text())
    return tuple(trial_record_from_json(item) for item in raw["records"])


# --- Corpus ---


def test_corpus_loads_versioned_with_smoke8_and_splits() -> None:
    manifest = load_corpus_manifest(FIXTURES / "corpus.v1.json")
    assert manifest.schema_version == "experiment-corpus.v1"
    assert manifest.corpus_revision == "corpus.v1"
    assert len(manifest.tasks) == 12
    assert len(manifest.smoke_tasks()) == 8
    assert manifest.split_tasks(CorpusSplit.HELD_OUT)
    assert manifest.split_tasks(CorpusSplit.DEV)
    assert manifest.split_tasks(CorpusSplit.CALIBRATION)
    assert manifest.content_hash == load_corpus_manifest(FIXTURES / "corpus.v1.json").content_hash
    assert any(task.stratum.value == "jev_negative" for task in manifest.smoke_tasks())


def test_corpus_rejects_duplicate_task_ids() -> None:
    manifest = load_corpus_manifest(FIXTURES / "corpus.v1.json")
    first = manifest.tasks[0]
    with pytest.raises(ValidationError, match="unique"):
        CorpusManifest(
            corpus_revision="corpus.v1",
            tasks=(first, first),
        )


# --- Arms ---


def test_three_arms_have_distinct_documented_execution_paths() -> None:
    traces = {arm: describe_execution_path(arm) for arm in ExperimentArm}
    step_traces = {
        arm: tuple(step["step_type"] for step in trace["steps"]) for arm, trace in traces.items()
    }
    assert len(set(step_traces.values())) == 3
    assert step_traces[ExperimentArm.STRONG_SOLO] == (
        "interpret",
        "prepare_worktree",
        "execute_code",
    )
    assert "review" in step_traces[ExperimentArm.HYBRID]
    assert "approval" in step_traces[ExperimentArm.CURRENT]
    assert "approval" not in step_traces[ExperimentArm.STRONG_SOLO]
    workflow_types = {trace["workflow_type"] for trace in traces.values()}
    assert len(workflow_types) == 3
    budget_modes = {trace["budget_mode"] for trace in traces.values()}
    assert budget_modes == {"strong", "efficient", "balanced"}


def test_cohort_pairs_task_ids_across_arms() -> None:
    manifest = load_corpus_manifest(FIXTURES / "corpus.v1.json")
    planned = plan_cohort(manifest, tuple(ExperimentArm), (1, 2), shuffle_seed=7, only_smoke=True)
    assert len(planned) == 8 * 2 * 3
    by_pair: dict[str, set[str]] = {}
    for run in planned:
        by_pair.setdefault(run.pair_id, set()).add(run.arm.value)
        assert run.pair_id == f"{run.corpus_task_id}:seed-{run.seed}"
    assert all(arms == {"current", "strong_solo", "hybrid"} for arms in by_pair.values())


def test_cohort_order_is_randomized_but_reproducible() -> None:
    manifest = load_corpus_manifest(FIXTURES / "corpus.v1.json")
    first = plan_cohort(manifest, tuple(ExperimentArm), (1,), shuffle_seed=7)
    second = plan_cohort(manifest, tuple(ExperimentArm), (1,), shuffle_seed=7)
    third = plan_cohort(manifest, tuple(ExperimentArm), (1,), shuffle_seed=99)
    assert [run.pair_id for run in first] == [run.pair_id for run in second]
    assert [run.pair_id for run in first] != [run.pair_id for run in third]
    assert [run.order_index for run in first] == list(range(len(first)))
    assert [run.pair_id for run in first] != sorted(run.pair_id for run in first)


# --- Snapshot ---


def test_policy_snapshot_is_immutable_and_hash_pinned() -> None:
    values = {
        "snapshot_id": "snap-1",
        "policy_revision": "policy-v1",
        "configuration_revision": "config-abc",
        "profiles": (
            {"profile_id": "worker", "provider": "test", "model": "m", "roles": ["executor"]},
        ),
        "pricing": ({"pricing_revision": "trial-v1", "configured_cost_per_call": Decimal("0.01")},),
        "prompt_versions": {"worker": "step09a-worker-v2"},
        "tool_versions": {"git": "2.43.0"},
        "environment": {"sandbox": "v1"},
        "cache_policy": "cold-measured",
        "created_at": "2026-09-28T00:00:00Z",
    }
    snapshot = PolicySnapshot.model_validate(values)
    assert snapshot.snapshot_hash == PolicySnapshot.model_validate(values).snapshot_hash
    assert snapshot.pricing_for("trial-v1") is not None
    assert snapshot.pricing_for("other") is None
    with pytest.raises(ValidationError):
        snapshot.policy_revision = "mutated"


# --- Analysis ---


def test_smoke8_report_without_live() -> None:
    records = _smoke_records()
    assert len(records) == 24
    report = build_report(records, hypothesis_ids=("H1",))
    assert report["schema_version"] == "experiment-analysis.v1"
    assert set(report["arms"]) == {"current", "strong_solo", "hybrid"}
    hybrid = report["arms"]["hybrid"]
    assert hybrid["n"] == 8
    assert hybrid["successes"] == 6
    assert hybrid["autonomous_successes"] == 5
    assert hybrid["unknown_cost_records"] == 0


def test_denominator_keeps_failures_and_censored() -> None:
    records = (
        _record(status="failed", verified=False, cost=Decimal("0.02")),
        _record(
            status="censored",
            verified=False,
            cost=None,
            cost_unknown=True,
            pair_id="t2:seed-1",
            task_id="t2",
        ),
    )
    summary = summarize_arm(records)
    assert summary["n"] == 2
    assert summary["successes"] == 0
    assert summary["c_success"] is None
    assert summary["c_success_undefined"] is True
    assert summary["measured_cost"] == "0.02"
    assert summary["unknown_cost_records"] == 1


def test_all_failed_cohort_is_inconclusive_not_victory() -> None:
    records = tuple(
        _record(
            status="failed",
            verified=False,
            pair_id=f"t{i}:seed-1",
            task_id=f"t{i}",
            family="isolated",
            arm=arm,
        )
        for i in range(8)
        for arm in ("current", "hybrid")
    )
    comparison = compare_arms(records, "current", "hybrid")
    assert comparison["inconclusive"] is True
    assert any("undefined" in reason for reason in comparison["inconclusive_reasons"])


def test_censored_deadline_overrides_claimed_success() -> None:
    record = _record(duration_ms=61_000, deadline_ms=60_000)
    assert record.effective_status() == "censored"
    assert summarize_arm((record,))["successes"] == 0


def test_paired_deltas_and_deterministic_bootstrap_ci() -> None:
    records = _smoke_records()
    deltas = paired_deltas(records, "hybrid", "strong_solo")
    assert deltas["paired_n"] == 8
    assert deltas["arm_a"] == "hybrid"
    assert set(deltas["success_deltas_by_family"]) >= {"isolated", "research"}
    values = tuple(float(value) for value in deltas["success_deltas"])
    clusters = ("isolated",) * len(values)
    first = cluster_bootstrap_ci(values, clusters, seed=11)
    second = cluster_bootstrap_ci(values, clusters, seed=11)
    assert first == second
    assert first["reps"] == 2000
    assert first["undefined"] is False
    empty = cluster_bootstrap_ci((), ())
    assert empty["undefined"] is True


def test_pricing_mismatch_blocks_cash_comparison() -> None:
    records = _smoke_records()
    assert check_pricing_consistency(records)["consistent"] is True
    mixed = (
        *records,
        _record(pricing_revision="trial-v2", pair_id="x:seed-1", task_id="x"),
    )
    assert check_pricing_consistency(mixed)["consistent"] is False
    comparison = compare_arms(mixed, "current", "hybrid")
    assert comparison["inconclusive"] is True
    assert any("pricing" in reason for reason in comparison["inconclusive_reasons"])


def test_hypothesis_registry_supports_all_nine_questions() -> None:
    from vuzol.experiments.analysis import HYPOTHESES

    records = _smoke_records()
    assert len(HYPOTHESES) == 9
    for hypothesis_id in ("H1", "H2", "H3", "H4", "H5", "H6", "H7", "H8", "H9"):
        result = analyze_hypothesis(records, hypothesis_id, bootstrap_reps=50)
        assert result["hypothesis_id"] == hypothesis_id
        assert "comparison" in result
    with pytest.raises(ValueError, match="unknown hypothesis"):
        analyze_hypothesis(records, "H10")


def test_telemetry_outcome_mapping_never_trusts_self_score() -> None:
    assert classify_telemetry_outcome("accepted_first_pass", verified=True) == "verified_success"
    assert classify_telemetry_outcome("accepted_first_pass", verified=False) == "censored"
    assert classify_telemetry_outcome("accepted_after_major_repair", verified=False) == "censored"
    assert classify_telemetry_outcome("lead_takeover", verified=True) == "failed"
    assert classify_telemetry_outcome("discarded", verified=False) == "failed"
    assert classify_telemetry_outcome("blocked_environment", verified=False) == "censored"
    with pytest.raises(ValueError, match="unknown ReviewOutcome"):
        classify_telemetry_outcome("nope", verified=True)


# --- Joint export ---


def test_joint_export_keeps_projections_separate() -> None:
    from vuzol.experiments.export import EXPORT_SCHEMA

    summary = {"task_count": 2}
    export = joint_export(
        summary,
        (("review", Decimal("0.01"), 3), ("coding", Decimal("0.05"), 5)),
        (Decimal("0.02"), 2),
        experiment_id="exp-1",
    )
    assert export["schema_version"] == EXPORT_SCHEMA
    assert export["harness"] == summary
    assert export["ledger"]["purpose_cost_units"] == "0.06"
    assert export["ledger"]["purpose_invocations"] == 8
    assert export["ledger"]["retry_subtotal"]["invocations"] == 2
    assert export["double_count_check"]["retry_invocations_le_purpose_invocations"] is True
    with pytest.raises(ValueError, match="inconsistent"):
        joint_export(
            summary,
            (("review", Decimal("0.01"), 1),),
            (Decimal("0.02"), 5),
            experiment_id="e",
        )


def test_pricing_gate_requires_single_known_revision() -> None:
    assert pricing_comparable(["trial-v1", "trial-v1"])["comparable"] is True
    assert pricing_comparable(["trial-v1", "trial-v2"])["comparable"] is False
    assert pricing_comparable([None])["comparable"] is False


def test_aggregate_measured_totals_with_unavailable_counts() -> None:
    from vuzol.experiments.telemetry import aggregate_trials

    context = ContextManifest(role="worker")
    trial = telemetry(
        invocations=(
            InvocationTelemetry(
                role="worker",
                profile_id="p",
                model="m",
                context=context,
                usage=ReportedUsage(input_tokens=100, output_tokens=20),
                duration_ms=1,
            ),
            InvocationTelemetry(
                role="reviewer",
                profile_id="q",
                model="m",
                context=ContextManifest(role="reviewer"),
                usage=ReportedUsage(unavailable_reason="no usage exposed"),
                duration_ms=1,
            ),
        )
    )
    summary = aggregate_trials((trial,))
    assert summary["provider_input_tokens"] == 100
    assert summary["provider_output_tokens"] == 20
    assert summary["provider_input_tokens_complete"] is False
    assert summary["provider_usage_unavailable_invocations"] == 1


# --- CLI plan/analyze ---


class _Engine:
    def __init__(self) -> None:
        self.dispose = AsyncMock()


class _Factory:
    def __init__(self) -> None:
        self.session = MagicMock()

    @asynccontextmanager
    async def begin(self):  # type: ignore[no-untyped-def]
        yield self.session

    @asynccontextmanager
    async def __call__(self):  # type: ignore[no-untyped-def]
        yield self.session


@pytest.mark.anyio
async def test_cli_plan_and_analyze(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import vuzol.cli.experiment as cli

    runtime = SimpleNamespace(settings=object(), registries=object())
    engine = _Engine()
    monkeypatch.setattr(cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(cli, "resolve_database_dsn", lambda _settings: "dsn")
    monkeypatch.setattr(cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(cli, "create_session_factory", lambda _engine: _Factory())

    plan_path = tmp_path / "plan.json"
    await cli._run(
        argparse.Namespace(
            command="plan",
            corpus=FIXTURES / "corpus.v1.json",
            arms=["current", "hybrid"],
            seeds=[1],
            shuffle_seed=7,
            smoke_only=True,
            json=plan_path,
        )
    )
    plan = json.loads(plan_path.read_text())
    assert plan["schema_version"] == "experiment-run-plan.v1"
    assert len(plan["runs"]) == 8 * 1 * 2
    assert plan["runs"][0]["order_index"] == 0
    assert json.loads(capsys.readouterr().out)["runs"] == 16

    report_path = tmp_path / "report.json"
    await cli._run(
        argparse.Namespace(
            command="analyze",
            trials=FIXTURES / "smoke-outcomes.v1.json",
            ledger=None,
            hypotheses=["H1"],
            bootstrap_reps=50,
            bootstrap_seed=0,
            json=report_path,
        )
    )
    report = json.loads(report_path.read_text())
    assert report["schema_version"] == "experiment-analysis.v1"
    assert report["records"] == 24
    assert json.loads(capsys.readouterr().out)["records"] == 24
    assert engine.dispose.await_count == 2
