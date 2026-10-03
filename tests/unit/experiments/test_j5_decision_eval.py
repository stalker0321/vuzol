"""J5 decision corpus, replay, canary and threshold evaluation tests."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from vuzol.experiments.canary import (
    CanaryPolicy,
    KillSwitch,
    cohort_bucket,
    in_cohort,
)
from vuzol.experiments.decision import WhitelistGate
from vuzol.experiments.decision_corpus import (
    DecisionCorpus,
    DecisionCorpusError,
    DecisionFamily,
    DecisionLabel,
    DecisionOpportunity,
    DecisionSplit,
    load_decision_corpus,
    validate_no_temporal_leakage,
)
from vuzol.experiments.decision_eval import (
    DEFAULT_THRESHOLDS,
    EvalThresholds,
    evaluate_traces,
)
from vuzol.experiments.replay import ReplayStage, replay_intake

ROOT = Path(__file__).resolve().parents[3]
CORPUS_PATH = ROOT / "tests/fixtures/experiments/decision-corpus.v3.json"


def _corpus() -> DecisionCorpus:
    return load_decision_corpus(CORPUS_PATH)


def test_seed_corpus_loads_without_leakage_and_covers_families() -> None:
    corpus = _corpus()
    assert len(corpus.opportunities) == 16
    assert len(corpus.families()) == len(DecisionFamily)
    assert corpus.by_split(DecisionSplit.HELD_OUT)
    assert corpus.content_hash == corpus.content_hash


def test_temporal_leakage_rejected() -> None:
    def opportunity(oid: str, split: DecisionSplit) -> DecisionOpportunity:
        return DecisionOpportunity(
            opportunity_id=oid,
            group_id="shared-group",
            family=DecisionFamily.INTENT,
            split=split,
            current_turn="hello",
            snapshot_ref="snapshot:1",
            label=DecisionLabel(effect="respond"),
        )

    corpus = DecisionCorpus(
        corpus_revision="leaky",
        opportunities=(
            opportunity("one", DecisionSplit.DEV),
            opportunity("two", DecisionSplit.HELD_OUT),
        ),
    )
    with pytest.raises(DecisionCorpusError, match="temporal leakage"):
        validate_no_temporal_leakage(corpus)


def test_replay_explains_parsing_mapping_application() -> None:
    corpus = _corpus()
    opportunity = next(
        item for item in corpus.opportunities if item.opportunity_id == "ref-continue-task"
    )
    assert opportunity.recorded_response is not None
    trace = replay_intake(opportunity, opportunity.recorded_response)
    stages = {stage.stage: stage for stage in trace.stages}
    assert stages[ReplayStage.PARSING].ok is True
    assert stages[ReplayStage.MAPPING].ok is True
    assert "execute_request" in stages[ReplayStage.MAPPING].detail
    assert trace.route_hint == "execute_request"
    assert trace.target_ref == "task:abc"
    assert trace.applied is False
    assert trace.reason_code == "advisory_only"


def test_replay_invalid_response_skips_application() -> None:
    corpus = _corpus()
    opportunity = next(
        item for item in corpus.opportunities if item.opportunity_id == "intent-quoted-delete"
    )
    trace = replay_intake(opportunity, {"schema": "decision.v3", "decision_kind": "intake"})
    stages = {stage.stage: stage for stage in trace.stages}
    assert stages[ReplayStage.PARSING].ok is False
    assert stages[ReplayStage.MAPPING].ok is False
    assert stages[ReplayStage.APPLICATION].ok is False
    assert trace.applied is False


def test_canary_is_deterministic_and_one_kind_only() -> None:
    assert cohort_bucket("stable-id") == cohort_bucket("stable-id")
    assert in_cohort("stable-id", 100) is True
    assert in_cohort("stable-id", 0) is False

    policy = CanaryPolicy(
        enabled_kind="intake",
        cohort_percent=100,
        allowlist=WhitelistGate(enabled_kinds=frozenset({"intake"})),
    )
    admitted = policy.admit(decision_kind="intake", opportunity_id="op-1")
    assert admitted.allowed is True and admitted.reason == "admitted"
    other = policy.admit(decision_kind="work_shape", opportunity_id="op-1")
    assert other.allowed is False and other.reason == "kind_not_allowlisted"

    not_whitelisted = CanaryPolicy(
        enabled_kind="intake", cohort_percent=100, allowlist=WhitelistGate()
    ).admit(decision_kind="intake", opportunity_id="op-1")
    assert not_whitelisted.allowed is False
    assert not_whitelisted.reason == "not_whitelisted"


def test_kill_switch_and_rollback_block_apply() -> None:
    switch = KillSwitch()
    policy = CanaryPolicy(
        enabled_kind="intake",
        cohort_percent=100,
        allowlist=WhitelistGate(enabled_kinds=frozenset({"intake"})),
        kill_switch=switch,
    )
    assert policy.admit(decision_kind="intake", opportunity_id="op-1").allowed is True
    policy.rollback()
    blocked = policy.admit(decision_kind="intake", opportunity_id="op-1")
    assert blocked.allowed is False and blocked.reason == "kill_switch"
    switch.thaw("intake")
    assert policy.admit(decision_kind="intake", opportunity_id="op-1").allowed is True


def test_evaluate_seed_corpus_meets_pre_registered_thresholds() -> None:
    corpus = _corpus()
    policy = CanaryPolicy(
        enabled_kind="intake",
        cohort_percent=100,
        allowlist=WhitelistGate(enabled_kinds=frozenset({"intake"})),
    )
    traces = {
        item.opportunity_id: replay_intake(item, item.recorded_response or {})
        for item in corpus.opportunities
    }
    admissions = {
        item.opportunity_id: policy.admit(
            decision_kind="intake", opportunity_id=item.opportunity_id
        ).allowed
        for item in corpus.opportunities
    }
    report = evaluate_traces(corpus, traces, admissions=admissions)
    assert report.thresholds is DEFAULT_THRESHOLDS
    assert len(report.per_family) == len(DecisionFamily)
    assert report.overall.false_execute == 0
    assert report.thresholds_met is True
    assert 0.0 <= report.overall.target_accuracy <= 1.0
    assert report.overall.target_accuracy >= DEFAULT_THRESHOLDS.min_target_accuracy
    assert report.overall.coverage >= DEFAULT_THRESHOLDS.min_decided_coverage
    # The seed corpus records no cost/latency data: those thresholds stay
    # honestly unevaluated and are not claimed as met.
    assert set(report.unevaluated) == {"max_cost_per_decision", "max_p95_latency_ms"}
    assert report.fully_evaluated is False


def test_cost_and_latency_thresholds_are_evaluated_when_provided() -> None:
    corpus = _corpus()
    traces = {
        item.opportunity_id: replay_intake(item, item.recorded_response or {})
        for item in corpus.opportunities
    }
    within = evaluate_traces(
        corpus,
        traces,
        cost_per_decision=Decimal("0.001"),
        p95_latency_ms=100,
    )
    assert within.failures == ()
    assert within.unevaluated == ()
    assert within.fully_evaluated is True

    over = evaluate_traces(
        corpus,
        traces,
        cost_per_decision=Decimal("1.0"),
        p95_latency_ms=999_999,
    )
    assert "max_cost_per_decision" in over.failures
    assert "max_p95_latency_ms" in over.failures
    assert over.thresholds_met is False


def test_abstain_correctness_does_not_inflate_target_accuracy() -> None:
    corpus = DecisionCorpus(
        corpus_revision="abstain-only",
        opportunities=(
            DecisionOpportunity(
                opportunity_id="abstain-one",
                group_id="abstain-one",
                family=DecisionFamily.COMPOUND,
                split=DecisionSplit.DEV,
                current_turn="two independent requests",
                snapshot_ref="snapshot:abstain",
                allowed_refs=("turn:current",),
                label=DecisionLabel(effect=None, abstain_allowed=True),
                recorded_response={
                    "schema": "decision.v3",
                    "decision_kind": "intake",
                    "state_revision": 0,
                    "effect": "respond",
                    "relation": "none",
                    "target_id": None,
                    "support_refs": [],
                    "coverage": "complete",
                    "abstain": True,
                    "reason_code": "compound_intent",
                    "input_fingerprint": "a" * 64,
                },
            ),
        ),
    )
    traces = {
        item.opportunity_id: replay_intake(item, item.recorded_response or {})
        for item in corpus.opportunities
    }
    report = evaluate_traces(corpus, traces)
    assert report.overall.decided == 0
    assert report.overall.correct == 0
    assert report.overall.abstain_correct == 1
    assert report.overall.target_accuracy == 0.0


def test_applied_outside_cohort_fails_thresholds() -> None:
    corpus = _corpus()
    opportunity = next(
        item for item in corpus.opportunities if item.opportunity_id == "ref-continue-task"
    )
    trace = replay_intake(
        opportunity, opportunity.recorded_response or {}, application=lambda _decision: None
    )
    assert trace.applied is True
    report = evaluate_traces(
        corpus,
        {opportunity.opportunity_id: trace},
        admissions={},
        thresholds=EvalThresholds(max_unauthorized_transitions=0),
    )
    assert report.overall.unauthorized == 1
    assert "unauthorized_transitions" in report.failures
    assert report.thresholds_met is False
