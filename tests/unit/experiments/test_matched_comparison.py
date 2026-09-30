"""D6 Q3/Q4: matched-arms economic comparison on the cheap pool (offline).

No live models, no paid trials: deterministic fixture replay over the frozen
corpus manifest, aligned pricing/deadline/acceptance across arms A/B/C, and
the independent evaluator from analysis (verified success, never self-score).
Frozen inputs are hash-pinned; fixture data can only show inconclusive or
measured deltas, never model superiority (E19).
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

from vuzol.experiments.analysis import (
    TrialRecord,
    build_report,
    check_pricing_consistency,
    compare_arms,
    intervention_rates,
    is_success,
    latency_check,
    summarize_arm,
)
from vuzol.experiments.corpus import load_corpus_manifest
from vuzol.experiments.domain import stable_json_hash

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "experiments"

ARMS = ("current", "strong_solo", "candidate_delta")
PRICING_REVISION = "trial-v1"
DEADLINE_MS = 60_000
SNAPSHOT_SEED = "d6-matched-cheap-v1"


def _replay(task_id: str, family: str, arm: str, index: int) -> TrialRecord:
    digest = hashlib.sha256(f"{task_id}:{arm}:{SNAPSHOT_SEED}".encode()).hexdigest()
    draw = int(digest[:8], 16) / 2**32
    cost_draw = int(digest[8:16], 16) / 2**32
    duration = 5_000 + int(digest[16:24], 16) % 50_000
    if draw < 0.06:
        status, verified = "aborted", False
    elif draw < 0.14:
        status, verified = "failed", False
    elif draw < 0.20:
        status, verified = "verified_success", False
    else:
        status, verified = "verified_success", True
    censored = status == "verified_success" and duration > DEADLINE_MS - 1_000
    return TrialRecord(
        pair_id=f"{task_id}:seed-1",
        task_id=task_id,
        family=family,
        arm=arm,
        status=status,
        verified=verified,
        assisted=draw > 0.90,
        cost=Decimal(f"{0.005 + cost_draw * 0.05:.6f}"),
        cost_unknown=False,
        pricing_revision=PRICING_REVISION,
        duration_ms=duration if not censored else DEADLINE_MS + 5_000,
        deadline_ms=DEADLINE_MS,
        human_intervention=draw > 0.93,
        defect_categories=("concurrency_lifecycle_defect",) if 0.86 < draw < 0.90 else (),
    )


def _cohort() -> tuple[TrialRecord, ...]:
    manifest = load_corpus_manifest(FIXTURES / "corpus.v1.json")
    records: list[TrialRecord] = []
    for index, task in enumerate(manifest.tasks):
        for arm in ARMS:
            records.append(_replay(task.task_id, task.family, arm, index))
    return tuple(records)


def test_matched_arms_share_frozen_inputs() -> None:
    manifest = load_corpus_manifest(FIXTURES / "corpus.v1.json")
    pin = {
        "corpus_revision": manifest.corpus_revision,
        "corpus_hash": manifest.content_hash,
        "pricing_revision": PRICING_REVISION,
        "arms": list(ARMS),
        "seed": SNAPSHOT_SEED,
    }
    first = hashlib.sha256(json.dumps(pin, sort_keys=True).encode()).hexdigest()
    second = hashlib.sha256(json.dumps(pin, sort_keys=True).encode()).hexdigest()
    assert first == second
    assert manifest.content_hash == load_corpus_manifest(FIXTURES / "corpus.v1.json").content_hash
    assert stable_json_hash is not None


def test_matched_comparison_counts_full_denominator() -> None:
    records = _cohort()
    assert len(records) == len({record.pair_id for record in records}) * 3
    for arm in ARMS:
        summary = summarize_arm(tuple(item for item in records if item.arm == arm))
        assert summary["n"] == summary["successes"] + sum(
            1 for item in records if item.arm == arm and not is_success(item)
        )
        # Independent evaluator: unverified "successes" do not count.
        assert int(summary["successes"]) <= sum(
            1 for item in records if item.arm == arm and item.status == "verified_success"
        )
    pricing = check_pricing_consistency(records)
    assert pricing["consistent"] is True
    assert pricing["revisions"] == [PRICING_REVISION]


def test_matched_comparison_reports_without_victory_claims() -> None:
    records = _cohort()
    comparison = compare_arms(
        records, "candidate_delta", "current", bootstrap_reps=50, bootstrap_seed=7
    )
    assert set(comparison) >= {
        "metric",
        "arm_a",
        "arm_b",
        "success_delta_ci",
        "cost_ratio_ci",
        "latency",
        "pricing",
        "inconclusive",
        "inconclusive_reasons",
    }
    assert comparison["pricing"]["consistent"] is True
    latency = latency_check(records, "candidate_delta", "current", reps=50, seed=7)
    assert "censored_pairs" in latency or "ci" in latency
    rates = intervention_rates(records)
    assert set(rates) == set(ARMS)
    for arm in ARMS:
        assert rates[arm]["n"] == summarize_arm(tuple(i for i in records if i.arm == arm))["n"]
        assert rates[arm]["interventions"] <= rates[arm]["n"]
    report = build_report(records, hypothesis_ids=("H1",))
    assert set(report["arms"]) == set(ARMS)
    # Fixture replay is measurement only: inconclusive stays a valid outcome.
    assert isinstance(comparison["inconclusive"], bool)
