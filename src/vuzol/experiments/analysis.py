"""Deterministic outcome analysis for the controlled harness (WP13).

Standard library only: no providers, no database. Inputs are plain frozen
records; money uses Decimal. Rules (EXPERIMENTS.md §4, baseline.md §2):

- failed/aborted/censored runs stay in the denominator and cost numerator;
- successes=0 → cost-to-success is undefined (None + flag), never 0;
- proposer self-score is never ground truth: only ``verified`` successes
  count (independent Git/holdout verification happens outside this module);
- paired deltas aggregate by task family; cluster bootstrap over families;
- inconclusive (not victory) when uncertainty forbids a conclusion.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import TypedDict, cast

ANALYSIS_SCHEMA = "experiment-analysis.v1"

STATUS_SUCCESS = "verified_success"
STATUS_FAILED = "failed"
STATUS_ABORTED = "aborted"
STATUS_CENSORED = "censored"

HYPOTHESES: dict[str, dict[str, str]] = {
    "H1": {
        "question": "Hybrid дешевле strong-solo",
        "arms": "hybrid,strong_solo",
        "metric": "c_success",
        "experiments": "EX01",
    },
    "H2": {
        "question": "Planner нужен на горизонте",
        "arms": "hybrid,current",
        "metric": "success_rate",
        "experiments": "EX02",
    },
    "H3": {
        "question": "Jev экономит",
        "arms": "hybrid,current",
        "metric": "c_success",
        "experiments": "EX03",
    },
    "H4": {
        "question": "Review на boundary достаточно",
        "arms": "hybrid,current",
        "metric": "success_rate",
        "experiments": "EX04",
    },
    "H5": {
        "question": "Bounded context полезен",
        "arms": "hybrid,strong_solo",
        "metric": "c_success",
        "experiments": "EX05",
    },
    "H6": {
        "question": "Parallel implementation ускоряет",
        "arms": "hybrid,strong_solo",
        "metric": "latency",
        "experiments": "EX06",
    },
    "H7": {
        "question": "Capabilities действительно накапливаются",
        "arms": "hybrid,current",
        "metric": "c_success",
        "experiments": "EX07",
    },
    "H8": {
        "question": "Автономный recovery лучше pause",
        "arms": "hybrid,current",
        "metric": "success_rate",
        "experiments": "EX08",
    },
    "H9": {
        "question": "Knowledge edges нужны",
        "arms": "hybrid,current",
        "metric": "success_rate",
        "experiments": "EX09",
    },
}


@dataclass(frozen=True, slots=True)
class TrialRecord:
    pair_id: str
    task_id: str
    family: str
    arm: str
    status: str
    verified: bool = False
    assisted: bool = False
    cost: Decimal | None = None
    cost_unknown: bool = False
    pricing_revision: str | None = None
    duration_ms: int = 0
    deadline_ms: int | None = None

    def effective_status(self) -> str:
        if (
            self.deadline_ms is not None
            and self.duration_ms > self.deadline_ms
            and self.status == STATUS_SUCCESS
        ):
            return STATUS_CENSORED
        return self.status


def is_success(record: TrialRecord) -> bool:
    return record.effective_status() == STATUS_SUCCESS and record.verified


def is_autonomous_success(record: TrialRecord) -> bool:
    return is_success(record) and not record.assisted


class ArmSummary(TypedDict):
    n: int
    successes: int
    autonomous_successes: int
    success_rate: float
    autonomous_success_rate: float
    measured_cost: str
    unknown_cost_records: int
    c_success: str | None
    c_success_undefined: bool


class PairedDeltas(TypedDict):
    arm_a: str
    arm_b: str
    paired_n: int
    success_deltas: list[int]
    mean_success_delta: float
    success_deltas_by_family: dict[str, int]
    cost_deltas: list[str]
    unknown_cost_pairs: int


class CiResult(TypedDict):
    lo: float | None
    hi: float | None
    reps: int
    undefined: bool


class PricingCheck(TypedDict):
    consistent: bool
    revisions: list[str]


class ArmComparison(TypedDict):
    arm_a: ArmSummary
    arm_b: ArmSummary
    deltas: PairedDeltas
    success_delta_ci: CiResult
    pricing: PricingCheck
    inconclusive: bool
    inconclusive_reasons: list[str]


class HypothesisResult(TypedDict):
    hypothesis_id: str
    question: str
    metric: str
    experiments: str
    comparison: ArmComparison


class AnalysisReport(TypedDict):
    schema_version: str
    records: int
    denominator_note: str
    arms: dict[str, ArmSummary]
    hypotheses: list[HypothesisResult]


def summarize_arm(records: tuple[TrialRecord, ...]) -> ArmSummary:
    """Per-arm summary; denominator always includes failures/aborts/censored."""

    denominator = len(records)
    successes = sum(1 for item in records if is_success(item))
    autonomous = sum(1 for item in records if is_autonomous_success(item))
    measured_cost = sum((item.cost for item in records if item.cost is not None), Decimal("0"))
    unknown_cost = sum(1 for item in records if item.cost is None or item.cost_unknown)
    if successes:
        c_success: Decimal | None = (measured_cost / successes).quantize(Decimal("0.000001"))
        c_undefined = False
    else:
        c_success = None
        c_undefined = True
    return {
        "n": denominator,
        "successes": successes,
        "autonomous_successes": autonomous,
        "success_rate": (successes / denominator) if denominator else 0.0,
        "autonomous_success_rate": (autonomous / denominator) if denominator else 0.0,
        "measured_cost": str(measured_cost),
        "unknown_cost_records": unknown_cost,
        "c_success": str(c_success) if c_success is not None else None,
        "c_success_undefined": c_undefined,
    }


def paired_deltas(records: tuple[TrialRecord, ...], arm_a: str, arm_b: str) -> PairedDeltas:
    """Paired per-pair deltas (a-b) for success and cost, grouped by family."""

    by_pair: dict[str, dict[str, TrialRecord]] = {}
    for item in records:
        if item.arm in (arm_a, arm_b):
            by_pair.setdefault(item.pair_id, {})[item.arm] = item
    pairs = {pair: arms for pair, arms in by_pair.items() if len(arms) == 2}
    success_deltas: list[int] = []
    cost_deltas: list[str] = []
    unknown_cost_pairs = 0
    families: dict[str, list[int]] = {}
    for _pair, arms in sorted(pairs.items()):
        first, second = arms[arm_a], arms[arm_b]
        delta = int(is_success(first)) - int(is_success(second))
        success_deltas.append(delta)
        families.setdefault(first.family, []).append(delta)
        if first.cost is None or second.cost is None:
            unknown_cost_pairs += 1
            continue
        cost_deltas.append(str(first.cost - second.cost))
    mean = (sum(success_deltas) / len(success_deltas)) if success_deltas else 0.0
    return {
        "arm_a": arm_a,
        "arm_b": arm_b,
        "paired_n": len(pairs),
        "success_deltas": success_deltas,
        "mean_success_delta": mean,
        "success_deltas_by_family": {family: sum(values) for family, values in families.items()},
        "cost_deltas": cost_deltas,
        "unknown_cost_pairs": unknown_cost_pairs,
    }


def cluster_bootstrap_ci(
    values: tuple[float, ...],
    clusters: tuple[str, ...],
    *,
    reps: int = 2000,
    seed: int = 0,
) -> CiResult:
    """Cluster bootstrap CI for the mean (resample clusters, deterministic seed)."""

    if len(values) != len(clusters):
        raise ValueError("values and clusters must align")
    unique = sorted(set(clusters))
    if not unique or not values:
        return {"lo": None, "hi": None, "reps": 0, "undefined": True}
    rng = random.Random(seed)  # noqa: S311 - deterministic bootstrap, not security
    index_by_cluster: dict[str, list[int]] = {}
    for position, cluster in enumerate(clusters):
        index_by_cluster.setdefault(cluster, []).append(position)
    means: list[float] = []
    for _ in range(reps):
        sample: list[float] = []
        for _ in unique:
            chosen = unique[rng.randrange(len(unique))]
            sample.extend(values[position] for position in index_by_cluster[chosen])
        means.append(sum(sample) / len(sample))
    means.sort()
    lo = means[max(0, math.ceil(0.025 * reps) - 1)]
    hi = means[min(len(means) - 1, math.floor(0.975 * reps) - 1)]
    return {"lo": lo, "hi": hi, "reps": reps, "undefined": False}


def check_pricing_consistency(records: tuple[TrialRecord, ...]) -> PricingCheck:
    revisions = sorted({item.pricing_revision or "unknown" for item in records})
    return {"consistent": len(revisions) == 1, "revisions": revisions}


def compare_arms(
    records: tuple[TrialRecord, ...],
    arm_a: str,
    arm_b: str,
    *,
    bootstrap_reps: int = 2000,
    bootstrap_seed: int = 0,
    min_paired_n: int = 8,
) -> ArmComparison:
    """Compare two arms; inconclusive (never victory) when evidence is thin."""

    arms = {item.arm for item in records}
    reasons: list[str] = []
    if arm_a not in arms or arm_b not in arms:
        reasons.append("missing arm data")
    deltas = paired_deltas(records, arm_a, arm_b)
    paired_n = deltas["paired_n"]
    if paired_n < min_paired_n:
        reasons.append(f"paired_n={paired_n} below minimum {min_paired_n}")
    summary_a = summarize_arm(tuple(item for item in records if item.arm == arm_a))
    summary_b = summarize_arm(tuple(item for item in records if item.arm == arm_b))
    if summary_a["c_success_undefined"] or summary_b["c_success_undefined"]:
        reasons.append("c_success undefined for an arm (0 successes)")
    pricing = check_pricing_consistency(
        tuple(item for item in records if item.arm in (arm_a, arm_b))
    )
    if not pricing["consistent"]:
        reasons.append(f"pricing revisions differ: {pricing['revisions']}")
    success_values = tuple(float(value) for value in deltas["success_deltas"])
    clusters = _pair_families(records, arm_a, deltas)
    ci = cluster_bootstrap_ci(success_values, clusters, reps=bootstrap_reps, seed=bootstrap_seed)
    if not ci["undefined"] and ci["lo"] is not None and ci["hi"] is not None:
        if float(ci["lo"]) <= 0.0 <= float(ci["hi"]):
            reasons.append("success-delta CI includes 0")
    else:
        reasons.append("CI undefined")
    return {
        "arm_a": summary_a,
        "arm_b": summary_b,
        "deltas": deltas,
        "success_delta_ci": ci,
        "pricing": pricing,
        "inconclusive": bool(reasons),
        "inconclusive_reasons": reasons,
    }


def _pair_families(
    records: tuple[TrialRecord, ...], arm: str, deltas: PairedDeltas
) -> tuple[str, ...]:
    family_by_pair = {item.pair_id: item.family for item in records if item.arm == arm}
    return tuple(family_by_pair[pair] for pair in _ordered_pairs(records, deltas))


def _ordered_pairs(records: tuple[TrialRecord, ...], deltas: PairedDeltas) -> list[str]:
    arms = deltas["arm_a"], deltas["arm_b"]
    by_pair: dict[str, dict[str, TrialRecord]] = {}
    for item in records:
        if item.arm in arms:
            by_pair.setdefault(item.pair_id, {})[item.arm] = item
    return sorted(pair for pair, members in by_pair.items() if len(members) == 2)


def analyze_hypothesis(
    records: tuple[TrialRecord, ...],
    hypothesis_id: str,
    *,
    bootstrap_reps: int = 2000,
    bootstrap_seed: int = 0,
) -> HypothesisResult:
    """Run one preregistered hypothesis comparison (fixed arms + metric)."""

    hypothesis = HYPOTHESES.get(hypothesis_id)
    if hypothesis is None:
        raise ValueError(f"unknown hypothesis: {hypothesis_id}")
    arm_a, arm_b = hypothesis["arms"].split(",")
    comparison = compare_arms(
        records, arm_a, arm_b, bootstrap_reps=bootstrap_reps, bootstrap_seed=bootstrap_seed
    )
    return {
        "hypothesis_id": hypothesis_id,
        "question": hypothesis["question"],
        "metric": hypothesis["metric"],
        "experiments": hypothesis["experiments"],
        "comparison": comparison,
    }


def classify_telemetry_outcome(final_outcome: str, *, verified: bool) -> str:
    """Map a harness ReviewOutcome to analysis status (contract unchanged).

    Accepted outcomes count as successes only with independent verification
    (Git verifier / holdout), never on proposer self-score. Unverified
    accepts stay censored: out of the numerator, in the denominator.
    """

    if final_outcome in {
        "accepted_first_pass",
        "accepted_after_minor_repair",
        "accepted_after_major_repair",
    }:
        return STATUS_SUCCESS if verified else STATUS_CENSORED
    if final_outcome in {"blocked_environment", "blocked_requirements"}:
        return STATUS_CENSORED
    if final_outcome in {"lead_takeover", "discarded"}:
        return STATUS_FAILED
    raise ValueError(f"unknown ReviewOutcome for analysis: {final_outcome}")


def trial_record_from_json(data: dict[str, object]) -> TrialRecord:
    """Parse one trial record from plain JSON (local analysis script input)."""

    raw_cost = data.get("cost")
    cost = Decimal(str(raw_cost)) if raw_cost is not None else None
    status = str(data["status"])
    if status not in {STATUS_SUCCESS, STATUS_FAILED, STATUS_ABORTED, STATUS_CENSORED}:
        raise ValueError(f"unknown trial status for analysis: {status}")
    deadline = data.get("deadline_ms")
    return TrialRecord(
        pair_id=str(data["pair_id"]),
        task_id=str(data["task_id"]),
        family=str(data["family"]),
        arm=str(data["arm"]),
        status=status,
        verified=bool(data.get("verified", False)),
        assisted=bool(data.get("assisted", False)),
        cost=cost,
        cost_unknown=bool(data.get("cost_unknown", cost is None)),
        pricing_revision=(
            str(data["pricing_revision"]) if data.get("pricing_revision") is not None else None
        ),
        duration_ms=cast(int, data.get("duration_ms", 0)),
        deadline_ms=cast(int, deadline) if deadline is not None else None,
    )


def build_report(
    records: tuple[TrialRecord, ...],
    *,
    hypothesis_ids: tuple[str, ...] = ("H1",),
    bootstrap_reps: int = 2000,
    bootstrap_seed: int = 0,
) -> AnalysisReport:
    arms = sorted({item.arm for item in records})
    return {
        "schema_version": ANALYSIS_SCHEMA,
        "records": len(records),
        "denominator_note": "failures/aborts/censored stay in the denominator and cost numerator",
        "arms": {
            arm: summarize_arm(tuple(item for item in records if item.arm == arm)) for arm in arms
        },
        "hypotheses": [
            analyze_hypothesis(
                records, hypothesis_id, bootstrap_reps=bootstrap_reps, bootstrap_seed=bootstrap_seed
            )
            for hypothesis_id in hypothesis_ids
        ],
    }
