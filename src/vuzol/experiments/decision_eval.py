"""Offline decision evaluation: per-family metrics, thresholds and accounting (J5).

Thresholds are declared here, before any arm is chosen, and the seed corpus is
evaluated against them. Accounting folds the existing usage ledger so retries,
fallbacks and downstream calls are part of the total, not just the first call.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.experiments.decision_corpus import DecisionCorpus, DecisionFamily
from vuzol.experiments.replay import ReplayTrace
from vuzol.providers.budgets import usage_retry_subtotal, usage_totals_by_purpose


@dataclass(frozen=True, slots=True)
class EvalThresholds:
    """Pre-registered targets. Chosen before the arm, never after."""

    min_target_accuracy: float = 0.8
    max_false_execute_rate: float = 0.0
    max_unauthorized_transitions: int = 0
    min_decided_coverage: float = 0.5
    max_p95_latency_ms: int = 5_000
    max_cost_per_decision: Decimal = Decimal("0.05")


# Frozen defaults, declared in-module before any evaluation run.
DEFAULT_THRESHOLDS = EvalThresholds()


@dataclass(frozen=True, slots=True)
class FamilyMetrics:
    family: str
    total: int
    decided: int
    correct: int
    false_execute: int
    unauthorized: int
    abstain_correct: int = 0

    @property
    def coverage(self) -> float:
        return self.decided / self.total if self.total else 0.0

    @property
    def target_accuracy(self) -> float:
        """Accuracy among decided outputs only; always in [0, 1]."""

        return self.correct / self.decided if self.decided else 0.0

    @property
    def false_execute_rate(self) -> float:
        return self.false_execute / self.total if self.total else 0.0


@dataclass(frozen=True, slots=True)
class EvalReport:
    per_family: tuple[FamilyMetrics, ...]
    overall: FamilyMetrics
    thresholds: EvalThresholds
    failures: tuple[str, ...]
    unevaluated: tuple[str, ...] = ()

    @property
    def thresholds_met(self) -> bool:
        """True when every *evaluated* threshold passed."""

        return not self.failures

    @property
    def fully_evaluated(self) -> bool:
        """False when a declared threshold had no data to evaluate."""

        return not self.unevaluated


# One row per opportunity: (decided, correct, false_execute, unauthorized, abstain_correct).
_Row = tuple[bool, bool, bool, bool, bool]


def _metrics(family: str, items: Sequence[_Row]) -> FamilyMetrics:
    total = len(items)
    decided = sum(1 for row in items if row[0])
    correct = sum(1 for row in items if row[1])
    false_execute = sum(1 for row in items if row[0] and row[2])
    unauthorized = sum(1 for row in items if row[3])
    abstain_correct = sum(1 for row in items if row[4])
    return FamilyMetrics(
        family=family,
        total=total,
        decided=decided,
        correct=correct,
        false_execute=false_execute,
        unauthorized=unauthorized,
        abstain_correct=abstain_correct,
    )


def evaluate_traces(
    corpus: DecisionCorpus,
    traces: Mapping[str, ReplayTrace],
    *,
    admissions: Mapping[str, bool] | None = None,
    thresholds: EvalThresholds = DEFAULT_THRESHOLDS,
    cost_per_decision: Decimal | None = None,
    p95_latency_ms: int | None = None,
) -> EvalReport:
    """Per-family metrics from replay traces against pre-registered thresholds.

    ``cost_per_decision`` and ``p95_latency_ms`` are optional: when supplied
    they are evaluated against the thresholds; when absent they are reported as
    ``unevaluated`` and never counted as met.
    """

    admissions = admissions or {}
    rows: dict[DecisionFamily, list[_Row]] = {}
    for opportunity in corpus.opportunities:
        trace = traces.get(opportunity.opportunity_id)
        decided = trace is not None and trace.route_hint is not None
        expected_effect = opportunity.label.effect
        if expected_effect is None:
            # Abstain is a correct selective outcome, but it is not a target
            # decision: it must never inflate target_accuracy.
            is_correct = False
            abstain_correct = not decided
        else:
            abstain_correct = False
            is_correct = (
                decided
                and trace is not None
                and trace.effect == expected_effect
                and (
                    opportunity.label.target_ref is None
                    or trace.target_ref == opportunity.label.target_ref
                )
            )
        is_false_execute = (
            decided
            and trace is not None
            and trace.effect == "execute_request"
            and expected_effect != "execute_request"
        )
        is_unauthorized = bool(
            trace is not None
            and trace.applied
            and not admissions.get(opportunity.opportunity_id, False)
        )
        rows.setdefault(opportunity.family, []).append(
            (
                bool(decided),
                bool(is_correct),
                bool(is_false_execute),
                is_unauthorized,
                bool(abstain_correct),
            )
        )

    per_family = tuple(_metrics(family.value, rows[family]) for family in corpus.families())
    all_items = tuple(item for family_rows in rows.values() for item in family_rows)
    overall = _metrics("overall", all_items)
    unauthorized_total = sum(metric.unauthorized for metric in per_family)
    failures: list[str] = []
    unevaluated: list[str] = []
    if overall.target_accuracy < thresholds.min_target_accuracy:
        failures.append("target_accuracy")
    if overall.false_execute_rate > thresholds.max_false_execute_rate:
        failures.append("false_execute_rate")
    if unauthorized_total > thresholds.max_unauthorized_transitions:
        failures.append("unauthorized_transitions")
    if overall.coverage < thresholds.min_decided_coverage:
        failures.append("decided_coverage")
    if cost_per_decision is None:
        unevaluated.append("max_cost_per_decision")
    elif cost_per_decision > thresholds.max_cost_per_decision:
        failures.append("max_cost_per_decision")
    if p95_latency_ms is None:
        unevaluated.append("max_p95_latency_ms")
    elif p95_latency_ms > thresholds.max_p95_latency_ms:
        failures.append("max_p95_latency_ms")
    return EvalReport(
        per_family=per_family,
        overall=overall,
        thresholds=thresholds,
        failures=tuple(failures),
        unevaluated=tuple(unevaluated),
    )


@dataclass(frozen=True, slots=True)
class DecisionAccounting:
    total_cost_units: Decimal
    total_calls: int
    retry_cost_units: Decimal
    retry_calls: int
    by_purpose: tuple[tuple[str | None, Decimal, int], ...]


async def load_decision_accounting(
    session: AsyncSession, *, task_id: uuid.UUID | None = None
) -> DecisionAccounting:
    """Full ledger view: all calls, retry/fallback subtotal, cost by purpose."""

    by_purpose = tuple(await usage_totals_by_purpose(session, task_id=task_id))
    retry_cost, retry_calls = await usage_retry_subtotal(session, task_id=task_id)
    total_cost = sum((row[1] for row in by_purpose), Decimal("0"))
    total_calls = sum(row[2] for row in by_purpose)
    return DecisionAccounting(
        total_cost_units=total_cost,
        total_calls=total_calls,
        retry_cost_units=retry_cost,
        retry_calls=retry_calls,
        by_purpose=by_purpose,
    )
