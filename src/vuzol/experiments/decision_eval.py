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

    @property
    def coverage(self) -> float:
        return self.decided / self.total if self.total else 0.0

    @property
    def target_accuracy(self) -> float:
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

    @property
    def thresholds_met(self) -> bool:
        return not self.failures


def _metrics(
    family: str,
    items: Sequence[tuple[bool, bool, bool, bool]],
) -> FamilyMetrics:
    total = len(items)
    decided = sum(1 for is_decided, _, _, _ in items if is_decided)
    correct = sum(1 for _, is_correct, _, _ in items if is_correct)
    false_execute = sum(1 for is_decided, _, is_false, _ in items if is_decided and is_false)
    unauthorized = sum(1 for _, _, _, is_unauthorized in items if is_unauthorized)
    return FamilyMetrics(
        family=family,
        total=total,
        decided=decided,
        correct=correct,
        false_execute=false_execute,
        unauthorized=unauthorized,
    )


def evaluate_traces(
    corpus: DecisionCorpus,
    traces: Mapping[str, ReplayTrace],
    *,
    admissions: Mapping[str, bool] | None = None,
    thresholds: EvalThresholds = DEFAULT_THRESHOLDS,
) -> EvalReport:
    """Per-family metrics from replay traces against pre-registered thresholds."""

    admissions = admissions or {}
    rows: dict[DecisionFamily, list[tuple[bool, bool, bool, bool]]] = {}
    for opportunity in corpus.opportunities:
        trace = traces.get(opportunity.opportunity_id)
        decided = trace is not None and trace.route_hint is not None
        expected_effect = opportunity.label.effect
        if expected_effect is None:
            is_correct = not decided
        else:
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
            (bool(decided), bool(is_correct), bool(is_false_execute), is_unauthorized)
        )

    per_family = tuple(_metrics(family.value, rows[family]) for family in corpus.families())
    all_items = tuple(item for family_rows in rows.values() for item in family_rows)
    overall = _metrics("overall", all_items)
    unauthorized_total = sum(metric.unauthorized for metric in per_family)
    failures: list[str] = []
    if overall.target_accuracy < thresholds.min_target_accuracy:
        failures.append("target_accuracy")
    if overall.false_execute_rate > thresholds.max_false_execute_rate:
        failures.append("false_execute_rate")
    if unauthorized_total > thresholds.max_unauthorized_transitions:
        failures.append("unauthorized_transitions")
    if overall.coverage < thresholds.min_decided_coverage:
        failures.append("decided_coverage")
    return EvalReport(
        per_family=per_family,
        overall=overall,
        thresholds=thresholds,
        failures=tuple(failures),
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
