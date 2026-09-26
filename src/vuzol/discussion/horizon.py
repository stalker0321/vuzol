"""Horizon v1 contract helpers over WorkPackage (WP08, ADR-A01.5).

Pure, side-effect free: the runtime owns transitions, this module only maps
status, validates the optional horizon contract, evaluates acceptance and
composes the lifetime budget. Horizon behaviour is opt-in behind a flag.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from vuzol.storage.types import WorkPackageStatus

HORIZON_PHASES = frozenset(
    {"evaluating", "waiting_resource", "waiting_approval", "waiting_input"}
)

# Frozen mapping WorkPackageStatus(+phase,+accepted) <-> ADR-A01.5 horizon.status.
# Changing this is a contract change (documented in docs/HORIZON_RUNTIME.md).
HORIZON_STATUS_MAPPING = {
    "draft": "draft",
    "ready": "ready",
    "running": "running",
    "evaluating": "evaluating",
    "waiting_resource": "waiting_resource",
    "waiting_approval": "waiting_approval",
    "waiting_input": "waiting_input",
    "paused": "paused",
    "succeeded": "succeeded",
    "failed": "failed",
    "cancelled": "cancelled",
}


class HorizonBudgetState(StrEnum):
    WITHIN = "within"
    EXHAUSTED = "exhausted"


@dataclass(frozen=True, slots=True)
class HorizonBudget:
    max_cost: float | None
    max_attempts: int | None
    currency: str | None = None
    pricing_revision: str | None = None


def is_horizon(goal: str | None, exit_criteria: object) -> bool:
    """A package is a horizon when it declares a goal (or exit criteria)."""

    if goal is not None and goal.strip():
        return True
    return isinstance(exit_criteria, list) and len(exit_criteria) > 0


def horizon_status(status: WorkPackageStatus, phase: str | None, accepted: bool) -> str:
    """Map persisted package state onto the frozen ADR-A01.5 status vocabulary."""

    if status is WorkPackageStatus.RUNNING and phase in HORIZON_PHASES:
        return HORIZON_STATUS_MAPPING[phase]
    if status is WorkPackageStatus.COMPLETED:
        mapped = "succeeded" if accepted else "running"
        return HORIZON_STATUS_MAPPING[mapped]
    if status is WorkPackageStatus.DRAFT:
        return HORIZON_STATUS_MAPPING["draft"]
    if status is WorkPackageStatus.APPROVED:
        return HORIZON_STATUS_MAPPING["ready"]
    if status is WorkPackageStatus.RUNNING:
        return HORIZON_STATUS_MAPPING["running"]
    if status is WorkPackageStatus.PAUSED:
        return HORIZON_STATUS_MAPPING["paused"]
    if status is WorkPackageStatus.STOPPED:
        return HORIZON_STATUS_MAPPING["failed"]
    return HORIZON_STATUS_MAPPING["cancelled"]


def unmet_exit_criteria(
    exit_criteria: object, satisfied: frozenset[str]
) -> tuple[str, ...]:
    """Return the criteria ids that are not backed by retained evidence.

    An empty/missing criteria list is *not* success: it is reported as unmet
    (``("__no_exit_criteria__",)``) so an empty queue cannot mark a horizon done.
    """

    if not isinstance(exit_criteria, list) or not exit_criteria:
        return ("__no_exit_criteria__",)
    unmet: list[str] = []
    for entry in exit_criteria:
        if not isinstance(entry, dict):
            continue
        criterion_id = entry.get("criterion_id")
        if isinstance(criterion_id, str) and criterion_id not in satisfied:
            unmet.append(criterion_id)
    return tuple(unmet)


def parse_budget(raw: object) -> HorizonBudget | None:
    if not isinstance(raw, dict):
        return None
    max_cost = raw.get("max_cost")
    max_attempts = raw.get("max_attempts")
    return HorizonBudget(
        max_cost=float(max_cost) if isinstance(max_cost, int | float) else None,
        max_attempts=int(max_attempts) if isinstance(max_attempts, int) else None,
        currency=str(raw["currency"]) if isinstance(raw.get("currency"), str) else None,
        pricing_revision=(
            str(raw["pricing_revision"]) if isinstance(raw.get("pricing_revision"), str) else None
        ),
    )


def budget_state(
    budget: HorizonBudget | None, *, spent_cost: float, spent_attempts: int
) -> HorizonBudgetState:
    """Lifetime budget composes over Task.budget_epoch: retry epochs never reset it."""

    if budget is None:
        return HorizonBudgetState.WITHIN
    if budget.max_cost is not None and spent_cost >= budget.max_cost:
        return HorizonBudgetState.EXHAUSTED
    if budget.max_attempts is not None and spent_attempts >= budget.max_attempts:
        return HorizonBudgetState.EXHAUSTED
    return HorizonBudgetState.WITHIN


def deadline_exceeded(*, deadline: object, now: object) -> bool:
    from datetime import datetime

    if not isinstance(deadline, datetime) or not isinstance(now, datetime):
        return False
    return now >= deadline


def needs_approval_gate(needs_approval: bool) -> bool:
    """Item-level approval requirement is a runtime gate, not just stored data."""

    return bool(needs_approval)
