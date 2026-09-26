"""Pure, fail-closed recovery decision table and failure fingerprints (WP04).

The table is side-effect free so it can be unit-tested exhaustively. Callers
(``workflows.service``) assemble a :class:`RecoveryState` from persisted facts
and execute the returned action. Unknown effects and exhausted bounds always
fail closed to ``ATTENTION``; nothing here grants attempts or permissions.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from vuzol.ops.disk_pressure import DISK_PRESSURE_CATEGORY

FINGERPRINT_SCHEMA = "failure-fingerprint.v1"
FINGERPRINT_HISTORY_LIMIT = 8

# Transient host/provider backpressure must not burn an LLM attempt. It is
# retried after a delay, bounded by the backpressure wait cap. disk_pressure is
# the preserved precedent.
BACKPRESSURE_CATEGORIES = frozenset(
    {
        DISK_PRESSURE_CATEGORY,
        "rate_limited",
        "quota_exhausted",
        "provider_unavailable",
    }
)

REPAIRABLE_CATEGORY_PREFIXES = ("validation_", "review_")
REPAIRABLE_STEP_TYPES = frozenset({"validate", "review"})

# Actions bounded by the table. TAKEOVER and WAIT are modelled explicitly even
# though only WAIT is produced today (TAKEOVER has no producer yet; ADR-A01
# keeps the label available for a future bounded hand-off).
class RecoveryAction(StrEnum):
    RETRY = "retry"
    REPAIR = "repair"
    TAKEOVER = "takeover"
    WAIT = "wait"
    ATTENTION = "attention"


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    step_repair_cap: int = 3
    task_repair_cap: int = 6
    backpressure_wait_cap: int = 5
    recovery_deadline_seconds: int = 3_600

    def __post_init__(self) -> None:
        for name, value in (
            ("step_repair_cap", self.step_repair_cap),
            ("task_repair_cap", self.task_repair_cap),
            ("backpressure_wait_cap", self.backpressure_wait_cap),
            ("recovery_deadline_seconds", self.recovery_deadline_seconds),
        ):
            if value < 1:
                raise ValueError(f"{name} must be positive")


DEFAULT_RECOVERY_POLICY = RecoveryPolicy()


def recovery_policy_from_settings(settings: object) -> RecoveryPolicy:
    return RecoveryPolicy(
        step_repair_cap=int(getattr(settings, "max_step_repairs", 3)),
        task_repair_cap=int(getattr(settings, "max_task_repairs", 6)),
        backpressure_wait_cap=int(getattr(settings, "max_backpressure_waits", 5)),
        recovery_deadline_seconds=int(getattr(settings, "recovery_deadline_seconds", 3_600)),
    )


def is_backpressure(category: str | None) -> bool:
    return category in BACKPRESSURE_CATEGORIES


def normalize_failure_category(category: str | None) -> str:
    """Normalize a category for fingerprinting without erasing its identity."""

    if not category:
        return "unknown"
    return " ".join(category.strip().casefold().split())


def _stable_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def fingerprint_components(
    *,
    step_type: str,
    category: str | None,
    evidence_hash: str | None,
    environment_hash: str | None,
    result_hash: str | None,
    strategy_hash: str | None,
) -> dict[str, str]:
    return {
        "schema": FINGERPRINT_SCHEMA,
        "step_type": step_type,
        "category": normalize_failure_category(category),
        "evidence_hash": evidence_hash or "",
        "environment_hash": environment_hash or "",
        "result_hash": result_hash or "",
        "strategy_hash": strategy_hash or "",
    }


def failure_fingerprint(components: Mapping[str, Any]) -> str:
    return _stable_hash(components)


@dataclass(frozen=True, slots=True)
class RecoveryState:
    outcome_kind: str
    category: str | None
    step_type: str
    unknown_effects: bool
    retryable: bool
    fingerprint: str | None = None
    seen_fingerprints: frozenset[str] = field(default_factory=frozenset)
    repair_count: int = 0
    task_repair_count: int = 0
    backpressure_count: int = 0
    deadline_exceeded: bool = False

    @property
    def repairable(self) -> bool:
        return self.step_type in REPAIRABLE_STEP_TYPES and (
            (self.category or "").startswith(REPAIRABLE_CATEGORY_PREFIXES)
        )


def decide_recovery(
    state: RecoveryState, policy: RecoveryPolicy = DEFAULT_RECOVERY_POLICY
) -> RecoveryAction:
    """Return the bounded recovery action for one failed step outcome."""

    if state.unknown_effects:
        return RecoveryAction.ATTENTION
    if state.deadline_exceeded:
        return RecoveryAction.ATTENTION
    if is_backpressure(state.category):
        if state.backpressure_count >= policy.backpressure_wait_cap:
            return RecoveryAction.ATTENTION
        return RecoveryAction.WAIT
    if state.repairable:
        if state.fingerprint is not None and state.fingerprint in state.seen_fingerprints:
            # Same normalized failure without new evidence: never schedule an
            # identical repair (or an A->B->A oscillation).
            return RecoveryAction.ATTENTION
        if state.repair_count >= policy.step_repair_cap:
            return RecoveryAction.ATTENTION
        if state.task_repair_count >= policy.task_repair_cap:
            return RecoveryAction.ATTENTION
        return RecoveryAction.REPAIR
    if state.outcome_kind == "transient_failure" and state.retryable:
        return RecoveryAction.RETRY
    return RecoveryAction.ATTENTION


def recovery_attempt_summary(state: RecoveryState, action: RecoveryAction) -> dict[str, Any]:
    """Operator-visible, bounded summary of the recovery decision."""

    return {
        "decision": action.value,
        "fingerprint": state.fingerprint,
        "fingerprint_schema": FINGERPRINT_SCHEMA,
        "seen_fingerprints": len(state.seen_fingerprints),
        "repair_count": state.repair_count,
        "task_repair_count": state.task_repair_count,
        "backpressure_count": state.backpressure_count,
        "category": normalize_failure_category(state.category),
        "step_type": state.step_type,
    }


def append_fingerprint_history(
    existing: object, fingerprint: str
) -> list[str]:
    """Append a fingerprint to a bounded history list, newest last."""

    history = [str(item) for item in existing] if isinstance(existing, list) else []
    history.append(fingerprint)
    return history[-FINGERPRINT_HISTORY_LIMIT:]
