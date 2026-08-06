"""Deterministic and intentionally conservative Step 09A routing policy."""

from vuzol.experiments.domain import (
    BoundedLevel,
    ExecutionStrategy,
    RiskLevel,
    TaskClass,
    TaskClassification,
)

GATED_CLASSES = frozenset(
    {
        TaskClass.SECURITY,
        TaskClass.RUNTIME_LIFECYCLE,
        TaskClass.DEPLOYMENT,
        TaskClass.INFRASTRUCTURE,
        TaskClass.UNKNOWN,
    }
)


def classify_execution_strategy(classification: TaskClassification) -> ExecutionStrategy:
    """Recommend provider-neutral orchestration; this grants no capability."""
    if classification.task_class in GATED_CLASSES:
        return ExecutionStrategy.GATED
    if (
        classification.risk in {RiskLevel.HIGH, RiskLevel.PRIVILEGED}
        or classification.credentials
        or classification.networking
        or classification.persistence
        or classification.concurrency
        or classification.deployment
        or classification.security_boundary
        or classification.coupling is BoundedLevel.HIGH
        or classification.novelty is BoundedLevel.HIGH
        or classification.blast_radius is BoundedLevel.HIGH
        or classification.testability is BoundedLevel.LOW
        or classification.expected_file_count > 8
    ):
        return ExecutionStrategy.GATED
    if (
        classification.complexity is BoundedLevel.LOW
        and classification.risk is RiskLevel.LOW
        and classification.testability is BoundedLevel.HIGH
        and classification.blast_radius is BoundedLevel.LOW
        and classification.coupling is BoundedLevel.LOW
        and classification.novelty is BoundedLevel.LOW
        and classification.expected_file_count <= 2
    ):
        return ExecutionStrategy.SOLO
    return ExecutionStrategy.REVIEWED


def enforce_security_escalation(
    classification: TaskClassification, requested: ExecutionStrategy
) -> ExecutionStrategy:
    """Prevent an operator hint from lowering classified review intensity."""
    recommended = classify_execution_strategy(classification)
    rank = {
        ExecutionStrategy.SOLO: 0,
        ExecutionStrategy.REVIEWED: 1,
        ExecutionStrategy.GATED: 2,
        ExecutionStrategy.MULTI_AGENT: 2,
    }
    return recommended if rank[requested] < rank[recommended] else requested


def scopes_conflict(first: tuple[str, ...], second: tuple[str, ...]) -> bool:
    def overlaps(left: str, right: str) -> bool:
        left_parts = left.rstrip("/").split("/")
        right_parts = right.rstrip("/").split("/")
        size = min(len(left_parts), len(right_parts))
        return left_parts[:size] == right_parts[:size]

    return any(overlaps(left, right) for left in first for right in second)
