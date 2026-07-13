"""Deterministic and intentionally conservative Step 09A routing policy."""

from vuzol.experiments.domain import (
    BoundedLevel,
    ExecutionMode,
    RiskLevel,
    TaskClass,
    TaskClassification,
)

SOL_ONLY_CLASSES = frozenset(
    {
        TaskClass.SECURITY,
        TaskClass.RUNTIME_LIFECYCLE,
        TaskClass.DEPLOYMENT,
        TaskClass.INFRASTRUCTURE,
        TaskClass.UNKNOWN,
    }
)


def classify_execution_mode(classification: TaskClassification) -> ExecutionMode:
    """Recommend a mode; this function grants no capability or permission."""
    if classification.task_class in SOL_ONLY_CLASSES:
        return ExecutionMode.SOL_SOLO
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
        return ExecutionMode.SOL_SOLO
    return ExecutionMode.GROK_REVIEWED


def enforce_security_escalation(
    classification: TaskClassification, requested: ExecutionMode
) -> ExecutionMode:
    """Prevent model output or an operator hint from lowering classified risk."""
    recommended = classify_execution_mode(classification)
    if recommended is ExecutionMode.SOL_SOLO:
        return recommended
    return requested


def scopes_conflict(first: tuple[str, ...], second: tuple[str, ...]) -> bool:
    def overlaps(left: str, right: str) -> bool:
        left_parts = left.rstrip("/").split("/")
        right_parts = right.rstrip("/").split("/")
        size = min(len(left_parts), len(right_parts))
        return left_parts[:size] == right_parts[:size]

    return any(overlaps(left, right) for left in first for right in second)
