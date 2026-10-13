"""Shared fail-closed policy for user-requested step retries."""

from vuzol.storage.models import Step
from vuzol.storage.types import StepStatus

EXPLICITLY_RETRYABLE_FAILURES = frozenset(
    {
        "authentication",
        "budget_exhausted",
        "cancelled",
        "dependency_provisioning_failed",
        "independent_review_required",
        "quota_exhausted",
        "rate_limited",
        "timeout",
        "provider_unavailable",
    }
)


def current_retry_target(steps: tuple[Step, ...]) -> Step | None:
    """One blocked leaf, allowing only a mutually bound existing repair pair."""
    blocked = tuple(step for step in steps if step.status is StepStatus.BLOCKED)
    suspended: set[object] = set()
    for parent in blocked:
        if parent.unknown_effects or parent.step_type not in {"validate", "review"}:
            continue
        parent_payload = parent.payload if isinstance(parent.payload, dict) else {}
        for child in blocked:
            payload = child.payload if isinstance(child.payload, dict) else {}
            metadata = (
                child.dependency_metadata if isinstance(child.dependency_metadata, dict) else {}
            )
            if (
                child.step_type == "execute_code"
                and child.run_id == parent.run_id
                and payload.get("repair_for_step_id") == str(parent.id)
                and parent_payload.get("repair_scheduled_step_id") == str(child.id)
                and metadata.get("template_key") == "repair_code"
                and metadata.get("repair_for_ordinal") == parent.ordinal
            ):
                suspended.add(parent.id)
    leaves = [step for step in blocked if step.id not in suspended]
    return leaves[0] if len(leaves) == 1 else None


def blocked_step_is_retryable(step: Step) -> bool:
    if step.unknown_effects:
        return False
    category = step.failure_category or ""
    return (
        step.attempt_count < step.max_attempts
        or category in EXPLICITLY_RETRYABLE_FAILURES
        # Validation is deterministic and has no external side effects. An explicit
        # retry is safe after code, configuration, or a guardrail changes.
        or category.startswith("validation_")
    )


def failed_item_is_rematerializable(step: Step) -> bool:
    """A manual package retry creates a fresh Task; terminal evidence is immutable."""

    return (
        step.status is StepStatus.FAILED
        and not step.unknown_effects
        and step.step_type
        in {
            "prepare_context",
            "prepare_worktree",
            "validate",
            "review",
            "build_static",
            "publish_preview",
        }
    )
