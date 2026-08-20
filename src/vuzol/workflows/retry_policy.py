"""Shared fail-closed policy for user-requested step retries."""

from vuzol.storage.models import Step
from vuzol.storage.types import StepStatus

EXPLICITLY_RETRYABLE_FAILURES = frozenset(
    {
        "authentication",
        "cancelled",
        "dependency_provisioning_failed",
        "independent_review_required",
        "quota_exhausted",
        "rate_limited",
        "timeout",
        "provider_unavailable",
    }
)


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
