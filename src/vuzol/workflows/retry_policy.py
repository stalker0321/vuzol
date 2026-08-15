"""Shared fail-closed policy for user-requested step retries."""

from vuzol.storage.models import Step

EXPLICITLY_RETRYABLE_FAILURES = frozenset(
    {
        "authentication",
        "cancelled",
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
