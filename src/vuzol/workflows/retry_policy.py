"""Shared fail-closed policy for user-requested step retries."""

from vuzol.storage.models import Step

EXPLICITLY_RETRYABLE_FAILURES = frozenset(
    {"cancelled", "quota_exhausted", "rate_limited", "timeout", "provider_unavailable"}
)


def blocked_step_is_retryable(step: Step) -> bool:
    if step.unknown_effects:
        return False
    return step.attempt_count < step.max_attempts or (
        step.failure_category in EXPLICITLY_RETRYABLE_FAILURES
    )
