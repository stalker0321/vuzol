from types import SimpleNamespace
from typing import Any, cast

from vuzol.storage.models import Step
from vuzol.workflows.retry_policy import blocked_step_is_retryable


def _step(**overrides: object) -> Step:
    values: dict[str, object] = {
        "unknown_effects": False,
        "attempt_count": 1,
        "max_attempts": 2,
        "failure_category": "validation_failed",
    }
    values.update(overrides)
    return cast(Step, cast(Any, SimpleNamespace(**values)))


def test_retry_policy_allows_remaining_bounded_attempt() -> None:
    assert blocked_step_is_retryable(_step())


def test_retry_policy_allows_explicit_retry_after_transient_exhaustion() -> None:
    assert blocked_step_is_retryable(
        _step(attempt_count=2, max_attempts=2, failure_category="quota_exhausted")
    )


def test_retry_policy_rejects_unknown_effects_and_non_retryable_exhaustion() -> None:
    assert not blocked_step_is_retryable(_step(unknown_effects=True))
    assert not blocked_step_is_retryable(
        _step(attempt_count=2, max_attempts=2, failure_category="policy_denied")
    )


def test_retry_policy_allows_explicit_validation_retry_after_exhaustion() -> None:
    assert blocked_step_is_retryable(_step(attempt_count=2, max_attempts=2))
