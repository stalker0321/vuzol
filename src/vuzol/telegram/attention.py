"""Meaningful attention notification policy (WP09, lead decision 3).

Pure decision table over domain transition kinds. Only meaningful
transitions notify (completion, action-needed, attention); transient
mechanics (retry, backoff, shuffle, unchanged state, duplicate delivery)
never notify. Each rule has a no-notification test.
"""

from __future__ import annotations

from enum import StrEnum


class AttentionEvent(StrEnum):
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    APPROVAL_NEEDED = "approval_needed"
    ATTENTION_FLAGGED = "attention_flagged"
    PACKAGE_COMPLETED = "package_completed"
    PACKAGE_ATTENTION = "package_attention"
    RETRY_SCHEDULED = "retry_scheduled"
    BACKOFF_DEFERRED = "backoff_deferred"
    QUEUE_SHUFFLED = "queue_shuffled"
    STATE_UNCHANGED = "state_unchanged"
    DUPLICATE_DELIVERY = "duplicate_delivery"


_MEANINGFUL = frozenset(
    {
        AttentionEvent.TASK_COMPLETED,
        AttentionEvent.TASK_FAILED,
        AttentionEvent.APPROVAL_NEEDED,
        AttentionEvent.ATTENTION_FLAGGED,
        AttentionEvent.PACKAGE_COMPLETED,
        AttentionEvent.PACKAGE_ATTENTION,
    }
)


def should_notify(event: AttentionEvent) -> bool:
    """True only for meaningful transitions; transient mechanics stay silent."""

    return event in _MEANINGFUL
