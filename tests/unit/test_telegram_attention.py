"""Unit tests for the attention notification policy (WP09, no-spam)."""

from vuzol.telegram.attention import AttentionEvent, should_notify


def test_completion_is_visible() -> None:
    assert should_notify(AttentionEvent.TASK_COMPLETED) is True
    assert should_notify(AttentionEvent.PACKAGE_COMPLETED) is True


def test_action_needed_is_visible() -> None:
    assert should_notify(AttentionEvent.APPROVAL_NEEDED) is True
    assert should_notify(AttentionEvent.ATTENTION_FLAGGED) is True
    assert should_notify(AttentionEvent.TASK_FAILED) is True
    assert should_notify(AttentionEvent.PACKAGE_ATTENTION) is True


def test_transient_retry_does_not_notify() -> None:
    assert should_notify(AttentionEvent.RETRY_SCHEDULED) is False
    assert should_notify(AttentionEvent.BACKOFF_DEFERRED) is False
    assert should_notify(AttentionEvent.QUEUE_SHUFFLED) is False


def test_unchanged_state_does_not_notify() -> None:
    assert should_notify(AttentionEvent.STATE_UNCHANGED) is False


def test_duplicate_delivery_does_not_notify() -> None:
    assert should_notify(AttentionEvent.DUPLICATE_DELIVERY) is False


def test_policy_covers_every_event() -> None:
    assert {event for event in AttentionEvent} == {
        AttentionEvent.TASK_COMPLETED,
        AttentionEvent.TASK_FAILED,
        AttentionEvent.APPROVAL_NEEDED,
        AttentionEvent.ATTENTION_FLAGGED,
        AttentionEvent.PACKAGE_COMPLETED,
        AttentionEvent.PACKAGE_ATTENTION,
        AttentionEvent.RETRY_SCHEDULED,
        AttentionEvent.BACKOFF_DEFERRED,
        AttentionEvent.QUEUE_SHUFFLED,
        AttentionEvent.STATE_UNCHANGED,
        AttentionEvent.DUPLICATE_DELIVERY,
    }
