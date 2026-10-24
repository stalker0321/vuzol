"""Unit tests for the attention notification policy (WP09, no-spam)."""

# ruff: noqa: RUF001

import pytest

from vuzol.telegram.attention import (
    ATTENTION_REASONS,
    AttentionEvent,
    attention_text,
    project_message_url,
    should_notify,
)


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


@pytest.mark.parametrize(
    ("reason", "icon"),
    [
        ("plan_approval", "🟡"),
        ("owner", "🟡"),
        ("waiting_approval", "🟡"),
        ("some_new_owner_reason", "🟡"),
        ("failed", "🔴"),
        ("blocked", "🔴"),
        ("unknown_effects_open", "🔴"),
        ("quota_exhausted", "🔴"),
        ("lead_budget_exceeded", "🔴"),
        ("lead_model_unavailable", "🔴"),
        ("planner_provider_unavailable", "🔴"),
        ("planner_timeout", "🔴"),
        ("delegation_expired", "🔴"),
        ("delegation_revoked", "🔴"),
        ("completed", "✅"),
    ],
)
def test_attention_reason_levels_and_resolved_text(reason, icon):
    text = attention_text(reason, "Сайт школы")
    assert text.startswith(f"{icon} Сайт школы\n")
    assert len(text.splitlines()) == 2
    assert reason not in text
    assert attention_text(reason, "Сайт школы", resolved=True) == (
        "✅ Сайт школы\nрешено: " + text.splitlines()[1]
    )


def test_attention_never_displays_internal_identifiers_or_model_text():
    identifier = "a1234567-b123-c123-d123-e12345678901"
    commit = "f" * 40
    for reason in (*ATTENTION_REASONS, identifier):
        text = attention_text(reason, f"Сайт <школы> {identifier} {commit}")
        assert identifier not in text and commit not in text
        assert "&lt;школы&gt;" in text
        assert reason not in text


def test_supergroup_message_and_thread_links():
    assert project_message_url(-1001234567890, 17, 321) == "https://t.me/c/1234567890/17/321"
    assert project_message_url(-1001234567890, 17) == "https://t.me/c/1234567890/17"
