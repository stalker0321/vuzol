"""Concise, topic-aware Telegram ``/help`` UX."""

from decimal import Decimal

import pytest

from vuzol.config import TopicKind
from vuzol.telegram.layout import (
    build_help_card,
    is_help_command,
    is_receipt_command,
    parse_receipt_amount,
)


def test_help_command_is_bare_and_allows_bot_suffix() -> None:
    assert is_help_command("/help")
    assert is_help_command("  /help@vuzol_bot ")
    assert not is_help_command("/help now")
    assert not is_help_command("/helper")
    assert not is_help_command(None)


def test_project_help_is_concise_and_context_aware() -> None:
    html = build_help_card(TopicKind.PROJECT)

    assert "/model" in html
    assert "/think" in html and "<code>/lead</code>" in html
    assert "не команда запуска" in html
    assert all(
        command in html
        for command in ("/lead_status", "/lead_goal", "/lead_details", "/lead_debug")
    )
    assert "/update" not in html
    assert "голосом" in html
    assert len(html) < 500


def test_control_topics_only_show_relevant_action() -> None:
    assert "/update" in build_help_card(TopicKind.TASK_DASHBOARD)
    assert "предложит названия" in build_help_card(TopicKind.INBOX)
    assert "выберите действие кнопкой" in build_help_card(TopicKind.APPROVALS)


def test_receipt_command_requires_a_positive_amount() -> None:
    assert is_receipt_command("/receipt 0.25")
    assert is_receipt_command("/receipt@bot 1")
    assert not is_receipt_command("please /receipt 1")
    assert parse_receipt_amount("/receipt 0.25") == Decimal("0.25")
    with pytest.raises(ValueError, match="usage"):
        parse_receipt_amount("/receipt")
    with pytest.raises(ValueError, match="not zero"):
        parse_receipt_amount("/receipt 0")
    with pytest.raises(ValueError, match="not zero"):
        parse_receipt_amount("/receipt -1")
