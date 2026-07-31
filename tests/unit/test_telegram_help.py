"""Concise, topic-aware Telegram ``/help`` UX."""

from vuzol.config import TopicKind
from vuzol.telegram.layout import build_help_card, is_help_command


def test_help_command_is_bare_and_allows_bot_suffix() -> None:
    assert is_help_command("/help")
    assert is_help_command("  /help@vuzol_bot ")
    assert not is_help_command("/help now")
    assert not is_help_command("/helper")
    assert not is_help_command(None)


def test_project_help_is_concise_and_context_aware() -> None:
    html = build_help_card(TopicKind.PROJECT)

    assert "/model" in html
    assert "/update" not in html
    assert "голосом" in html
    assert len(html) < 500


def test_control_topics_only_show_relevant_action() -> None:
    assert "/update" in build_help_card(TopicKind.TASK_DASHBOARD)
    assert "предложит названия" in build_help_card(TopicKind.INBOX)
    assert "выберите действие кнопкой" in build_help_card(TopicKind.APPROVALS)
