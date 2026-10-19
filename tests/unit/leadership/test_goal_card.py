"""Goal card and provider response validation. No database and no provider call."""

import inspect

from vuzol.cli import worker as worker_cli
from vuzol.leadership.card import render_goal_card, render_question
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.provider_model import parse_lead_response
from vuzol.telegram.layout import parse_lead_command


def test_goal_card_names_status_blocker_package_next_and_result() -> None:
    text = render_goal_card(
        {
            "lifecycle": "waiting",
            "waiting_reason": "owner",
            "package_line": "running abcdef12",
            "next_operation": "ответ владельца",
            "result_summary": "доказательств нет",
            "question": "Нужен ли отдельный вход?",
        }
    )
    assert "Руководитель" in text
    assert "Статус: waiting" in text
    assert "Блокер: owner" in text
    assert "Пакет: running abcdef12" in text
    assert "Дальше: ответ владельца" in text
    assert "Результат: доказательств нет" in text
    assert "Вопрос: Нужен ли отдельный вход?" in text


def test_question_notice_binds_the_version() -> None:
    text = render_question("Нужен ли отдельный вход?", 4)
    assert "Вопрос: Нужен ли отдельный вход?" in text
    assert "Версия: 4" in text


def test_lead_answer_is_a_command_and_enable_still_needs_text() -> None:
    assert parse_lead_command("/lead_answer да, в рамках маркера") == (
        "answer",
        "да, в рамках маркера",
    )
    assert parse_lead_command("/lead_answer") is None
    assert parse_lead_command("/lead_status") == ("status", "")
    assert parse_lead_command("/lead") is None


def test_provider_response_rejects_an_owner_waiver() -> None:
    try:
        parse_lead_response({"commands": [{"name": "accept_criterion", "payload": {}}]})
    except LeadModelUnavailable as error:
        assert str(error) == "lead_response_invalid"
    else:
        raise AssertionError("owner waiver was accepted as a lead command")
    decision = parse_lead_response({"commands": []})
    assert decision.commands == ()


def test_worker_entrypoint_uses_the_profile_factory() -> None:
    source = inspect.getsource(worker_cli.run)
    assert "build_project_lead_worker" in source
    assert "UnconfiguredLeadModel" not in source
