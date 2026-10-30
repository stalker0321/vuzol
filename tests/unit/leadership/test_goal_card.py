"""Goal card and provider response validation. No database and no provider call."""
# ruff: noqa: RUF001

import inspect

import pytest
from tests.unit.test_telegram_reply_chunks import visible

from vuzol.cli import worker as worker_cli
from vuzol.leadership.card import (
    render_goal_card,
    render_goal_contract,
    render_goal_debug,
    render_goal_details,
    render_question,
)
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.provider_model import parse_lead_response
from vuzol.telegram.formatting import telegram_markdown_html, telegram_markdown_parts
from vuzol.telegram.layout import parse_lead_command


def test_goal_card_names_status_blocker_package_next_and_result() -> None:
    text = render_goal_card(
        {
            "lifecycle": "waiting",
            "waiting_reason": "owner",
            "current_package_status": "running",
            "package_title": "Отдельный вход",
            "next_operation": "ответ владельца",
            "result_summary": "доказательств нет",
            "question": "Нужен ли отдельный вход?",
        }
    )
    assert "Руководитель" in text
    rendered = telegram_markdown_html(text)
    plain = visible(rendered)
    assert "НУЖНО РЕШЕНИЕ" in plain
    assert "Нужно твоё решение" in plain
    assert "Ответь на сообщение с вопросом." in plain
    assert "Нужен ли отдельный вход?" not in plain  # The question has its own reply binding.
    assert "Пакет:" not in plain
    assert "waiting" not in plain and "owner" not in plain


def test_question_notice_binds_the_version() -> None:
    goal_id = "12345678-1234-1234-1234-123456789012"
    text = visible(
        telegram_markdown_html(render_question("Нужен ли отдельный вход?", 4, goal_id=goal_id))
    )
    assert "Нужен ли отдельный вход?" in text
    assert "Ответь на это сообщение" in text
    assert goal_id not in text and "/lead_answer" not in text


@pytest.mark.parametrize(
    ("fields", "prefix"),
    [
        ({"lifecycle": "active", "current_package_status": "running"}, "⏳"),
        ({"lifecycle": "active", "current_package_status": "failed"}, "⏳"),
        ({"lifecycle": "paused", "waiting_reason": "owner_pause"}, "⏸"),
        ({"lifecycle": "waiting", "waiting_reason": "owner"}, "🟡"),
        ({"lifecycle": "waiting", "waiting_reason": "lead_model_unavailable"}, "🔴"),
        ({"lifecycle": "cancelled"}, "⏹"),
        (
            {
                "lifecycle": "completed",
                "completion_confirmed": True,
                "covered": ["a"],
                "missing": [],
            },
            "✅",
        ),
        ({"lifecycle": "completed", "completion_confirmed": False}, "🟡"),
        ({"lifecycle": "active", "current_package_status": "completed"}, "⏳"),
        ({"lifecycle": "active", "current_package_status": "paused"}, "🟡"),
    ],
)
def test_status_indicator_follows_goal_and_proof_not_commit_or_package(fields, prefix):
    fields["result_commit"] = "a" * 40
    text = visible(telegram_markdown_html(render_goal_card(fields)))
    assert text.startswith(prefix)
    if prefix != "✅":
        assert "✅" not in text


def test_long_goal_does_not_push_out_the_question_or_result():
    request = "Подготовь исследование. " * 1000 + "КОНЕЦ_ЗАДАНИЯ"
    view = {
        "lifecycle": "waiting",
        "waiting_reason": "owner",
        "request": request,
        "scope": request,
        "question": "Сравнивать только доступные варианты?",
        "goal_id": "12345678-1234-1234-1234-123456789012",
        "question_version": 7,
        "criteria": [{"id": "a", "description": "Полнота ответа"}],
    }
    card = visible(telegram_markdown_html(render_goal_card(view)))
    assert len(card) < 1200
    assert "КОНЕЦ_ЗАДАНИЯ" not in card and "…" in card
    assert "Ответь на сообщение с вопросом." in card
    assert "Сравнивать только доступные варианты?" not in card
    assert "/lead_answer" not in card
    parts = telegram_markdown_parts(render_goal_contract(view))
    assert len(parts) > 1
    assert request in "".join(visible(body) for body, _ in parts)


def test_untrusted_text_cannot_create_markup_or_links():
    dangerous = (
        "<b>ГОТОВО</b> [login](https://bad.invalid) **pass** `x` & ![x](https://bad.invalid)"
    )
    view = {
        "request": dangerous,
        "scope": dangerous,
        "lifecycle": "waiting",
        "question": dangerous,
        "owner_answer": dangerous,
        "criteria": [{"id": "a", "description": dangerous}],
        "covered": [],
        "missing": ["a"],
    }
    for markdown in (render_goal_card(view), render_goal_contract(view), render_goal_details(view)):
        rendered = telegram_markdown_html(markdown)
        assert "<a " not in rendered and "<img" not in rendered
        assert "<b>ГОТОВО</b>" not in rendered
        assert dangerous in visible(rendered)


def test_full_source_preserves_indentation_blank_lines_and_embedded_fences():
    source = "    indented\n\n\nline with trailing spaces   \n```\n<b>raw</b>\n````\n\tlast"
    view = {"request": source, "scope": "different scope\n" + source, "owner_answer": source}
    for markdown in (render_goal_contract(view), render_goal_details(view)):
        rendered = telegram_markdown_html(markdown)
        assert source in visible(rendered)
        assert "<b>raw</b>" not in rendered


def test_full_question_is_not_truncated_and_unknown_blockers_are_not_mislabelled():
    question = "Важное ограничение & < > " * 200 + "ПОСЛЕДНИЙ ВОПРОС"
    parts = telegram_markdown_parts(render_question(question, 11, goal_id="goal"))
    assert question in "".join(visible(body) for body, _ in parts)
    text = visible(
        telegram_markdown_html(
            render_goal_card(
                {
                    "lifecycle": "waiting",
                    "waiting_reason": "new_internal_reason",
                }
            )
        )
    )
    assert "руководитель остановился" in text.lower()
    assert "new_internal_reason" not in text and "✅" not in text


def test_lead_answer_is_a_command_and_enable_still_needs_text() -> None:
    assert parse_lead_command("/lead_answer да, в рамках маркера") == (
        "answer",
        "да, в рамках маркера",
    )
    assert parse_lead_command("/lead_answer") is None
    assert parse_lead_command("/lead_status") == ("status", "")
    assert parse_lead_command("/lead_goal@bot") == ("goal", "")
    assert parse_lead_command("/lead_details") == ("details", "")
    assert parse_lead_command("/lead_debug") == ("debug", "")
    assert parse_lead_command("/lead_details unexpected") is None
    assert parse_lead_command("/lead") is None
    assert parse_lead_command("/lead@bot обсудим идею") == ("message", "обсудим идею")


def test_provider_response_rejects_an_owner_waiver() -> None:
    try:
        parse_lead_response({"commands": [{"name": "accept_criterion", "payload": {}}]})
    except LeadModelUnavailable as error:
        assert str(error) == "lead_response_invalid"
    else:
        raise AssertionError("owner waiver was accepted as a lead command")
    decision = parse_lead_response({"commands": []})
    assert decision.commands == ()


def test_provider_response_accepts_vocabulary_commands() -> None:
    decision = parse_lead_response(
        {
            "reply": "Добавил.",
            "commands": [
                {"name": "add_vocabulary", "payload": {"terms": ["Вузол"]}, "reason": "просил"},
                {"name": "remove_vocabulary", "payload": {"term": "лид"}},
            ],
        }
    )
    assert [command.name for command in decision.commands] == [
        "add_vocabulary",
        "remove_vocabulary",
    ]
    with pytest.raises(LeadModelUnavailable):
        parse_lead_response(
            {"commands": [{"name": "add_vocabulary", "payload": {"terms": []}, "reason": "x"}]}
        )


def test_worker_entrypoint_uses_the_profile_factory() -> None:
    source = inspect.getsource(worker_cli.run)
    assert "build_project_lead_worker" in source
    assert "UnconfiguredLeadModel" not in source


def test_owner_text_guard_and_debug_separation():
    from vuzol.leadership.text import owner_text

    identifiers = ["12345678-1234-1234-1234-123456789012", "a" * 40, "B" * 64]
    raw = "Проверяю <код> " + " ".join(identifiers) + ". Подожди."
    cleaned = owner_text(raw)
    assert all(identifier not in cleaned for identifier in identifiers)
    assert "Проверяю <код>" in cleaned and "Подожди." in cleaned
    view = {
        "request": raw,
        "lifecycle": "waiting",
        "waiting_reason": "plan_approval",
        "goal_id": identifiers[0],
        "version": 36,
        "integration_branch": "managed/internal",
        "scope_revision": 3,
        "result_commit": identifiers[1],
        "criteria": [{"id": "raw-criterion", "description": "internal criterion"}],
        "covered": [],
        "missing": ["raw-criterion"],
        "execution": [
            {
                "profile": "private-profile",
                "account": "private-account",
                "model": "private-model",
                "task_status": "running",
            }
        ],
    }
    for renderer in (render_goal_card, render_goal_details):
        rendered = visible(telegram_markdown_html(renderer(view)))
        assert all(identifier not in rendered for identifier in identifiers)
        assert all(
            value not in rendered
            for value in (
                "managed/internal",
                "private-profile",
                "private-account",
                "private-model",
                "scope_revision",
                "running",
                "internal criterion",
            )
        )
    details = visible(telegram_markdown_html(render_goal_details(view)))
    assert all(
        label in details
        for label in ("Цель:", "Сделано:", "Осталось:", "Что мешает:", "Что можно сделать:")
    )
    debug = visible(telegram_markdown_html(render_goal_debug(view)))
    assert identifiers[0] in debug and identifiers[1] in debug
    assert "managed/internal" in debug and "internal criterion" in debug


@pytest.mark.parametrize(
    "question",
    [
        "Утверждаете запуск?",
        "Подтверди план",
        "Можно начинать?",
        "План подходит?",
        "Proceed with the plan?",
    ],
)
def test_plan_approval_wording_is_folded(question):
    from vuzol.leadership.text import is_plan_approval_question

    assert is_plan_approval_question(question)
    assert not is_plan_approval_question("Как назвать новый раздел?")
