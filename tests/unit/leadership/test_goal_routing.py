"""Separate requests must be explicit, owner-confirmed handoffs."""

import uuid

import pytest
from tests.unit.leadership.test_tool_loop import snapshot

from vuzol.leadership.card import render_goal_card
from vuzol.leadership.provider_model import PROMPT_REVISION, _prompt, parse_lead_response
from vuzol.telegram.formatting import telegram_markdown_html


def test_prompt_distinguishes_refinement_from_separate_work():
    prompt = _prompt(snapshot())
    assert PROMPT_REVISION == "project-lead-prompt.v19"
    assert "If the current request differs from the new message, first amend_goal" not in prompt
    assert "refining the SAME goal" in prompt
    assert "approved/proposed plan, task, result, or waiting question" in prompt
    assert "bare draft with no plan and no work" in prompt
    assert "Дополнение к текущей" in prompt
    assert "Новая задача, текущую закрыть" in prompt
    assert "Новая задача после текущей" in prompt
    assert "resend" in prompt
    assert "restart_from_event_id" in prompt


@pytest.mark.parametrize("source", [None, str(uuid.uuid4())])
def test_cancel_payload_accepts_optional_handoff_source(source):
    payload = {} if source is None else {"restart_from_event_id": source}
    decision = parse_lead_response({"commands": [{"name": "cancel_goal", "payload": payload}]})
    assert decision.commands[0].payload.get("restart_from_event_id") == source


def test_cancelled_card_explains_the_new_task_handoff():
    card = telegram_markdown_html(
        render_goal_card(
            {
                "lifecycle": "cancelled",
                "waiting_reason": "closed_for_new_task",
            }
        )
    )
    assert "Цель закрыта для новой задачи" in card
    assert "Сохранённые результаты не удалены" in card
    assert "closed_for_new_task" not in card
    plain = telegram_markdown_html(render_goal_card({"lifecycle": "cancelled"}))
    assert "Работа отменена" in plain
