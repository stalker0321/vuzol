"""Owner-facing model failures and the existing reply-to-resume boundary."""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from tests.unit.leadership.test_model_failures import outage
from tests.unit.leadership.test_model_failures import turns as turns
from tests.unit.test_telegram_reply_chunks import visible

from vuzol.leadership import service
from vuzol.leadership.actor import Actor, ActorKind
from vuzol.leadership.card import render_goal_card
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.model import LeadDecision, LeadModelUnavailable
from vuzol.leadership.notices import planner_failure_notice
from vuzol.telegram.formatting import telegram_markdown_html


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize("review", [False, True])
@pytest.mark.parametrize(
    "category,reason_text",
    [
        ("provider_unavailable", "провайдер недоступен"),
        ("rate_limited", "лимит запросов"),
        ("timeout", "таймаут"),
    ],
)
async def test_exhaustion_names_part_reason_retries_and_action(
    turns, monkeypatch, review, category, reason_text
):
    turns.recovery.side_effect = service.request_owner_recovery
    notice = AsyncMock()
    monkeypatch.setattr("vuzol.telegram.projections.enqueue_chat_notice", notice)
    monkeypatch.setattr(service, "enqueue_goal_status", AsyncMock())
    fail = turns.verifier.verify if review else turns.model.decide
    fail.side_effect = LeadModelUnavailable(
        "goal_review_unavailable" if review else "lead_model_unavailable",
        definitive=True,
        failure_category=category,
    )
    item = turns.first
    for _ in range(3):
        await turns.run(item)
        (item,) = turns.pending()
    await turns.run(item)
    notice.assert_awaited_once()
    text = visible(telegram_markdown_html(notice.call_args.kwargs["text"]))
    part = "Проверка цели" if review else "Модель руководителя"
    for expected in (part, reason_text, "Повторил 3 раза", "Напиши «продолжай»"):
        assert expected in text
    assert "ВОПРОС" not in text
    for term in ("lead_", "goal_", "retry", "профиль", "уточни следующий шаг", "после устранения"):
        assert term not in text.lower()
    card = visible(
        telegram_markdown_html(
            render_goal_card(
                {
                    "lifecycle": turns.goal.lifecycle,
                    "waiting_reason": turns.goal.waiting_reason,
                    "question": turns.goal.wakeup_condition["question"],
                    "model_failure": turns.goal.wakeup_condition.get("model_failure"),
                }
            )
        )
    )
    assert part in card and reason_text in card and "Повторил 3 раза" in card


@pytest.mark.anyio
@pytest.mark.parametrize(
    "reason,part",
    [
        ("lead_response_invalid", "Модель руководителя"),
        ("goal_review_invalid", "Проверка цели"),
    ],
)
async def test_unparseable_answer_has_plain_reason_and_one_retry(turns, monkeypatch, reason, part):
    turns.recovery.side_effect = service.request_owner_recovery
    enqueue = AsyncMock()
    monkeypatch.setattr(service, "_enqueue_owner_question", enqueue)
    turns.model.decide.side_effect = LeadModelUnavailable(reason)
    await turns.run(turns.first)
    (retry,) = turns.pending()
    await turns.run(retry)
    question = enqueue.call_args.args[2]
    assert part in question and "ответ не удалось разобрать" in question
    assert "Повторил 1 раз" in question and "Напиши «продолжай»" in question


@pytest.mark.anyio
async def test_continue_reply_after_exhaustion_starts_a_new_attempt(turns, monkeypatch):
    turns.recovery.side_effect = service.request_owner_recovery
    monkeypatch.setattr(service, "_enqueue_owner_question", AsyncMock())
    turns.model.decide.side_effect = outage()
    item = turns.first
    for _ in range(3):
        await turns.run(item)
        (item,) = turns.pending()
    await turns.run(item)
    command = LeadCommand(
        name="answer_permission",
        project_id="p",
        goal_id=turns.goal.id,
        expected_version=turns.goal.version,
        idempotency_key="continue",
        payload={"answer": "продолжай", "question_version": turns.goal.version},
    )
    actor = Actor(ActorKind.HUMAN, "1", "telegram")
    outcome = await service._answer_permission(turns.session, actor, command, turns.goal)
    assert outcome.status == "applied"
    assert turns.goal.lifecycle == "active" and turns.goal.waiting_reason is None
    (fresh,) = turns.pending()
    assert fresh.payload["wakeup_kind"] == "resume"
    assert "retry_of" not in fresh.payload and "model_retry_count" not in fresh.payload
    turns.model.decide.side_effect = None
    turns.model.decide.return_value = LeadDecision(reply="Продолжаю")
    await turns.run(fresh)
    assert turns.model.decide.await_count == 5
    turns.worker._apply_if_current.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "reason,detail",
    [
        ("planner_provider_unavailable", "провайдер недоступен"),
        ("planner_rate_limited", "лимит запросов"),
        ("planner_timeout", "таймаут"),
        ("planner_invalid_structured_output", "ответ не удалось разобрать"),
    ],
)
async def test_planner_notice_names_part_reason_and_action(turns, monkeypatch, reason, detail):
    notice = AsyncMock()
    monkeypatch.setattr("vuzol.leadership.notices.enqueue_chat_notice", notice)
    turns.goal.discussion_session_id = uuid.uuid4()
    turns.rows[turns.goal.discussion_session_id] = SimpleNamespace(chat_id=1, message_thread_id=2)
    await planner_failure_notice(turns.session, turns.goal, "planner-failed", reason)
    text = notice.call_args.kwargs["text"]
    assert "Планировщик" in text and detail in text and "Напиши «продолжай»" in text
    assert "профиль" not in text and "контекст" not in text and "ревизия" not in text
