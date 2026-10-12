"""Chat retry wire binding, feedback and opt-in memory contract."""

import uuid

import pytest
from telegram import Update

from vuzol.config import Settings
from vuzol.discussion.agent import DiscussionAgentReply
from vuzol.telegram.adapter import _callback_answer, _control_markup, control_update
from vuzol.telegram.domain import ControlUpdate, IngressResult, IngressStatus


def test_chat_retry_callback_preserves_exact_error_message_and_topic_binding() -> None:
    task_id = uuid.uuid4()
    data = f"v1:chat_retry:{task_id}"
    assert len(data.encode()) <= 64
    update = Update.de_json(
        {
            "update_id": 3,
            "callback_query": {
                "id": "chat-retry",
                "from": {"id": 42, "is_bot": False, "first_name": "User"},
                "chat_instance": "forum",
                "data": data,
                "message": {
                    "message_id": 77,
                    "message_thread_id": 10,
                    "date": 0,
                    "chat": {"id": -100, "type": "supergroup"},
                },
            },
        },
        None,
    )
    converted = control_update(update, "main")
    assert isinstance(converted, ControlUpdate)
    assert (converted.task_id, converted.message_id, converted.message_thread_id) == (
        task_id,
        77,
        10,
    )
    assert converted.action_kind == "chat_retry" and converted.step_id is None
    markup = _control_markup(
        (), task_id=None, approval_id=None, callback_buttons=((("Ещё раз", data),),)
    )
    assert markup.inline_keyboard[0][0].callback_data == data


@pytest.mark.parametrize(
    "reason,status",
    [
        ("chat_reply_retry", IngressStatus.HANDLED),
        ("chat_delivery_retry", IngressStatus.HANDLED),
        ("chat_retry_pending", IngressStatus.DUPLICATE),
        ("chat_retry_stale", IngressStatus.REJECTED),
        ("chat_retry_ambiguous", IngressStatus.REJECTED),
    ],
)
def test_chat_retry_feedback_does_not_refer_to_task_cards(reason, status) -> None:
    text, alert = _callback_answer(IngressResult(status=status, reason=reason))
    assert text and "карточку" not in text
    assert alert is (status is IngressStatus.REJECTED)


def test_memory_opt_in_and_old_reply_contract_remain_compatible() -> None:
    assert Settings(environment="test").chat.memory_enabled is False
    reply = DiscussionAgentReply.model_validate({"reply": "Ответ"})
    assert reply.memory_summary is None
    assert DiscussionAgentReply(reply="Ответ", memory_summary="я" * 2001).reply == "Ответ"
