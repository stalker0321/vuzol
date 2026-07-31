# ruff: noqa: RUF001

import asyncio
import uuid
from datetime import UTC, datetime

import pytest

from vuzol.storage.types import ApprovalStatus, StepStatus, TaskStatus, WorktreeDeliveryState
from vuzol.telegram.projections import (
    EditRateLimiter,
    _approval_status_label,
    _format_duration_ru,
    delivery_state_label,
    split_message,
    status_buttons,
    step_status_label,
    step_type_label,
    telegram_html,
    user_status_label,
)
from vuzol.workflows.definitions import WORKFLOW_DEFINITIONS


def test_html_is_escaped_and_messages_are_bounded() -> None:
    assert telegram_html('<b x="1">&') == "&lt;b x=&quot;1&quot;&gt;&amp;"
    assert split_message("abcdef", limit=2) == ("ab", "cd", "ef")
    assert split_message("") == ("",)
    with pytest.raises(ValueError):
        split_message("x", limit=0)


def test_edit_rate_limiter_serializes_one_projection() -> None:
    async def scenario() -> None:
        limiter = EditRateLimiter(2)
        task_id = uuid.uuid4()
        now = datetime.now(UTC)
        assert await limiter.reserve(task_id, now) == now
        assert (await limiter.reserve(task_id, now) - now).total_seconds() == 2
        assert await limiter.reserve(uuid.uuid4(), now) == now

    asyncio.run(scenario())


def test_all_status_and_delivery_values_have_russian_labels() -> None:
    for task_status in TaskStatus:
        assert user_status_label(task_status)
        assert user_status_label(task_status) != task_status.value
    for step_status in StepStatus:
        assert step_status_label(step_status)
        assert step_status_label(step_status) != step_status.value
    for state in WorktreeDeliveryState:
        assert delivery_state_label(state)
        assert delivery_state_label(state) != state.value


def test_all_registered_workflow_steps_have_dedicated_labels() -> None:
    step_types = {
        step.step_type for definition in WORKFLOW_DEFINITIONS for step in definition.steps
    }
    for step_type in step_types:
        assert step_type_label(step_type) != step_type


def test_unknown_labels_are_safely_escaped() -> None:
    assert user_status_label("<future>") == "&lt;future&gt;"
    assert step_status_label("<future>") == "&lt;future&gt;"
    assert step_type_label("<future>") == "&lt;future&gt;"
    assert delivery_state_label("<future>") == "&lt;future&gt;"


def test_russian_duration_format() -> None:
    assert _format_duration_ru(0) == "0 с"
    assert _format_duration_ru(45) == "45 с"
    assert _format_duration_ru(125) == "2 мин 5 с"
    assert _format_duration_ru(3723) == "1 ч 2 мин"


def test_approval_decision_labels_are_russian() -> None:
    assert _approval_status_label(ApprovalStatus.APPROVED) == "Принято"
    assert _approval_status_label(ApprovalStatus.CONSUMED) == "Принято"
    assert _approval_status_label(ApprovalStatus.REJECTED) == "Отклонено"
    assert _approval_status_label(ApprovalStatus.EXPIRED) == "Истекло"


def test_task_status_button_matrix_is_exhaustive_and_has_no_retry_ui() -> None:
    pause_cancel = {
        TaskStatus.RECEIVED,
        TaskStatus.CONTEXT_PREPARED,
        TaskStatus.PLANNED,
        TaskStatus.WAITING_APPROVAL,
        TaskStatus.EXECUTING,
        TaskStatus.VALIDATING,
        TaskStatus.REVIEWING,
        TaskStatus.RETRYING,
    }
    expected = {status: ("pause", "cancel") for status in pause_cancel} | {
        TaskStatus.PAUSED: ("resume", "cancel")
    }

    for status in TaskStatus:
        buttons = tuple(status_buttons(status.value))
        assert buttons == expected.get(status, ())
        assert "retry" not in buttons
