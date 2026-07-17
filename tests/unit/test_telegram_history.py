"""History topic completion report helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vuzol.storage.types import ApprovalStatus, TaskStatus
from vuzol.telegram.layout import HISTORY_TOPIC_KIND
from vuzol.telegram.projections import (
    TASK_HISTORY_ROLE,
    _bounded_report,
    _completion_report,
    _concise_completion_report,
    _failure_details,
    _format_count,
    _format_duration,
    _history_work_seconds,
    _one_line_summary,
    _task_outcome_label,
    _task_worker_label,
    build_task_history_report,
    enqueue_task_history_report,
)


def test_history_topic_is_changelog() -> None:
    assert HISTORY_TOPIC_KIND.value == "changelog"
    assert TASK_HISTORY_ROLE == "task_history"


def test_format_count_and_duration() -> None:
    assert _format_count(141_387) == "141,387"
    assert _format_count(0) == "0"
    assert _format_duration(0) == "0s"
    assert _format_duration(45) == "45s"
    assert _format_duration(125) == "2m 5s"
    assert _format_duration(3723) == "1h 2m 3s"


def test_one_line_summary_truncates() -> None:
    assert _one_line_summary("First sentence. Second.") == "First sentence"
    long = "x" * 400
    assert _one_line_summary(long).endswith("…")
    assert len(_one_line_summary(long)) <= 280
    assert _one_line_summary("   ") == "No description"
    assert _one_line_summary("") == "No description"


def test_terminal_report_formatting_is_bounded() -> None:
    assert _task_outcome_label(TaskStatus.COMPLETED) == "✅ Завершена успешно"
    assert "заблокирована" in _task_outcome_label(TaskStatus.BLOCKED)
    assert _task_outcome_label(TaskStatus.FAILED) == "❌ Завершена неудачно"
    assert _bounded_report(" first \n\n second ") == "first\nsecond"
    assert _bounded_report("x" * 20, 10) == "xxxxxxxxx…"
    assert _bounded_report("   ") == "Без описания"


def test_completion_report_keeps_facts_and_drops_handoff_sections() -> None:
    text = """Готово — добавлен выбор позиций.

Реализовано:
- чекбоксы для каждой позиции;
- отдельная сумма выбранных позиций;
- действия «Выбрать всё» и «Сбросить».

Файлы:
- [app.js](/workspace/app.js)

Для локального запуска:
python3 -m http.server
"""
    assert _concise_completion_report(text) == (
        "Готово — добавлен выбор позиций.\n"
        "• чекбоксы для каждой позиции;\n"
        "• отдельная сумма выбранных позиций;\n"
        "• действия «Выбрать всё» и «Сбросить»."
    )


@pytest.mark.anyio
async def test_worker_label_uses_execution_model_not_planner() -> None:
    step = SimpleNamespace(
        executor_profile_id="codex-subscription-prod",
        result={"model": "gpt-5.6-sol"},
    )
    session = MagicMock()
    session.scalar = AsyncMock(return_value=step)

    assert await _task_worker_label(session, uuid4()) == "Codex Sol"


@pytest.mark.anyio
async def test_completion_report_prefers_approval_and_gate_names() -> None:
    task = SimpleNamespace(id=uuid4())
    approval = SimpleNamespace(
        step_id=uuid4(),
        human_summary="Implemented the terminal report with bounded details.",
    )
    approval_step = SimpleNamespace(
        payload={
            "action_envelope": {
                "gates": [
                    {"name": "format-check"},
                    {"name": "test"},
                    {"exit_code": 0},
                    "invalid",
                ]
            }
        }
    )
    session = MagicMock()
    session.scalar = AsyncMock(return_value=approval)
    session.get = AsyncMock(return_value=approval_step)

    report, gates = await _completion_report(session, task)  # type: ignore[arg-type]

    assert report == "Implemented the terminal report with bounded details."
    assert gates == ("format-check", "test")


@pytest.mark.anyio
async def test_completion_and_failure_reports_have_canonical_fallbacks() -> None:
    task = SimpleNamespace(
        id=uuid4(),
        task_draft={"task_summary": "Fallback task summary."},
        original_text="fallback",
    )
    run = SimpleNamespace(id=uuid4())
    result_step = SimpleNamespace(
        result={"structured_output": {"implementation_summary": "Structured result."}}
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[None, run])
    session.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: [result_step]))
    report, gates = await _completion_report(session, task)  # type: ignore[arg-type]
    assert report == "Structured result."
    assert gates == ()

    event = SimpleNamespace(payload={"reason": "Project topic creation was ambiguous."})
    session.scalar = AsyncMock(side_effect=[None, event])
    stage, reason = await _failure_details(session, task)  # type: ignore[arg-type]
    assert stage is None
    assert reason == "Project topic creation was ambiguous."


@pytest.mark.anyio
async def test_work_seconds_prefers_usage_durations() -> None:
    task = SimpleNamespace(
        id=uuid4(),
        created_at=datetime(2026, 7, 16, 10, 0, tzinfo=UTC),
        updated_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    )
    usage_rows = [
        SimpleNamespace(duration_ms=60_000),
        SimpleNamespace(duration_ms=125_000),
    ]
    session = MagicMock()
    session.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: usage_rows))
    assert await _history_work_seconds(session, task) == 185  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_work_seconds_falls_back_excluding_approval_wait() -> None:
    task = SimpleNamespace(
        id=uuid4(),
        created_at=datetime(2026, 7, 16, 10, 0, tzinfo=UTC),
        updated_at=datetime(2026, 7, 16, 11, 0, tzinfo=UTC),  # 3600s wall
    )
    approval = SimpleNamespace(
        requested_at=datetime(2026, 7, 16, 10, 10, tzinfo=UTC),
        decided_at=datetime(2026, 7, 16, 10, 40, tzinfo=UTC),  # 1800s wait
    )
    session = MagicMock()
    # First call: usage empty; second: approvals
    session.scalars = AsyncMock(
        side_effect=[
            SimpleNamespace(all=lambda: []),
            SimpleNamespace(all=lambda: [approval]),
        ]
    )
    assert await _history_work_seconds(session, task) == 1800  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_prepare_task_history_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    from vuzol.telegram.delivery import DeliveryAction, PermanentDeliveryError, prepare_delivery
    from vuzol.telegram.projections import HistoryReport

    task_id = uuid4()
    item = SimpleNamespace(
        operation_type="send_message",
        payload={"role": TASK_HISTORY_ROLE, "task_id": str(task_id), "chat_id": -100},
        linked_entity_type="task",
        linked_entity_id=task_id,
    )
    report = HistoryReport(
        task_id=task_id,
        chat_id=-100,
        thread_id=13,
        html="<b>#1</b>",
    )
    session = MagicMock()
    session.scalar = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "vuzol.telegram.delivery.build_task_history_report",
        AsyncMock(return_value=report),
    )
    prepared = await prepare_delivery(session, item)  # type: ignore[arg-type]
    assert prepared.action == DeliveryAction.SEND_STATUS
    assert prepared.thread_id == 13
    assert prepared.message_role == TASK_HISTORY_ROLE
    assert prepared.html == "<b>#1</b>"

    failed_item = SimpleNamespace(
        operation_type="send_message",
        payload={
            "role": TASK_HISTORY_ROLE,
            "task_id": str(task_id),
            "chat_id": -100,
            "terminal_status": "failed",
        },
        linked_entity_type="task",
        linked_entity_id=task_id,
    )
    prepared_failed = await prepare_delivery(session, failed_item)  # type: ignore[arg-type]
    assert prepared_failed.message_role == f"{TASK_HISTORY_ROLE}_failed"

    bad = SimpleNamespace(
        operation_type="send_message",
        payload={"role": TASK_HISTORY_ROLE, "task_id": "not-a-uuid"},
        linked_entity_type="task",
        linked_entity_id=None,
    )
    with pytest.raises(PermanentDeliveryError):
        await prepare_delivery(session, bad)  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_history_summary_from_step_result() -> None:
    from vuzol.telegram.projections import _history_summary

    task = SimpleNamespace(
        id=uuid4(),
        task_draft={},
        original_text="fallback task text for the summary",
    )
    run = SimpleNamespace(id=uuid4(), created_at=datetime(2026, 7, 16, tzinfo=UTC))
    step = SimpleNamespace(
        result={"implementation_summary": "Implemented the API endpoint cleanly."},
        ordinal=3,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[None, run])  # no approval, then run
    session.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: [step]))
    summary = await _history_summary(session, task)  # type: ignore[arg-type]
    assert summary == "Implemented the API endpoint cleanly"


@pytest.mark.anyio
async def test_build_report_requires_completed_task() -> None:
    session = MagicMock()
    session.get = AsyncMock(
        return_value=SimpleNamespace(
            id=uuid4(),
            status=TaskStatus.EXECUTING,
            source_chat_id=-100,
        )
    )
    assert await build_task_history_report(session, uuid4()) is None


@pytest.mark.anyio
async def test_failed_task_history_reports_stage_and_reason() -> None:
    task_id = uuid4()
    run = SimpleNamespace(
        id=uuid4(),
        created_at=datetime(2026, 7, 16, 10, 0, tzinfo=UTC),
        failure_category="validation_failed",
        failure_summary="Trusted tests did not pass.",
    )
    failed_step = SimpleNamespace(
        step_type="validate",
        failure_category="validation_failed",
        failure_summary="API contract test failed on the response schema.",
    )
    mapping = SimpleNamespace(message_thread_id=13)
    task = SimpleNamespace(
        id=task_id,
        status=TaskStatus.FAILED,
        source_chat_id=-100,
        project_id="vuzol",
        public_task_number=730005,
        topic_task_number=5,
        task_draft={"task_summary": "Update the API response contract."},
        original_text="update api",
        created_at=datetime(2026, 7, 16, 10, 0, tzinfo=UTC),
        updated_at=datetime(2026, 7, 16, 10, 2, tzinfo=UTC),
    )
    session = MagicMock()
    session.get = AsyncMock(return_value=task)

    async def scalar(stmt: object) -> object:
        query = str(stmt).lower()
        if "topic_mapping" in query:
            return mapping
        if "from runs" in query:
            return run
        if "from steps" in query:
            return failed_step
        return None

    session.scalar = AsyncMock(side_effect=scalar)
    session.scalars = AsyncMock(
        side_effect=[
            SimpleNamespace(all=lambda: []),
            SimpleNamespace(all=lambda: []),
            SimpleNamespace(all=lambda: []),
        ]
    )

    report = await build_task_history_report(session, task_id)

    assert report is not None
    assert "Завершена неудачно" in report.html
    assert "Этап:</b> validate" in report.html
    assert "API contract test failed on the response schema" in report.html
    assert "Результат:</b>" not in report.html


@pytest.mark.anyio
async def test_build_and_enqueue_history_report() -> None:
    task_id = uuid4()
    mapping_id = uuid4()
    task = SimpleNamespace(
        id=task_id,
        status=TaskStatus.COMPLETED,
        source_chat_id=-1003950752781,
        project_id="bill-buddy",
        public_task_number=730004,
        topic_task_number=1,
        task_draft={
            "normalized_title": "Build the landing page.",
            "task_summary": "Build a responsive landing page for Bill Buddy.",
        },
        original_text="landing",
        created_at=datetime(2026, 7, 16, 10, 0, tzinfo=UTC),
        updated_at=datetime(2026, 7, 16, 10, 5, tzinfo=UTC),
    )
    mapping = SimpleNamespace(
        id=mapping_id,
        chat_id=-1003950752781,
        message_thread_id=13,
        topic_kind="changelog",
        enabled=True,
    )
    usage = SimpleNamespace(
        input_tokens=1000,
        output_tokens=200,
        cached_tokens=50,
        duration_ms=90_000,
    )
    approval = SimpleNamespace(
        human_summary="Landing page implemented with responsive layout.",
        status=ApprovalStatus.APPROVED,
        requested_at=datetime(2026, 7, 16, 10, 3, tzinfo=UTC),
        decided_at=datetime(2026, 7, 16, 10, 4, tzinfo=UTC),
    )
    run = SimpleNamespace(id=uuid4(), created_at=datetime(2026, 7, 16, 10, 0, tzinfo=UTC))
    worker_step = SimpleNamespace(
        executor_profile_id="codex-subscription-prod",
        result={"model": "gpt-5.6-sol"},
    )

    session = MagicMock()
    session.get = AsyncMock(return_value=task)
    session.add = MagicMock()

    async def scalar(stmt: object) -> object:
        text = str(stmt)
        if "topic_mappings" in text or "TopicMapping" in text:
            return mapping
        if "approvals" in text or "Approval" in text:
            return approval
        if "JOIN runs" in text and "steps" in text:
            return worker_step
        if "runs" in text or "Run" in text:
            return run
        if "transactional_outbox" in text or "TransactionalOutbox" in text:
            return None
        return None

    session.scalar = AsyncMock(side_effect=scalar)
    session.scalars = AsyncMock(
        side_effect=[
            # usage for tokens in build
            SimpleNamespace(all=lambda: [usage]),
            # usage for work seconds
            SimpleNamespace(all=lambda: [usage]),
            # provider steps for summary fallback (not used when approval present)
            SimpleNamespace(all=lambda: []),
        ]
    )

    report = await build_task_history_report(
        session, task_id, project_names={"bill-buddy": "Bill Buddy"}
    )
    assert report is not None
    assert report.thread_id == 13
    assert "#730004" in report.html
    assert "Bill Buddy" in report.html
    assert "Задача:</b> Build a responsive landing page for Bill Buddy" in report.html
    assert "Результат:</b> Landing page implemented" in report.html
    assert "Worker:</b> Codex Sol" in report.html
    assert "Landing page implemented" in report.html
    assert "1,000" in report.html
    assert "200" in report.html
    assert "50" in report.html
    assert "1m 30s" in report.html

    async def scalar2(stmt: object) -> object:
        text = str(stmt).lower()
        if "topic_mapping" in text or "topic_mappings" in text:
            return mapping
        if "approval" in text:
            return approval
        if "join runs" in text and "steps" in text:
            return worker_step
        if " run " in text or "runs" in text:
            return run
        if "outbox" in text or "transactional" in text:
            return None
        return None

    session.get = AsyncMock(return_value=task)
    session.scalar = AsyncMock(side_effect=scalar2)
    session.scalars = AsyncMock(
        side_effect=[
            SimpleNamespace(all=lambda: [usage]),
            SimpleNamespace(all=lambda: [usage]),
            SimpleNamespace(all=lambda: []),
        ]
    )
    session.add = MagicMock()
    await enqueue_task_history_report(session, task_id)
    assert session.add.called
    outbox = session.add.call_args[0][0]
    assert outbox.payload["role"] == TASK_HISTORY_ROLE
    assert outbox.idempotency_key == (
        f"telegram:{TASK_HISTORY_ROLE}:task:{task_id}:outcome:completed"
    )
    assert outbox.payload["terminal_status"] == "completed"
