import uuid
from typing import cast
from unittest.mock import MagicMock

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.interpretation.domain import (
    InterpretationResult,
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.models import (
    Interpretation,
    Task,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    RiskLevel,
    TaskStatus,
)
from vuzol.telegram.tracing import (
    INTERPRETER_TRACE_KIND,
    ORCHESTRATION_TRACE_ROLE,
    build_interpreter_trace_html,
    enqueue_interpreter_trace,
    interpreter_trace_is_anomalous,
    orchestration_trace_sample_bucket,
    should_deliver_orchestration_trace,
)


def _draft(*, summary: str = "Implement selection") -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.CODING,
        operation=TaskOperation.MODIFY,
        project_id="bill-buddy",
        goal="Implement item selection",
        task_summary=summary,
        requested_outcomes=("Selected total",),
        required_capabilities=frozenset(),
        suggested_complexity=SuggestedComplexity.MEDIUM,
        suggested_risk=RiskLevel.MEDIUM,
        needs_planning=True,
        needs_clarification=False,
        normalized_title="Add item selection",
    )


def _task() -> Task:
    return Task(
        id=uuid.uuid4(),
        user_id=1,
        source_chat_id=-1001,
        source_thread_id=73,
        public_task_number=730010,
        project_id="bill-buddy",
        original_text="<select items>",
        task_draft=_draft().model_dump(mode="json"),
        status=TaskStatus.INTERPRETED,
        risk=RiskLevel.MEDIUM,
        task_type="coding",
    )


def _interpretation(task: Task, *, effective: TaskDraft | None = None) -> Interpretation:
    return Interpretation(
        id=uuid.uuid4(),
        task_id=task.id,
        original_input_hash="a" * 64,
        task_draft=(effective or _draft()).model_dump(mode="json"),
        profile_id="openai-interpreter",
        model="gpt-4o-mini",
        prompt_version="architecture-routing-v8",
        schema_version="1.4",
    )


def test_interpreter_trace_shows_raw_and_policy_adjusted_drafts() -> None:
    task = _task()
    raw = _draft(summary="Raw <summary>")
    effective = _draft(summary="Policy summary")
    interpretation = _interpretation(task, effective=effective)
    html = build_interpreter_trace_html(
        task,
        interpretation,
        {
            "model_task_draft": raw.model_dump(mode="json"),
            "input_tokens": 120,
            "output_tokens": 80,
            "duration_ms": 1_250,
            "repaired": True,
        },
    )
    assert "Интерпретатор · #730010" in html
    assert "120 in / 80 out · 1.2s" in html
    assert "repair-попытка" in html
    assert "После deterministic policy" in html
    assert "Raw &lt;summary&gt;" in html
    assert "Policy summary" in html


def test_interpreter_trace_handles_unknown_metrics_without_policy_change() -> None:
    task = _task()
    interpretation = _interpretation(task)
    html = build_interpreter_trace_html(
        task,
        interpretation,
        {
            "model_task_draft": interpretation.task_draft,
            "input_tokens": None,
            "output_tokens": None,
            "duration_ms": None,
            "repaired": False,
        },
    )
    assert "? in / ? out · ? ms" in html
    assert "После deterministic policy" not in html


def test_trace_enqueues_are_durable_and_idempotent() -> None:
    task = _task()
    interpretation = _interpretation(task)
    result = InterpretationResult(
        draft=_draft(),
        profile_id="openai-interpreter",
        model="gpt-4o-mini",
        input_tokens=100,
        output_tokens=50,
        duration_ms=900,
    )
    session = MagicMock(spec=AsyncSession)
    enqueue_interpreter_trace(
        session,
        task=task,
        interpretation=interpretation,
        result=result,
    )
    first = cast(TransactionalOutbox, session.add.call_args_list[0].args[0])
    assert first.payload["role"] == ORCHESTRATION_TRACE_ROLE
    assert first.payload["trace_kind"] == INTERPRETER_TRACE_KIND
    assert first.idempotency_key.endswith(str(interpretation.id))


def test_trace_sampling_is_stable_at_task_level_and_anomaly_aware() -> None:
    task_id = uuid.uuid4()
    bucket = orchestration_trace_sample_bucket(task_id)

    assert 0 <= bucket < 100
    assert orchestration_trace_sample_bucket(task_id) == bucket
    assert not should_deliver_orchestration_trace(
        task_id,
        enabled=True,
        sample_percent=bucket,
        anomalous=False,
        always_include_anomalies=True,
    )
    assert should_deliver_orchestration_trace(
        task_id,
        enabled=True,
        sample_percent=bucket + 1,
        anomalous=False,
        always_include_anomalies=True,
    )
    assert should_deliver_orchestration_trace(
        task_id,
        enabled=True,
        sample_percent=0,
        anomalous=True,
        always_include_anomalies=True,
    )
    assert not should_deliver_orchestration_trace(
        task_id,
        enabled=False,
        sample_percent=100,
        anomalous=True,
        always_include_anomalies=True,
    )


def test_trace_anomaly_classification_matches_visible_warnings() -> None:
    task = _task()
    interpretation = _interpretation(task)
    unchanged = {
        "model_task_draft": interpretation.task_draft,
        "repaired": False,
    }
    assert not interpreter_trace_is_anomalous(interpretation, unchanged)
    assert interpreter_trace_is_anomalous(
        interpretation,
        {**unchanged, "model_task_draft": {"task_type": "research"}},
    )
    assert interpreter_trace_is_anomalous(interpretation, {**unchanged, "repaired": True})
