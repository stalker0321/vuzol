import uuid
from types import SimpleNamespace
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
    ProviderBudgetReservation,
    Step,
    Task,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.storage.types import (
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RiskLevel,
    StepStatus,
    TaskStatus,
)
from vuzol.telegram.tracing import (
    INTERPRETER_TRACE_KIND,
    ORCHESTRATION_TRACE_ROLE,
    PLANNER_TRACE_KIND,
    build_interpreter_trace_html,
    build_planner_trace_html,
    enqueue_interpreter_trace,
    enqueue_planner_trace,
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


def _plan_step(*, status: StepStatus = StepStatus.COMPLETED) -> Step:
    return Step(
        id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        ordinal=1,
        dependency_metadata={},
        step_type="plan",
        queue_class=QueueClass.LIGHT,
        status=status,
        executor_profile_id="openai-planner-prod",
        required_capabilities=[],
        payload={"budget_reservation_id": str(uuid.uuid4())},
        result={
            "model": "gpt-5-nano-2025-08-07",
            "profile_id": "openai-planner-prod",
            "text": "",
            "finish_reason": "length",
            "structured_output": None,
        },
        retry_class=RetryClass.TRANSIENT,
        idempotency_class=IdempotencyClass.READ_ONLY,
        attempt_count=1,
        max_attempts=3,
        timeout_seconds=600,
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


def test_planner_trace_makes_empty_truncated_failure_visible() -> None:
    task = _task()
    step = _plan_step(status=StepStatus.QUEUED)
    step.result = {
        "model": "gpt-5-nano-2025-08-07",
        "profile_id": "openai-planner-prod",
        "text": "",
        "finish_reason": "length",
        "structured_output": None,
        "handoff": {"status": "rejected", "reason": "planner_truncated"},
    }
    step.failure_category = "planner_truncated"
    step.failure_summary = "planner output stopped at the token limit"
    usage = cast(
        UsageRecord,
        SimpleNamespace(
            model="gpt-5-nano-2025-08-07",
            input_tokens=375,
            output_tokens=1_000,
        ),
    )
    reservation = cast(
        ProviderBudgetReservation,
        SimpleNamespace(reserved_output_tokens=1_000),
    )
    html = build_planner_trace_html(task, step, usage=usage, reservation=reservation)
    assert "Планировщик · #730010" in html
    assert "375 in / 1,000 out · лимит 1,000 out" in html
    assert "finish_reason=length" in html
    assert "Пустой вывод" in html
    assert "Handoff воркеру: <b>ожидает retry</b>" in html
    assert "planner_truncated" in html


def test_planner_trace_reports_ready_handoff_on_completed_plan() -> None:
    task = _task()
    step = _plan_step()
    step.result = {
        "model": "gpt-5-nano-2025-08-07",
        "profile_id": "openai-planner-prod",
        "text": "1. Inspect\n2. Edit\n3. Validate",
        "finish_reason": "stop",
        "handoff": {"status": "ready"},
    }
    html = build_planner_trace_html(task, step, usage=None, reservation=None)
    assert "Handoff воркеру: <b>подключён</b>" in html
    assert "ProviderRequest" in html
    assert "1. Inspect" in html


def test_planner_trace_bounds_structured_failure_output() -> None:
    task = _task()
    step = _plan_step(status=StepStatus.FAILED)
    step.result = {
        "text": "x" * 2_500,
        "structured_output": {"steps": ["inspect", "edit", "verify"]},
        "finish_reason": "stop",
        "handoff": {"status": "rejected", "reason": "planner_invalid"},
    }
    step.failure_category = "planner_invalid"
    step.failure_summary = "unsafe <shape>"
    html = build_planner_trace_html(task, step, usage=None, reservation=None)
    assert "planner_invalid" in html
    assert "unsafe &lt;shape&gt;" in html
    assert "Structured output" in html
    assert "…</pre>" in html
    assert "Handoff воркеру: <b>не выполнен</b>" in html


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
    enqueue_planner_trace(session, task=task, step=_plan_step())
    first = cast(TransactionalOutbox, session.add.call_args_list[0].args[0])
    second = cast(TransactionalOutbox, session.add.call_args_list[1].args[0])
    assert first.payload["role"] == ORCHESTRATION_TRACE_ROLE
    assert first.payload["trace_kind"] == INTERPRETER_TRACE_KIND
    assert first.idempotency_key.endswith(str(interpretation.id))
    assert second.payload["trace_kind"] == PLANNER_TRACE_KIND
    assert ":1:completed" in second.idempotency_key
