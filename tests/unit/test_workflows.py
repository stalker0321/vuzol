import asyncio
import uuid
from dataclasses import replace

import pytest
from pydantic import ValidationError

from vuzol.config import WorkflowSettings
from vuzol.interpretation.domain import (
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.errors import IllegalTransition
from vuzol.storage.models import Step
from vuzol.storage.types import (
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.workflows import WORKFLOW_DEFINITIONS, compile_workflow
from vuzol.workflows import transitions as workflow_transitions
from vuzol.workflows.definitions import validate_definition
from vuzol.workflows.domain import StepOutcome, WorkflowDefinition, WorkflowDefinitionError
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.service import derive_task_status


def draft(
    task_type: TaskType = TaskType.CODING,
    *,
    planning: bool = False,
    risk: RiskLevel = RiskLevel.LOW,
) -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=task_type,
        operation=TaskOperation.MODIFY,
        goal="Implement the request",
        suggested_complexity=SuggestedComplexity.SMALL,
        suggested_risk=risk,
        needs_planning=planning,
        needs_clarification=False,
        normalized_title="Implement request",
    )


def test_definitions_are_valid_and_stable() -> None:
    assert [item.stable_id for item in WORKFLOW_DEFINITIONS] == [
        "simple_model.v1",
        "coding.v1",
        "research.v1",
        "infrastructure.v1",
    ]
    for definition in WORKFLOW_DEFINITIONS:
        validate_definition(definition)


def test_compiler_resolves_optional_predecessors() -> None:
    interpretation_id = uuid.uuid4()
    without_optional = compile_workflow(draft(), interpretation_id=interpretation_id)
    with_optional = compile_workflow(
        draft(planning=True, risk=RiskLevel.HIGH), interpretation_id=interpretation_id
    )

    assert without_optional.steps[0].status is StepStatus.COMPLETED
    assert [step.key for step in without_optional.steps] == [
        "interpret",
        "prepare_context",
        "prepare_worktree",
        "execute_code",
        "validate",
        "await_apply_or_complete",
        "finalize",
    ]
    assert without_optional.steps[1].predecessor_ordinals == (0,)
    assert "plan" in [step.key for step in with_optional.steps]
    assert "review" in [step.key for step in with_optional.steps]


def test_compiler_rejects_unknown_or_incompatible_workflow() -> None:
    with pytest.raises(WorkflowDefinitionError, match="incompatible"):
        compile_workflow(
            draft(TaskType.RESEARCH),
            interpretation_id=uuid.uuid4(),
            configured_workflow="coding.v1",
        )


def test_definition_validation_rejects_duplicate_and_missing_edges() -> None:
    valid = WORKFLOW_DEFINITIONS[0]
    duplicate = WorkflowDefinition(
        workflow_type="bad",
        version="1",
        task_types=frozenset({"general"}),
        steps=(valid.steps[0], valid.steps[0]),
    )
    with pytest.raises(WorkflowDefinitionError, match="duplicate"):
        validate_definition(duplicate)
    missing = WorkflowDefinition(
        workflow_type="bad",
        version="1",
        task_types=frozenset({"general"}),
        steps=(valid.steps[1],),
    )
    with pytest.raises(WorkflowDefinitionError, match="predecessor"):
        validate_definition(missing)


def test_step_outcome_and_workflow_timing_contracts() -> None:
    assert StepOutcome.succeeded({"ok": True}).result == {"ok": True}
    assert WorkflowSettings().heartbeat_seconds == 15
    with pytest.raises(ValidationError, match="one third"):
        WorkflowSettings(lease_seconds=45, heartbeat_seconds=15)
    with pytest.raises(ValidationError, match="minimum"):
        WorkflowSettings(retry_min_seconds=5, retry_max_seconds=1)


def test_definition_validation_rejects_unsafe_retry_and_limits() -> None:
    valid = WORKFLOW_DEFINITIONS[0]
    unsafe = replace(
        valid.steps[1],
        max_attempts=2,
        idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
    )
    with pytest.raises(WorkflowDefinitionError, match="unsafe"):
        validate_definition(replace(valid, steps=(valid.steps[0], unsafe)))
    invalid_limits = replace(valid.steps[0], timeout_seconds=0)
    with pytest.raises(WorkflowDefinitionError, match="limits"):
        validate_definition(replace(valid, steps=(invalid_limits,)))


def test_task_status_derivation_covers_run_and_step_categories() -> None:
    def step(step_type: str, status: StepStatus = StepStatus.QUEUED) -> Step:
        return Step(
            run_id=uuid.uuid4(),
            ordinal=1,
            step_type=step_type,
            status=status,
            queue_class=QueueClass.LIGHT,
            required_capabilities=[],
            payload={},
            retry_class=RetryClass.NEVER,
            idempotency_class=IdempotencyClass.READ_ONLY,
            max_attempts=1,
            timeout_seconds=60,
        )

    assert derive_task_status((), RunStatus.CREATED) is TaskStatus.INTERPRETED
    assert derive_task_status((), RunStatus.PAUSED) is TaskStatus.PAUSED
    assert derive_task_status((), RunStatus.BLOCKED) is TaskStatus.BLOCKED
    assert derive_task_status((), RunStatus.FAILED) is TaskStatus.FAILED
    assert derive_task_status((), RunStatus.CANCELLED) is TaskStatus.CANCELLED
    assert derive_task_status((), RunStatus.COMPLETED) is TaskStatus.COMPLETED
    assert (
        derive_task_status((step("approval", StepStatus.WAITING_APPROVAL),), RunStatus.RUNNING)
        is TaskStatus.WAITING_APPROVAL
    )
    assert (
        derive_task_status((step("question", StepStatus.AWAITING_USER),), RunStatus.RUNNING)
        is TaskStatus.AWAITING_USER
    )
    assert derive_task_status((step("validate"),), RunStatus.RUNNING) is TaskStatus.VALIDATING
    assert derive_task_status((step("review"),), RunStatus.RUNNING) is TaskStatus.REVIEWING
    assert (
        derive_task_status((step("prepare_context"),), RunStatus.RUNNING)
        is TaskStatus.CONTEXT_PREPARED
    )


def test_cancellation_context_notifies_waiters() -> None:
    async def scenario() -> None:
        context = CancellationContext()
        waiter = asyncio.create_task(context.wait())
        context.request()
        await waiter
        assert context.requested

    asyncio.run(scenario())


def test_illegal_transition_is_rejected_before_mutation() -> None:
    with pytest.raises(IllegalTransition, match="completed -> running"):
        workflow_transitions._check(
            RunStatus.COMPLETED,
            RunStatus.RUNNING,
            workflow_transitions.RUN_TRANSITIONS,
        )


def test_coding_workflow_and_heavy_queue():
    """Test coding workflow (Step 08) has correct heavy queue and unknown effects (real behavior)."""
    from vuzol.workflows.definitions import WORKFLOW_DEFINITIONS
    from vuzol.storage.types import QueueClass, IdempotencyClass

    coding = next(d for d in WORKFLOW_DEFINITIONS if d.workflow_type == "coding")
    exec_step = next(s for s in coding.steps if s.key == "execute_code")
    assert exec_step.queue_class == QueueClass.HEAVY
    assert exec_step.idempotency_class == IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE
    # This exercises the definition validation and step properties.
