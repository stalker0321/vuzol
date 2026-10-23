import asyncio
import uuid
from dataclasses import replace

import pytest
from pydantic import ValidationError

from vuzol.config import Capability, WorkflowSettings
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
        task_summary="Implement the requested change",
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
        "architecture.v1",
        "research.v1",
        "infrastructure.v1",
        "coding.v2",
        "coding.v3",
        "coding.v4",
    ]
    for definition in WORKFLOW_DEFINITIONS:
        validate_definition(definition)


def test_compiler_skips_disabled_planner() -> None:
    interpretation_id = uuid.uuid4()
    without_optional = compile_workflow(draft(), interpretation_id=interpretation_id)
    with_optional = compile_workflow(
        draft(planning=True, risk=RiskLevel.HIGH), interpretation_id=interpretation_id
    )
    assert without_optional.steps[0].status is StepStatus.COMPLETED
    assert [step.key for step in without_optional.steps] == [
        "interpret",
        "ensure_capabilities",
        "prepare_context",
        "prepare_worktree",
        "execute_code",
        "ensure_dependencies",
        "validate",
        "review",
        "produce_artifacts",
        "build_static",
        "approve_result",
        "publish_static",
        "finalize",
    ]
    approve = next(step for step in without_optional.steps if step.key == "approve_result")
    assert approve.step_type == "approval"
    assert approve.payload == {"requested_action": "apply_result", "result_kind": "change"}
    assert without_optional.steps[1].predecessor_ordinals == (0,)
    # Legacy planning hints cannot change the fixed executor chain.
    assert "plan" not in [step.key for step in without_optional.steps]
    assert without_optional.steps == with_optional.steps
    assert "review" in [step.key for step in with_optional.steps]


def test_factory_has_no_template_selection() -> None:
    import inspect

    assert set(inspect.signature(compile_workflow).parameters) == {"draft", "interpretation_id"}


@pytest.mark.parametrize("task_type", [TaskType.ARCHITECTURE, TaskType.RESEARCH])
def test_read_only_tasks_use_the_same_executor_stages(task_type: TaskType) -> None:
    workflow = compile_workflow(draft(task_type, planning=True), interpretation_id=uuid.uuid4())
    assert workflow.workflow_type == "fixed_executor"
    assert [s.step_type for s in workflow.steps] == [
        "interpret",
        "ensure_capabilities",
        "prepare_context",
        "prepare_worktree",
        "execute_agent",
        "ensure_dependencies",
        "validate",
        "review",
        "approval",
        "finalize",
    ]
    agent = next(s for s in workflow.steps if s.step_type == "execute_agent")
    assert Capability.REPOSITORY_READ in agent.capabilities
    assert (Capability.WEB_RESEARCH in agent.capabilities) == (task_type is TaskType.RESEARCH)
    assert agent.idempotency_class is IdempotencyClass.READ_ONLY
    assert agent.payload == {"result_kind": "report"}


@pytest.mark.parametrize("web", [False, True])
def test_research_capabilities_follow_the_task_contract(web: bool) -> None:
    capabilities = {Capability.REPOSITORY_READ}
    if web:
        capabilities.add(Capability.WEB_RESEARCH)
    task = draft(TaskType.RESEARCH).model_copy(
        update={"required_capabilities": frozenset(capabilities)}
    )
    workflow = compile_workflow(task, interpretation_id=uuid.uuid4())
    agent = next(s for s in workflow.steps if s.step_type == "execute_agent")
    assert agent.capabilities == capabilities


@pytest.mark.parametrize("complexity", list(SuggestedComplexity))
def test_size_does_not_change_stages(complexity: SuggestedComplexity) -> None:
    identifier = uuid.uuid4()
    expected = compile_workflow(draft(), interpretation_id=identifier)
    actual = compile_workflow(
        draft().model_copy(update={"suggested_complexity": complexity}),
        interpretation_id=identifier,
    )
    assert actual == expected


@pytest.mark.parametrize("version", ["1", "2", "3", "4"])
def test_legacy_definitions_remain_available_for_stored_runs(version: str) -> None:
    from vuzol.workflows.definitions import WORKFLOW_REGISTRY

    definition = WORKFLOW_REGISTRY[f"coding.v{version}"]
    assert definition.version == version
    assert any(s.key == "plan" for s in definition.steps)


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
    capability_preflight = derive_task_status(
        (
            step("ensure_capabilities"),
            step("prepare_context", StepStatus.PENDING),
        ),
        RunStatus.RUNNING,
    )
    prepared_context = derive_task_status(
        (
            step("ensure_capabilities", StepStatus.COMPLETED),
            step("prepare_context"),
        ),
        RunStatus.RUNNING,
    )
    assert capability_preflight is prepared_context is TaskStatus.CONTEXT_PREPARED


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


def test_planned_task_can_advance_to_prepared_context() -> None:
    workflow_transitions._check(
        TaskStatus.PLANNED,
        TaskStatus.CONTEXT_PREPARED,
        workflow_transitions.TASK_TRANSITIONS,
    )


def test_prepared_context_can_wait_for_stack_approval() -> None:
    workflow_transitions._check(
        TaskStatus.CONTEXT_PREPARED,
        TaskStatus.WAITING_APPROVAL,
        workflow_transitions.TASK_TRANSITIONS,
    )


def test_retrying_task_can_wait_for_stack_approval() -> None:
    workflow_transitions._check(
        TaskStatus.RETRYING,
        TaskStatus.WAITING_APPROVAL,
        workflow_transitions.TASK_TRANSITIONS,
    )


def test_executing_task_can_return_to_prepared_context_after_preflight() -> None:
    workflow_transitions._check(
        TaskStatus.EXECUTING,
        TaskStatus.CONTEXT_PREPARED,
        workflow_transitions.TASK_TRANSITIONS,
    )


@pytest.mark.parametrize("target", [TaskStatus.PLANNED, TaskStatus.CONTEXT_PREPARED])
def test_retrying_task_can_restore_early_workflow_status(target: TaskStatus) -> None:
    workflow_transitions._check(
        TaskStatus.RETRYING,
        target,
        workflow_transitions.TASK_TRANSITIONS,
    )


def test_coding_workflow_and_heavy_queue() -> None:
    """Test the coding workflow queue and effects policy."""
    from vuzol.storage.types import IdempotencyClass, QueueClass
    from vuzol.workflows.definitions import WORKFLOW_DEFINITIONS

    coding = next(d for d in WORKFLOW_DEFINITIONS if d.workflow_type == "coding")
    exec_step = next(s for s in coding.steps if s.key == "execute_code")
    assert exec_step.queue_class == QueueClass.HEAVY
    assert exec_step.idempotency_class == IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE
    # This exercises the definition validation and step properties.


@pytest.mark.parametrize(
    "field,value",
    [
        ("summary", " "),
        ("findings", []),
        ("sources", []),
        ("sources", [" "]),
        ("limitations", ["x" * 4_001]),
    ],
)
def test_report_contract_rejects_missing_or_unbounded_evidence(field: str, value: object) -> None:
    from vuzol.research.report import TaskReport

    report = {
        "summary": "Repository inspected",
        "findings": ["Read README"],
        "sources": ["README.md:1"],
        "limitations": [],
    }
    report[field] = value
    with pytest.raises(ValidationError):
        TaskReport.model_validate(report)


def test_lead_accept_result_preserves_report_hash() -> None:
    from vuzol.leadership.provider_model import parse_lead_response

    decision = parse_lead_response(
        {
            "commands": [
                {
                    "name": "accept_result",
                    "payload": {
                        "task_id": str(uuid.uuid4()),
                        "scope_revision": 1,
                        "plan_revision_id": str(uuid.uuid4()),
                        "spec_revision": "a" * 64,
                        "result_commit": "b" * 40,
                        "report_hash": "c" * 64,
                    },
                }
            ]
        }
    )
    assert decision.commands[0].payload["report_hash"] == "c" * 64
