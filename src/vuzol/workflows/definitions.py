"""Closed versioned workflow registry for the MVP."""

from vuzol.config import Capability
from vuzol.storage.types import IdempotencyClass, QueueClass, RetryClass
from vuzol.workflows.domain import (
    StepDefinition,
    WorkflowDefinition,
    WorkflowDefinitionError,
)


def _step(
    key: str,
    *predecessors: str,
    queue: QueueClass = QueueClass.LIGHT,
    capabilities: frozenset[Capability] = frozenset(),
    retry: RetryClass = RetryClass.NEVER,
    idempotency: IdempotencyClass = IdempotencyClass.READ_ONLY,
    timeout: int = 600,
    attempts: int = 1,
    optional: str | None = None,
    internal: bool = False,
    step_type: str | None = None,
) -> StepDefinition:
    return StepDefinition(
        key=key,
        step_type=step_type or key,
        predecessors=predecessors,
        queue_class=queue,
        capabilities=capabilities,
        retry_class=retry,
        idempotency_class=idempotency,
        timeout_seconds=timeout,
        max_attempts=attempts,
        optional_flag=optional,
        internal=internal,
    )


INTERPRET = _step("interpret", internal=True)
FINALIZE = _step("finalize", queue=QueueClass.CONTROL, internal=True)

WORKFLOW_DEFINITIONS: tuple[WorkflowDefinition, ...] = (
    WorkflowDefinition(
        workflow_type="simple_model",
        version="1",
        task_types=frozenset({"general", "file_processing"}),
        steps=(
            INTERPRET,
            _step("execute_model", "interpret", retry=RetryClass.TRANSIENT, attempts=3),
            _step("format_result", "execute_model"),
            _step("finalize", "format_result", queue=QueueClass.CONTROL, internal=True),
        ),
    ),
    WorkflowDefinition(
        workflow_type="coding",
        version="1",
        task_types=frozenset({"coding"}),
        steps=(
            INTERPRET,
            _step(
                "plan",
                "interpret",
                retry=RetryClass.TRANSIENT,
                attempts=3,
                optional="needs_planning",
            ),
            _step("prepare_context", "plan", capabilities=frozenset({Capability.REPOSITORY_READ})),
            _step(
                "prepare_worktree",
                "prepare_context",
                queue=QueueClass.HEAVY,
                capabilities=frozenset({Capability.GIT, Capability.FILESYSTEM_WRITE}),
                idempotency=IdempotencyClass.ISOLATED_RETRYABLE,
            ),
            _step(
                "execute_code",
                "prepare_worktree",
                queue=QueueClass.HEAVY,
                capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
                retry=RetryClass.POLICY,
                idempotency=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                timeout=3_600,
            ),
            _step(
                "validate",
                "execute_code",
                queue=QueueClass.HEAVY,
                capabilities=frozenset({Capability.PROJECT_SHELL}),
            ),
            _step(
                "review",
                "validate",
                optional="needs_review",
                # Mechanical/read-only; allow requeue after worker crash mid-commit.
                retry=RetryClass.TRANSIENT,
                attempts=3,
            ),
            _step(
                "approve_result",
                "review",
                step_type="approval",
                queue=QueueClass.PRIVILEGED,
                capabilities=frozenset({Capability.GIT}),
                idempotency=IdempotencyClass.IDEMPOTENT,
                timeout=120,
                attempts=2,
            ),
            _step("finalize", "approve_result", queue=QueueClass.CONTROL, internal=True),
        ),
    ),
    WorkflowDefinition(
        workflow_type="architecture",
        version="1",
        task_types=frozenset({"architecture"}),
        steps=(
            INTERPRET,
            _step(
                "plan",
                "interpret",
                retry=RetryClass.TRANSIENT,
                attempts=3,
                optional="needs_planning",
            ),
            _step("prepare_context", "plan", capabilities=frozenset({Capability.REPOSITORY_READ})),
            _step(
                "prepare_worktree",
                "prepare_context",
                queue=QueueClass.HEAVY,
                capabilities=frozenset({Capability.GIT, Capability.FILESYSTEM_WRITE}),
                idempotency=IdempotencyClass.ISOLATED_RETRYABLE,
            ),
            _step(
                "execute_agent",
                "prepare_worktree",
                queue=QueueClass.HEAVY,
                capabilities=frozenset({Capability.REPOSITORY_READ}),
                retry=RetryClass.TRANSIENT,
                attempts=3,
                timeout=3_600,
            ),
            _step("format_result", "execute_agent"),
            _step("finalize", "format_result", queue=QueueClass.CONTROL, internal=True),
        ),
    ),
    WorkflowDefinition(
        workflow_type="research",
        version="1",
        task_types=frozenset({"research"}),
        steps=(
            INTERPRET,
            _step(
                "research_execute",
                "interpret",
                capabilities=frozenset({Capability.WEB_RESEARCH}),
                retry=RetryClass.TRANSIENT,
                attempts=3,
            ),
            _step(
                "synthesize",
                "research_execute",
                retry=RetryClass.TRANSIENT,
                attempts=3,
            ),
            _step("finalize", "synthesize", queue=QueueClass.CONTROL, internal=True),
        ),
    ),
    WorkflowDefinition(
        workflow_type="infrastructure",
        version="1",
        task_types=frozenset({"infrastructure"}),
        steps=(
            INTERPRET,
            _step(
                "inspect",
                "interpret",
                queue=QueueClass.PRIVILEGED,
                capabilities=frozenset({Capability.HOST_ADMIN}),
            ),
            _step("plan", "inspect", retry=RetryClass.TRANSIENT, attempts=3),
            _step("approval", "plan", queue=QueueClass.CONTROL, internal=True),
            _step(
                "privileged_execute",
                "approval",
                queue=QueueClass.PRIVILEGED,
                capabilities=frozenset({Capability.HOST_ADMIN}),
                idempotency=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
            ),
            _step(
                "validate",
                "privileged_execute",
                queue=QueueClass.PRIVILEGED,
                capabilities=frozenset({Capability.HOST_ADMIN}),
            ),
            _step("complete_or_block", "validate", queue=QueueClass.CONTROL, internal=True),
            _step("finalize", "complete_or_block", queue=QueueClass.CONTROL, internal=True),
        ),
    ),
)


def validate_definition(definition: WorkflowDefinition) -> None:
    keys = [step.key for step in definition.steps]
    if len(keys) != len(set(keys)):
        raise WorkflowDefinitionError(f"duplicate step key in {definition.stable_id}")
    known: set[str] = set()
    for step in definition.steps:
        if step.timeout_seconds <= 0 or step.max_attempts <= 0:
            raise WorkflowDefinitionError(f"invalid limits for {step.key}")
        missing = set(step.predecessors) - known
        if missing:
            raise WorkflowDefinitionError(
                f"unknown or cyclic predecessor for {step.key}: {missing}"
            )
        if step.queue_class is QueueClass.PRIVILEGED and not (
            Capability.HOST_ADMIN in step.capabilities or Capability.GIT in step.capabilities
        ):
            raise WorkflowDefinitionError(
                f"privileged step lacks host_admin or git capability: {step.key}"
            )
        if step.max_attempts > 1 and step.idempotency_class in {
            IdempotencyClass.NON_IDEMPOTENT,
            IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
        }:
            raise WorkflowDefinitionError(f"unsafe automatic retries for {step.key}")
        known.add(step.key)


for _definition in WORKFLOW_DEFINITIONS:
    validate_definition(_definition)

WORKFLOW_REGISTRY = {definition.stable_id: definition for definition in WORKFLOW_DEFINITIONS}
