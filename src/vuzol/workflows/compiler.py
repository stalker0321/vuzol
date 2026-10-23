"""Factory for the fixed executor chain; stored workflows are never recompiled."""

import uuid

from vuzol.config import Capability
from vuzol.interpretation.domain import TaskDraft, TaskOperation, TaskType
from vuzol.storage.types import IdempotencyClass, QueueClass, RetryClass, StepStatus
from vuzol.workflows.definitions import _step
from vuzol.workflows.domain import MaterializedStep, MaterializedWorkflow

EXECUTION_CONTRACT_VERSION = "execution-contract.v2"


def compile_workflow(draft: TaskDraft, *, interpretation_id: uuid.UUID) -> MaterializedWorkflow:
    report = draft.task_type in {TaskType.ARCHITECTURE, TaskType.RESEARCH, TaskType.GENERAL} or (
        draft.operation in {TaskOperation.INSPECT, TaskOperation.EXPLAIN, TaskOperation.MONITOR}
    )
    capabilities = {Capability.REPOSITORY_READ} if report else set()
    if report:
        if Capability.WEB_RESEARCH in draft.required_capabilities or (
            draft.task_type is TaskType.RESEARCH and not draft.required_capabilities
        ):
            capabilities.add(Capability.WEB_RESEARCH)
    else:
        capabilities.update({Capability.CODE_EDIT, Capability.PROJECT_SHELL})
    # Preflights are technical checks, never planning. Delivery remains behind
    # the existing immutable result approval and its specialised consumers.
    definitions = [
        _step("interpret"),
        _step(
            "ensure_capabilities",
            queue=QueueClass.PRIVILEGED,
            capabilities=frozenset({Capability.HOST_ADMIN}),
            idempotency=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
            timeout=1_800,
        ),
        _step("prepare_context", capabilities=frozenset({Capability.REPOSITORY_READ})),
        _step(
            "prepare_worktree",
            queue=QueueClass.HEAVY,
            capabilities=frozenset({Capability.GIT, Capability.FILESYSTEM_WRITE}),
            idempotency=IdempotencyClass.ISOLATED_RETRYABLE,
        ),
        _step(
            "execute_agent" if report else "execute_code",
            queue=QueueClass.HEAVY,
            capabilities=frozenset(capabilities),
            retry=RetryClass.POLICY,
            idempotency=IdempotencyClass.READ_ONLY
            if report
            else IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
            timeout=3_600,
        ),
        _step(
            "ensure_dependencies",
            queue=QueueClass.HEAVY,
            capabilities=frozenset({Capability.PROJECT_SHELL, Capability.FILESYSTEM_WRITE}),
            idempotency=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
            timeout=1_800,
        ),
        _step(
            "validate",
            queue=QueueClass.HEAVY,
            capabilities=frozenset(
                {Capability.REPOSITORY_READ} if report else {Capability.PROJECT_SHELL}
            ),
        ),
        _step("review", retry=RetryClass.TRANSIENT, attempts=3),
    ]
    if not report:
        definitions.extend(
            [
                _step(
                    "produce_artifacts",
                    queue=QueueClass.HEAVY,
                    capabilities=frozenset({Capability.PROJECT_SHELL}),
                    idempotency=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                ),
                _step(
                    "build_static",
                    queue=QueueClass.HEAVY,
                    capabilities=frozenset({Capability.PROJECT_SHELL}),
                    retry=RetryClass.TRANSIENT,
                    idempotency=IdempotencyClass.ISOLATED_RETRYABLE,
                    attempts=2,
                ),
            ]
        )
    definitions.append(
        _step(
            "approve_result",
            step_type="approval",
            queue=QueueClass.PRIVILEGED,
            capabilities=frozenset({Capability.GIT}),
            idempotency=IdempotencyClass.IDEMPOTENT,
            timeout=120,
            attempts=2,
        )
    )
    if not report:
        definitions.append(
            _step(
                "publish_static",
                capabilities=frozenset({Capability.FILESYSTEM_WRITE}),
                retry=RetryClass.TRANSIENT,
                idempotency=IdempotencyClass.IDEMPOTENT,
                timeout=120,
                attempts=3,
            )
        )
    definitions.append(_step("finalize", queue=QueueClass.CONTROL))
    steps = tuple(
        MaterializedStep(
            ordinal=index,
            key=step.key,
            step_type=step.step_type,
            predecessor_ordinals=(index - 1,) if index else (),
            queue_class=step.queue_class,
            capabilities=step.capabilities,
            retry_class=step.retry_class,
            idempotency_class=step.idempotency_class,
            timeout_seconds=step.timeout_seconds,
            max_attempts=step.max_attempts,
            priority=step.priority,
            status=StepStatus.COMPLETED if index == 0 else StepStatus.PENDING,
            payload={
                "result_kind": "report" if report else "change",
                **({"interpretation_id": str(interpretation_id)} if index == 0 else {}),
                **({"requested_action": "apply_result"} if step.step_type == "approval" else {}),
            },
        )
        for index, step in enumerate(definitions)
    )
    return MaterializedWorkflow("fixed_executor", "1", interpretation_id, steps)
