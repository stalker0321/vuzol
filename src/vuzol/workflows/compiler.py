"""Deterministic TaskDraft to materialized-workflow compiler."""

import uuid

from vuzol.interpretation.domain import TaskDraft, TaskType
from vuzol.storage.types import RiskLevel, StepStatus
from vuzol.workflows.definitions import WORKFLOW_REGISTRY
from vuzol.workflows.domain import MaterializedStep, MaterializedWorkflow, WorkflowDefinitionError

TASK_WORKFLOWS = {
    TaskType.CODING: "coding.v1",
    TaskType.ARCHITECTURE: "architecture.v1",
    TaskType.RESEARCH: "research.v1",
    TaskType.INFRASTRUCTURE: "infrastructure.v1",
    TaskType.FILE_PROCESSING: "simple_model.v1",
    TaskType.GENERAL: "simple_model.v1",
}


def compile_workflow(
    draft: TaskDraft,
    *,
    interpretation_id: uuid.UUID,
    configured_workflow: str | None = None,
) -> MaterializedWorkflow:
    stable_id = configured_workflow or TASK_WORKFLOWS[draft.task_type]
    definition = WORKFLOW_REGISTRY.get(stable_id)
    if definition is None or draft.task_type.value not in definition.task_types:
        raise WorkflowDefinitionError(f"incompatible workflow: {stable_id}")

    flags = {
        "needs_planning": draft.needs_planning,
        "needs_review": draft.suggested_risk
        in {RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.PRIVILEGED},
    }
    included = [
        step for step in definition.steps if step.optional_flag is None or flags[step.optional_flag]
    ]
    ordinals = {step.key: index for index, step in enumerate(included)}
    original = {step.key: step for step in definition.steps}

    def resolved_predecessors(key: str) -> tuple[int, ...]:
        resolved: set[int] = set()
        pending = list(original[key].predecessors)
        while pending:
            predecessor = pending.pop()
            if predecessor in ordinals:
                resolved.add(ordinals[predecessor])
            else:
                pending.extend(original[predecessor].predecessors)
        return tuple(sorted(resolved))

    def payload_for(step_key: str, step_type: str) -> dict[str, str]:
        if step_key == "interpret":
            return {"interpretation_id": str(interpretation_id)}
        if step_type == "approval" or step_key == "approve_result":
            return {"requested_action": "apply_result"}
        return {}

    steps = tuple(
        MaterializedStep(
            ordinal=index,
            key=step.key,
            step_type=step.step_type,
            predecessor_ordinals=resolved_predecessors(step.key),
            queue_class=step.queue_class,
            capabilities=step.capabilities,
            retry_class=step.retry_class,
            idempotency_class=step.idempotency_class,
            timeout_seconds=step.timeout_seconds,
            max_attempts=step.max_attempts,
            priority=step.priority,
            status=StepStatus.COMPLETED if step.key == "interpret" else StepStatus.PENDING,
            payload=payload_for(step.key, step.step_type),
        )
        for index, step in enumerate(included)
    )
    return MaterializedWorkflow(
        workflow_type=definition.workflow_type,
        version=definition.version,
        interpretation_id=interpretation_id,
        steps=steps,
    )
