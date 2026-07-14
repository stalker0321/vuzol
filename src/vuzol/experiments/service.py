"""Durable admin trigger for production-path Step 09A trial executions."""

import hashlib
import uuid
from dataclasses import dataclass

from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import Capability
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.git import LocalGit
from vuzol.execution.paths import worktree_branch
from vuzol.experiments.domain import (
    ContextManifest,
    ExecutionMode,
    FrozenModel,
    RequiredGate,
    TaskClassification,
    WorkerTaskCapsule,
)
from vuzol.experiments.policy import classify_execution_mode, enforce_security_escalation
from vuzol.interpretation.domain import (
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.models import Interpretation, Task
from vuzol.storage.types import (
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RiskLevel,
    StepStatus,
    TaskStatus,
)
from vuzol.workflows.domain import MaterializedStep, MaterializedWorkflow
from vuzol.workflows.service import materialize_run

TRIAL_POLICY_REVISION = hashlib.sha256(b"step09a-bounded-trial-policy-v1").hexdigest()


class TrialSeedRequest(FrozenModel):
    experiment_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    task_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    worker_profile: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    project_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    base_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    goal: str = Field(min_length=1, max_length=4_000)
    classification: TaskClassification
    actual_mode: ExecutionMode | None = None
    override_reason: str | None = Field(default=None, max_length=1_000)
    allowed_paths: tuple[str, ...] = Field(min_length=1, max_length=100)
    relevant_symbols: tuple[str, ...] = Field(default=(), max_length=100)
    acceptance_criteria: tuple[str, ...] = Field(min_length=1, max_length=100)
    forbidden_changes: tuple[str, ...] = Field(default=(), max_length=100)
    required_gates: tuple[RequiredGate, ...] = Field(min_length=1, max_length=30)
    maximum_execution_seconds: int = Field(default=1_800, ge=1, le=3_600)
    maximum_repair_count: int = Field(default=2, ge=0, le=2)
    context_manifest: ContextManifest
    attempt: int = Field(default=1, ge=1, le=3)


@dataclass(frozen=True, slots=True)
class SeededTrial:
    task_uuid: uuid.UUID
    run_uuid: uuid.UUID
    interpretation_uuid: uuid.UUID
    capsule: WorkerTaskCapsule


async def seed_trial(
    session: AsyncSession,
    registries: ConfigurationBundle,
    request: TrialSeedRequest,
) -> SeededTrial:
    """Create one bounded two-step coding workflow in the existing repositories."""
    project = registries.projects.get(request.project_id)
    profile = registries.profiles.get(request.worker_profile)
    if not project.enabled or not profile.enabled:
        raise ValueError("trial project and worker profile must be enabled")
    if "coding" not in profile.supported_task_types:
        raise ValueError("trial worker profile does not support coding")
    predicted = classify_execution_mode(request.classification)
    desired = request.actual_mode or predicted
    actual = enforce_security_escalation(request.classification, desired)
    override = request.override_reason
    if actual != predicted and not override:
        raise ValueError("trial mode override requires an explicit reason")
    if actual != desired:
        override = override or "security policy escalated the requested execution mode"
    if actual is ExecutionMode.SOL_SOLO and profile.provider != "codex":
        raise ValueError("SOL_SOLO requires the isolated Sol/Codex worker profile")
    if actual is not ExecutionMode.SOL_SOLO and profile.provider != "grok":
        raise ValueError("Grok execution modes require an isolated Grok worker profile")
    git = LocalGit()
    await git.require_clean_source(project.repository_path)
    actual_base = await git.resolve_commit(project.repository_path, project.default_branch)
    if actual_base != request.base_commit:
        raise ValueError("trial base does not match the configured project revision")

    task_uuid = uuid.uuid4()
    interpretation_uuid = uuid.uuid4()
    draft = _draft(request)
    task = Task(
        id=task_uuid,
        user_id=0,
        source_chat_id=0,
        project_id=request.project_id,
        original_text="pending bounded worker capsule",
        task_draft=draft.model_dump(mode="json"),
        draft_schema_version="1.0",
        interpreter_profile="step09a-admin-trigger",
        prompt_version="step09a-worker-v2",
        status=TaskStatus.INTERPRETED,
        risk=RiskLevel(request.classification.risk.value),
        task_type="coding",
    )
    session.add(task)
    await session.flush()
    interpretation = Interpretation(
        id=interpretation_uuid,
        task_id=task_uuid,
        original_input_hash=hashlib.sha256(request.goal.encode()).hexdigest(),
        task_draft=draft.model_dump(mode="json"),
        profile_id="step09a-admin-trigger",
        model="deterministic",
        prompt_version="step09a-worker-v2",
        schema_version="1.0",
    )
    session.add(interpretation)
    await session.flush()
    workflow = _trial_workflow(interpretation_uuid, request.maximum_execution_seconds)
    run = await materialize_run(
        session,
        task_id=task_uuid,
        workflow=workflow,
        configuration_revision=registries.revision,
        policy_revision=TRIAL_POLICY_REVISION,
        prompt_revision="step09a-worker-v2",
        automatic_start=True,
        budget_mode="strong",
    )
    capsule = WorkerTaskCapsule(
        experiment_id=request.experiment_id,
        task_id=request.task_id,
        worker_profile=request.worker_profile,
        base_commit=request.base_commit,
        target_branch=worktree_branch(task_uuid, run.id),
        goal=request.goal,
        classification=request.classification,
        predicted_mode=predicted,
        actual_mode=actual,
        override_reason=override,
        allowed_paths=request.allowed_paths,
        relevant_symbols=request.relevant_symbols,
        acceptance_criteria=request.acceptance_criteria,
        forbidden_changes=request.forbidden_changes,
        required_gates=request.required_gates,
        maximum_execution_seconds=request.maximum_execution_seconds,
        maximum_repair_count=request.maximum_repair_count,
        context_manifest=request.context_manifest,
        parent_attempt=request.attempt - 1 or None,
    )
    task.original_text = render_worker_prompt(capsule, repository_id=request.project_id)
    task.task_draft = {
        **draft.model_dump(mode="json"),
        "step09a_capsule": capsule.model_dump(mode="json"),
    }
    run.selected_route = {
        "schema_version": "step09a-route.v1",
        "experiment_id": request.experiment_id,
        "experiment_task_id": request.task_id,
        "trusted_profile_id": request.worker_profile,
        "execution_mode": actual.value,
    }
    await session.flush()
    return SeededTrial(
        task_uuid=task_uuid,
        run_uuid=run.id,
        interpretation_uuid=interpretation_uuid,
        capsule=capsule,
    )


def render_worker_prompt(capsule: WorkerTaskCapsule, *, repository_id: str) -> str:
    return (
        "You are a bounded implementation worker, not the technical lead or reviewer.\n"
        f"Repository logical identity: {repository_id}. Sandbox worktree: /workspace.\n"
        f"Expected branch: {capsule.target_branch}. Exact base SHA: {capsule.base_commit}.\n"
        "Vuzol has already prepared and verified the isolated worktree. Do not invoke Git, "
        "shell commands, or required gates. Use only repository read/search/edit tools and do "
        "not touch another VPS project.\n"
        "The complete immutable task capsule follows as JSON:\n"
        f"{capsule.model_dump_json()}\n"
        "Only change allowed paths. Do not modify forbidden files. Do not relax, skip, fake, or "
        "swallow tests; do not use forced-success assertions. Vuzol will inspect the real diff, "
        "enforce scope, run trusted gates, stage exact paths, create the commit, and construct the "
        "authoritative result manifest after you exit.\n"
        "Your final response must be only one JSON object conforming to "
        f"{capsule.expected_edit_report_version}; include only the experiment ID, task ID, "
        "attempt, claimed completion, limitations, failure classification, and provider usage "
        "only when reliably exposed. Do not claim changed files, gate results, branch identity, "
        "or a result commit."
    )


def _draft(request: TrialSeedRequest) -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.CODING,
        operation=TaskOperation.MODIFY,
        project_id=request.project_id,
        goal=request.goal,
        requested_outcomes=request.acceptance_criteria,
        constraints=request.forbidden_changes,
        required_capabilities=frozenset(
            {
                Capability.REPOSITORY_READ,
                Capability.CODE_EDIT,
                Capability.GIT,
                Capability.PROJECT_SHELL,
            }
        ),
        suggested_complexity={
            "low": SuggestedComplexity.SMALL,
            "medium": SuggestedComplexity.MEDIUM,
            "high": SuggestedComplexity.LARGE,
        }[request.classification.complexity.value],
        suggested_risk=RiskLevel(request.classification.risk.value),
        needs_planning=False,
        needs_clarification=False,
        normalized_title=request.task_id.replace("_", " ")[:120],
    )


def _trial_workflow(interpretation_id: uuid.UUID, timeout: int) -> MaterializedWorkflow:
    return MaterializedWorkflow(
        workflow_type="adaptive_worker_trial",
        version="1",
        interpretation_id=interpretation_id,
        steps=(
            MaterializedStep(
                ordinal=0,
                key="interpret",
                step_type="interpret",
                predecessor_ordinals=(),
                queue_class=QueueClass.LIGHT,
                capabilities=frozenset(),
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.READ_ONLY,
                timeout_seconds=60,
                max_attempts=1,
                priority=100,
                status=StepStatus.COMPLETED,
            ),
            MaterializedStep(
                ordinal=1,
                key="prepare_worktree",
                step_type="prepare_worktree",
                predecessor_ordinals=(0,),
                queue_class=QueueClass.HEAVY,
                capabilities=frozenset({Capability.GIT, Capability.FILESYSTEM_WRITE}),
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                timeout_seconds=600,
                max_attempts=1,
                priority=100,
            ),
            MaterializedStep(
                ordinal=2,
                key="execute_code",
                step_type="execute_code",
                predecessor_ordinals=(1,),
                queue_class=QueueClass.HEAVY,
                capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                timeout_seconds=timeout,
                max_attempts=1,
                priority=100,
            ),
        ),
    )
