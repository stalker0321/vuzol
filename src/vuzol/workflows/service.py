"""Transactional workflow materialization, activation, and fenced outcome commits."""

import uuid
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.ops.disk_pressure import DISK_PRESSURE_CATEGORY
from vuzol.storage.errors import EntityNotFound, LeaseLost
from vuzol.storage.models import (
    Event,
    Run,
    Step,
    Task,
)
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
from vuzol.telegram.projections import (
    enqueue_task_status_projection,
    enqueue_terminal_task_projections,
)
from vuzol.telegram.tracing import enqueue_planner_trace
from vuzol.workflows.domain import MaterializedWorkflow, OutcomeKind, StepOutcome
from vuzol.workflows.transitions import transition_run, transition_step, transition_task


async def materialize_run(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    workflow: MaterializedWorkflow,
    configuration_revision: str,
    policy_revision: str,
    prompt_revision: str | None,
    automatic_start: bool,
    budget_mode: str = "balanced",
) -> Run:
    existing = await session.scalar(
        select(Run).where(Run.source_interpretation_id == workflow.interpretation_id)
    )
    if existing is not None:
        return existing
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise EntityNotFound(f"task not found: {task_id}")
    if task.status is not TaskStatus.INTERPRETED:
        raise ValueError(f"task is not ready for a run: {task.status.value}")

    run = Run(
        task_id=task.id,
        source_interpretation_id=workflow.interpretation_id,
        workflow_type=workflow.workflow_type,
        workflow_version=workflow.version,
        status=RunStatus.CREATED,
        selected_route={},
        budget_mode=budget_mode,
        configuration_revision=configuration_revision,
        policy_revision=policy_revision,
        prompt_revision=prompt_revision,
    )
    session.add(run)
    await session.flush()
    session.add(
        Event(
            entity_type="run",
            entity_id=run.id,
            event_type="run.created",
            actor_type="workflow_manager",
            new_state=RunStatus.CREATED.value,
            payload={
                "workflow_type": workflow.workflow_type,
                "workflow_version": workflow.version,
                "interpretation_id": str(workflow.interpretation_id),
            },
        )
    )
    for item in workflow.steps:
        step = Step(
            run_id=run.id,
            ordinal=item.ordinal,
            dependency_metadata={
                "template_key": item.key,
                "predecessor_ordinals": list(item.predecessor_ordinals),
            },
            step_type=item.step_type,
            queue_class=item.queue_class,
            status=item.status,
            required_capabilities=sorted(capability.value for capability in item.capabilities),
            payload=dict(item.payload or {}),
            result={"interpretation_id": str(workflow.interpretation_id)}
            if item.step_type == "interpret"
            else None,
            retry_class=item.retry_class,
            idempotency_class=item.idempotency_class,
            max_attempts=item.max_attempts,
            priority=item.priority,
            timeout_seconds=item.timeout_seconds,
        )
        session.add(step)
        await session.flush()
        session.add(
            Event(
                entity_type="step",
                entity_id=step.id,
                event_type="step.created",
                actor_type="workflow_manager",
                new_state=step.status.value,
                payload={"ordinal": step.ordinal, "step_type": step.step_type},
            )
        )
    task.version += 1
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.workflow_materialized",
            actor_type="workflow_manager",
            payload={"run_id": str(run.id), "workflow_type": workflow.workflow_type},
        )
    )
    if automatic_start:
        await start_run(session, run, task=task, actor_type="workflow_manager")
    await session.flush()
    return run


async def start_run(
    session: AsyncSession,
    run: Run,
    *,
    task: Task | None = None,
    actor_type: str,
    actor_id: str | None = None,
) -> None:
    if run.status is RunStatus.RUNNING:
        await _record_noop(session, run.id, "start", actor_type, actor_id)
        return
    if run.status is not RunStatus.CREATED:
        raise ValueError(f"run cannot start from {run.status.value}")
    if task is None:
        task = await session.scalar(select(Task).where(Task.id == run.task_id).with_for_update())
        assert task is not None
    await transition_run(session, run, RunStatus.RUNNING, actor_type=actor_type, actor_id=actor_id)
    run.started_at = func.now()
    await activate_ready_steps(session, run)
    target = derive_task_status(await _steps_for_run(session, run.id), run.status)
    if target is not task.status:
        await transition_task(session, task, target, actor_type=actor_type, actor_id=actor_id)


async def activate_ready_steps(session: AsyncSession, run: Run) -> tuple[Step, ...]:
    if run.status is not RunStatus.RUNNING:
        return ()
    steps = list(await _steps_for_run(session, run.id, for_update=True))
    by_ordinal = {step.ordinal: step for step in steps}
    activated: list[Step] = []
    for step in steps:
        if step.status is not StepStatus.PENDING:
            continue
        predecessors = _predecessors(step.dependency_metadata)
        if not all(by_ordinal[value].status is StepStatus.COMPLETED for value in predecessors):
            continue
        target = StepStatus.WAITING_APPROVAL if step.step_type == "approval" else StepStatus.QUEUED
        await transition_step(session, step, target, actor_type="workflow_manager")
        if target is StepStatus.WAITING_APPROVAL:
            from vuzol.workflows.result_approval import ensure_result_approval

            await ensure_result_approval(
                session,
                run=run,
                approval_step=step,
                steps_by_ordinal=by_ordinal,
            )
        activated.append(step)
    return tuple(activated)


async def commit_step_outcome(
    session: AsyncSession,
    token: LeaseToken,
    outcome: StepOutcome,
    *,
    retry_delay_seconds: float = 0,
) -> None:
    step = await session.scalar(select(Step).where(Step.id == token.step.id).with_for_update())
    if (
        step is None
        or step.lease_owner != token.owner
        or step.lease_generation != token.generation
        or step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
    ):
        raise LeaseLost(f"step lease lost: {token.step.id}")
    run = await session.scalar(select(Run).where(Run.id == step.run_id).with_for_update())
    assert run is not None
    if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.COMPLETED}:
        raise LeaseLost(f"parent run is terminal: {run.id}")
    if outcome.kind is OutcomeKind.SUCCEEDED:
        await transition_step(session, step, StepStatus.COMPLETED, actor_type="worker")
        step.result = outcome.result
    elif outcome.kind is OutcomeKind.TRANSIENT_FAILURE and (
        outcome.category == DISK_PRESSURE_CATEGORY or _can_retry(step)
    ):
        # disk_pressure is host backpressure: always requeue (even NEVER/ISOLATED_RETRYABLE
        # heavy steps such as prepare_worktree) and refund the claim attempt burn.
        await transition_step(session, step, StepStatus.QUEUED, actor_type="worker")
        step.available_at = func.now() + timedelta(seconds=retry_delay_seconds)
        step.failure_category = outcome.category
        step.failure_summary = outcome.summary
        if outcome.category == DISK_PRESSURE_CATEGORY and step.attempt_count > 0:
            step.attempt_count -= 1
        if step.step_type == "plan" and outcome.result:
            # Retry diagnostics only for plan outcomes (handoff rejection payload).
            step.result = outcome.result
    elif outcome.kind is OutcomeKind.NEEDS_USER_INPUT:
        await transition_step(session, step, StepStatus.AWAITING_USER, actor_type="worker")
        await transition_run(session, run, RunStatus.AWAITING_USER, actor_type="worker")
    elif outcome.kind is OutcomeKind.NEEDS_APPROVAL:
        await transition_step(session, step, StepStatus.WAITING_APPROVAL, actor_type="worker")
    elif outcome.kind is OutcomeKind.BLOCKED or outcome.unknown_effects:
        if not await _schedule_bounded_repair(session, run, step, outcome):
            await transition_step(session, step, StepStatus.BLOCKED, actor_type="worker")
            await transition_run(session, run, RunStatus.BLOCKED, actor_type="worker")
            step.unknown_effects = outcome.unknown_effects
    elif outcome.kind is OutcomeKind.CANCELLED:
        await transition_step(session, step, StepStatus.CANCELLED, actor_type="worker")
    else:
        await transition_step(session, step, StepStatus.FAILED, actor_type="worker")
        await transition_run(session, run, RunStatus.FAILED, actor_type="worker")
        if step.step_type == "plan" and outcome.result:
            step.result = outcome.result
    if outcome.kind is not OutcomeKind.SUCCEEDED:
        step.failure_category = outcome.category
        step.failure_summary = outcome.summary
        if run.status in {RunStatus.BLOCKED, RunStatus.FAILED, RunStatus.CANCELLED}:
            run.failure_category = outcome.category
            run.failure_summary = outcome.summary
    step.lease_owner = None
    step.lease_expires_at = None
    if step.status is StepStatus.COMPLETED:
        await _resume_repaired_step(session, run, step)
        await _resume_review_after_validation(session, run, step)
        await activate_ready_steps(session, run)
        await finalize_if_complete(session, run)
    task = await session.scalar(select(Task).where(Task.id == run.task_id).with_for_update())
    assert task is not None
    if step.step_type == "plan":
        enqueue_planner_trace(session, task=task, step=step)
    target = derive_task_status(await _steps_for_run(session, run.id), run.status)
    if target is not task.status:
        await transition_task(session, task, target, actor_type="workflow_manager")
        if task.task_type == "discussion_agent_internal":
            pass
        elif target is TaskStatus.WAITING_APPROVAL:
            # Project card in the project topic + decision card in Апрувы.
            await _enqueue_telegram_projection(session, task, run, role="intake_ack")
            await _enqueue_telegram_projection(session, task, run, role="approval_card")
        elif target in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.BLOCKED}:
            await enqueue_terminal_task_projections(session, task, run)
        else:
            await _enqueue_telegram_projection(session, task, run)


async def _schedule_bounded_repair(
    session: AsyncSession,
    run: Run,
    failed_step: Step,
    outcome: StepOutcome,
) -> bool:
    """Schedule one worker repair without turning the static workflow into an unsafe cycle."""

    if (
        outcome.unknown_effects
        or failed_step.step_type not in {"validate", "review"}
        or not (outcome.category or "").startswith(("validation_", "review_"))
        or int(failed_step.payload.get("repair_count", 0)) >= 1
    ):
        return False
    worker = await session.scalar(
        select(Step)
        .where(
            Step.run_id == run.id,
            Step.step_type == "execute_code",
            Step.status == StepStatus.COMPLETED,
        )
        .order_by(Step.ordinal.desc())
        .limit(1)
    )
    if worker is None:
        return False
    ordinal = (
        int(
            await session.scalar(
                select(func.coalesce(func.max(Step.ordinal), 0)).where(Step.run_id == run.id)
            )
            or 0
        )
        + 1
    )
    repair = Step(
        run_id=run.id,
        ordinal=ordinal,
        dependency_metadata={
            "template_key": "repair_code",
            "predecessor_ordinals": [],
            "repair_for_ordinal": failed_step.ordinal,
        },
        step_type="execute_code",
        queue_class=worker.queue_class,
        status=StepStatus.QUEUED,
        required_capabilities=list(worker.required_capabilities),
        payload={
            "repair_for_step_id": str(failed_step.id),
            "repair_attempt": 1,
            "repair_context": {
                "source": failed_step.step_type,
                "category": outcome.category,
                "summary": (outcome.summary or "")[:2_000],
                "validation_result": outcome.result,
            },
        },
        retry_class=worker.retry_class,
        idempotency_class=worker.idempotency_class,
        max_attempts=1,
        priority=worker.priority,
        timeout_seconds=worker.timeout_seconds,
    )
    session.add(repair)
    await session.flush()
    await transition_step(session, failed_step, StepStatus.BLOCKED, actor_type="workflow_manager")
    failed_step.failure_category = outcome.category
    failed_step.failure_summary = outcome.summary
    failed_step.unknown_effects = False
    failed_step.payload = {
        **failed_step.payload,
        "repair_count": 1,
        "repair_scheduled_step_id": str(repair.id),
    }
    session.add(
        Event(
            entity_type="run",
            entity_id=run.id,
            event_type="workflow.repair_scheduled",
            actor_type="workflow_manager",
            payload={
                "failed_step_id": str(failed_step.id),
                "repair_step_id": str(repair.id),
                "category": outcome.category,
                "repair_count": 1,
            },
        )
    )
    return True


async def _resume_repaired_step(session: AsyncSession, run: Run, repair_step: Step) -> bool:
    raw_target = repair_step.payload.get("repair_for_step_id")
    if not isinstance(raw_target, str):
        return False
    try:
        target_id = uuid.UUID(raw_target)
    except ValueError:
        return False
    target = await session.scalar(
        select(Step).where(Step.id == target_id, Step.run_id == run.id).with_for_update()
    )
    if target is None or target.status is not StepStatus.BLOCKED:
        return False
    if target.step_type == "review":
        validation = await session.scalar(
            select(Step)
            .where(
                Step.run_id == run.id,
                Step.step_type == "validate",
                Step.status == StepStatus.COMPLETED,
            )
            .order_by(Step.ordinal.desc())
            .limit(1)
        )
        if validation is None:
            return False
        ordinal = (
            int(
                await session.scalar(
                    select(func.coalesce(func.max(Step.ordinal), 0)).where(Step.run_id == run.id)
                )
                or 0
            )
            + 1
        )
        revalidation = Step(
            run_id=run.id,
            ordinal=ordinal,
            dependency_metadata={
                "template_key": "repair_validate",
                "predecessor_ordinals": [],
                "repair_for_ordinal": target.ordinal,
            },
            step_type="validate",
            queue_class=validation.queue_class,
            status=StepStatus.QUEUED,
            required_capabilities=list(validation.required_capabilities),
            payload={
                "amend_repaired_result": True,
                "resume_after_validation_step_id": str(target.id),
                "repair_step_id": str(repair_step.id),
            },
            retry_class=validation.retry_class,
            idempotency_class=validation.idempotency_class,
            max_attempts=1,
            priority=validation.priority,
            timeout_seconds=validation.timeout_seconds,
        )
        session.add(revalidation)
        await session.flush()
        return True
    await transition_step(session, target, StepStatus.QUEUED, actor_type="workflow_manager")
    # The original validation claim already consumed its configured attempt. A successful
    # repair grants exactly one revalidation, while repair_count still prevents another loop.
    if target.attempt_count >= target.max_attempts:
        target.max_attempts += 1
    target.failure_category = None
    target.failure_summary = None
    target.unknown_effects = False
    target.payload = {**target.payload, "repair_completed_step_id": str(repair_step.id)}
    session.add(
        Event(
            entity_type="run",
            entity_id=run.id,
            event_type="workflow.repair_completed",
            actor_type="workflow_manager",
            payload={"repair_step_id": str(repair_step.id), "resumed_step_id": str(target.id)},
        )
    )
    return True


async def _resume_review_after_validation(
    session: AsyncSession, run: Run, validation_step: Step
) -> bool:
    raw_target = validation_step.payload.get("resume_after_validation_step_id")
    if not isinstance(raw_target, str):
        return False
    try:
        target_id = uuid.UUID(raw_target)
    except ValueError:
        return False
    target = await session.scalar(
        select(Step).where(Step.id == target_id, Step.run_id == run.id).with_for_update()
    )
    if target is None or target.step_type != "review" or target.status is not StepStatus.BLOCKED:
        return False
    await transition_step(session, target, StepStatus.QUEUED, actor_type="workflow_manager")
    if target.attempt_count >= target.max_attempts:
        target.max_attempts += 1
    target.failure_category = None
    target.failure_summary = None
    target.payload = {**target.payload, "repair_validation_step_id": str(validation_step.id)}
    return True


async def _enqueue_telegram_projection(
    session: AsyncSession,
    task: Task,
    run: Run,
    *,
    role: str | None = None,
) -> None:
    await enqueue_task_status_projection(session, task, run, role=role)


async def finalize_if_complete(session: AsyncSession, run: Run) -> bool:
    steps = await _steps_for_run(session, run.id)
    if not steps or any(step.status is not StepStatus.COMPLETED for step in steps):
        return False
    if run.status is not RunStatus.RUNNING:
        return False
    await transition_run(session, run, RunStatus.COMPLETED, actor_type="workflow_manager")
    run.ended_at = func.now()
    return True


def derive_task_status(steps: tuple[Step, ...], run_status: RunStatus) -> TaskStatus:
    if run_status is RunStatus.CREATED:
        return TaskStatus.INTERPRETED
    direct = {
        RunStatus.PAUSED: TaskStatus.PAUSED,
        RunStatus.AWAITING_USER: TaskStatus.AWAITING_USER,
        RunStatus.BLOCKED: TaskStatus.BLOCKED,
        RunStatus.FAILED: TaskStatus.FAILED,
        RunStatus.CANCELLED: TaskStatus.CANCELLED,
        RunStatus.COMPLETED: TaskStatus.COMPLETED,
    }
    if run_status in direct:
        return direct[run_status]
    active_repair_targets = {
        value
        for step in steps
        if step.status
        not in {StepStatus.COMPLETED, StepStatus.CANCELLED, StepStatus.FAILED, StepStatus.BLOCKED}
        and isinstance((value := step.payload.get("repair_for_step_id")), str)
    }
    active_repair_targets.update(
        value
        for step in steps
        if step.status
        not in {StepStatus.COMPLETED, StepStatus.CANCELLED, StepStatus.FAILED, StepStatus.BLOCKED}
        and isinstance((value := step.payload.get("resume_after_validation_step_id")), str)
    )
    active = next(
        (
            step
            for step in steps
            if step.status not in {StepStatus.PENDING, StepStatus.COMPLETED, StepStatus.CANCELLED}
            and not (step.status is StepStatus.BLOCKED and str(step.id) in active_repair_targets)
        ),
        None,
    )
    if active is None:
        return TaskStatus.EXECUTING
    if active.status is StepStatus.WAITING_APPROVAL:
        return TaskStatus.WAITING_APPROVAL
    if active.status is StepStatus.AWAITING_USER:
        return TaskStatus.AWAITING_USER
    if active.status is StepStatus.BLOCKED:
        return TaskStatus.BLOCKED
    if active.status is StepStatus.FAILED:
        return TaskStatus.FAILED
    if isinstance(active.payload.get("repair_for_step_id"), str) or isinstance(
        active.payload.get("resume_after_validation_step_id"), str
    ):
        return TaskStatus.RETRYING
    mapping = {
        "prepare_context": TaskStatus.CONTEXT_PREPARED,
        "plan": TaskStatus.PLANNED,
        "validate": TaskStatus.VALIDATING,
        "review": TaskStatus.REVIEWING,
    }
    return mapping.get(active.step_type, TaskStatus.EXECUTING)


def _can_retry(step: Step) -> bool:
    from vuzol.storage.types import IdempotencyClass, RetryClass

    return (
        step.retry_class is RetryClass.TRANSIENT
        and step.attempt_count < step.max_attempts
        and step.idempotency_class in {IdempotencyClass.READ_ONLY, IdempotencyClass.IDEMPOTENT}
    )


async def _steps_for_run(
    session: AsyncSession, run_id: uuid.UUID, *, for_update: bool = False
) -> tuple[Step, ...]:
    statement = select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
    if for_update:
        statement = statement.with_for_update()
    return tuple((await session.scalars(statement)).all())


def _predecessors(metadata: Mapping[str, Any]) -> tuple[int, ...]:
    raw = metadata.get("predecessor_ordinals", [])
    return tuple(int(value) for value in raw) if isinstance(raw, list) else ()


async def _record_noop(
    session: AsyncSession,
    run_id: uuid.UUID,
    action: str,
    actor_type: str,
    actor_id: str | None,
) -> None:
    session.add(
        Event(
            entity_type="run",
            entity_id=run_id,
            event_type="workflow.control_noop",
            actor_type=actor_type,
            actor_id=actor_id,
            payload={"action": action},
        )
    )
    await session.flush()
