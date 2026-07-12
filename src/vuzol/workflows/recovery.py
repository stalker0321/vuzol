"""Idempotent expired-lease recovery policy."""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Run, Step, Task
from vuzol.storage.types import IdempotencyClass, RunStatus, StepStatus, TaskStatus
from vuzol.workflows.transitions import transition_run, transition_step, transition_task


async def recover_expired_steps(session: AsyncSession, *, batch_size: int) -> int:
    statement = (
        select(Step)
        .where(
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
            Step.lease_expires_at < func.now(),
        )
        .order_by(Step.lease_expires_at, Step.id)
        .with_for_update(skip_locked=True)
        .limit(batch_size)
    )
    steps = tuple((await session.scalars(statement)).all())
    for step in steps:
        await _recover_one(session, step)
    return len(steps)


async def _recover_one(session: AsyncSession, step: Step) -> None:
    run = await session.scalar(select(Run).where(Run.id == step.run_id).with_for_update())
    assert run is not None
    task = await session.scalar(select(Task).where(Task.id == run.task_id).with_for_update())
    assert task is not None
    previous_status = step.status
    safe = previous_status is StepStatus.LEASED or (
        step.idempotency_class is IdempotencyClass.READ_ONLY
        or (
            step.idempotency_class is IdempotencyClass.IDEMPOTENT
            and step.external_idempotency_key is not None
        )
    )
    attempts_remain = step.attempt_count < step.max_attempts
    payload = {
        "expired_owner": step.lease_owner,
        "generation": step.lease_generation,
        "idempotency_class": step.idempotency_class.value,
    }
    if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.COMPLETED}:
        await transition_step(
            session, step, StepStatus.CANCELLED, actor_type="recovery", payload=payload
        )
    elif safe and attempts_remain:
        await transition_step(
            session, step, StepStatus.QUEUED, actor_type="recovery", payload=payload
        )
    elif safe:
        await transition_step(
            session, step, StepStatus.FAILED, actor_type="recovery", payload=payload
        )
        if run.status is RunStatus.RUNNING:
            await transition_run(session, run, RunStatus.FAILED, actor_type="recovery")
        if task.status not in {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.COMPLETED}:
            await transition_task(session, task, TaskStatus.FAILED, actor_type="recovery")
    else:
        step.unknown_effects = True
        await transition_step(
            session, step, StepStatus.BLOCKED, actor_type="recovery", payload=payload
        )
        if run.status is RunStatus.RUNNING:
            await transition_run(session, run, RunStatus.BLOCKED, actor_type="recovery")
        if task.status not in {TaskStatus.BLOCKED, TaskStatus.CANCELLED, TaskStatus.COMPLETED}:
            await transition_task(session, task, TaskStatus.BLOCKED, actor_type="recovery")
    step.lease_owner = None
    step.lease_expires_at = None
