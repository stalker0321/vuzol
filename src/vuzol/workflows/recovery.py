"""Idempotent expired-lease recovery policy."""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.providers.budgets import close_step_reservations, release_orphan_reservations
from vuzol.storage.models import Event, Run, Step, Task
from vuzol.storage.types import IdempotencyClass, RunStatus, StepStatus, TaskStatus
from vuzol.telegram.projections import (
    enqueue_task_status_projection,
    enqueue_terminal_task_projections,
)
from vuzol.workflows.locking import lock_task_run_step
from vuzol.workflows.retry_policy import (
    LEASE_EXPIRED_ATTEMPTS_EXHAUSTED,
    LEASE_EXPIRED_UNKNOWN_EFFECTS,
    repeatable_without_new_mutation,
)
from vuzol.workflows.transitions import transition_run, transition_step, transition_task

# Reservations younger than this are left alone: a live claim may still settle.
ORPHAN_RESERVATION_MIN_AGE_SECONDS = 900


async def recover_expired_steps(session: AsyncSession, *, batch_size: int) -> int:
    statement = (
        select(Step.id)
        .join(Run, Run.id == Step.run_id)
        .where(
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
            Step.lease_expires_at < func.now(),
        )
        .order_by(Run.task_id, Step.lease_expires_at, Step.id)
        .limit(batch_size)
    )
    step_ids = tuple((await session.scalars(statement)).all())
    recovered = 0
    for step_id in step_ids:
        if await _recover_locked(session, step_id):
            recovered += 1
    # Bounded sweep for reservations leaked outside the expired-lease path
    # (handler crash/shutdown after the step already became terminal).
    await release_orphan_reservations(
        session,
        older_than_seconds=ORPHAN_RESERVATION_MIN_AGE_SECONDS,
        batch_size=batch_size,
    )
    return recovered


async def _recover_locked(session: AsyncSession, step_id: uuid.UUID) -> bool:
    locked = await lock_task_run_step(session, step_id)
    if locked is None:
        return False
    task, run, step = locked
    still_expired = await session.scalar(
        select(Step.id).where(
            Step.id == step.id,
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
            Step.lease_expires_at < func.now(),
        )
    )
    if still_expired is None:
        return False
    await _recover_one(session, step, run, task)
    return True


async def _recover_one(session: AsyncSession, step: Step, run: Run, task: Task) -> None:
    previous_status = step.status
    # LEASED means start_step never committed — no handler effects. Always requeue and
    # refund the claim attempt so a worker crash before start cannot burn max_attempts.
    leased_only = previous_status is StepStatus.LEASED
    safe = leased_only or (
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
    terminal_outcome = False
    if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.COMPLETED}:
        await transition_step(
            session, step, StepStatus.CANCELLED, actor_type="recovery", payload=payload
        )
    elif run.status is RunStatus.PAUSED:
        # The owner's pause holds the wait. Parking the step must not move
        # Task or Run, or the parent and child stop agreeing.
        if leased_only or (safe and attempts_remain):
            await transition_step(
                session, step, StepStatus.QUEUED, actor_type="recovery", payload=payload
            )
            if leased_only and step.attempt_count > 0:
                step.attempt_count -= 1
            hold_reason = "lease_expired_before_start" if leased_only else "lease_expired_requeued"
        elif repeatable_without_new_mutation(step):
            summary = (
                "Исполнитель не завершил этап до истечения lease; безопасные повторы исчерпаны. "
                "Пауза сохранена: можно продолжить или отменить."
            )
            await transition_step(
                session, step, StepStatus.BLOCKED, actor_type="recovery", payload=payload
            )
            step.failure_category = LEASE_EXPIRED_ATTEMPTS_EXHAUSTED
            step.failure_summary = summary
            step.unknown_effects = False
            hold_reason = LEASE_EXPIRED_ATTEMPTS_EXHAUSTED
        else:
            summary = (
                "Аренда этапа истекла во время паузы, а внешний эффект нельзя безопасно повторить. "  # noqa: RUF001
                "Сверьте эффект или отмените задачу."
            )
            step.unknown_effects = True
            await transition_step(
                session, step, StepStatus.BLOCKED, actor_type="recovery", payload=payload
            )
            step.failure_category = LEASE_EXPIRED_UNKNOWN_EFFECTS
            step.failure_summary = summary
            hold_reason = LEASE_EXPIRED_UNKNOWN_EFFECTS
        await _note_pause_hold(session, task, run, step, hold_reason)
    elif leased_only or (safe and attempts_remain):
        await transition_step(
            session, step, StepStatus.QUEUED, actor_type="recovery", payload=payload
        )
        if leased_only and step.attempt_count > 0:
            step.attempt_count -= 1
    elif safe:
        category = "lease_expired_attempts_exhausted"
        summary = "Исполнитель не завершил этап до истечения lease; безопасные повторы исчерпаны."
        await transition_step(
            session, step, StepStatus.FAILED, actor_type="recovery", payload=payload
        )
        step.failure_category = category
        step.failure_summary = summary
        if run.status is RunStatus.RUNNING:
            await transition_run(session, run, RunStatus.FAILED, actor_type="recovery")
        if task.status not in {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.COMPLETED}:
            await transition_task(
                session,
                task,
                TaskStatus.FAILED,
                actor_type="recovery",
                payload={"category": category, "summary": summary},
            )
        run.failure_category = category
        run.failure_summary = summary
        terminal_outcome = True
    else:
        category = "lease_expired_unknown_effects"
        summary = (
            "Аренда этапа истекла, а внешние эффекты нельзя безопасно определить или повторить."  # noqa: RUF001
        )
        step.unknown_effects = True
        await transition_step(
            session, step, StepStatus.BLOCKED, actor_type="recovery", payload=payload
        )
        step.failure_category = category
        step.failure_summary = summary
        if run.status is RunStatus.RUNNING:
            await transition_run(session, run, RunStatus.BLOCKED, actor_type="recovery")
        if task.status not in {TaskStatus.BLOCKED, TaskStatus.CANCELLED, TaskStatus.COMPLETED}:
            await transition_task(
                session,
                task,
                TaskStatus.BLOCKED,
                actor_type="recovery",
                payload={"category": category, "summary": summary},
            )
        run.failure_category = category
        run.failure_summary = summary
        terminal_outcome = True
    step.lease_owner = None
    step.lease_expires_at = None
    # Close any budget reservation left behind by the dead worker. A step that
    # never started (LEASED only) is released; anything that may have reached the
    # provider is charged conservatively so a crashed call does not leak caps.
    await close_step_reservations(
        session,
        step_id=step.id,
        outcome="lease_expired" if not leased_only else "lease_expired_before_start",
        release=leased_only,
    )
    if terminal_outcome:
        if step.step_type == "ensure_capabilities":
            from vuzol.projects.capability_provisioning import abandon_capability_installation

            await abandon_capability_installation(session, step.id, reason="leader_lease_expired")
        await enqueue_terminal_task_projections(session, task, run)


async def _note_pause_hold(
    session: AsyncSession, task: Task, run: Run, step: Step, reason: str
) -> None:
    """Refresh the paused card without changing the pause itself."""

    task.version += 1
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.pause_hold",
            actor_type="recovery",
            payload={
                "run_id": str(run.id),
                "step_id": str(step.id),
                "reason": reason,
                "step_status": step.status.value,
                "failure_category": step.failure_category,
                "unknown_effects": step.unknown_effects,
            },
        )
    )
    await enqueue_task_status_projection(session, task, run)
