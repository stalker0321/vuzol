"""Persisted workflow control semantics."""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Settings
from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item
from vuzol.storage.models import (
    Event,
    Run,
    Step,
    Task,
    TelegramControlAction,
    TransactionalOutbox,
)
from vuzol.storage.types import ControlActionStatus, RunStatus, StepStatus, TaskStatus
from vuzol.workflows.service import activate_ready_steps, derive_task_status, start_run
from vuzol.workflows.transitions import transition_run, transition_step, transition_task

RUN_TERMINAL = {RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.COMPLETED}
STEP_TERMINAL = {StepStatus.FAILED, StepStatus.CANCELLED, StepStatus.COMPLETED}


class WorkflowControlConsumer:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
    ) -> None:
        self._settings = settings
        self._factory = session_factory
        self._owner = owner

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._settings.workflow.lease_seconds,
                allowed_destinations=frozenset({"workflow_control"}),
            )
        if token is None:
            return False
        async with self._factory.begin() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            if item is None or item.linked_entity_type != "telegram_control_action":
                raise ValueError("workflow control outbox item is invalid")
            action = await session.scalar(
                select(TelegramControlAction)
                .where(TelegramControlAction.id == item.linked_entity_id)
                .with_for_update()
            )
            if action is None:
                raise ValueError("workflow control action is missing")
            try:
                await self._apply(session, action)
            except ValueError as error:
                action.status = ControlActionStatus.REJECTED
                action.payload = {**action.payload, "rejection_reason": str(error)[:500]}
            else:
                action.status = ControlActionStatus.PROCESSED
            action.processed_at = func.now()
            await complete_outbox_item(session, token)
        return True

    async def _apply(self, session: AsyncSession, action: TelegramControlAction) -> None:
        actor_id = str(action.requested_by_user_id)
        if action.action_kind == "retry":
            if action.step_id is None:
                raise ValueError("retry requires a step target")
            await retry_blocked_step(session, action.step_id, actor_id=actor_id)
            return
        if action.task_id is None:
            raise ValueError("workflow control requires a task target")
        if action.action_kind == "pause":
            await pause_task(session, action.task_id, actor_id=actor_id)
        elif action.action_kind == "resume":
            await resume_task(session, action.task_id, actor_id=actor_id)
        elif action.action_kind == "cancel":
            await cancel_task(session, action.task_id, actor_id=actor_id)
        elif action.action_kind == "start":
            task, run, _steps = await _locked_context(session, action.task_id)
            await start_run(session, run, task=task, actor_type="user", actor_id=actor_id)
        else:
            raise ValueError(f"unsupported workflow control: {action.action_kind}")


async def pause_task(
    session: AsyncSession, task_id: uuid.UUID, *, actor_id: str | None = None
) -> None:
    task, run, steps = await _locked_context(session, task_id)
    if run.status is RunStatus.PAUSED:
        await _noop(session, run.id, "pause", actor_id)
        return
    if run.status in RUN_TERMINAL:
        raise ValueError(f"terminal run cannot pause: {run.status.value}")
    await transition_run(session, run, RunStatus.PAUSED, actor_type="user", actor_id=actor_id)
    await transition_task(session, task, TaskStatus.PAUSED, actor_type="user", actor_id=actor_id)
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.pause_effective",
            actor_type="user",
            actor_id=actor_id,
            payload={
                "active_step_ids": [
                    str(step.id)
                    for step in steps
                    if step.status in {StepStatus.LEASED, StepStatus.RUNNING}
                ]
            },
        )
    )


async def resume_task(
    session: AsyncSession, task_id: uuid.UUID, *, actor_id: str | None = None
) -> None:
    task, run, _current_steps = await _locked_context(session, task_id)
    if run.status is RunStatus.RUNNING:
        await _noop(session, run.id, "resume", actor_id)
        return
    if run.status is not RunStatus.PAUSED:
        raise ValueError(f"run cannot resume from {run.status.value}")
    await transition_run(session, run, RunStatus.RUNNING, actor_type="user", actor_id=actor_id)
    await activate_ready_steps(session, run)
    target = derive_task_status(await _steps(session, run.id), run.status)
    await transition_task(session, task, target, actor_type="user", actor_id=actor_id)


async def cancel_task(
    session: AsyncSession, task_id: uuid.UUID, *, actor_id: str | None = None
) -> None:
    task, run, steps = await _locked_context(session, task_id)
    if run.status is RunStatus.CANCELLED:
        await _noop(session, run.id, "cancel", actor_id)
        return
    if run.status in {RunStatus.FAILED, RunStatus.COMPLETED}:
        raise ValueError(f"terminal run cannot cancel: {run.status.value}")
    for step in steps:
        if step.status in STEP_TERMINAL:
            continue
        if step.status is StepStatus.RUNNING:
            step.unknown_effects = True
        await transition_step(
            session, step, StepStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        step.lease_owner = None
        step.lease_expires_at = None
    await transition_run(session, run, RunStatus.CANCELLED, actor_type="user", actor_id=actor_id)
    await transition_task(session, task, TaskStatus.CANCELLED, actor_type="user", actor_id=actor_id)
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.cancel_requested",
            actor_type="user",
            actor_id=actor_id,
            payload={"rollback_attempted": False},
        )
    )


async def retry_blocked_step(
    session: AsyncSession, step_id: uuid.UUID, *, actor_id: str | None = None
) -> None:
    step = await session.scalar(select(Step).where(Step.id == step_id).with_for_update())
    if step is None or step.status is not StepStatus.BLOCKED:
        raise ValueError("only a blocked step can be retried")
    if step.unknown_effects or step.attempt_count >= step.max_attempts:
        raise ValueError("blocked step is not safely retryable")
    run = await session.scalar(select(Run).where(Run.id == step.run_id).with_for_update())
    assert run is not None
    task = await session.scalar(select(Task).where(Task.id == run.task_id).with_for_update())
    assert task is not None
    await transition_step(session, step, StepStatus.QUEUED, actor_type="user", actor_id=actor_id)
    if run.status is RunStatus.BLOCKED:
        await transition_run(session, run, RunStatus.RUNNING, actor_type="user", actor_id=actor_id)
    if task.status is TaskStatus.BLOCKED:
        await transition_task(
            session, task, TaskStatus.RETRYING, actor_type="user", actor_id=actor_id
        )


async def _locked_context(
    session: AsyncSession, task_id: uuid.UUID
) -> tuple[Task, Run, tuple[Step, ...]]:
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    run = await session.scalar(
        select(Run).where(Run.task_id == task.id).order_by(Run.created_at.desc()).with_for_update()
    )
    if run is None:
        raise ValueError(f"task has no run: {task_id}")
    return task, run, await _steps(session, run.id, for_update=True)


async def _steps(
    session: AsyncSession, run_id: uuid.UUID, *, for_update: bool = False
) -> tuple[Step, ...]:
    statement = select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
    if for_update:
        statement = statement.with_for_update()
    return tuple((await session.scalars(statement)).all())


async def _noop(
    session: AsyncSession, run_id: uuid.UUID, action: str, actor_id: str | None
) -> None:
    session.add(
        Event(
            entity_type="run",
            entity_id=run_id,
            event_type="workflow.control_noop",
            actor_type="user",
            actor_id=actor_id,
            payload={"action": action},
        )
    )
    await session.flush()
