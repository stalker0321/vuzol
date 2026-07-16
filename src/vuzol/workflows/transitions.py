"""Legal audited task, run, and step state transitions."""

import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.errors import IllegalTransition
from vuzol.storage.models import Event, Run, Step, Task
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus

TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.RECEIVED: frozenset(
        {TaskStatus.INTERPRETED, TaskStatus.AWAITING_USER, TaskStatus.FAILED, TaskStatus.CANCELLED}
    ),
    TaskStatus.INTERPRETED: frozenset(
        {
            TaskStatus.CONTEXT_PREPARED,
            TaskStatus.PLANNED,
            TaskStatus.EXECUTING,
            TaskStatus.AWAITING_USER,
            TaskStatus.PAUSED,
            TaskStatus.QUOTA_EXHAUSTED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.COMPLETED,
        }
    ),
    TaskStatus.CONTEXT_PREPARED: frozenset(
        {
            TaskStatus.PLANNED,
            TaskStatus.EXECUTING,
            TaskStatus.AWAITING_USER,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.PLANNED: frozenset(
        {
            TaskStatus.CONTEXT_PREPARED,
            TaskStatus.WAITING_APPROVAL,
            TaskStatus.EXECUTING,
            TaskStatus.AWAITING_USER,
            TaskStatus.PAUSED,
            TaskStatus.QUOTA_EXHAUSTED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.WAITING_APPROVAL: frozenset(
        {
            TaskStatus.EXECUTING,
            TaskStatus.AWAITING_USER,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.EXECUTING: frozenset(
        {
            TaskStatus.VALIDATING,
            TaskStatus.REVIEWING,
            TaskStatus.WAITING_APPROVAL,
            TaskStatus.AWAITING_USER,
            TaskStatus.RETRYING,
            TaskStatus.QUOTA_EXHAUSTED,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.COMPLETED,
        }
    ),
    TaskStatus.VALIDATING: frozenset(
        {
            TaskStatus.REVIEWING,
            TaskStatus.EXECUTING,
            TaskStatus.AWAITING_USER,
            TaskStatus.RETRYING,
            TaskStatus.QUOTA_EXHAUSTED,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.COMPLETED,
        }
    ),
    TaskStatus.REVIEWING: frozenset(
        {
            TaskStatus.EXECUTING,
            TaskStatus.VALIDATING,
            TaskStatus.WAITING_APPROVAL,
            TaskStatus.AWAITING_USER,
            TaskStatus.QUOTA_EXHAUSTED,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.COMPLETED,
        }
    ),
    TaskStatus.AWAITING_USER: frozenset(
        {
            TaskStatus.INTERPRETED,
            TaskStatus.EXECUTING,
            TaskStatus.VALIDATING,
            TaskStatus.REVIEWING,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.PAUSED: frozenset(
        {
            TaskStatus.INTERPRETED,
            TaskStatus.CONTEXT_PREPARED,
            TaskStatus.PLANNED,
            TaskStatus.WAITING_APPROVAL,
            TaskStatus.EXECUTING,
            TaskStatus.VALIDATING,
            TaskStatus.REVIEWING,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.RETRYING: frozenset(
        {
            TaskStatus.CONTEXT_PREPARED,
            TaskStatus.PLANNED,
            TaskStatus.EXECUTING,
            TaskStatus.VALIDATING,
            TaskStatus.AWAITING_USER,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.QUOTA_EXHAUSTED: frozenset(
        {TaskStatus.RETRYING, TaskStatus.AWAITING_USER, TaskStatus.BLOCKED, TaskStatus.CANCELLED}
    ),
    TaskStatus.BLOCKED: frozenset(
        {TaskStatus.RETRYING, TaskStatus.AWAITING_USER, TaskStatus.FAILED, TaskStatus.CANCELLED}
    ),
    TaskStatus.COMPLETED: frozenset({TaskStatus.ROLLED_BACK}),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.ROLLED_BACK: frozenset(),
}

RUN_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.CREATED: frozenset(
        {
            RunStatus.RUNNING,
            RunStatus.AWAITING_USER,
            RunStatus.PAUSED,
            RunStatus.BLOCKED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        }
    ),
    RunStatus.RUNNING: frozenset(
        {
            RunStatus.AWAITING_USER,
            RunStatus.PAUSED,
            RunStatus.BLOCKED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.COMPLETED,
        }
    ),
    RunStatus.AWAITING_USER: frozenset(
        {
            RunStatus.RUNNING,
            RunStatus.PAUSED,
            RunStatus.BLOCKED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        }
    ),
    RunStatus.PAUSED: frozenset(
        {RunStatus.RUNNING, RunStatus.BLOCKED, RunStatus.FAILED, RunStatus.CANCELLED}
    ),
    RunStatus.BLOCKED: frozenset(
        {RunStatus.RUNNING, RunStatus.AWAITING_USER, RunStatus.FAILED, RunStatus.CANCELLED}
    ),
    RunStatus.FAILED: frozenset(),
    RunStatus.CANCELLED: frozenset(),
    RunStatus.COMPLETED: frozenset(),
}

STEP_TRANSITIONS: dict[StepStatus, frozenset[StepStatus]] = {
    StepStatus.PENDING: frozenset(
        {
            StepStatus.QUEUED,
            StepStatus.WAITING_APPROVAL,
            StepStatus.AWAITING_USER,
            StepStatus.BLOCKED,
            StepStatus.FAILED,
            StepStatus.CANCELLED,
        }
    ),
    StepStatus.QUEUED: frozenset(
        {
            StepStatus.LEASED,
            StepStatus.AWAITING_USER,
            StepStatus.BLOCKED,
            StepStatus.FAILED,
            StepStatus.CANCELLED,
        }
    ),
    StepStatus.LEASED: frozenset(
        {
            StepStatus.RUNNING,
            StepStatus.QUEUED,
            StepStatus.BLOCKED,
            StepStatus.FAILED,
            StepStatus.CANCELLED,
        }
    ),
    StepStatus.RUNNING: frozenset(
        {
            StepStatus.COMPLETED,
            StepStatus.QUEUED,
            StepStatus.WAITING_APPROVAL,
            StepStatus.AWAITING_USER,
            StepStatus.BLOCKED,
            StepStatus.FAILED,
            StepStatus.CANCELLED,
        }
    ),
    StepStatus.WAITING_APPROVAL: frozenset(
        {
            StepStatus.QUEUED,
            StepStatus.BLOCKED,
            StepStatus.FAILED,
            StepStatus.CANCELLED,
        }
    ),
    StepStatus.AWAITING_USER: frozenset(
        {StepStatus.QUEUED, StepStatus.BLOCKED, StepStatus.FAILED, StepStatus.CANCELLED}
    ),
    StepStatus.BLOCKED: frozenset({StepStatus.QUEUED, StepStatus.FAILED, StepStatus.CANCELLED}),
    StepStatus.FAILED: frozenset(),
    StepStatus.CANCELLED: frozenset(),
    StepStatus.COMPLETED: frozenset(),
}


def _check[StateT: (TaskStatus, RunStatus, StepStatus)](
    previous: StateT, target: StateT, table: Mapping[StateT, frozenset[StateT]]
) -> None:
    if target not in table[previous]:
        raise IllegalTransition(f"illegal transition: {previous.value} -> {target.value}")


async def transition_task(
    session: AsyncSession,
    task: Task,
    target: TaskStatus,
    *,
    actor_type: str,
    actor_id: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> None:
    previous = task.status
    _check(previous, target, TASK_TRANSITIONS)
    task.status = target
    task.version += 1
    await _event(
        session, task.id, "task", previous.value, target.value, actor_type, actor_id, payload
    )


async def transition_run(
    session: AsyncSession,
    run: Run,
    target: RunStatus,
    *,
    actor_type: str,
    actor_id: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> None:
    previous = run.status
    _check(previous, target, RUN_TRANSITIONS)
    run.status = target
    run.version += 1
    await _event(
        session, run.id, "run", previous.value, target.value, actor_type, actor_id, payload
    )


async def transition_step(
    session: AsyncSession,
    step: Step,
    target: StepStatus,
    *,
    actor_type: str,
    actor_id: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> None:
    previous = step.status
    _check(previous, target, STEP_TRANSITIONS)
    step.status = target
    step.version += 1
    await _event(
        session, step.id, "step", previous.value, target.value, actor_type, actor_id, payload
    )


async def _event(
    session: AsyncSession,
    entity_id: uuid.UUID,
    entity_type: str,
    previous: str,
    target: str,
    actor_type: str,
    actor_id: str | None,
    payload: Mapping[str, Any] | None,
) -> None:
    session.add(
        Event(
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=f"{entity_type}.status_changed",
            actor_type=actor_type,
            actor_id=actor_id,
            previous_state=previous,
            new_state=target,
            payload=dict(payload or {}),
        )
    )
    await session.flush()
