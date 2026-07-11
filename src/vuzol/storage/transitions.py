"""Atomic state transition and audit-event service."""

import uuid

from vuzol.storage.errors import IllegalTransition
from vuzol.storage.models import Event
from vuzol.storage.records import TaskRecord
from vuzol.storage.repositories.core import task_record
from vuzol.storage.types import TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork

TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.RECEIVED: frozenset(
        {TaskStatus.INTERPRETED, TaskStatus.CANCELLED, TaskStatus.FAILED}
    ),
    TaskStatus.INTERPRETED: frozenset(
        {TaskStatus.CONTEXT_PREPARED, TaskStatus.AWAITING_USER, TaskStatus.CANCELLED}
    ),
    TaskStatus.CONTEXT_PREPARED: frozenset({TaskStatus.PLANNED, TaskStatus.EXECUTING}),
    TaskStatus.PLANNED: frozenset({TaskStatus.WAITING_APPROVAL, TaskStatus.EXECUTING}),
    TaskStatus.WAITING_APPROVAL: frozenset(
        {TaskStatus.EXECUTING, TaskStatus.CANCELLED, TaskStatus.BLOCKED}
    ),
    TaskStatus.EXECUTING: frozenset(
        {TaskStatus.VALIDATING, TaskStatus.RETRYING, TaskStatus.BLOCKED, TaskStatus.FAILED}
    ),
    TaskStatus.VALIDATING: frozenset(
        {TaskStatus.REVIEWING, TaskStatus.COMPLETED, TaskStatus.BLOCKED, TaskStatus.FAILED}
    ),
    TaskStatus.REVIEWING: frozenset(
        {TaskStatus.COMPLETED, TaskStatus.EXECUTING, TaskStatus.BLOCKED}
    ),
    TaskStatus.AWAITING_USER: frozenset({TaskStatus.INTERPRETED, TaskStatus.CANCELLED}),
    TaskStatus.PAUSED: frozenset({TaskStatus.EXECUTING, TaskStatus.CANCELLED}),
    TaskStatus.RETRYING: frozenset({TaskStatus.EXECUTING, TaskStatus.BLOCKED, TaskStatus.FAILED}),
    TaskStatus.QUOTA_EXHAUSTED: frozenset({TaskStatus.RETRYING, TaskStatus.CANCELLED}),
    TaskStatus.BLOCKED: frozenset({TaskStatus.RETRYING, TaskStatus.CANCELLED}),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.ROLLED_BACK: frozenset(),
    TaskStatus.COMPLETED: frozenset({TaskStatus.ROLLED_BACK}),
}


async def transition_task(
    uow: UnitOfWork,
    *,
    task_id: uuid.UUID,
    target: TaskStatus,
    actor_type: str,
) -> TaskRecord:
    task = await uow.tasks.get(task_id, for_update=True)
    previous = task.status
    if target not in TASK_TRANSITIONS[previous]:
        raise IllegalTransition(f"illegal task transition: {previous.value} -> {target.value}")
    task.status = target
    task.version += 1
    assert uow.session is not None
    uow.session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.status_changed",
            actor_type=actor_type,
            previous_state=previous.value,
            new_state=target.value,
            payload={},
        )
    )
    await uow.session.flush()
    return task_record(task)
