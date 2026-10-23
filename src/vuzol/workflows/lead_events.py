"""Workflow-owned orchestration notifications, committed with state changes."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    Event,
    MaterializationLink,
    Step,
    Task,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import TaskStatus

OBSERVED_TASK_STATUSES = frozenset(
    {
        TaskStatus.COMPLETED,
        TaskStatus.CANCELLED,
        TaskStatus.FAILED,
        TaskStatus.BLOCKED,
        TaskStatus.QUOTA_EXHAUSTED,
        TaskStatus.WAITING_APPROVAL,
    }
)


async def publish_task_event(session: AsyncSession, task: Task) -> None:
    if task.status not in OBSERVED_TASK_STATUSES:
        return
    link = await session.scalar(
        select(MaterializationLink).where(
            MaterializationLink.task_id == task.id,
        )
    )
    if link is None:
        return
    if task.status is not TaskStatus.WAITING_APPROVAL:
        await _enqueue(
            session,
            TransactionalOutbox(
                destination="work_package_sequence",
                idempotency_key=f"work-package:terminal:{task.id}:{task.version}",
                linked_entity_type="task",
                linked_entity_id=task.id,
                operation_type="observe_task_terminal",
                payload={
                    "work_package_id": str(link.work_package_id),
                    "ordinal": link.ordinal,
                    "task_status": task.status.value,
                },
            ),
        )
    package = await session.get(WorkPackage, link.work_package_id)
    if package is None or package.managed_goal_id is None:
        return
    kind = (
        "limit_hit"
        if task.status is TaskStatus.QUOTA_EXHAUSTED
        else ("review_result" if task.status is TaskStatus.WAITING_APPROVAL else "task_finished")
    )
    await _enqueue(
        session,
        TransactionalOutbox(
            destination="project_lead",
            idempotency_key=f"lead:task:{task.id}:{task.version}:{kind}",
            linked_entity_type="managed_goal",
            linked_entity_id=package.managed_goal_id,
            operation_type="lead_wakeup",
            payload={
                "goal_id": str(package.managed_goal_id),
                "package_id": str(package.id),
                "task_id": str(task.id),
                "generation": task.version,
                "wakeup_kind": kind,
            },
        ),
    )


async def publish_review_event(session: AsyncSession, task: Task, step: Step) -> None:
    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task.id)
    )
    package = await session.get(WorkPackage, link.work_package_id) if link else None
    if package is None or package.managed_goal_id is None:
        return
    await _enqueue(
        session,
        TransactionalOutbox(
            destination="project_lead",
            idempotency_key=f"lead:review:{step.id}:{step.lease_generation}:{step.version}",
            linked_entity_type="managed_goal",
            linked_entity_id=package.managed_goal_id,
            operation_type="lead_wakeup",
            payload={
                "goal_id": str(package.managed_goal_id),
                "package_id": str(package.id),
                "task_id": str(task.id),
                "step_id": str(step.id),
                "generation": step.lease_generation,
                "step_version": step.version,
                "wakeup_kind": "review_result",
            },
        ),
    )


async def _enqueue(session: AsyncSession, row: TransactionalOutbox) -> None:
    existing = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == row.destination,
            TransactionalOutbox.idempotency_key == row.idempotency_key,
        )
    )
    if existing is None:
        session.add(row)
        await session.flush()


async def publish_planning_request(
    session: AsyncSession, task: Task, interpretation_id: uuid.UUID
) -> None:
    """Retain legacy planning intent for the lead, without invoking a task planner."""
    from vuzol.leadership.persistence import ManagedGoal

    key = f"lead:planning-request:{interpretation_id}"
    existing = await session.scalar(
        select(Event.id).where(
            Event.entity_type == "task",
            Event.entity_id == task.id,
            Event.event_type == "task.planning_requested",
            Event.payload["interpretation_id"].as_string() == str(interpretation_id),
        )
    )
    if existing is not None:
        return
    payload = {
        "source_task_id": str(task.id),
        "interpretation_id": str(interpretation_id),
        "wakeup_kind": "planning_requested",
    }
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.planning_requested",
            actor_type="workflow_manager",
            payload=payload,
        )
    )
    await session.flush()
    goal = (
        await session.scalar(
            select(ManagedGoal).where(
                ManagedGoal.project_id == task.project_id,
                ManagedGoal.lifecycle.in_(("draft", "active", "waiting", "paused")),
            )
        )
        if task.project_id
        else None
    )
    if goal is not None:
        await _enqueue(
            session,
            TransactionalOutbox(
                destination="project_lead",
                idempotency_key=key,
                linked_entity_type="managed_goal",
                linked_entity_id=goal.id,
                operation_type="lead_wakeup",
                payload={**payload, "goal_id": str(goal.id)},
            ),
        )
