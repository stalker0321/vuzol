"""Canonical workflow repositories."""

import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.errors import EntityNotFound
from vuzol.storage.models import Event, Run, Step, Task, TopicTaskCounter
from vuzol.storage.records import StepRecord, TaskRecord
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)


def task_record(task: Task) -> TaskRecord:
    return TaskRecord(
        id=task.id,
        topic_task_number=task.topic_task_number,
        public_task_number=task.public_task_number,
        status=task.status,
        original_text=task.original_text,
        task_draft=dict(task.task_draft),
        version=task.version,
    )


def step_record(step: Step) -> StepRecord:
    return StepRecord(
        id=step.id,
        run_id=step.run_id,
        status=step.status,
        lease_generation=step.lease_generation,
        lease_owner=step.lease_owner,
        lease_expires_at=step.lease_expires_at,
    )


class TaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        user_id: int,
        chat_id: int,
        original_text: str,
        task_type: str,
        task_draft: Mapping[str, Any] | None = None,
        thread_id: int | None = None,
        project_id: str | None = None,
    ) -> TaskRecord:
        topic_task_number: int | None = None
        public_task_number: int | None = None
        if thread_id is not None:
            statement = (
                postgres_insert(TopicTaskCounter)
                .values(chat_id=chat_id, message_thread_id=thread_id, last_number=1)
                .on_conflict_do_update(
                    index_elements=["chat_id", "message_thread_id"],
                    set_={"last_number": TopicTaskCounter.last_number + 1},
                )
                .returning(TopicTaskCounter.last_number)
            )
            topic_task_number = (await self._session.execute(statement)).scalar_one()
            if topic_task_number > 9_999:
                raise ValueError("topic task number capacity exceeded")
            public_task_number = thread_id * 10_000 + topic_task_number
        task = Task(
            user_id=user_id,
            source_chat_id=chat_id,
            source_thread_id=thread_id,
            topic_task_number=topic_task_number,
            public_task_number=public_task_number,
            project_id=project_id,
            original_text=original_text,
            task_type=task_type,
            task_draft=dict(task_draft or {}),
            status=TaskStatus.RECEIVED,
        )
        self._session.add(task)
        await self._session.flush()
        return task_record(task)

    async def get(self, task_id: uuid.UUID, *, for_update: bool = False) -> Task:
        statement = select(Task).where(Task.id == task_id)
        if for_update:
            statement = statement.with_for_update()
        task = await self._session.scalar(statement)
        if task is None:
            raise EntityNotFound(f"task not found: {task_id}")
        return task

    async def record(self, task_id: uuid.UUID) -> TaskRecord:
        return task_record(await self.get(task_id))

    async def active_in_topic(self, chat_id: int, thread_id: int) -> tuple[TaskRecord, ...]:
        tasks = (
            await self._session.scalars(
                select(Task)
                .where(
                    Task.source_chat_id == chat_id,
                    Task.source_thread_id == thread_id,
                    Task.status.not_in(USER_TERMINAL_TASK_STATUSES),
                )
                .order_by(Task.created_at, Task.id)
            )
        ).all()
        return tuple(task_record(task) for task in tasks)


class RunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        task_id: uuid.UUID,
        workflow_type: str,
        workflow_version: str,
        budget_mode: str,
        configuration_revision: str,
        policy_revision: str,
        source_interpretation_id: uuid.UUID | None = None,
        status: RunStatus = RunStatus.CREATED,
    ) -> uuid.UUID:
        run = Run(
            task_id=task_id,
            workflow_type=workflow_type,
            workflow_version=workflow_version,
            status=status,
            selected_route={},
            budget_mode=budget_mode,
            configuration_revision=configuration_revision,
            policy_revision=policy_revision,
            source_interpretation_id=source_interpretation_id,
        )
        self._session.add(run)
        await self._session.flush()
        return run.id


class StepRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        run_id: uuid.UUID,
        ordinal: int,
        step_type: str,
        idempotency_class: IdempotencyClass,
        required_capabilities: list[str] | None = None,
        status: StepStatus = StepStatus.PENDING,
        queue_class: QueueClass = QueueClass.LIGHT,
        retry_class: RetryClass = RetryClass.NEVER,
        max_attempts: int = 1,
        timeout_seconds: int = 600,
        dependency_metadata: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
        priority: int = 100,
    ) -> StepRecord:
        step = Step(
            run_id=run_id,
            ordinal=ordinal,
            step_type=step_type,
            status=status,
            queue_class=queue_class,
            required_capabilities=required_capabilities or [],
            dependency_metadata=dict(dependency_metadata or {}),
            payload=dict(payload or {}),
            retry_class=retry_class,
            idempotency_class=idempotency_class,
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
            priority=priority,
        )
        self._session.add(step)
        await self._session.flush()
        return step_record(step)


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(
        self,
        *,
        entity_type: str,
        entity_id: uuid.UUID,
        event_type: str,
        actor_type: str,
        previous_state: str | None = None,
        new_state: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> uuid.UUID:
        event = Event(
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type,
            actor_type=actor_type,
            previous_state=previous_state,
            new_state=new_state,
            payload=dict(payload or {}),
        )
        self._session.add(event)
        await self._session.flush()
        return event.id
