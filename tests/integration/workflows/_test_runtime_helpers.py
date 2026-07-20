import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update

from vuzol.config import (
    RegistryDocument,
    RuntimeConfiguration,
    Settings,
    WorkflowSettings,
    build_bundle,
)
from vuzol.interpretation.domain import (
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import (
    Event,
    Interpretation,
    Run,
    Step,
    Task,
    TelegramControlAction,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    ControlActionStatus,
    IdempotencyClass,
    QueueClass,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import build_status_card
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.controls import (
    WorkflowControlConsumer,
    cancel_task,
    pause_task,
    resume_task,
)
from vuzol.workflows.dispatch import WorkflowDispatcher
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.recovery import recover_expired_steps
from vuzol.workflows.service import commit_step_outcome, materialize_run, start_run
from vuzol.workflows.worker import CompleteHandler, WorkflowWorker

from ..storage.helpers import storage

__all__ = [
    "UTC",
    "CancellationContext",
    "CompleteHandler",
    "ControlActionStatus",
    "Event",
    "IdempotencyClass",
    "Interpretation",
    "OutcomeKind",
    "QueueClass",
    "RegistryDocument",
    "RiskLevel",
    "Run",
    "RunStatus",
    "RuntimeConfiguration",
    "Settings",
    "Step",
    "StepOutcome",
    "StepStatus",
    "SuggestedComplexity",
    "Task",
    "TaskAction",
    "TaskDraft",
    "TaskOperation",
    "TaskStatus",
    "TaskType",
    "TelegramControlAction",
    "TransactionalOutbox",
    "UnitOfWork",
    "WorkflowControlConsumer",
    "WorkflowDispatcher",
    "WorkflowSettings",
    "WorkflowWorker",
    "asyncio",
    "build_bundle",
    "build_status_card",
    "cancel_task",
    "claim_step",
    "commit_step_outcome",
    "compile_workflow",
    "datetime",
    "func",
    "materialize_run",
    "pause_task",
    "planned_coding_draft",
    "pytest",
    "recover_expired_steps",
    "resume_task",
    "seed_interpreted",
    "select",
    "simple_draft",
    "start_run",
    "start_step",
    "storage",
    "timedelta",
    "update",
    "uuid",
]


def simple_draft() -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.GENERAL,
        operation=TaskOperation.EXPLAIN,
        goal="Answer the question",
        task_summary="Answer the user's question",
        suggested_complexity=SuggestedComplexity.SMALL,
        suggested_risk=RiskLevel.LOW,
        needs_planning=False,
        needs_clarification=False,
        normalized_title="Answer question",
    )


def planned_coding_draft() -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.CODING,
        operation=TaskOperation.MODIFY,
        project_id="vuzol",
        goal="Implement the change",
        task_summary="Implement the requested code change",
        suggested_complexity=SuggestedComplexity.MEDIUM,
        suggested_risk=RiskLevel.MEDIUM,
        needs_planning=True,
        needs_clarification=False,
        normalized_title="Implement change",
    )


async def seed_interpreted(
    factory: object, task_draft: TaskDraft | None = None
) -> tuple[uuid.UUID, uuid.UUID]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed_factory: async_sessionmaker[AsyncSession] = factory
    draft = task_draft or simple_draft()
    async with UnitOfWork(typed_factory) as uow:
        task_record = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="answer this",
            task_type="general",
            task_draft=draft.model_dump(mode="json"),
        )
        assert uow.session is not None
        task = await uow.session.get(Task, task_record.id)
        assert task is not None
        task.status = TaskStatus.INTERPRETED
        interpretation = Interpretation(
            task_id=task.id,
            original_input_hash="a" * 64,
            task_draft=draft.model_dump(mode="json"),
            profile_id="fake",
            model="fake",
            prompt_version="step-05-v1",
            schema_version="1.0",
        )
        uow.session.add(interpretation)
        await uow.session.flush()
        return task.id, interpretation.id
