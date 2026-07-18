import uuid

from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from vuzol.config import Settings
from vuzol.storage import create_engine, create_session_factory
from vuzol.storage.models import Task
from vuzol.storage.records import StepRecord, TaskRecord
from vuzol.storage.types import (
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork


def storage(postgres_dsn: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    settings = Settings(environment="test")
    engine = create_engine(settings, SecretStr(postgres_dsn))
    return engine, create_session_factory(engine)


async def seed_task_run_step(
    factory: async_sessionmaker[AsyncSession],
    *,
    step_status: StepStatus = StepStatus.QUEUED,
    capabilities: list[str] | None = None,
    queue_class: QueueClass = QueueClass.LIGHT,
    retry_class: RetryClass = RetryClass.NEVER,
    max_attempts: int = 3,
    idempotency_class: IdempotencyClass = IdempotencyClass.ISOLATED_RETRYABLE,
    step_type: str = "execute_code",
) -> tuple[TaskRecord, uuid.UUID, StepRecord]:
    async with UnitOfWork(factory) as uow:
        task = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="test request",
            task_type="coding",
        )
        # Align with post-start runtime so outcome commit may derive task status.
        session = uow.session
        assert session is not None
        row = await session.get(Task, task.id)
        if row is not None:
            row.status = TaskStatus.EXECUTING
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        step = await uow.steps.create(
            run_id=run_id,
            ordinal=1,
            step_type=step_type,
            idempotency_class=idempotency_class,
            required_capabilities=capabilities,
            status=step_status,
            queue_class=queue_class,
            retry_class=retry_class,
            max_attempts=max_attempts,
        )
    return task, run_id, step
