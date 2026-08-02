from __future__ import annotations

import pytest
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from vuzol.config import Settings
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.discussion.sequencer import WorkPackageSequenceConsumer, WorkPackageSequencer
from vuzol.discussion.service import RevisionResult
from vuzol.interpretation.domain import TaskDraft
from vuzol.storage.models import (
    Event,
    Interpretation,
    MaterializationLink,
    Step,
    Task,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import (
    PlanRevisionCreatedBy,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorkPackagePauseReason,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.service import materialize_run

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _plan() -> PlanDraft:
    return PlanDraft(
        title="Sequential package",
        items=tuple(
            PlanItemDraft(
                local_id=f"step-{ordinal}",
                summary=f"Implement step {ordinal}",
                goal=f"Finish goal {ordinal}",
                expected_outcome=f"Outcome {ordinal}",
                completion_criteria=(f"Check {ordinal} passes",),
                allowed_scope="src/vuzol/**",
            )
            for ordinal in (1, 2)
        ),
    )


async def _approved(factory: object) -> RevisionResult:
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
        created = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=_plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
        await WorkPackageService(uow).approve(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=1,
            user_id=42,
        )
    return created


async def test_sequencer_is_one_ahead_idempotent_and_completes(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _approved(factory)
    async with UnitOfWork(factory) as uow:
        first = await WorkPackageSequencer(uow).start(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=2,
            user_id=42,
        )
    assert first.ordinal == 1 and first.task_id is not None

    async with factory.begin() as session:
        task = await session.get(Task, first.task_id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.COMPLETED
    async with UnitOfWork(factory) as uow:
        second = await WorkPackageSequencer(uow).observe_terminal(task_id=first.task_id)
    assert second is not None and second.ordinal == 2 and second.task_id is not None

    async with UnitOfWork(factory) as uow:
        duplicate = await WorkPackageSequencer(uow).observe_terminal(task_id=first.task_id)
    assert duplicate is not None and duplicate.task_id == first.task_id
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Task)) == 2

    async with factory.begin() as session:
        task = await session.get(Task, second.task_id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.COMPLETED
    async with UnitOfWork(factory) as uow:
        completed = await WorkPackageSequencer(uow).observe_terminal(task_id=second.task_id)
    assert completed is not None and completed.completed
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        assert package is not None and package.status is WorkPackageStatus.COMPLETED
        assert package.cursor_ordinal is None
        assert await session.scalar(select(func.count()).select_from(Task)) == 2
        assert await session.scalar(select(func.count()).select_from(MaterializationLink)) == 2
        event_types = set(
            await session.scalars(
                select(Event.event_type).where(Event.entity_id == created.package_id)
            )
        )
    assert {"work_package.started", "work_package.completed"} <= event_types
    await engine.dispose()


@pytest.mark.parametrize(
    ("task_status", "reason"),
    [
        (TaskStatus.FAILED, WorkPackagePauseReason.ITEM_FAILED),
        (TaskStatus.BLOCKED, WorkPackagePauseReason.ITEM_BLOCKED),
    ],
)
async def test_terminal_failure_pauses_without_materializing_next(
    postgres_dsn: str, task_status: TaskStatus, reason: WorkPackagePauseReason
) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _approved(factory)
    async with UnitOfWork(factory) as uow:
        first = await WorkPackageSequencer(uow).start(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=2,
            user_id=42,
        )
    assert first.task_id is not None
    async with factory.begin() as session:
        task = await session.get(Task, first.task_id, with_for_update=True)
        assert task is not None
        task.status = task_status
    async with UnitOfWork(factory) as uow:
        await WorkPackageSequencer(uow).observe_terminal(task_id=first.task_id)
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        assert package is not None and package.status is WorkPackageStatus.PAUSED
        assert package.pause_reason is reason and package.cursor_ordinal == 1
        assert package.last_failure_task_id == first.task_id
        assert await session.scalar(select(func.count()).select_from(Task)) == 1
    await engine.dispose()


async def test_sequence_consumer_is_default_off_and_recovers_from_outbox(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _approved(factory)
    async with UnitOfWork(factory) as uow:
        first = await WorkPackageSequencer(uow).start(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=2,
            user_id=42,
        )
    assert first.task_id is not None
    async with factory.begin() as session:
        task = await session.get(Task, first.task_id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.COMPLETED
        session.add(
            TransactionalOutbox(
                destination="work_package_sequence",
                operation_type="observe_task_terminal",
                linked_entity_type="task",
                linked_entity_id=task.id,
                idempotency_key=f"test:work-package-terminal:{task.id}",
                payload={},
            )
        )
    settings = Settings(environment="test")
    disabled = WorkPackageSequenceConsumer(
        settings, factory, owner="sequence-disabled", enabled=False
    )
    assert not await disabled.process_one()
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        assert package is not None and package.cursor_ordinal == 1

    enabled = WorkPackageSequenceConsumer(settings, factory, owner="sequence", enabled=True)
    assert await enabled.process_one()
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        assert package is not None and package.cursor_ordinal == 2
        assert await session.scalar(select(func.count()).select_from(Task)) == 2
    await engine.dispose()


async def test_package_retry_delegates_only_to_safe_blocked_workflow(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    created = await _approved(factory)
    async with UnitOfWork(factory) as uow:
        first = await WorkPackageSequencer(uow).start(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=2,
            user_id=42,
        )
    assert first.task_id is not None
    async with factory.begin() as session:
        interpretation = await session.scalar(
            select(Interpretation).where(Interpretation.task_id == first.task_id)
        )
        assert interpretation is not None
        run = await materialize_run(
            session,
            task_id=first.task_id,
            workflow=compile_workflow(
                TaskDraft.model_validate(interpretation.task_draft),
                interpretation_id=interpretation.id,
            ),
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            prompt_revision=interpretation.prompt_version,
            automatic_start=True,
        )
        step = await session.scalar(
            select(Step).where(Step.run_id == run.id, Step.status == StepStatus.QUEUED)
        )
        task = await session.get(Task, first.task_id)
        assert step is not None and task is not None
        step.status = StepStatus.BLOCKED
        step.failure_category = "provider_unavailable"
        step.failure_summary = "temporary"
        run.status = RunStatus.BLOCKED
        task.status = TaskStatus.BLOCKED
        step_id = step.id
    async with UnitOfWork(factory) as uow:
        paused = await WorkPackageSequencer(uow).observe_terminal(task_id=first.task_id)
    assert paused is not None
    async with UnitOfWork(factory) as uow:
        generation = await WorkPackageService(uow).retry_item(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=paused.status_generation,
            user_id=42,
        )
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        task = await session.get(Task, first.task_id)
        step = await session.get(Step, step_id)
        assert package is not None and package.status is WorkPackageStatus.RUNNING
        assert package.version == generation and package.cursor_ordinal == 1
        assert task is not None and task.status is TaskStatus.RETRYING
        assert step is not None and step.status is StepStatus.QUEUED
    await engine.dispose()
