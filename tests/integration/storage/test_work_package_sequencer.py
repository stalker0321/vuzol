from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from vuzol.config import Settings
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.discussion.sequencer import WorkPackageSequenceConsumer, WorkPackageSequencer
from vuzol.discussion.service import RevisionResult
from vuzol.interpretation.domain import TaskDraft
from vuzol.storage.models import (
    Approval,
    Event,
    Interpretation,
    MaterializationLink,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    UsageRecord,
    WorkPackage,
)
from vuzol.storage.types import (
    ApprovalStatus,
    DeliveryStatus,
    IdempotencyClass,
    PlanRevisionCreatedBy,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorkPackagePauseReason,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.work_package_projections import (
    _work_package_token_totals,
    build_work_package_action_card,
    build_work_package_status_card,
)
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.recovery import recover_expired_steps
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
    async with factory() as session:
        status_card = await build_work_package_status_card(session, created.package_id)
    assert status_card.html.startswith("<b>Working | Auto</b>\n1/2 · Implement step 1")

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


async def test_final_item_approval_is_rendered_as_package_result(
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
    async with UnitOfWork(factory) as uow:
        second = await WorkPackageSequencer(uow).observe_terminal(task_id=first.task_id)
    assert second is not None and second.task_id is not None

    async with factory.begin() as session:
        task = await session.get(Task, second.task_id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.WAITING_APPROVAL
        run = Run(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            status=RunStatus.RUNNING,
            selected_route={},
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
        )
        session.add(run)
        await session.flush()
        step = Step(
            run_id=run.id,
            ordinal=7,
            dependency_metadata={},
            step_type="approval",
            status=StepStatus.WAITING_APPROVAL,
            required_capabilities=[],
            payload={},
            retry_class=RetryClass.NEVER,
            idempotency_class=IdempotencyClass.IDEMPOTENT,
            max_attempts=1,
            timeout_seconds=60,
        )
        session.add(step)
        await session.flush()
        approval = Approval(
            step_id=step.id,
            action_envelope_hash="c" * 64,
            requested_action="apply_result",
            normalized_target="vuzol:main",
            human_summary="Package result",
            token_hash="d" * 64,
            status=ApprovalStatus.PENDING,
            expires_at=datetime.now(UTC) + timedelta(days=1),
        )
        session.add(approval)
        await session.flush()
        approval_id = approval.id

    async with factory() as session:
        card = await build_work_package_action_card(session, created.package_id)
    assert "<b>Plan completed</b>" in card.html
    assert "✅ 1. Implement step 1" in card.html
    assert "✅ 2. Implement step 2" in card.html
    callbacks = {label: callback for row in card.callback_buttons for label, callback in row}
    assert callbacks["Принять"] == f"v1:approve:{approval_id}"
    assert callbacks["Изменить"] == f"v1:redo:{approval_id}"
    assert callbacks["Отклонить"] == f"v1:reject:{approval_id}"
    await engine.dispose()


async def test_replanned_sequence_keeps_unchanged_completed_prefix(
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
    async with UnitOfWork(factory) as uow:
        second = await WorkPackageSequencer(uow).observe_terminal(task_id=first.task_id)
    assert second is not None and second.task_id is not None
    async with factory.begin() as session:
        task = await session.get(Task, second.task_id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.FAILED
    async with UnitOfWork(factory) as uow:
        paused = await WorkPackageSequencer(uow).observe_terminal(task_id=second.task_id)
    assert paused is not None

    async with UnitOfWork(factory) as uow:
        revised = await WorkPackageService(uow).revise_draft(
            package_id=created.package_id,
            expected_status_generation=paused.status_generation,
            plan=_plan(),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
        )
        approved_generation = await WorkPackageService(uow).approve(
            package_id=created.package_id,
            revision_number=2,
            h8=revised.content_hash[:8],
            expected_status_generation=revised.status_generation,
            user_id=42,
        )
    async with UnitOfWork(factory) as uow:
        resumed = await WorkPackageSequencer(uow).start(
            package_id=created.package_id,
            revision_number=2,
            h8=revised.content_hash[:8],
            expected_status_generation=approved_generation,
            user_id=42,
        )

    assert resumed.ordinal == 2 and resumed.task_id is not None
    async with factory.begin() as session:
        runs: list[Run] = []
        for task_id in (first.task_id, resumed.task_id):
            run = Run(
                task_id=task_id,
                workflow_type="coding",
                workflow_version="1",
                status=RunStatus.COMPLETED,
                selected_route={},
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
            )
            session.add(run)
            await session.flush()
            runs.append(run)
        session.add_all(
            [
                UsageRecord(
                    provider="test",
                    profile_id="test-a",
                    model="test-model",
                    task_id=first.task_id,
                    run_id=runs[0].id,
                    input_tokens=100,
                    output_tokens=20,
                    cached_tokens=5,
                    duration_ms=1,
                    outcome="success",
                ),
                UsageRecord(
                    provider="test",
                    profile_id="test-a",
                    model="test-model",
                    task_id=resumed.task_id,
                    run_id=runs[1].id,
                    input_tokens=200,
                    output_tokens=40,
                    cached_tokens=10,
                    duration_ms=1,
                    outcome="success",
                ),
            ]
        )
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        links = tuple(
            (
                await session.scalars(
                    select(MaterializationLink).where(
                        MaterializationLink.work_package_id == created.package_id
                    )
                )
            ).all()
        )
        token_totals = await _work_package_token_totals(session, created.package_id)
    assert package is not None and package.cursor_ordinal == 2
    assert len(links) == 3
    assert sum(link.ordinal == 1 for link in links) == 1
    assert token_totals == (300, 60, 15)
    async with factory() as session:
        status_card = await build_work_package_status_card(session, created.package_id)
        action_card = await build_work_package_action_card(session, created.package_id)
    assert "Токены:" not in status_card.html
    assert "Токены: 300 вх / 60 вых / 15 кэш" in action_card.html
    await engine.dispose()


async def test_stop_cancels_current_item_and_resume_keeps_completed_prefix(
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
    async with UnitOfWork(factory) as uow:
        second = await WorkPackageSequencer(uow).observe_terminal(task_id=first.task_id)
    assert second is not None and second.task_id is not None and second.ordinal == 2

    async with UnitOfWork(factory) as uow:
        stopped_generation = await WorkPackageService(uow).stop_package(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=second.status_generation,
            user_id=42,
        )
    async with factory() as session:
        stopped_task = await session.get(Task, second.task_id)
        assert stopped_task is not None and stopped_task.status is TaskStatus.CANCELLED

    async with UnitOfWork(factory) as uow:
        restarted = await WorkPackageService(uow).restart_plan(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=stopped_generation,
            user_id=42,
        )
        approved = await WorkPackageService(uow).approve(
            package_id=created.package_id,
            revision_number=restarted.revision_number,
            h8=restarted.content_hash[:8],
            expected_status_generation=restarted.status_generation,
            user_id=42,
        )
    async with UnitOfWork(factory) as uow:
        resumed = await WorkPackageSequencer(uow).start(
            package_id=created.package_id,
            revision_number=restarted.revision_number,
            h8=restarted.content_hash[:8],
            expected_status_generation=approved,
            user_id=42,
        )

    assert resumed.ordinal == 2 and resumed.task_id not in {None, second.task_id}
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        links = tuple(
            (
                await session.scalars(
                    select(MaterializationLink).where(
                        MaterializationLink.work_package_id == created.package_id
                    )
                )
            ).all()
        )
    assert package is not None and package.cursor_ordinal == 2
    assert len(links) == 3
    assert sum(link.ordinal == 1 for link in links) == 1
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


async def test_sequence_consumer_dead_letters_permanent_poison_without_crashing_worker(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    async with factory.begin() as session:
        task_id = uuid.uuid4()
        poison = TransactionalOutbox(
            destination="work_package_sequence",
            operation_type="wrong_operation",
            linked_entity_type="task",
            linked_entity_id=task_id,
            idempotency_key=f"test:poison:{task_id}",
            payload={},
        )
        session.add(poison)
        await session.flush()
        poison_id = poison.id

    consumer = WorkPackageSequenceConsumer(
        Settings(environment="test"), factory, owner="sequence-poison", enabled=True
    )
    assert await consumer.process_one()
    async with factory() as session:
        item = await session.get(TransactionalOutbox, poison_id)
        assert item is not None and item.status is DeliveryStatus.DEAD_LETTER
        assert item.last_error_category == "work_package_sequence_invalid_sequence_item"
    assert not await consumer.process_one()
    await engine.dispose()


async def test_expired_unsafe_step_pauses_owning_package_via_durable_sequence(
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
        task = await session.get(Task, first.task_id)
        interpretation = await session.scalar(
            select(Interpretation).where(Interpretation.task_id == first.task_id)
        )
        assert task is not None and interpretation is not None
        run = await materialize_run(
            session,
            task_id=task.id,
            workflow=compile_workflow(
                TaskDraft.model_validate(task.task_draft), interpretation_id=interpretation.id
            ),
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            prompt_revision=None,
            automatic_start=True,
        )
        step = await session.scalar(
            select(Step).where(Step.run_id == run.id, Step.status == StepStatus.QUEUED)
        )
        assert step is not None
        step.status = StepStatus.RUNNING
        step.idempotency_class = IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE
        step.lease_owner = "crashed-worker"
        step.lease_generation = 1
        step.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)

    async with factory.begin() as session:
        assert await recover_expired_steps(session, batch_size=1) == 1
    async with factory() as session:
        task = await session.get(Task, first.task_id)
        sequence_count = await session.scalar(
            select(func.count())
            .select_from(TransactionalOutbox)
            .where(
                TransactionalOutbox.destination == "work_package_sequence",
                TransactionalOutbox.linked_entity_id == first.task_id,
            )
        )
        assert task is not None and task.status is TaskStatus.BLOCKED
        assert sequence_count == 1

    consumer = WorkPackageSequenceConsumer(
        Settings(environment="test"), factory, owner="recovery-sequence", enabled=True
    )
    assert await consumer.process_one()
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        assert package is not None and package.status is WorkPackageStatus.PAUSED
        assert package.pause_reason is WorkPackagePauseReason.ITEM_BLOCKED
        assert package.last_failure_task_id == first.task_id
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
