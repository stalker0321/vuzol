import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import storage
from vuzol.config import Settings
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import Event, Run, Step, Task
from vuzol.storage.types import (
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.service import commit_step_outcome

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def test_validation_failure_gets_one_worker_repair_then_revalidates(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        task_record = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="repair the project",
            task_type="coding",
            project_id="vuzol",
        )
        task = await uow.session.get(Task, task_record.id)  # type: ignore[union-attr]
        assert task is not None
        task.status = TaskStatus.VALIDATING
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        await uow.steps.create(
            run_id=run_id,
            ordinal=1,
            step_type="execute_code",
            idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
            retry_class=RetryClass.POLICY,
            queue_class=QueueClass.HEAVY,
            status=StepStatus.COMPLETED,
            max_attempts=1,
        )
        await uow.steps.create(
            run_id=run_id,
            ordinal=2,
            step_type="validate",
            idempotency_class=IdempotencyClass.READ_ONLY,
            retry_class=RetryClass.NEVER,
            queue_class=QueueClass.HEAVY,
            status=StepStatus.QUEUED,
            max_attempts=1,
        )

    async with factory.begin() as session:
        validation_token = await claim_step(
            session,
            owner="validator",
            lease_seconds=60,
            capabilities=frozenset(),
            queue_classes=frozenset({QueueClass.HEAVY}),
            settings=Settings(environment="test"),
        )
    assert validation_token is not None
    async with factory.begin() as session:
        await start_step(session, validation_token)
        await commit_step_outcome(
            session,
            validation_token,
            StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"gate": "make test", "exit_code": 1},
                category="validation_gate_failed",
                summary="make test failed",
            ),
        )

    async with factory() as session:
        steps = tuple(
            (
                await session.scalars(
                    select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
                )
            ).all()
        )
        task = await session.get(Task, task_record.id)
        run = await session.get(Run, run_id)
    assert [step.status for step in steps] == [
        StepStatus.COMPLETED,
        StepStatus.BLOCKED,
        StepStatus.QUEUED,
    ]
    assert steps[2].payload["repair_for_step_id"] == str(steps[1].id)
    assert steps[2].payload["repair_context"]["summary"] == "make test failed"
    assert task is not None and task.status is TaskStatus.RETRYING
    assert run is not None and run.status is RunStatus.RUNNING

    async with factory.begin() as session:
        repair_token = await claim_step(
            session,
            owner="worker",
            lease_seconds=60,
            capabilities=frozenset(),
            queue_classes=frozenset({QueueClass.HEAVY}),
            settings=Settings(environment="test"),
        )
    assert repair_token is not None and repair_token.step.id == steps[2].id
    async with factory.begin() as session:
        await start_step(session, repair_token)
        await commit_step_outcome(session, repair_token, StepOutcome.succeeded())
    async with factory() as session:
        validation = await session.get(Step, steps[1].id)
    assert validation is not None and validation.status is StepStatus.QUEUED

    async with factory.begin() as session:
        retry_token = await claim_step(
            session,
            owner="validator",
            lease_seconds=60,
            capabilities=frozenset(),
            queue_classes=frozenset({QueueClass.HEAVY}),
            settings=Settings(environment="test"),
        )
    assert retry_token is not None and retry_token.step.id == steps[1].id
    async with factory.begin() as session:
        await start_step(session, retry_token)
        await commit_step_outcome(session, retry_token, StepOutcome.succeeded())
    async with factory() as session:
        task = await session.get(Task, task_record.id)
        run = await session.get(Run, run_id)
        events = set(
            await session.scalars(select(Event.event_type).where(Event.entity_id == run_id))
        )
    assert task is not None and task.status is TaskStatus.COMPLETED
    assert run is not None and run.status is RunStatus.COMPLETED
    assert {"workflow.repair_scheduled", "workflow.repair_completed"} <= events
    await engine.dispose()


async def test_fourth_validation_failure_stops_instead_of_looping(postgres_dsn: str) -> None:
    # The end-to-end case above proves the loop. This focused state check protects its hard bound.
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        task_record = await uow.tasks.create(
            user_id=1, chat_id=-100, original_text="bounded repair", task_type="coding"
        )
        task = await uow.session.get(Task, task_record.id)  # type: ignore[union-attr]
        assert task is not None
        task.status = TaskStatus.VALIDATING
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        await uow.steps.create(
            run_id=run_id,
            ordinal=1,
            step_type="validate",
            idempotency_class=IdempotencyClass.READ_ONLY,
            retry_class=RetryClass.NEVER,
            queue_class=QueueClass.HEAVY,
            status=StepStatus.QUEUED,
            max_attempts=1,
            payload={"repair_count": 3, "repair_epoch": 0},
        )
    async with factory.begin() as session:
        token = await claim_step(
            session,
            owner="validator",
            lease_seconds=60,
            capabilities=frozenset(),
            queue_classes=frozenset({QueueClass.HEAVY}),
            settings=Settings(environment="test"),
        )
    assert token is not None
    async with factory.begin() as session:
        await start_step(session, token)
        await commit_step_outcome(
            session,
            token,
            StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="validation_gate_failed",
                summary="still broken",
            ),
        )
    async with factory() as session:
        run = await session.get(Run, run_id)
        step_count = len(tuple(await session.scalars(select(Step).where(Step.run_id == run_id))))
    assert run is not None and run.status is RunStatus.BLOCKED
    assert step_count == 1
    await engine.dispose()


async def test_review_repair_is_revalidated_before_review_resumes(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        task_record = await uow.tasks.create(
            user_id=1, chat_id=-100, original_text="review repair", task_type="coding"
        )
        task = await uow.session.get(Task, task_record.id)  # type: ignore[union-attr]
        assert task is not None
        task.status = TaskStatus.REVIEWING
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        for ordinal, step_type, status in (
            (1, "execute_code", StepStatus.COMPLETED),
            (2, "validate", StepStatus.COMPLETED),
            (3, "review", StepStatus.QUEUED),
        ):
            await uow.steps.create(
                run_id=run_id,
                ordinal=ordinal,
                step_type=step_type,
                idempotency_class=IdempotencyClass.READ_ONLY,
                retry_class=RetryClass.NEVER,
                queue_class=QueueClass.HEAVY,
                status=status,
                max_attempts=1,
            )

    async def complete_next(owner: str, outcome: StepOutcome) -> Step:
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner=owner,
                lease_seconds=60,
                capabilities=frozenset(),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=Settings(environment="test"),
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
            await commit_step_outcome(session, token, outcome)
        async with factory() as session:
            persisted = await session.get(Step, token.step.id)
            assert persisted is not None
            return persisted

    review = await complete_next(
        "reviewer",
        StepOutcome(
            kind=OutcomeKind.BLOCKED,
            result={"finding": "missing edge case"},
            category="review_changes_requested",
            summary="cover the edge case",
        ),
    )
    repair = await complete_next("worker", StepOutcome.succeeded())
    assert repair.payload["repair_for_step_id"] == str(review.id)

    async with factory() as session:
        revalidation = await session.scalar(
            select(Step).where(
                Step.run_id == run_id,
                Step.payload["resume_after_validation_step_id"].as_string() == str(review.id),
            )
        )
        persisted_review = await session.get(Step, review.id)
    assert revalidation is not None and revalidation.status is StepStatus.QUEUED
    assert revalidation.payload["amend_repaired_result"] is True
    assert persisted_review is not None and persisted_review.status is StepStatus.BLOCKED

    completed_validation = await complete_next("validator", StepOutcome.succeeded())
    assert completed_validation.id == revalidation.id
    async with factory() as session:
        persisted_review = await session.get(Step, review.id)
        task = await session.get(Task, task_record.id)
    assert persisted_review is not None and persisted_review.status is StepStatus.QUEUED
    assert task is not None and task.status is TaskStatus.REVIEWING
    await engine.dispose()
