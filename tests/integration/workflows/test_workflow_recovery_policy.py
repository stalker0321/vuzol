"""WP04 integration: fingerprint-gated recovery, shared caps, backpressure."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import Artifact, Run, Step, Task
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import (
    ArtifactStorageState,
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.controls import retry_blocked_step
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.recovery_policy import RecoveryPolicy
from vuzol.workflows.service import commit_step_outcome

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def _seed_run(
    factory: async_sessionmaker[AsyncSession],
    *,
    steps: list[tuple[int, str, StepStatus, dict[str, object]]],
    project_id: str | None = None,
) -> tuple[uuid.UUID, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        task_record = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="recover the project",
            task_type="coding",
            project_id=project_id,
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
        for ordinal, step_type, status, payload in steps:
            await uow.steps.create(
                run_id=run_id,
                ordinal=ordinal,
                step_type=step_type,
                idempotency_class=IdempotencyClass.READ_ONLY,
                retry_class=RetryClass.NEVER,
                queue_class=QueueClass.HEAVY,
                status=status,
                max_attempts=1,
                payload=payload,
            )
    return task_record.id, run_id


async def _start(
    factory: async_sessionmaker[AsyncSession], step_id: uuid.UUID, *, attempt_count: int = 1
) -> LeaseToken:
    async with factory.begin() as session:
        step = await session.get(Step, step_id, with_for_update=True)
        assert step is not None
        step.status = StepStatus.RUNNING
        step.lease_owner = "probe"
        step.lease_generation = 1
        step.attempt_count = attempt_count
    return LeaseToken(
        step=StepRecord(
            id=step_id,
            run_id=(await _run_id(factory, step_id)),
            status=StepStatus.RUNNING,
            lease_generation=1,
            lease_owner="probe",
            lease_expires_at=None,
        ),
        owner="probe",
        generation=1,
    )


async def _run_id(factory: async_sessionmaker[AsyncSession], step_id: uuid.UUID) -> uuid.UUID:
    async with factory() as session:
        step = await session.get(Step, step_id)
        assert step is not None
        return step.run_id


def _validation_failure(result: dict[str, object], summary: str = "gate failed") -> StepOutcome:
    return StepOutcome(
        kind=OutcomeKind.BLOCKED,
        result=result,
        category="validation_gate_failed",
        summary=summary,
    )


async def _steps(factory: async_sessionmaker[AsyncSession], run_id: uuid.UUID) -> list[Step]:
    async with factory() as session:
        return list(
            (
                await session.scalars(
                    select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
                )
            ).all()
        )


async def test_identical_failure_does_not_schedule_a_second_repair(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, run_id = await _seed_run(
        factory,
        steps=[
            (1, "execute_code", StepStatus.COMPLETED, {}),
            (2, "validate", StepStatus.QUEUED, {}),
        ],
    )
    steps = await _steps(factory, run_id)
    validate_id = steps[1].id

    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, token, _validation_failure({"exit_code": 1}))
    first = await _steps(factory, run_id)
    assert len(first) == 3 and first[2].dependency_metadata["template_key"] == "repair_code"
    persisted = next(step for step in first if step.id == validate_id)
    assert (
        persisted.payload["failure_fingerprint"]
        in persisted.payload["failure_fingerprint_history"]
    )

    # Complete the repair so the same validate step is queued again.
    repair_id = first[2].id
    repair_token = await _start(factory, repair_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, repair_token, StepOutcome.succeeded())

    second_token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(
            session, second_token, _validation_failure({"exit_code": 1})
        )

    after = await _steps(factory, run_id)
    assert len(after) == 3, "identical failure must not schedule an identical repair"
    persisted = next(step for step in after if step.id == validate_id)
    assert persisted.status is StepStatus.BLOCKED
    async with factory() as session:
        run = await session.get(Run, run_id)
        assert run is not None and run.status is RunStatus.BLOCKED
    await engine.dispose()


async def test_changed_evidence_schedules_another_repair(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, run_id = await _seed_run(
        factory,
        steps=[
            (1, "execute_code", StepStatus.COMPLETED, {}),
            (2, "validate", StepStatus.QUEUED, {}),
        ],
    )
    steps = await _steps(factory, run_id)
    validate_id = steps[1].id

    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, token, _validation_failure({"exit_code": 1}))
    repair_id = (await _steps(factory, run_id))[2].id
    repair_token = await _start(factory, repair_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, repair_token, StepOutcome.succeeded())

    second_token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(
            session, second_token, _validation_failure({"exit_code": 2})
        )
    after = await _steps(factory, run_id)
    assert len(after) == 4, "changed evidence must allow another repair"
    assert after[3].dependency_metadata["template_key"] == "repair_code"
    await engine.dispose()


async def test_oscillation_a_b_a_stops(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, run_id = await _seed_run(
        factory,
        steps=[
            (1, "execute_code", StepStatus.COMPLETED, {}),
            (2, "validate", StepStatus.QUEUED, {}),
        ],
    )
    validate_id = (await _steps(factory, run_id))[1].id

    async def fail_once(result: dict[str, object]) -> None:
        token = await _start(factory, validate_id)
        async with factory.begin() as session:
            await commit_step_outcome(session, token, _validation_failure(result))
        steps = await _steps(factory, run_id)
        repair = steps[-1]
        if repair.step_type == "execute_code" and repair.id != validate_id:
            repair_token = await _start(factory, repair.id)
            async with factory.begin() as session:
                await commit_step_outcome(session, repair_token, StepOutcome.succeeded())

    await fail_once({"a": 1})
    await fail_once({"b": 2})
    before = len(await _steps(factory, run_id))
    # A second failure with the first evidence must be recognized as seen.
    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, token, _validation_failure({"a": 1}))
    after = await _steps(factory, run_id)
    assert len(after) == before
    assert next(step for step in after if step.id == validate_id).status is StepStatus.BLOCKED
    await engine.dispose()


async def test_task_repair_cap_is_shared_across_steps(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, run_id = await _seed_run(
        factory,
        steps=[
            (1, "execute_code", StepStatus.COMPLETED, {}),
            (
                2,
                "execute_code",
                StepStatus.COMPLETED,
                {},
            ),
            (3, "validate", StepStatus.QUEUED, {}),
        ],
    )
    # Mark the second executor step as an existing repair, consuming the task cap.
    async with factory.begin() as session:
        existing = await session.get(Step, (await _steps(factory, run_id))[1].id)
        assert existing is not None
        existing.dependency_metadata = {"template_key": "repair_code"}

    validate_id = (await _steps(factory, run_id))[2].id
    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(
            session,
            token,
            _validation_failure({"exit_code": 1}),
            recovery_policy=RecoveryPolicy(task_repair_cap=1),
        )
    after = await _steps(factory, run_id)
    assert len(after) == 3, "task-wide repair cap must block the repair"
    assert next(step for step in after if step.id == validate_id).status is StepStatus.BLOCKED
    await engine.dispose()


async def test_manual_retry_does_not_rearm_repairs(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, run_id = await _seed_run(
        factory,
        steps=[
            (1, "execute_code", StepStatus.COMPLETED, {}),
            (2, "validate", StepStatus.QUEUED, {"repair_count": 3, "repair_epoch": 0}),
        ],
    )
    validate_id = (await _steps(factory, run_id))[1].id
    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, token, _validation_failure({"exit_code": 1}))
    persisted = next(step for step in await _steps(factory, run_id) if step.id == validate_id)
    assert persisted.status is StepStatus.BLOCKED

    async with factory.begin() as session:
        await retry_blocked_step(session, validate_id, actor_id="user")
    async with factory() as session:
        task = await session.scalar(
            select(Task).join(Run, Run.task_id == Task.id).where(Run.id == run_id)
        )
        assert task is not None and task.budget_epoch == 1

    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, token, _validation_failure({"exit_code": 1}))
    after = await _steps(factory, run_id)
    assert len(after) == 2, "manual retry must not silently re-arm repairs"
    await engine.dispose()


async def test_backpressure_does_not_burn_attempt_and_is_bounded(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    _, run_id = await _seed_run(
        factory,
        steps=[(1, "execute_model", StepStatus.QUEUED, {})],
    )
    async with factory.begin() as session:
        step = await session.get(Step, (await _steps(factory, run_id))[0].id)
        assert step is not None
        step.retry_class = RetryClass.TRANSIENT
        step.idempotency_class = IdempotencyClass.IDEMPOTENT
        step.max_attempts = 1

    step_id = (await _steps(factory, run_id))[0].id
    policy = RecoveryPolicy(backpressure_wait_cap=2)

    token = await _start(factory, step_id, attempt_count=1)
    async with factory.begin() as session:
        await commit_step_outcome(
            session,
            token,
            StepOutcome(
                kind=OutcomeKind.TRANSIENT_FAILURE,
                result={},
                category="rate_limited",
                summary="slow down",
            ),
            recovery_policy=policy,
        )
    async with factory() as session:
        persisted = await session.get(Step, step_id)
        assert persisted is not None
        assert persisted.status is StepStatus.QUEUED
        assert persisted.attempt_count == 0
        assert persisted.payload["backpressure_count"] == 1

    token = await _start(factory, step_id, attempt_count=1)
    async with factory.begin() as session:
        await commit_step_outcome(
            session,
            token,
            StepOutcome(
                kind=OutcomeKind.TRANSIENT_FAILURE,
                result={},
                category="rate_limited",
                summary="slow down again",
            ),
            recovery_policy=policy,
        )
    # Backpressure wait cap reached: fail closed to attention.
    token = await _start(factory, step_id, attempt_count=1)
    async with factory.begin() as session:
        await commit_step_outcome(
            session,
            token,
            StepOutcome(
                kind=OutcomeKind.TRANSIENT_FAILURE,
                result={},
                category="rate_limited",
                summary="still limited",
            ),
            recovery_policy=policy,
        )
    async with factory() as session:
        persisted = await session.get(Step, step_id)
        run = await session.get(Run, run_id)
        assert persisted is not None and persisted.status is StepStatus.BLOCKED
        assert run is not None and run.status is RunStatus.BLOCKED
    await engine.dispose()


async def test_restart_between_decision_and_enqueue_cannot_double_schedule(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    _, run_id = await _seed_run(
        factory,
        steps=[
            (1, "execute_code", StepStatus.COMPLETED, {}),
            (2, "validate", StepStatus.QUEUED, {}),
        ],
    )
    validate_id = (await _steps(factory, run_id))[1].id
    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, token, _validation_failure({"exit_code": 1}))
    assert len(await _steps(factory, run_id)) == 3

    # A replayed/duplicated commit must not schedule a second repair.
    with pytest.raises(LeaseLost):
        async with factory.begin() as session:
            await commit_step_outcome(session, token, _validation_failure({"exit_code": 1}))
    assert len(await _steps(factory, run_id)) == 3
    await engine.dispose()


async def test_partial_artifacts_survive_attention(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    task_id, run_id = await _seed_run(
        factory,
        steps=[
            (1, "execute_code", StepStatus.COMPLETED, {}),
            (2, "validate", StepStatus.QUEUED, {"repair_count": 3, "repair_epoch": 0}),
        ],
    )
    validate_id = (await _steps(factory, run_id))[1].id
    async with factory.begin() as session:
        validate = await session.get(Step, validate_id)
        assert validate is not None
        validate.result = {"kept": True}
        artifact = Artifact(
            task_id=task_id,
            run_id=run_id,
            step_id=validate_id,
            artifact_type="validation_report",
            content_uri="artifact:kept",
            size_bytes=4,
            content_hash="0" * 64,
            media_type="application/json",
            sensitivity="internal",
            visibility="private",
            storage_state=ArtifactStorageState.AVAILABLE,
            retention_until=datetime.now(UTC) + timedelta(days=1),
        )
        session.add(artifact)
        await session.flush()
        artifact_id = artifact.id

    token = await _start(factory, validate_id)
    async with factory.begin() as session:
        await commit_step_outcome(session, token, _validation_failure({"exit_code": 1}))
    async with factory() as session:
        validate = await session.get(Step, validate_id)
        loaded_artifact = await session.get(Artifact, artifact_id)
        assert validate is not None and validate.result == {"kept": True}
        assert loaded_artifact is not None
        assert validate.status is StepStatus.BLOCKED
    await engine.dispose()
