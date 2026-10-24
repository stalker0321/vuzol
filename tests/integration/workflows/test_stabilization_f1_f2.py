"""F1/F2: pause stays jointly continuable, and a read-only timeout is not a mutation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config import Settings
from vuzol.storage.models import Run, Step, Task
from vuzol.storage.types import IdempotencyClass, RunStatus, StepStatus, TaskStatus
from vuzol.workflows.application import Principal, apply_task_command
from vuzol.workflows.controls import cancel_task, resume_task
from vuzol.workflows.domain import StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.recovery import recover_expired_steps
from vuzol.workflows.retry_policy import (
    LEASE_EXPIRED_ATTEMPTS_EXHAUSTED,
    blocked_step_is_retryable,
)
from vuzol.workflows.worker import WorkflowWorker


class _SlowHandler:
    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        del request, cancellation
        await asyncio.Event().wait()
        return StepOutcome.succeeded()


async def _expire_running_review(factory, *, idempotency: IdempotencyClass, attempts: int) -> tuple:
    task, run_id, step = await seed_task_run_step(
        factory,
        step_status=StepStatus.RUNNING,
        step_type="review",
        max_attempts=attempts,
        idempotency_class=idempotency,
    )
    async with factory.begin() as session:
        row = await session.get(Step, step.id)
        assert row is not None
        row.attempt_count = attempts
        row.lease_owner = "dead-worker"
        row.lease_generation = 1
        row.lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)
        code = Step(
            run_id=run_id,
            ordinal=2,
            step_type="execute_code",
            status=StepStatus.COMPLETED,
            idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
            queue_class=row.queue_class,
            retry_class=row.retry_class,
            max_attempts=1,
            timeout_seconds=row.timeout_seconds,
            required_capabilities=["code_edit"],
        )
        session.add(code)
        await session.flush()
        code_id = code.id
        # Fault fixture for a persisted pre-T133 soft pause followed by a crash.
        # New pause_task revokes this lease immediately; keep testing recovery
        # of historical rows rather than weakening the lease-expiry assertions.
        task_row = await session.get(Task, task.id)
        run_row = await session.get(Run, run_id)
        task_row.status = TaskStatus.PAUSED
        run_row.status = RunStatus.PAUSED
    return task.id, run_id, step.id, code_id


@pytest.mark.postgresql
def test_paused_exhausted_readonly_lease_stays_continuable(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, run_id, step_id, code_id = await _expire_running_review(
            factory, idempotency=IdempotencyClass.READ_ONLY, attempts=1
        )
        async with factory.begin() as session:
            assert await recover_expired_steps(session, batch_size=10) == 1
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            code = await session.get(Step, code_id)
            assert task is not None and task.status is TaskStatus.PAUSED
            assert run is not None and run.status is RunStatus.PAUSED
            assert step is not None
            assert step.status is StepStatus.BLOCKED
            assert step.failure_category == LEASE_EXPIRED_ATTEMPTS_EXHAUSTED
            assert step.unknown_effects is False
            assert code is not None and code.status is StepStatus.COMPLETED
            version = task.version
            attempts_before = step.attempt_count
        async with factory() as session, session.begin():
            with pytest.raises(ValueError, match="run is paused"):
                await apply_task_command(
                    session,
                    task_id=task_id,
                    command="retry",
                    principal=Principal(1, "cli"),
                    expected_task_version=version,
                    step_id=step_id,
                )
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="stale task version"):
                await resume_task(session, task_id, actor_id="1", expected_task_version=version - 1)
        async with factory.begin() as session:
            await resume_task(session, task_id, actor_id="1", expected_task_version=version)
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            code = await session.get(Step, code_id)
            assert task is not None and task.status is not TaskStatus.FAILED
            assert run is not None and run.status is RunStatus.RUNNING
            assert step is not None and step.status is StepStatus.QUEUED
            assert step.attempt_count == attempts_before
            assert step.max_attempts == attempts_before + 1
            assert code is not None and code.status is StepStatus.COMPLETED
            resumed_version = task.version
        async with factory.begin() as session:
            await resume_task(session, task_id, actor_id="1", expected_task_version=resumed_version)
        async with factory() as session:
            run = await session.get(Run, run_id)
            assert run is not None and run.status is RunStatus.RUNNING
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_paused_unknown_write_can_be_cancelled_but_not_blindly_resumed(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, run_id, step_id, _code_id = await _expire_running_review(
            factory, idempotency=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE, attempts=1
        )
        async with factory.begin() as session:
            assert await recover_expired_steps(session, batch_size=10) == 1
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            assert task is not None and task.status is TaskStatus.PAUSED
            assert run is not None and run.status is RunStatus.PAUSED
            assert step is not None and step.status is StepStatus.BLOCKED
            assert step.unknown_effects is True
            version = task.version
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="unknown external effects"):
                await resume_task(session, task_id, actor_id="1", expected_task_version=version)
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task is not None and task.status is TaskStatus.PAUSED
            version = task.version
        async with factory.begin() as session:
            await cancel_task(session, task_id, actor_id="1", expected_task_version=version)
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            assert task is not None and task.status is TaskStatus.CANCELLED
            assert run is not None and run.status is RunStatus.CANCELLED
            assert step is not None and step.status is StepStatus.CANCELLED
        async with factory.begin() as session:
            await cancel_task(session, task_id, actor_id="1")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_legacy_failed_task_with_paused_run_can_cancel_without_rewriting_failure(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, run_id, step = await seed_task_run_step(
            factory,
            step_status=StepStatus.FAILED,
            step_type="review",
            idempotency_class=IdempotencyClass.READ_ONLY,
        )
        async with factory.begin() as session:
            task_row = await session.get(Task, task.id)
            run_row = await session.get(Run, run_id)
            assert task_row is not None and run_row is not None
            task_row.status = TaskStatus.FAILED
            run_row.status = RunStatus.PAUSED
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="no automatic repair"):
                await resume_task(session, task.id, actor_id="1")
            await cancel_task(session, task.id, actor_id="1")
        async with factory() as session:
            task_row = await session.get(Task, task.id)
            run_row = await session.get(Run, run_id)
            step_row = await session.get(Step, step.id)
            assert task_row is not None and task_row.status is TaskStatus.FAILED
            assert run_row is not None and run_row.status is RunStatus.CANCELLED
            assert step_row is not None and step_row.status is StepStatus.FAILED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_pause_lock_and_recovery_do_not_deadlock(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, run_id, step = await seed_task_run_step(
            factory,
            step_status=StepStatus.RUNNING,
            step_type="review",
            max_attempts=3,
            idempotency_class=IdempotencyClass.READ_ONLY,
        )
        async with factory.begin() as session:
            row = await session.get(Step, step.id)
            assert row is not None
            row.attempt_count = 1
            row.lease_owner = "dead-worker"
            row.lease_generation = 1
            row.lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)

        started = asyncio.Event()
        release = asyncio.Event()

        async def hold_parent_locks() -> None:
            async with factory.begin() as session:
                await session.scalar(select(Task).where(Task.id == task.id).with_for_update())
                await session.scalar(select(Run).where(Run.id == run_id).with_for_update())
                started.set()
                await release.wait()
                await session.scalar(select(Step).where(Step.id == step.id).with_for_update())

        async def recover() -> None:
            await started.wait()
            async with factory.begin() as session:
                await recover_expired_steps(session, batch_size=10)

        holder = asyncio.create_task(hold_parent_locks())
        await asyncio.wait_for(started.wait(), timeout=5)
        recoverer = asyncio.create_task(recover())
        await asyncio.sleep(0.4)
        release.set()
        await asyncio.wait_for(holder, timeout=5)
        await asyncio.wait_for(recoverer, timeout=5)
        async with factory() as session:
            loaded = await session.get(Step, step.id)
            run = await session.get(Run, run_id)
            parent = await session.get(Task, task.id)
            assert loaded is not None and loaded.status is StepStatus.QUEUED
            assert run is not None and run.status is RunStatus.RUNNING
            assert parent is not None and parent.status is not TaskStatus.FAILED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("idempotency", "capabilities", "uncertain"),
    [
        (IdempotencyClass.READ_ONLY, None, False),
        (IdempotencyClass.READ_ONLY, ["project_shell"], True),
        (IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE, None, True),
    ],
)
def test_timeout_marks_mutation_only_when_a_repeat_could_write(
    postgres_dsn: str,
    idempotency: IdempotencyClass,
    capabilities: list[str] | None,
    uncertain: bool,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _task, _run_id, step = await seed_task_run_step(
            factory,
            step_type="review",
            max_attempts=3,
            idempotency_class=idempotency,
            capabilities=capabilities,
        )
        async with factory.begin() as session:
            row = await session.get(Step, step.id)
            assert row is not None
            row.timeout_seconds = 1
        worker = WorkflowWorker(
            Settings(environment="test"),
            factory,
            owner="timeout-worker",
            handlers={"review": _SlowHandler()},
        )
        assert await worker.process_one()
        async with factory() as session:
            loaded = await session.get(Step, step.id)
            assert loaded is not None
            assert loaded.status is StepStatus.BLOCKED
            assert loaded.failure_category == "timeout"
            assert loaded.unknown_effects is uncertain
            assert blocked_step_is_retryable(loaded) is not uncertain
        await engine.dispose()

    asyncio.run(scenario())
