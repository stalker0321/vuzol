"""Real claim/execute regression for an owner retry with an already-bound reviewer."""

import asyncio

import pytest

from vuzol.config import Settings
from vuzol.storage.models import Step, Task
from vuzol.storage.types import IdempotencyClass, QueueClass, StepStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.worker import CompleteHandler, WorkflowWorker

from ..storage.helpers import seed_task_run_step, storage


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("profile_limits", "exhausted", "claimed"),
    [
        (None, False, True),
        ({"grok-a": 1}, False, True),
        ({}, False, False),
        ({"other-profile": 1}, False, False),
        ({"grok-a": 1}, True, False),
    ],
)
def test_profile_bound_review_retry_runs_once(postgres_dsn, profile_limits, exhausted, claimed):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        try:
            task, _run, record = await seed_task_run_step(
                factory, step_type="review", queue_class=QueueClass.LIGHT, max_attempts=3
            )
            # Real review is not the final task step: successful retry hands off
            # to artifact production instead of completing directly from RETRYING.
            async with UnitOfWork(factory) as uow:
                successor = await uow.steps.create(
                    run_id=_run,
                    ordinal=2,
                    step_type="produce_artifacts",
                    idempotency_class=IdempotencyClass.READ_ONLY,
                    queue_class=QueueClass.HEAVY,
                    dependency_metadata={"predecessor_ordinals": [1]},
                )
            async with factory.begin() as session:
                step = await session.get(Step, record.id)
                step.executor_profile_id = "grok-a"
                step.attempt_count = 3 if exhausted else 2
                step.lease_generation = 2
                model = await session.get(Task, task.id)
                model.status = TaskStatus.RETRYING
            worker = WorkflowWorker(
                Settings(environment="test"),
                factory,
                owner="executor:review",
                handlers={"review": CompleteHandler()},
                queue_classes=frozenset({QueueClass.LIGHT}),
                profile_limits=profile_limits,
            )
            assert await worker.process_one() is claimed
            assert not await worker.process_one()
            async with factory() as session:
                step = await session.get(Step, record.id)
                assert step.status is (StepStatus.COMPLETED if claimed else StepStatus.QUEUED)
                assert step.attempt_count == (3 if claimed or exhausted else 2)
                assert step.lease_generation == (3 if claimed else 2)
                assert step.executor_profile_id == "grok-a"
                following = await session.get(Step, successor.id)
                assert following.status is (StepStatus.QUEUED if claimed else StepStatus.PENDING)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(("profile_limits", "expected"), [(None, 2), ({"grok-a": 1}, 1)])
def test_review_worker_profile_cap_still_fences_concurrent_claims(
    postgres_dsn, profile_limits, expected
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        try:
            for _ in range(2):
                _task, _run, record = await seed_task_run_step(
                    factory, step_type="review", queue_class=QueueClass.LIGHT
                )
                async with factory.begin() as session:
                    step = await session.get(Step, record.id)
                    step.executor_profile_id = "grok-a"
            worker = WorkflowWorker(
                Settings(environment="test"),
                factory,
                owner="executor:review",
                handlers={"review": CompleteHandler()},
                profile_limits=profile_limits,
                queue_classes=frozenset({QueueClass.LIGHT}),
            )

            async def claim():
                async with factory.begin() as session:
                    return await worker._claim(session, {QueueClass.LIGHT: 2})

            claims = await asyncio.gather(claim(), claim())
            assert sum(token is not None for token in claims) == expected
        finally:
            await engine.dispose()

    asyncio.run(scenario())
