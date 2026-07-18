"""Workflow recovery tests (split for cohesion)."""

from __future__ import annotations

from ._test_runtime_helpers import (
    IdempotencyClass,
    Run,
    RunStatus,
    Step,
    StepStatus,
    Task,
    TaskStatus,
    asyncio,
    claim_step,
    compile_workflow,
    func,
    materialize_run,
    pytest,
    recover_expired_steps,
    seed_interpreted,
    select,
    simple_draft,
    start_step,
    storage,
    timedelta,
    update,
)


@pytest.mark.postgresql
def test_recovery_requeues_safe_and_blocks_unknown(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory)
        async with factory.begin() as session:
            await materialize_run(
                session,
                task_id=task_id,
                workflow=compile_workflow(simple_draft(), interpretation_id=interpretation_id),
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision=None,
                automatic_start=True,
            )
        async with factory.begin() as session:
            first = await claim_step(
                session, owner="worker", lease_seconds=60, capabilities=frozenset()
            )
        assert first is not None
        async with factory.begin() as session:
            await start_step(session, first)
            await session.execute(
                update(Step)
                .where(Step.id == first.step.id)
                .values(lease_expires_at=func.now() - timedelta(seconds=1))
            )
            assert await recover_expired_steps(session, batch_size=10) == 1
        async with factory() as session:
            safe = await session.get(Step, first.step.id)
            assert safe is not None and safe.status is StepStatus.QUEUED
        async with factory.begin() as session:
            second = await claim_step(
                session, owner="worker", lease_seconds=60, capabilities=frozenset()
            )
        assert second is not None
        async with factory.begin() as session:
            await start_step(session, second)
            await session.execute(
                update(Step)
                .where(Step.id == second.step.id)
                .values(
                    idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                    lease_expires_at=func.now() - timedelta(seconds=1),
                )
            )
            assert await recover_expired_steps(session, batch_size=10) == 1
        async with factory() as session:
            blocked = await session.get(Step, second.step.id)
            assert blocked is not None and blocked.status is StepStatus.BLOCKED
            assert blocked.unknown_effects
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_recovery_fails_safe_step_when_attempts_are_exhausted(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory)
        async with factory.begin() as session:
            run = await materialize_run(
                session,
                task_id=task_id,
                workflow=compile_workflow(simple_draft(), interpretation_id=interpretation_id),
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision=None,
                automatic_start=True,
            )
            step = await session.scalar(
                select(Step).where(Step.run_id == run.id, Step.status == StepStatus.QUEUED)
            )
            assert step is not None
            step.max_attempts = 1
        async with factory.begin() as session:
            token = await claim_step(
                session, owner="worker", lease_seconds=60, capabilities=frozenset()
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
            await session.execute(
                update(Step)
                .where(Step.id == token.step.id)
                .values(lease_expires_at=func.now() - timedelta(seconds=1))
            )
            assert await recover_expired_steps(session, batch_size=1) == 1
        async with factory() as session:
            loaded_run = await session.get(Run, token.step.run_id)
            step = await session.get(Step, token.step.id)
            task = await session.get(Task, task_id)
            assert loaded_run is not None and loaded_run.status is RunStatus.FAILED
            assert step is not None and step.status is StepStatus.FAILED
            assert task is not None and task.status is TaskStatus.FAILED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_recovery_requeues_leased_only_even_when_attempts_exhausted(
    postgres_dsn: str,
) -> None:
    """LEASED without start is effect-free: refund the claim attempt and requeue."""

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory)
        async with factory.begin() as session:
            run = await materialize_run(
                session,
                task_id=task_id,
                workflow=compile_workflow(simple_draft(), interpretation_id=interpretation_id),
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision=None,
                automatic_start=True,
            )
            step = await session.scalar(
                select(Step).where(Step.run_id == run.id, Step.status == StepStatus.QUEUED)
            )
            assert step is not None
            step.max_attempts = 1
        async with factory.begin() as session:
            token = await claim_step(
                session, owner="worker", lease_seconds=60, capabilities=frozenset()
            )
        assert token is not None
        async with factory.begin() as session:
            # Do not start_step — still LEASED when lease expires.
            await session.execute(
                update(Step)
                .where(Step.id == token.step.id)
                .values(lease_expires_at=func.now() - timedelta(seconds=1))
            )
            assert await recover_expired_steps(session, batch_size=1) == 1
        async with factory() as session:
            loaded_run = await session.get(Run, token.step.run_id)
            step = await session.get(Step, token.step.id)
            task = await session.get(Task, task_id)
            assert loaded_run is not None and loaded_run.status is RunStatus.RUNNING
            assert step is not None and step.status is StepStatus.QUEUED
            assert step.attempt_count == 0
            assert task is not None and task.status is not TaskStatus.FAILED
        await engine.dispose()

    asyncio.run(scenario())
