"""Workflow dispatcher worker tests (split for cohesion)."""

from __future__ import annotations

from ._test_runtime_helpers import (
    CompleteHandler,
    OutcomeKind,
    RegistryDocument,
    Run,
    RunStatus,
    RuntimeConfiguration,
    Settings,
    Step,
    StepOutcome,
    StepStatus,
    Task,
    TaskStatus,
    TransactionalOutbox,
    WorkflowDispatcher,
    WorkflowWorker,
    asyncio,
    build_bundle,
    build_status_card,
    claim_step,
    commit_step_outcome,
    compile_workflow,
    materialize_run,
    pytest,
    seed_interpreted,
    select,
    simple_draft,
    start_step,
    storage,
)


@pytest.mark.postgresql
def test_dispatcher_materializes_once_and_manual_start_waits(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory)
        async with factory.begin() as session:
            session.add(
                TransactionalOutbox(
                    destination="workflow_dispatch",
                    operation_type="dispatch_interpretation",
                    linked_entity_type="interpretation",
                    linked_entity_id=interpretation_id,
                    idempotency_key=f"workflow:dispatch:{interpretation_id}",
                    payload={"task_id": str(task_id)},
                )
            )
        settings = Settings(environment="test")
        runtime = RuntimeConfiguration(
            settings=settings,
            registries=build_bundle(RegistryDocument(), settings),
        )
        dispatcher = WorkflowDispatcher(runtime, factory, owner="dispatcher")
        assert await dispatcher.process_one()
        assert not await dispatcher.process_one()
        async with factory() as session:
            runs = tuple((await session.scalars(select(Run))).all())
            assert len(runs) == 1 and runs[0].status is RunStatus.CREATED
            steps = tuple((await session.scalars(select(Step))).all())
            assert steps and all(
                step.status in {StepStatus.COMPLETED, StepStatus.PENDING} for step in steps
            )
            card = await build_status_card(session, task_id)
            assert card.buttons == ("start",)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_registered_worker_executes_simple_internal_test_handlers(postgres_dsn: str) -> None:
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
        handler = CompleteHandler()
        worker = WorkflowWorker(
            Settings(environment="test"),
            factory,
            owner="worker",
            handlers={
                "execute_model": handler,
                "format_result": handler,
                "finalize": handler,
            },
        )
        assert await worker.process_one()
        assert await worker.process_one()
        assert await worker.process_one()
        assert not await worker.process_one()
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task is not None and task.status is TaskStatus.COMPLETED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("outcome", "step_status", "run_status"),
    [
        (
            StepOutcome(
                kind=OutcomeKind.TRANSIENT_FAILURE,
                result={},
                category="temporary",
            ),
            StepStatus.QUEUED,
            RunStatus.RUNNING,
        ),
        (
            StepOutcome(kind=OutcomeKind.NEEDS_USER_INPUT, result={"question": "Which one?"}),
            StepStatus.AWAITING_USER,
            RunStatus.AWAITING_USER,
        ),
        (
            StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="unknown",
                unknown_effects=True,
            ),
            StepStatus.BLOCKED,
            RunStatus.BLOCKED,
        ),
        (
            StepOutcome(kind=OutcomeKind.PERMANENT_FAILURE, result={}, category="invalid"),
            StepStatus.FAILED,
            RunStatus.FAILED,
        ),
    ],
)
def test_outcome_state_matrix(
    postgres_dsn: str,
    outcome: StepOutcome,
    step_status: StepStatus,
    run_status: RunStatus,
) -> None:
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
            token = await claim_step(
                session, owner="worker", lease_seconds=60, capabilities=frozenset()
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
            await commit_step_outcome(session, token, outcome, retry_delay_seconds=2)
        async with factory() as session:
            step = await session.get(Step, token.step.id)
            loaded_run = await session.get(Run, token.step.run_id)
            task = await session.get(Task, task_id)
            assert step is not None and step.status is step_status
            assert loaded_run is not None and loaded_run.status is run_status
            assert task is not None
            if run_status in {RunStatus.BLOCKED, RunStatus.FAILED}:
                assert task.completed_at is not None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_worker_normalizes_handler_exception(postgres_dsn: str) -> None:
    class FailingHandler:
        async def execute(self, _request: object, _cancellation: object) -> StepOutcome:
            raise RuntimeError("provider detail must not escape")

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
        worker = WorkflowWorker(
            Settings(environment="test"),
            factory,
            owner="worker",
            handlers={"execute_model": FailingHandler()},
        )
        assert await worker.process_one()
        async with factory() as session:
            loaded_run = await session.scalar(select(Run).where(Run.task_id == task_id))
            assert loaded_run is not None and loaded_run.status is RunStatus.FAILED
            assert loaded_run.failure_category == "handler_exception"
            assert loaded_run.failure_summary == "RuntimeError"
            assert "provider detail" not in loaded_run.failure_summary
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_worker_shutdown_cancellation_records_uncertainty(postgres_dsn: str) -> None:
    started = asyncio.Event()

    class BlockingHandler:
        async def execute(self, _request: object, _cancellation: object) -> StepOutcome:
            started.set()
            await asyncio.Event().wait()
            return StepOutcome.succeeded()

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
        worker = WorkflowWorker(
            Settings(environment="test"),
            factory,
            owner="worker",
            handlers={"execute_model": BlockingHandler()},
        )
        execution = asyncio.create_task(worker.process_one())
        await started.wait()
        execution.cancel()
        assert await execution
        async with factory() as session:
            step = await session.scalar(select(Step).where(Step.step_type == "execute_model"))
            run = await session.scalar(select(Run).where(Run.task_id == task_id))
            assert step is not None and step.status is StepStatus.BLOCKED
            assert step.unknown_effects
            assert run is not None and run.status is RunStatus.BLOCKED
        await engine.dispose()

    asyncio.run(scenario())
