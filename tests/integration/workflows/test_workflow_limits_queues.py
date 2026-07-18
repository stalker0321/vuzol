"""Workflow limits queues tests (split for cohesion)."""

from __future__ import annotations

from ._test_runtime_helpers import *


@pytest.mark.postgresql
def test_heavy_class_limit_is_transactional(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        step_ids: list[uuid.UUID] = []
        for _ in range(2):
            async with UnitOfWork(factory) as uow:
                task = await uow.tasks.create(
                    user_id=1, chat_id=-100, original_text="code", task_type="coding"
                )
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
                    step_type="execute_code",
                    idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                    status=StepStatus.QUEUED,
                    queue_class=QueueClass.HEAVY,
                )
                step_ids.append(step.id)

        async def claim(owner: str) -> object:
            async with factory.begin() as session:
                return await claim_step(
                    session,
                    owner=owner,
                    lease_seconds=60,
                    capabilities=frozenset(),
                    queue_classes=frozenset({QueueClass.HEAVY}),
                    class_limits={QueueClass.HEAVY: 1},
                )

        claims = await asyncio.gather(claim("a"), claim("b"))
        assert sum(value is not None for value in claims) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_saturated_candidate_does_not_starve_available_queue(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)

        async def create_step(queue_class: QueueClass, status: StepStatus) -> uuid.UUID:
            async with UnitOfWork(factory) as uow:
                task = await uow.tasks.create(
                    user_id=1, chat_id=-100, original_text="work", task_type="general"
                )
                run_id = await uow.runs.create(
                    task_id=task.id,
                    workflow_type="simple_model",
                    workflow_version="1",
                    budget_mode="balanced",
                    configuration_revision="a" * 64,
                    policy_revision="b" * 64,
                    status=RunStatus.RUNNING,
                )
                record = await uow.steps.create(
                    run_id=run_id,
                    ordinal=1,
                    step_type="work",
                    idempotency_class=IdempotencyClass.READ_ONLY,
                    status=status,
                    queue_class=queue_class,
                )
                assert uow.session is not None
                step = await uow.session.get(Step, record.id)
                assert step is not None
                if status is StepStatus.LEASED:
                    step.lease_owner = "existing"
                    step.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
                return step.id

        await create_step(QueueClass.HEAVY, StepStatus.LEASED)
        await create_step(QueueClass.HEAVY, StepStatus.QUEUED)
        light_id = await create_step(QueueClass.LIGHT, StepStatus.QUEUED)
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="worker",
                lease_seconds=60,
                capabilities=frozenset(),
                class_limits={QueueClass.HEAVY: 1, QueueClass.LIGHT: 1},
                candidate_limit=20,
            )
        assert token is not None and token.step.id == light_id
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_profile_limit_is_transactional(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        for _ in range(2):
            async with UnitOfWork(factory) as uow:
                task = await uow.tasks.create(
                    user_id=1, chat_id=-100, original_text="model", task_type="general"
                )
                run_id = await uow.runs.create(
                    task_id=task.id,
                    workflow_type="simple_model",
                    workflow_version="1",
                    budget_mode="balanced",
                    configuration_revision="a" * 64,
                    policy_revision="b" * 64,
                    status=RunStatus.RUNNING,
                )
                step = await uow.steps.create(
                    run_id=run_id,
                    ordinal=1,
                    step_type="execute_model",
                    idempotency_class=IdempotencyClass.READ_ONLY,
                    status=StepStatus.QUEUED,
                )
                assert uow.session is not None
                model = await uow.session.get(Step, step.id)
                assert model is not None
                model.executor_profile_id = "profile-a"

        async def claim(owner: str) -> object:
            async with factory.begin() as session:
                return await claim_step(
                    session,
                    owner=owner,
                    lease_seconds=60,
                    capabilities=frozenset(),
                    class_limits={QueueClass.LIGHT: 2},
                    profile_limits={"profile-a": 1},
                )

        claims = await asyncio.gather(claim("a"), claim("b"))
        assert sum(value is not None for value in claims) == 1
        await engine.dispose()

    asyncio.run(scenario())
