import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update

from vuzol.config import (
    RegistryDocument,
    RuntimeConfiguration,
    Settings,
    WorkflowSettings,
    build_bundle,
)
from vuzol.interpretation.domain import (
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import (
    Event,
    Interpretation,
    Run,
    Step,
    Task,
    TelegramControlAction,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    ControlActionStatus,
    IdempotencyClass,
    QueueClass,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import build_status_card
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.controls import (
    WorkflowControlConsumer,
    cancel_task,
    pause_task,
    resume_task,
)
from vuzol.workflows.dispatch import WorkflowDispatcher
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.recovery import recover_expired_steps
from vuzol.workflows.service import commit_step_outcome, materialize_run, start_run
from vuzol.workflows.worker import CompleteHandler, WorkflowWorker

from ..storage.helpers import storage


def simple_draft() -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.GENERAL,
        operation=TaskOperation.EXPLAIN,
        goal="Answer the question",
        task_summary="Answer the user's question",
        suggested_complexity=SuggestedComplexity.SMALL,
        suggested_risk=RiskLevel.LOW,
        needs_planning=False,
        needs_clarification=False,
        normalized_title="Answer question",
    )


async def seed_interpreted(
    factory: object, task_draft: TaskDraft | None = None
) -> tuple[uuid.UUID, uuid.UUID]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed_factory: async_sessionmaker[AsyncSession] = factory
    draft = task_draft or simple_draft()
    async with UnitOfWork(typed_factory) as uow:
        task_record = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="answer this",
            task_type="general",
            task_draft=draft.model_dump(mode="json"),
        )
        assert uow.session is not None
        task = await uow.session.get(Task, task_record.id)
        assert task is not None
        task.status = TaskStatus.INTERPRETED
        interpretation = Interpretation(
            task_id=task.id,
            original_input_hash="a" * 64,
            task_draft=draft.model_dump(mode="json"),
            profile_id="fake",
            model="fake",
            prompt_version="step-05-v1",
            schema_version="1.0",
        )
        uow.session.add(interpretation)
        await uow.session.flush()
        return task.id, interpretation.id


@pytest.mark.postgresql
def test_materialized_workflow_reaches_completion(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory)
        workflow = compile_workflow(simple_draft(), interpretation_id=interpretation_id)
        async with factory.begin() as session:
            run = await materialize_run(
                session,
                task_id=task_id,
                workflow=workflow,
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision="step-05-v1",
                automatic_start=True,
            )
            run_id = run.id
        for _ in range(3):
            async with factory.begin() as session:
                token = await claim_step(
                    session, owner="worker", lease_seconds=60, capabilities=frozenset()
                )
            assert token is not None
            async with factory.begin() as session:
                await start_step(session, token)
                await commit_step_outcome(session, token, StepOutcome.succeeded())
        async with factory() as session:
            loaded_run = await session.get(Run, run_id)
            loaded_task = await session.get(Task, task_id)
            assert loaded_run is not None and loaded_run.status is RunStatus.COMPLETED
            assert loaded_task is not None and loaded_task.status is TaskStatus.COMPLETED
            event_count = await session.scalar(select(func.count()).select_from(Event))
            assert event_count is not None and event_count > 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_pause_resume_and_cancel_are_persisted(postgres_dsn: str) -> None:
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
            await pause_task(session, task_id, actor_id="1")
            await pause_task(session, task_id, actor_id="1")
        async with factory.begin() as session:
            assert (
                await claim_step(
                    session, owner="worker", lease_seconds=60, capabilities=frozenset()
                )
                is None
            )
        async with factory.begin() as session:
            await resume_task(session, task_id, actor_id="1")
            await resume_task(session, task_id, actor_id="1")
        async with factory.begin() as session:
            token = await claim_step(
                session, owner="worker", lease_seconds=60, capabilities=frozenset()
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
        async with factory.begin() as session:
            await cancel_task(session, task_id, actor_id="1")
        async with factory.begin() as session:
            await cancel_task(session, task_id, actor_id="1")
            with pytest.raises(ValueError, match="terminal run cannot pause"):
                await pause_task(session, task_id, actor_id="1")
            with pytest.raises(ValueError, match="cannot resume"):
                await resume_task(session, task_id, actor_id="1")
        async with factory() as session:
            task = await session.get(Task, task_id)
            step = await session.get(Step, token.step.id)
            assert task is not None and task.status is TaskStatus.CANCELLED
            assert step is not None and step.unknown_effects and step.status is StepStatus.CANCELLED
        await engine.dispose()

    asyncio.run(scenario())


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


@pytest.mark.postgresql
def test_workflow_control_outbox_applies_cancel(postgres_dsn: str) -> None:
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
            action = TelegramControlAction(
                external_action_id="cancel-1",
                action_kind="cancel",
                requested_by_user_id=1,
                task_id=task_id,
                payload={},
            )
            session.add(action)
            await session.flush()
            action_id = action.id
            session.add(
                TransactionalOutbox(
                    destination="workflow_control",
                    operation_type="cancel",
                    linked_entity_type="telegram_control_action",
                    linked_entity_id=action.id,
                    idempotency_key="workflow-control:cancel-1",
                    payload={},
                )
            )
        consumer = WorkflowControlConsumer(Settings(environment="test"), factory, owner="control")
        assert await consumer.process_one()
        assert not await consumer.process_one()
        async with factory() as session:
            task = await session.get(Task, task_id)
            loaded_action = await session.get(TelegramControlAction, action_id)
            assert task is not None and task.status is TaskStatus.CANCELLED
            assert (
                loaded_action is not None and loaded_action.status is ControlActionStatus.PROCESSED
            )
            assert loaded_action.processed_at is not None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_control_consumer_start_pause_resume_and_reject(postgres_dsn: str) -> None:
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
                automatic_start=False,
            )
        consumer = WorkflowControlConsumer(Settings(environment="test"), factory, owner="control")

        async def enqueue(kind: str, *, include_task: bool = True) -> uuid.UUID:
            async with factory.begin() as session:
                action = TelegramControlAction(
                    external_action_id=f"{kind}-{uuid.uuid4()}",
                    action_kind=kind,
                    requested_by_user_id=1,
                    task_id=task_id if include_task else None,
                    payload={},
                )
                session.add(action)
                await session.flush()
                session.add(
                    TransactionalOutbox(
                        destination="workflow_control",
                        operation_type=kind,
                        linked_entity_type="telegram_control_action",
                        linked_entity_id=action.id,
                        idempotency_key=f"workflow-control:{action.external_action_id}",
                        payload={},
                    )
                )
                return action.id

        for kind in ("start", "pause", "resume"):
            await enqueue(kind)
            assert await consumer.process_one()
        rejected_id = await enqueue("retry", include_task=False)
        assert await consumer.process_one()
        async with factory() as session:
            run = await session.scalar(select(Run).where(Run.task_id == task_id))
            rejected = await session.get(TelegramControlAction, rejected_id)
            assert run is not None and run.status is RunStatus.RUNNING
            assert rejected is not None and rejected.status is ControlActionStatus.REJECTED
            assert "step target" in str(rejected.payload["rejection_reason"])
        await engine.dispose()

    asyncio.run(scenario())


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
def test_dispatcher_applies_control_and_rejects_semantic_approval(postgres_dsn: str) -> None:
    async def enqueue_dispatch(
        factory: object, task_id: uuid.UUID, interpretation_id: uuid.UUID
    ) -> None:
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        assert isinstance(factory, async_sessionmaker)
        typed_factory: async_sessionmaker[AsyncSession] = factory
        async with typed_factory.begin() as session:
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

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        target_id, target_interpretation = await seed_interpreted(factory)
        async with factory.begin() as session:
            await materialize_run(
                session,
                task_id=target_id,
                workflow=compile_workflow(simple_draft(), interpretation_id=target_interpretation),
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision=None,
                automatic_start=True,
            )
        control_draft = simple_draft().model_copy(
            update={"action": TaskAction.CANCEL_TASK, "referenced_task_id": target_id}
        )
        carrier_id, control_interpretation = await seed_interpreted(factory, control_draft)
        await enqueue_dispatch(factory, carrier_id, control_interpretation)
        approval_draft = simple_draft().model_copy(update={"action": TaskAction.APPROVE_STEP})
        approval_id, approval_interpretation = await seed_interpreted(factory, approval_draft)
        await enqueue_dispatch(factory, approval_id, approval_interpretation)
        settings = Settings(environment="test")
        runtime = RuntimeConfiguration(
            settings=settings,
            registries=build_bundle(RegistryDocument(), settings),
        )
        dispatcher = WorkflowDispatcher(runtime, factory, owner="dispatcher")
        assert await dispatcher.process_one()
        assert await dispatcher.process_one()
        async with factory() as session:
            target = await session.get(Task, target_id)
            carrier = await session.get(Task, carrier_id)
            approval = await session.get(Task, approval_id)
            assert target is not None and target.status is TaskStatus.CANCELLED
            assert carrier is not None and carrier.status is TaskStatus.COMPLETED
            assert carrier.parent_task_id == target_id
            assert approval is not None and approval.status is TaskStatus.AWAITING_USER
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
def test_continuation_resumes_awaiting_step(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        target_id, target_interpretation = await seed_interpreted(factory)
        async with factory.begin() as session:
            run = await materialize_run(
                session,
                task_id=target_id,
                workflow=compile_workflow(simple_draft(), interpretation_id=target_interpretation),
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision=None,
                automatic_start=True,
            )
            step = await session.scalar(
                select(Step).where(Step.run_id == run.id, Step.status == StepStatus.QUEUED)
            )
            task = await session.get(Task, target_id)
            assert step is not None and task is not None
            step.status = StepStatus.AWAITING_USER
            run.status = RunStatus.AWAITING_USER
            task.status = TaskStatus.AWAITING_USER
        continuation = simple_draft().model_copy(
            update={"action": TaskAction.CONTINUE_TASK, "referenced_task_id": target_id}
        )
        carrier_id, interpretation_id = await seed_interpreted(factory, continuation)
        async with factory.begin() as session:
            session.add(
                TransactionalOutbox(
                    destination="workflow_dispatch",
                    operation_type="dispatch_interpretation",
                    linked_entity_type="interpretation",
                    linked_entity_id=interpretation_id,
                    idempotency_key=f"workflow:dispatch:{interpretation_id}",
                    payload={"task_id": str(carrier_id)},
                )
            )
        settings = Settings(environment="test")
        dispatcher = WorkflowDispatcher(
            RuntimeConfiguration(
                settings=settings,
                registries=build_bundle(RegistryDocument(), settings),
            ),
            factory,
            owner="dispatcher",
        )
        assert await dispatcher.process_one()
        async with factory() as session:
            target = await session.get(Task, target_id)
            carrier = await session.get(Task, carrier_id)
            resumed = await session.scalar(
                select(Step).where(
                    Step.run_id == run.id,
                    Step.payload["continuation_interpretation_id"].as_string()
                    == str(interpretation_id),
                )
            )
            assert target is not None and target.status is TaskStatus.EXECUTING
            assert carrier is not None and carrier.status is TaskStatus.COMPLETED
            assert resumed is not None and resumed.status is StepStatus.QUEUED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_explicit_retry_requeues_only_safe_blocked_step(postgres_dsn: str) -> None:
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
            task = await session.get(Task, task_id)
            assert step is not None and task is not None
            step.status = StepStatus.BLOCKED
            step.failure_category = "provider_unavailable"
            step.failure_summary = "provider temporarily unavailable"
            run.status = RunStatus.BLOCKED
            run.failure_category = "provider_unavailable"
            run.failure_summary = "provider temporarily unavailable"
            task.status = TaskStatus.BLOCKED
            await session.flush()
            step_id = step.id
            action = TelegramControlAction(
                external_action_id="retry-safe-1",
                action_kind="retry",
                requested_by_user_id=1,
                step_id=step.id,
                payload={},
            )
            session.add(action)
            await session.flush()
            session.add(
                TransactionalOutbox(
                    destination="workflow_control",
                    operation_type="retry",
                    linked_entity_type="telegram_control_action",
                    linked_entity_id=action.id,
                    idempotency_key="workflow-control:retry-safe-1",
                    payload={},
                )
            )
        consumer = WorkflowControlConsumer(Settings(environment="test"), factory, owner="control")
        assert await consumer.process_one()
        async with factory() as session:
            step = await session.get(Step, step_id)
            loaded_run = await session.scalar(select(Run).where(Run.task_id == task_id))
            task = await session.get(Task, task_id)
            assert step is not None and step.status is StepStatus.QUEUED
            assert step.failure_category is None and step.failure_summary is None
            assert loaded_run is not None and loaded_run.status is RunStatus.RUNNING
            assert loaded_run.failure_category is None and loaded_run.failure_summary is None
            assert task is not None and task.status is TaskStatus.RETRYING
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


@pytest.mark.postgresql
def test_manual_start_is_idempotent_and_heartbeat_uses_database_time(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory)
        workflow = compile_workflow(simple_draft(), interpretation_id=interpretation_id)
        async with factory.begin() as session:
            run = await materialize_run(
                session,
                task_id=task_id,
                workflow=workflow,
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision=None,
                automatic_start=False,
            )
            duplicate = await materialize_run(
                session,
                task_id=task_id,
                workflow=workflow,
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision=None,
                automatic_start=False,
            )
            assert duplicate.id == run.id
            await start_run(session, run, actor_type="user")
            await start_run(session, run, actor_type="user")
        async with factory.begin() as session:
            token = await claim_step(
                session, owner="worker", lease_seconds=15, capabilities=frozenset()
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
        settings = Settings(
            environment="test",
            workflow=WorkflowSettings(lease_seconds=15, heartbeat_seconds=1),
        )
        worker = WorkflowWorker(settings, factory, owner="worker", handlers={})
        cancellation = CancellationContext()
        heartbeat = asyncio.create_task(worker._heartbeat(token, cancellation))
        await asyncio.sleep(1.1)
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        async with factory() as session:
            step = await session.get(Step, token.step.id)
            assert step is not None and step.heartbeat_at is not None
            assert not cancellation.requested
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
