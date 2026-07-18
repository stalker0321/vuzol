"""Workflow lifecycle controls tests (split for cohesion)."""

from __future__ import annotations

from ._test_runtime_helpers import *


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
def test_completed_plan_enqueues_system_trace(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        draft = planned_coding_draft()
        task_id, interpretation_id = await seed_interpreted(factory, draft)
        async with factory.begin() as session:
            await materialize_run(
                session,
                task_id=task_id,
                workflow=compile_workflow(draft, interpretation_id=interpretation_id),
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                prompt_revision="trace-v1",
                automatic_start=True,
            )
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="planner",
                lease_seconds=60,
                capabilities=frozenset(),
                step_types=frozenset({"plan"}),
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
            await commit_step_outcome(
                session,
                token,
                StepOutcome.succeeded(
                    {
                        "model": "gpt-5-nano-2025-08-07",
                        "profile_id": "openai-planner-prod",
                        "text": "Inspect, edit, verify.",
                        "finish_reason": "stop",
                    }
                ),
            )
        async with factory() as session:
            trace = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["role"].as_string() == "orchestration_trace",
                    TransactionalOutbox.payload["trace_kind"].as_string() == "planner",
                )
            )
            assert trace is not None
            assert trace.linked_entity_id == token.step.id
            assert trace.payload["attempt"] == 1
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
