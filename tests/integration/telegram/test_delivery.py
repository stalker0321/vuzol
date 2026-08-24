import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, update
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError

from tests.integration.storage.helpers import storage
from vuzol.storage.models import (
    Approval,
    Interpretation,
    ProjectNamingRequest,
    Run,
    Step,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    ApprovalStatus,
    DeliveryStatus,
    IdempotencyClass,
    IntakeStatus,
    ProjectNamingStatus,
    QueueClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import (
    DeliveryAction,
    PermanentDeliveryError,
    TelegramDeliveryService,
    prepare_delivery,
)
from vuzol.telegram.layout import HISTORY_TOPIC_KIND, STATUS_DASHBOARD_TOPIC_KIND
from vuzol.telegram.projections import (
    PROJECT_STATUS_DASHBOARD_ROLE,
    TASK_HISTORY_ROLE,
    FakeTelegramClient,
    enqueue_task_status_projection,
)
from vuzol.telegram.tracing import (
    INTERPRETER_TRACE_KIND,
    ORCHESTRATION_TRACE_ROLE,
    PLANNER_TRACE_KIND,
)
from vuzol.telegram.work_package_projections import WORK_PACKAGE_PROJECTION_DESTINATION
from vuzol.workflows.result_approval import envelope_hash

pytestmark = pytest.mark.postgresql


async def seed_delivery(
    factory: Any,
    *,
    original_text: str = "request",
    task_id: uuid.UUID | None = None,
    candidates: tuple[uuid.UUID, ...] = (),
    message_id: int = 10,
) -> tuple[uuid.UUID | None, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        inbox_id, _ = await uow.inbox.receive_once(
            source="telegram",
            consumer="bot:main",
            external_event_id=str(message_id),
            payload_hash=f"{message_id:064d}",
        )
        if task_id is None and not candidates:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text=original_text,
                task_type="general",
            )
            task_id = task.id
        intake_id = await uow.telegram_intake.add(
            TelegramIntakeMessage(
                inbox_id=inbox_id,
                chat_id=-100,
                message_thread_id=10,
                message_id=message_id,
                user_id=42,
                task_id=task_id,
                original_text=original_text,
                affinity_kind="new_task" if task_id else None,
                ambiguous_task_ids=[str(value) for value in candidates],
                status=(
                    IntakeStatus.NEEDS_CLARIFICATION
                    if candidates
                    else IntakeStatus.AWAITING_INTERPRETATION
                ),
            )
        )
        outbox_id = await uow.outbox.enqueue(
            destination="telegram",
            operation_type="send_message",
            entity_type="telegram_intake",
            entity_id=intake_id,
            idempotency_key=f"intake:{message_id}",
            payload={"role": "clarification" if candidates else "intake_ack"},
        )
    return task_id, outbox_id


def service(
    factory: Any,
    client: FakeTelegramClient,
    *,
    owner: str = "delivery",
    max_attempts: int = 3,
    trace_enabled: bool = True,
    trace_sample_percent: int = 100,
    trace_always_include_anomalies: bool = True,
) -> TelegramDeliveryService:
    return TelegramDeliveryService(
        factory,
        client,
        owner=owner,
        lease_seconds=30,
        max_attempts=max_attempts,
        retry_min_seconds=1,
        retry_max_seconds=10,
        trace_enabled=trace_enabled,
        trace_sample_percent=trace_sample_percent,
        trace_always_include_anomalies=trace_always_include_anomalies,
    )


def test_task_without_intake_enqueues_direct_approval_projection(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            created = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="materialized plan item",
                task_type="coding",
            )
            assert uow.session is not None
            task = await uow.session.get(Task, created.id)
            assert task is not None
            await enqueue_task_status_projection(uow.session, task, role="approval_card")
        async with factory() as session:
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.linked_entity_type == "task",
                    TransactionalOutbox.linked_entity_id == created.id,
                )
            )
        assert item is not None
        assert item.payload["role"] == "approval_card"
        await engine.dispose()

    asyncio.run(scenario())


def test_acknowledgement_sends_once_and_persists_confirmed_link(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, outbox_id = await seed_delivery(factory, original_text="<unsafe & text>")
        client = FakeTelegramClient(next_message_id=77)
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        assert not await delivery.deliver_one()
        assert len(client.sent) == 1
        assert "<b>Задача №100001</b>" in client.sent[0][2]
        assert "&lt;unsafe &amp; text&gt;" not in client.sent[0][2]
        async with factory() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.task_id == task_id,
                    TelegramMessageLink.message_role == "task_status",
                )
            )
            assert item is not None and item.status == DeliveryStatus.DELIVERED
            assert link is not None and link.message_id == 77 and link.projection_revision == 1
        await engine.dispose()

    asyncio.run(scenario())


def test_help_card_delivery_is_context_aware(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            inbox_id, _ = await uow.inbox.receive_once(
                source="telegram",
                consumer="bot:main",
                external_event_id="help-1",
                payload_hash="1" * 64,
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="telegram_inbox",
                entity_id=inbox_id,
                idempotency_key="telegram:help:-100:help-1",
                payload={
                    "role": "help_card",
                    "chat_id": -100,
                    "message_thread_id": 10,
                    "topic_kind": "project",
                },
            )

        client = FakeTelegramClient(next_message_id=78)
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        assert not await delivery.deliver_one()
        assert len(client.sent) == 1
        assert client.sent[0][:2] == (-100, 10)
        assert "/model" in client.sent[0][2]
        assert "/update" not in client.sent[0][2]
        await engine.dispose()

    asyncio.run(scenario())


def test_interpreter_trace_is_delivered_to_system_topic(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=73,
                original_text="implement selection",
                task_type="coding",
            )
            task_row = await uow.session.get(Task, task.id)
            assert task_row is not None
            task_row.status = TaskStatus.INTERPRETED
            task_row.project_id = "bill-buddy"
            task_row.public_task_number = 730010
            interpretation = Interpretation(
                task_id=task_row.id,
                original_input_hash="a" * 64,
                task_draft={"task_type": "coding", "operation": "modify"},
                profile_id="openai-interpreter",
                model="gpt-4o-mini",
                prompt_version="architecture-routing-v8",
                schema_version="1.4",
            )
            uow.session.add(interpretation)
            await uow.session.flush()
            uow.session.add(
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=99,
                    topic_kind="system",
                    accepts_new_tasks=False,
                    default_workflow="simple_model",
                    enabled=True,
                )
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="interpretation",
                entity_id=interpretation.id,
                idempotency_key=f"trace:{interpretation.id}",
                payload={
                    "role": ORCHESTRATION_TRACE_ROLE,
                    "trace_kind": "interpreter",
                    "task_id": str(task_row.id),
                    "model_task_draft": {"task_type": "coding", "operation": "create"},
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "duration_ms": 800,
                    "repaired": False,
                },
            )
            routine_interpretation = Interpretation(
                task_id=task_row.id,
                original_input_hash="b" * 64,
                task_draft={"task_type": "coding", "operation": "modify"},
                profile_id="openai-interpreter",
                model="gpt-4o-mini",
                prompt_version="architecture-routing-v8",
                schema_version="1.4",
            )
            uow.session.add(routine_interpretation)
            await uow.session.flush()
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="interpretation",
                entity_id=routine_interpretation.id,
                idempotency_key=f"trace:{routine_interpretation.id}",
                payload={
                    "role": ORCHESTRATION_TRACE_ROLE,
                    "trace_kind": "interpreter",
                    "task_id": str(task_row.id),
                    "model_task_draft": {"task_type": "coding", "operation": "modify"},
                    "input_tokens": 80,
                    "output_tokens": 40,
                    "duration_ms": 700,
                    "repaired": False,
                },
            )
            task_id = task_row.id
        client = FakeTelegramClient(next_message_id=78)
        delivery = service(factory, client, trace_sample_percent=0)
        assert await delivery.deliver_one()
        assert await delivery.deliver_one()
        assert len(client.sent) == 1
        assert client.sent[0][0:2] == (-100, 99)
        assert "Интерпретатор · #730010" in client.sent[0][2]
        assert "После deterministic policy" in client.sent[0][2]
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.task_id == task_id,
                    TelegramMessageLink.message_role == "trace_interpreter",
                )
            )
            assert link is not None and link.message_thread_id == 99
        await engine.dispose()

    asyncio.run(scenario())


def test_planner_trace_is_delivered_with_step_identity(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task_record = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=73,
                original_text="implement selection",
                task_type="coding",
            )
            task = await uow.session.get(Task, task_record.id)
            assert task is not None
            task.status = TaskStatus.PLANNED
            task.project_id = "bill-buddy"
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
            uow.session.add(run)
            await uow.session.flush()
            step = Step(
                run_id=run.id,
                ordinal=1,
                dependency_metadata={},
                step_type="plan",
                queue_class=QueueClass.LIGHT,
                status=StepStatus.COMPLETED,
                executor_profile_id="openai-planner-prod",
                required_capabilities=[],
                payload={},
                result={
                    "model": "gpt-5-nano-2025-08-07",
                    "profile_id": "openai-planner-prod",
                    "text": "Inspect, implement, verify.",
                    "finish_reason": "stop",
                },
                retry_class=RetryClass.TRANSIENT,
                idempotency_class=IdempotencyClass.READ_ONLY,
                attempt_count=1,
                max_attempts=3,
                timeout_seconds=600,
            )
            uow.session.add(step)
            await uow.session.flush()
            uow.session.add(
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=99,
                    topic_kind="system",
                    accepts_new_tasks=False,
                    default_workflow="simple_model",
                    enabled=True,
                )
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="step",
                entity_id=step.id,
                idempotency_key=f"trace:{step.id}",
                payload={
                    "role": ORCHESTRATION_TRACE_ROLE,
                    "trace_kind": "planner",
                    "task_id": str(task.id),
                    "attempt": 1,
                },
            )
            task_id, run_id, step_id = task.id, run.id, step.id
        client = FakeTelegramClient(next_message_id=79)
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        assert client.sent[0][0:2] == (-100, 99)
        assert "Планировщик" in client.sent[0][2]
        assert "Inspect, implement, verify." in client.sent[0][2]
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.task_id == task_id,
                    TelegramMessageLink.message_role == "trace_planner",
                )
            )
            assert link is not None
            assert (link.run_id, link.step_id) == (run_id, step_id)
        await engine.dispose()

    asyncio.run(scenario())


def test_existing_status_is_edited_and_stale_revision_is_ignored(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, _ = await seed_delivery(factory, message_id=20)
        assert task_id is not None
        client = FakeTelegramClient(next_message_id=88)
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        await seed_delivery(factory, task_id=task_id, message_id=21)
        assert await delivery.deliver_one()
        assert client.edited == []
        async with factory.begin() as session:
            await session.execute(update(Task).where(Task.id == task_id).values(version=2))
        await seed_delivery(factory, task_id=task_id, message_id=22)
        assert await delivery.deliver_one()
        assert len(client.sent) == 1
        assert len(client.edited) == 1 and client.edited[0][1] == 88
        await engine.dispose()

    asyncio.run(scenario())


def test_transient_retry_then_max_attempts_dead_letters(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, outbox_id = await seed_delivery(factory, message_id=30)
        assert task_id is not None
        async with factory.begin() as session:
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=777,
                    task_id=task_id,
                    message_role="task_status",
                    projection_revision=0,
                )
            )
        delivery = service(factory, FakeTelegramClient(fail=NetworkError("offline")))
        assert await delivery.deliver_one()
        async with factory() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            now = await session.scalar(func.now())
            assert item is not None and item.status == DeliveryStatus.PENDING
            assert item.attempt_count == 1 and now is not None and item.available_at > now
        async with factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.id == outbox_id)
                .values(available_at=func.now())
            )
        maxed = service(factory, FakeTelegramClient(fail=NetworkError("offline")), max_attempts=2)
        assert await maxed.deliver_one()
        async with factory() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            assert item is not None and item.status == DeliveryStatus.DEAD_LETTER
            assert item.attempt_count == 2 and item.last_error_category == "networkerror"
        await engine.dispose()

    asyncio.run(scenario())


def test_retry_after_defers_without_consuming_attempt(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, outbox_id = await seed_delivery(factory, message_id=31)
        assert task_id is not None
        async with factory.begin() as session:
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=778,
                    task_id=task_id,
                    message_role="task_status",
                    projection_revision=0,
                )
            )
        delivery = service(
            factory,
            FakeTelegramClient(fail=RetryAfter(3)),
            max_attempts=1,
        )
        assert await delivery.deliver_one()
        async with factory() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            now = await session.scalar(func.now())
            assert item is not None and item.status == DeliveryStatus.PENDING
            assert item.attempt_count == 0
            assert item.last_error_category == "retryafter"
            assert now is not None and item.available_at > now
        await engine.dispose()

    asyncio.run(scenario())


def test_unknown_send_is_ambiguous_and_clarification_has_no_task_link(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _, unknown_id = await seed_delivery(factory, message_id=40)
        unknown = service(factory, FakeTelegramClient(fail=NetworkError("response lost")))
        assert await unknown.deliver_one()
        assert not await unknown.deliver_one()
        async with factory() as session:
            item = await session.get(TransactionalOutbox, unknown_id)
            assert item is not None and item.status == DeliveryStatus.AMBIGUOUS

        first, first_outbox = await seed_delivery(factory, original_text="first", message_id=41)
        second, second_outbox = await seed_delivery(
            factory, original_text="<second>", message_id=42
        )
        assert first is not None and second is not None
        async with factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.id.in_([first_outbox, second_outbox]))
                .values(status=DeliveryStatus.DELIVERED)
            )
        _, clarification_id = await seed_delivery(
            factory, candidates=(first, second), message_id=43
        )
        client = FakeTelegramClient(next_message_id=99)
        assert await service(factory, client).deliver_one()
        assert "multiple active tasks" in client.sent[0][2].lower()
        assert "&lt;second&gt;" in client.sent[0][2]
        async with factory() as session:
            item = await session.get(TransactionalOutbox, clarification_id)
            link = await session.scalar(
                select(TelegramMessageLink).where(TelegramMessageLink.message_id == 99)
            )
            assert item is not None and item.status == DeliveryStatus.DELIVERED
            assert link is not None and link.task_id is None
        await engine.dispose()

    asyncio.run(scenario())


def test_semantic_clarification_is_rebuilt_from_persisted_interpretation(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, original_outbox = await seed_delivery(factory, message_id=50)
        assert task_id is not None
        async with factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.id == original_outbox)
                .values(status=DeliveryStatus.DELIVERED)
            )
            interpretation = Interpretation(
                task_id=task_id,
                original_input_hash="a" * 64,
                task_draft={
                    "normalized_title": "Unsafe <title>",
                    "clarification_question": "Deploy to <production>?",
                },
                profile_id="fake",
                model="fake",
                prompt_version="step-05-v1",
                schema_version="1.0",
            )
            session.add(interpretation)
            await session.flush()
            intake_id = await session.scalar(
                select(TelegramIntakeMessage.id).where(TelegramIntakeMessage.task_id == task_id)
            )
            assert intake_id is not None
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="telegram_intake",
                    linked_entity_id=intake_id,
                    idempotency_key="semantic-clarification",
                    payload={
                        "role": "semantic_clarification",
                        "interpretation_id": str(interpretation.id),
                    },
                )
            )
        client = FakeTelegramClient(next_message_id=101)
        assert await service(factory, client).deliver_one()
        assert "Unsafe &lt;title&gt;" in client.sent[0][2]
        assert "Deploy to &lt;production&gt;?" in client.sent[0][2]
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(TelegramMessageLink.message_id == 101)
            )
            assert link is not None and link.task_id == task_id
        await engine.dispose()

    asyncio.run(scenario())


def test_project_name_options_are_sent_as_buttons_then_deleted(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        options = [
            {"display_name": f"Project {index + 1}", "project_id": f"project-{index + 1}"}
            for index in range(9)
        ]
        async with factory.begin() as session:
            task = Task(
                user_id=42,
                source_chat_id=-100,
                source_thread_id=10,
                original_text="Build a project",
                task_type="infrastructure",
                status=TaskStatus.AWAITING_USER,
            )
            session.add(task)
            await session.flush()
            naming = ProjectNamingRequest(
                task_id=task.id,
                requested_by_user_id=42,
                chat_id=-100,
                source_thread_id=10,
                description="Build <a useful project>",
                options=options,
                revision=1,
                status=ProjectNamingStatus.PENDING,
            )
            session.add(naming)
            await session.flush()
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="project_naming",
                    linked_entity_id=naming.id,
                    idempotency_key=f"names:{naming.id}:1",
                    payload={"role": "project_name_options", "revision": 1},
                )
            )
            naming_id = naming.id
            task_id = task.id
        client = FakeTelegramClient(next_message_id=120)
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        assert "&lt;a useful project&gt;" in client.sent[0][2]
        keyboard = client.sent_keyboards[0]
        assert [len(row) for row in keyboard] == [3, 3, 3, 1]
        assert keyboard[0][0][1] == f"v1:pn:{naming_id.hex}:1:0"
        assert keyboard[-1][0][1] == f"v1:pn:{naming_id.hex}:1:r"
        async with factory.begin() as session:
            persisted_naming = await session.get(
                ProjectNamingRequest, naming_id, with_for_update=True
            )
            assert persisted_naming is not None
            persisted_naming.status = ProjectNamingStatus.GENERATING
            persisted_naming.revision = 2
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="delete_message",
                    linked_entity_type="project_naming",
                    linked_entity_id=persisted_naming.id,
                    idempotency_key=f"names:{persisted_naming.id}:1:delete",
                    payload={"role": "project_name_options", "revision": 1},
                )
            )
        assert await delivery.deliver_one()
        assert client.deleted == [(-100, 120)]
        async with factory.begin() as session:
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="delete_message",
                    linked_entity_type="project_naming",
                    linked_entity_id=naming_id,
                    idempotency_key=f"names:{naming_id}:delete-again",
                    payload={"role": "project_name_options", "revision": 1},
                )
            )
        assert await delivery.deliver_one()
        assert client.deleted == [(-100, 120)]
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.task_id == task_id,
                    TelegramMessageLink.message_role == "project_naming",
                )
            )
            assert link is None
        await engine.dispose()

    asyncio.run(scenario())


def test_project_status_dashboard_sends_once_then_edits(postgres_dsn: str) -> None:
    async def scenario() -> None:
        from vuzol.storage.models import Run, Step, TopicMapping
        from vuzol.storage.types import (
            IdempotencyClass,
            QueueClass,
            RetryClass,
            RunStatus,
            StepStatus,
        )
        from vuzol.telegram.projections import (
            PROJECT_STATUS_DASHBOARD_ROLE,
            enqueue_project_status_dashboard,
        )

        # Isolated forum chat so leftover tasks from other tests are not listed.
        chat_id = -1003950752999
        dashboard_thread = 5
        engine, factory = storage(postgres_dsn)
        async with factory.begin() as session:
            session.add(
                TopicMapping(
                    chat_id=chat_id,
                    message_thread_id=dashboard_thread,
                    topic_kind="task_dashboard",
                    accepts_new_tasks=False,
                    default_workflow="simple_model_task",
                    enabled=True,
                )
            )
            task = Task(
                user_id=42,
                source_chat_id=chat_id,
                source_thread_id=10,
                topic_task_number=1,
                public_task_number=100001,
                project_id="vuzol",
                original_text="Build a dashboard. With many details later.",
                task_type="coding",
                status=TaskStatus.EXECUTING,
                task_draft={
                    "normalized_title": "Build a dashboard. With many details later.",
                    "goal": "Build a dashboard",
                },
                version=1,
            )
            session.add(task)
            await session.flush()
            run = Run(
                task_id=task.id,
                workflow_type="coding",
                workflow_version="1",
                status=RunStatus.RUNNING,
                selected_route={},
                budget_mode="balanced",
                configuration_revision="cfg",
                policy_revision="pol",
            )
            session.add(run)
            await session.flush()
            session.add(
                Step(
                    run_id=run.id,
                    ordinal=1,
                    step_type="execute_code",
                    queue_class=QueueClass.HEAVY,
                    status=StepStatus.RUNNING,
                    executor_profile_id="codex-subscription-prod",
                    required_capabilities=[],
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                    max_attempts=1,
                    timeout_seconds=600,
                )
            )
            await enqueue_project_status_dashboard(session, chat_id)
            task_id = task.id
        client = FakeTelegramClient(next_message_id=200)
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        assert len(client.sent) == 1
        html = client.sent[0][2]
        assert client.sent[0][0] == chat_id
        assert client.sent[0][1] == dashboard_thread
        assert "Статус проектов" in html
        assert "#100001" in html
        assert "Build a dashboard" in html
        assert "Codex" in html
        # Same content/revision must not enqueue another pending outbox row.
        async with factory.begin() as session:
            await enqueue_project_status_dashboard(session, chat_id)
            pending = await session.scalar(
                select(func.count())
                .select_from(TransactionalOutbox)
                .where(
                    TransactionalOutbox.destination == "telegram",
                    TransactionalOutbox.status == DeliveryStatus.PENDING,
                    TransactionalOutbox.idempotency_key.like(
                        f"%project_status_dashboard:{chat_id}:%"
                    ),
                )
            )
            assert pending == 0
        assert client.edited == []
        assert len(client.sent) == 1
        # Completing the task empties the dashboard and edits the same message.
        # Double enqueue in one session (intake_ack + approval_card path) must not
        # insert two outbox rows with the same idempotency key.
        async with factory.begin() as session:
            row = await session.get(Task, task_id)
            assert row is not None
            row.status = TaskStatus.COMPLETED
            row.version = 2
            await enqueue_project_status_dashboard(session, chat_id)
            await enqueue_project_status_dashboard(session, chat_id)
            pending = await session.scalar(
                select(func.count())
                .select_from(TransactionalOutbox)
                .where(
                    TransactionalOutbox.destination == "telegram",
                    TransactionalOutbox.status == DeliveryStatus.PENDING,
                    TransactionalOutbox.idempotency_key.like(
                        f"%project_status_dashboard:{chat_id}:%"
                    ),
                )
            )
            assert pending == 1
        assert await delivery.deliver_one()
        assert len(client.sent) == 1
        assert len(client.edited) == 1
        assert client.edited[0][1] == 200
        assert "Сейчас нет активных задач." in client.edited[0][2]
        async with factory() as session:
            links = (
                await session.scalars(
                    select(TelegramMessageLink).where(
                        TelegramMessageLink.chat_id == chat_id,
                        TelegramMessageLink.message_role == PROJECT_STATUS_DASHBOARD_ROLE,
                    )
                )
            ).all()
            assert len(links) == 1
            assert links[0].message_id == 200
            assert links[0].message_thread_id == dashboard_thread
            assert links[0].task_id is None
        await engine.dispose()

    asyncio.run(scenario())


def _outbox(**overrides: Any) -> TransactionalOutbox:
    values: dict[str, Any] = {
        "destination": "telegram",
        "operation_type": "send_message",
        "linked_entity_type": "none",
        "linked_entity_id": uuid.uuid4(),
        "idempotency_key": f"probe:{uuid.uuid4()}",
        "payload": {},
    }
    values.update(overrides)
    return TransactionalOutbox(**values)


async def _prepare_raises(
    factory: Any, item: TransactionalOutbox, category: str
) -> PermanentDeliveryError:
    async with factory() as session:
        with pytest.raises(PermanentDeliveryError) as excinfo:
            await prepare_delivery(session, item)
    assert excinfo.value.category == category
    return excinfo.value


@pytest.mark.postgresql
def test_prepare_rejects_unsupported_telegram_operation(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        error = await _prepare_raises(
            factory,
            _outbox(operation_type="edit_stuff"),
            "unsupported_telegram_operation",
        )
        assert error.category == "unsupported_telegram_operation"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("chat_id", "message_id"),
    [
        (None, 5),
        ("not-int", 5),
        (-100, None),
        (-100, "not-int"),
    ],
)
def test_command_delete_rejects_invalid_payload(
    postgres_dsn: str, chat_id: Any, message_id: Any
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        payload: dict[str, Any] = {"role": "user_command_delete"}
        if chat_id is not None:
            payload["chat_id"] = chat_id
        if message_id is not None:
            payload["message_id"] = message_id
        await _prepare_raises(
            factory,
            _outbox(operation_type="delete_message", payload=payload),
            "invalid_command_delete_payload",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_help_card_rejects_invalid_payload(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(payload={"role": "help_card", "chat_id": -100}),
            "invalid_help_payload",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_import_prompt_rejects_invalid_payload(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(
                payload={
                    "role": "project_import_prompt",
                    "chat_id": -100,
                    "message_thread_id": 10,
                }
            ),
            "invalid_project_import_prompt",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_secret_ingress_delivery_and_invalid_payload(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        valid = _outbox(
            payload={
                "role": "secret_ingress",
                "chat_id": -100,
                "message_thread_id": 10,
                "html": "<b>Token</b>",
                "callback_buttons": [[["Открыть", "v1:sec:abc"]]],
            }
        )
        async with factory() as session:
            prepared = await prepare_delivery(session, valid)
        assert prepared.html == "<b>Token</b>"
        assert prepared.message_role == "secret_ingress"
        assert prepared.callback_buttons == ((("Открыть", "v1:sec:abc"),),)
        broken = dict(valid.payload)
        del broken["callback_buttons"]
        await _prepare_raises(
            factory,
            _outbox(payload=broken),
            "invalid_secret_ingress_payload",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    "payload",
    [
        {"role": "project_model_picker", "message_thread_id": 5, "html": "pick"},
        {"role": "project_model_picker", "chat_id": -100, "message_thread_id": 5, "html": "  "},
        {
            "role": "project_model_picker",
            "chat_id": -100,
            "message_thread_id": 5,
            "html": "pick",
            "callback_buttons": "nope",
        },
        {
            "role": "project_model_picker",
            "chat_id": -100,
            "message_thread_id": 5,
            "html": "pick",
            "callback_buttons": ["nope"],
        },
        {
            "role": "project_model_picker",
            "chat_id": -100,
            "message_thread_id": 5,
            "html": "pick",
            "callback_buttons": [[["a", "b", "c"]]],
        },
        {
            "role": "project_model_picker",
            "chat_id": -100,
            "message_thread_id": 5,
            "html": "pick",
            "callback_buttons": [[[7, "d"]]],
        },
        {
            "role": "project_model_picker",
            "chat_id": -100,
            "message_thread_id": 5,
            "html": "pick",
            "message_id": "later",
        },
        {
            "role": "project_model_picker",
            "chat_id": -100,
            "message_thread_id": 5,
            "html": "pick",
            "message_id": 0,
        },
    ],
)
def test_model_picker_rejects_invalid_payloads(postgres_dsn: str, payload: dict[str, Any]) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(factory, _outbox(payload=payload), "invalid_model_picker_payload")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_naming_missing_row_fails_closed(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(linked_entity_type="project_naming", linked_entity_id=uuid.uuid4()),
            "project_naming_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_naming_stale_projection_is_noop(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        options = [
            {"display_name": f"Project {index + 1}", "project_id": f"project-{index + 1}"}
            for index in range(9)
        ]
        async with factory.begin() as session:
            task = Task(
                user_id=42,
                source_chat_id=-100,
                source_thread_id=10,
                original_text="Build a project",
                task_type="infrastructure",
                status=TaskStatus.AWAITING_USER,
            )
            session.add(task)
            await session.flush()
            naming = ProjectNamingRequest(
                task_id=task.id,
                requested_by_user_id=42,
                chat_id=-100,
                source_thread_id=10,
                description="Build another project",
                options=options,
                revision=2,
                status=ProjectNamingStatus.PENDING,
            )
            session.add(naming)
            await session.flush()
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=555,
                    task_id=naming.task_id,
                    message_role="project_naming",
                    projection_revision=3,
                )
            )
            item = TransactionalOutbox(
                destination="telegram",
                operation_type="send_message",
                linked_entity_type="project_naming",
                linked_entity_id=naming.id,
                idempotency_key=f"names:{naming.id}:2",
                payload={"role": "project_name_options", "revision": 2},
            )
        async with factory() as session:
            prepared = await prepare_delivery(session, item)
        assert prepared.action is DeliveryAction.NOOP
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_provisioning_projection_rejects_bad_role_and_missing_row(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="project_provisioning",
                payload={"role": "bogus"},
            ),
            "invalid_project_delivery_payload",
        )
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="project_provisioning",
                payload={"role": "project_created"},
            ),
            "project_provisioning_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_intake_projection_missing_row_fails_closed(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="telegram_intake",
                payload={"role": "intake_ack"},
            ),
            "telegram_intake_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_task_projection_requires_source_thread(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=None,
                original_text="no thread",
                task_type="general",
            )
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="task",
                linked_entity_id=task.id,
                payload={"role": "intake_ack"},
            ),
            "telegram_task_projection_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_semantic_clarification_validates_interpretation(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, _ = await seed_delivery(factory, original_text="unclear request")
        missing_interpretation = _outbox(
            linked_entity_type="telegram_intake",
            payload={"role": "semantic_clarification", "interpretation_id": str(uuid.uuid4())},
        )
        async with factory() as session:
            intake_id = await session.scalar(select(TelegramIntakeMessage.id))
        assert intake_id is not None
        missing_interpretation.linked_entity_id = intake_id
        await _prepare_raises(factory, missing_interpretation, "interpretation_missing")
        bad_uuid = _outbox(
            linked_entity_type="telegram_intake",
            linked_entity_id=intake_id,
            payload={"role": "semantic_clarification", "interpretation_id": "nope"},
        )
        await _prepare_raises(factory, bad_uuid, "invalid_interpretation_id")
        async with factory.begin() as session:
            interpretation = Interpretation(
                task_id=task_id,
                original_input_hash="c" * 64,
                task_draft={"task_type": "general", "operation": "create"},
                profile_id="interpreter",
                model="test-model",
                prompt_version="v1",
                schema_version="1.4",
            )
            session.add(interpretation)
            await session.flush()
            no_question = _outbox(
                linked_entity_type="telegram_intake",
                linked_entity_id=intake_id,
                payload={
                    "role": "semantic_clarification",
                    "interpretation_id": str(interpretation.id),
                },
            )
        await _prepare_raises(factory, no_question, "clarification_question_missing")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_clarification_rejects_invalid_candidate_ids(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _task_id, _outbox_id = await seed_delivery(
            factory, candidates=(uuid.uuid4(),), message_id=91
        )
        async with factory.begin() as session:
            intake = await session.scalar(select(TelegramIntakeMessage))
            assert intake is not None
            intake.ambiguous_task_ids = ["not-a-uuid"]
            intake_id = intake.id
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="telegram_intake",
                linked_entity_id=intake_id,
                payload={"role": "clarification"},
            ),
            "invalid_candidate_task_id",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_status_card_rejects_unknown_role(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _task_id, _outbox_id = await seed_delivery(factory, message_id=92)
        async with factory() as session:
            intake_id = await session.scalar(select(TelegramIntakeMessage.id))
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="telegram_intake",
                linked_entity_id=intake_id,
                payload={"role": "bogus"},
            ),
            "invalid_telegram_payload",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_approval_card_requires_topic_registry(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, _outbox_id = await seed_delivery(factory, message_id=93)
        async with factory.begin() as session:
            intake_id = await session.scalar(select(TelegramIntakeMessage.id))
            run = Run(
                task_id=task_id,
                workflow_type="coding",
                workflow_version="1",
                status=RunStatus.RUNNING,
                selected_route={},
                budget_mode="strong",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
            )
            session.add(run)
            await session.flush()
            step = Step(
                run_id=run.id,
                ordinal=1,
                step_type="approval",
                queue_class=QueueClass.PRIVILEGED,
                status=StepStatus.WAITING_APPROVAL,
                payload={},
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                max_attempts=1,
                timeout_seconds=120,
            )
            session.add(step)
            await session.flush()
            envelope = {"schema_version": "result-approval.v1", "step_id": str(step.id)}
            step.payload = {"action_envelope": envelope}
            session.add(
                Approval(
                    step_id=step.id,
                    action_envelope_hash=envelope_hash(envelope),
                    requested_action="apply_result",
                    normalized_target="vuzol:main",
                    human_summary="done",
                    token_hash=uuid.uuid4().hex + uuid.uuid4().hex,
                    status=ApprovalStatus.PENDING,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="telegram_intake",
                linked_entity_id=intake_id,
                payload={"role": "approval_card"},
            ),
            "approval_topic_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("operation_type", "linked_entity_type", "category"),
    [
        ("render_plan", "task", "invalid_work_package_projection_entity"),
        ("render_widget", "work_package", "invalid_work_package_projection_operation"),
    ],
)
def test_work_package_projection_rejects_wrong_entity_and_operation(
    postgres_dsn: str, operation_type: str, linked_entity_type: str, category: str
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(
                destination=WORK_PACKAGE_PROJECTION_DESTINATION,
                operation_type=operation_type,
                linked_entity_type=linked_entity_type,
            ),
            category,
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_work_package_render_wraps_projection_errors(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        bad_page = await _prepare_raises(
            factory,
            _outbox(
                destination=WORK_PACKAGE_PROJECTION_DESTINATION,
                operation_type="render_plan",
                linked_entity_type="work_package",
                payload={"page": "first"},
            ),
            "invalid_page",
        )
        assert bad_page.category == "invalid_page"
        missing = await _prepare_raises(
            factory,
            _outbox(
                destination=WORK_PACKAGE_PROJECTION_DESTINATION,
                operation_type="render_status",
                linked_entity_type="work_package",
            ),
            "package_missing",
        )
        assert missing.category == "package_missing"
        clear_noop = _outbox(
            destination=WORK_PACKAGE_PROJECTION_DESTINATION,
            operation_type="clear_detail",
            linked_entity_type="work_package",
        )
        async with factory() as session:
            prepared = await prepare_delivery(session, clear_noop)
        assert prepared.action is DeliveryAction.NOOP
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_topic_status_validates_mapping_and_payload(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(
                destination=WORK_PACKAGE_PROJECTION_DESTINATION,
                linked_entity_type="topic_mapping",
                operation_type="render_topic_status",
                payload={"role": "topic_idle"},
            ),
            "topic_status_mapping_missing",
        )
        async with factory.begin() as session:
            mapping = TopicMapping(
                chat_id=-100,
                message_thread_id=44,
                topic_kind="project",
                project_id="proj-1",
                accepts_new_tasks=False,
                default_workflow="simple_model",
                enabled=True,
            )
            session.add(mapping)
            await session.flush()
            mapping_id = mapping.id
        incomplete = _outbox(
            destination=WORK_PACKAGE_PROJECTION_DESTINATION,
            linked_entity_type="topic_mapping",
            linked_entity_id=mapping_id,
            operation_type="render_topic_status",
            payload={"role": "topic_idle", "chat_id": "-100", "thread_id": 44},
        )
        await _prepare_raises(factory, incomplete, "invalid_topic_status_payload")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_trace_rejects_invalid_or_missing_task(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(
                payload={
                    "role": ORCHESTRATION_TRACE_ROLE,
                    "trace_kind": INTERPRETER_TRACE_KIND,
                    "task_id": "nope",
                }
            ),
            "invalid_orchestration_trace_task_id",
        )
        await _prepare_raises(
            factory,
            _outbox(
                payload={
                    "role": ORCHESTRATION_TRACE_ROLE,
                    "trace_kind": INTERPRETER_TRACE_KIND,
                    "task_id": str(uuid.uuid4()),
                }
            ),
            "orchestration_trace_task_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


def _seed_interpreter_trace_outbox(task_id: uuid.UUID, interpretation_id: uuid.UUID) -> Any:
    return _outbox(
        linked_entity_type="interpretation",
        linked_entity_id=interpretation_id,
        payload={
            "role": ORCHESTRATION_TRACE_ROLE,
            "trace_kind": INTERPRETER_TRACE_KIND,
            "task_id": str(task_id),
            "model_task_draft": {"task_type": "coding", "operation": "create"},
            "input_tokens": 11,
            "output_tokens": 7,
            "duration_ms": 90,
            "repaired": False,
        },
    )


@pytest.mark.postgresql
def test_interpreter_trace_requires_matching_interpretation(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="trace me",
                task_type="coding",
            )
            task_id = task.id
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="interpretation",
                payload={
                    "role": ORCHESTRATION_TRACE_ROLE,
                    "trace_kind": INTERPRETER_TRACE_KIND,
                    "task_id": str(task_id),
                },
            ),
            "orchestration_trace_interpretation_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_trace_system_thread_falls_back_to_mapping_then_fails_closed(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="fallback trace",
                task_type="coding",
            )
            interpretation = Interpretation(
                task_id=task.id,
                original_input_hash="d" * 64,
                task_draft={
                    "task_type": "coding",
                    "operation": "create",
                    "normalized_title": "Fallback trace",
                    "clarification_question": "what scope?",
                },
                profile_id="interpreter",
                model="test-model",
                prompt_version="v1",
                schema_version="1.4",
            )
            uow.session.add(interpretation)
            await uow.session.flush()
            item = _seed_interpreter_trace_outbox(task.id, interpretation.id)
            uow.session.add(item)
            await uow.session.flush()
            item_id = item.id
        async with factory() as session:
            fresh = await session.get(TransactionalOutbox, item_id)
            assert fresh is not None
            with pytest.raises(PermanentDeliveryError) as excinfo:
                await prepare_delivery(session, fresh)
        assert excinfo.value.category == "system_topic_missing"
        async with factory.begin() as session:
            session.add(
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=66,
                    topic_kind="system",
                    accepts_new_tasks=False,
                    default_workflow="simple_model",
                    enabled=True,
                )
            )
        async with factory() as session:
            fresh = await session.get(TransactionalOutbox, item_id)
            assert fresh is not None
            prepared = await prepare_delivery(session, fresh)
        assert prepared.action is DeliveryAction.SEND_STATUS
        assert prepared.thread_id == 66
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_planner_trace_validates_kind_step_and_run(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            other = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="other task",
                task_type="coding",
            )
            run_id = await uow.runs.create(
                task_id=other.id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
            )
            step_id = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="plan",
                idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                max_attempts=3,
            )
        base = {
            "role": ORCHESTRATION_TRACE_ROLE,
            "task_id": str(other.id),
        }
        await _prepare_raises(
            factory,
            _outbox(payload={**base, "trace_kind": "mystery"}),
            "invalid_orchestration_trace_kind",
        )
        await _prepare_raises(
            factory,
            _outbox(
                linked_entity_type="step",
                payload={**base, "trace_kind": PLANNER_TRACE_KIND},
            ),
            "orchestration_trace_planner_missing",
        )
        mismatch = _outbox(
            linked_entity_type="step",
            linked_entity_id=step_id.id,
            payload={**base, "trace_kind": PLANNER_TRACE_KIND},
        )
        async with UnitOfWork(factory) as uow:
            victim = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=11,
                original_text="victim task",
                task_type="coding",
            )
            mismatch.payload["task_id"] = str(victim.id)
        await _prepare_raises(factory, mismatch, "orchestration_trace_run_missing")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_traces_are_skipped_when_sampling_disabled(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="sampled away",
                task_type="coding",
            )
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
            )
            step_id = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="plan",
                idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                max_attempts=3,
            )
        item = _outbox(
            linked_entity_type="step",
            linked_entity_id=step_id.id,
            payload={
                "role": ORCHESTRATION_TRACE_ROLE,
                "trace_kind": PLANNER_TRACE_KIND,
                "task_id": str(task.id),
            },
        )
        async with factory() as session:
            prepared = await prepare_delivery(session, item, trace_enabled=False)
        assert prepared.action is DeliveryAction.NOOP
        assert prepared.thread_id is None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_history_report_validates_status_and_skips_without_report(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(
                payload={
                    "role": TASK_HISTORY_ROLE,
                    "task_id": str(uuid.uuid4()),
                    "terminal_status": "bogus",
                }
            ),
            "invalid_history_terminal_status",
        )
        item = _outbox(payload={"role": TASK_HISTORY_ROLE, "task_id": str(uuid.uuid4())})
        async with factory() as session:
            prepared = await prepare_delivery(session, item)
        assert prepared.action is DeliveryAction.NOOP
        assert prepared.chat_id == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_history_report_is_sent_once_per_task(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with factory.begin() as session:
            task = Task(
                user_id=42,
                source_chat_id=-100,
                source_thread_id=10,
                original_text="finished work",
                task_type="general",
                status=TaskStatus.COMPLETED,
            )
            session.add(task)
            await session.flush()
            task_id = task.id
        async with factory.begin() as session:
            session.add(
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=10,
                    topic_kind=HISTORY_TOPIC_KIND.value,
                    default_workflow="simple_model",
                    enabled=True,
                )
            )
        item = _outbox(payload={"role": TASK_HISTORY_ROLE, "task_id": str(task_id)})
        async with factory() as session:
            first = await prepare_delivery(session, item)
        assert first.action is DeliveryAction.SEND_STATUS
        assert first.message_role == TASK_HISTORY_ROLE
        async with factory.begin() as session:
            session.add(
                TelegramMessageLink(
                    chat_id=first.chat_id,
                    message_thread_id=first.thread_id,
                    message_id=808,
                    task_id=task_id,
                    message_role=TASK_HISTORY_ROLE,
                    projection_revision=first.revision or 1,
                )
            )
        async with factory() as session:
            second = await prepare_delivery(session, item)
        assert second.action is DeliveryAction.NOOP
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_dashboard_rejects_invalid_chat_and_requires_mapping(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await _prepare_raises(
            factory,
            _outbox(payload={"role": PROJECT_STATUS_DASHBOARD_ROLE}),
            "invalid_dashboard_chat_id",
        )
        await _prepare_raises(
            factory,
            _outbox(payload={"role": PROJECT_STATUS_DASHBOARD_ROLE, "chat_id": -100}),
            "status_dashboard_topic_missing",
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_dashboard_same_revision_is_noop(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with factory.begin() as session:
            session.add(
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=77,
                    topic_kind=STATUS_DASHBOARD_TOPIC_KIND.value,
                    accepts_new_tasks=False,
                    default_workflow="simple_model",
                    enabled=True,
                )
            )
        item = _outbox(payload={"role": PROJECT_STATUS_DASHBOARD_ROLE, "chat_id": -100})
        async with factory() as session:
            first = await prepare_delivery(session, item)
        assert first.action is DeliveryAction.SEND_STATUS
        revision = first.revision
        assert revision is not None
        async with factory.begin() as session:
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=77,
                    message_id=909,
                    message_role=PROJECT_STATUS_DASHBOARD_ROLE,
                    projection_revision=revision,
                )
            )
        async with factory() as session:
            second = await prepare_delivery(session, item)
        assert second.action is DeliveryAction.NOOP
        await engine.dispose()

    asyncio.run(scenario())


class _ZeroMessageClient(FakeTelegramClient):
    async def send_message(self, **kwargs: Any) -> int:
        await super().send_message(**kwargs)
        return 0


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("failure", "expected_category"),
    [
        (Forbidden("bot blocked"), "telegram_forbidden"),
        (TelegramError("weird"), "telegram_telegramerror"),
    ],
)
def test_client_errors_dead_letter_outbox_item(
    postgres_dsn: str, failure: Exception, expected_category: str
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _task_id, outbox_id = await seed_delivery(factory, message_id=94)
        delivery = service(factory, FakeTelegramClient(fail=failure))
        assert await delivery.deliver_one()
        async with factory() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            assert item is not None
            assert item.status is DeliveryStatus.DEAD_LETTER
            assert item.last_error_category == expected_category
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_bad_request_delete_is_retried_as_transient(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        # BadRequest subclasses NetworkError, so the transient handler precedes the
        # dead-letter handler for every action; deletes are retried, not buried.
        async with UnitOfWork(factory) as uow:
            outbox_id = await uow.outbox.enqueue(
                destination="telegram",
                operation_type="delete_message",
                entity_type="none",
                entity_id=uuid.uuid4(),
                idempotency_key=f"delete:{uuid.uuid4()}",
                payload={"role": "user_command_delete", "chat_id": -100, "message_id": 55},
            )
        delivery = service(factory, FakeTelegramClient(fail=BadRequest("bad request")))
        assert await delivery.deliver_one()
        async with factory() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            assert item is not None
            assert item.status is DeliveryStatus.PENDING
            assert item.last_error_category == "badrequest"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_send_without_confirmed_message_id_marks_ambiguous(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _task_id, outbox_id = await seed_delivery(factory, message_id=95)
        client = _ZeroMessageClient(next_message_id=1)
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        assert len(client.sent) == 1
        async with factory() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            assert item is not None
            assert item.status is DeliveryStatus.AMBIGUOUS
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_repost_plan_sends_fresh_card_and_retires_stale_links(postgres_dsn: str) -> None:
    from tests.integration.telegram.test_projections import _materialize_plan_item
    from vuzol.telegram.work_package_projections import WORK_PACKAGE_PLAN_ROLE

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _, package_id = await _materialize_plan_item(factory, ordinal=2)
        async with factory() as session:
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=92,
                    message_id=111,
                    work_package_id=package_id,
                    message_role=WORK_PACKAGE_PLAN_ROLE,
                )
            )
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=92,
                    message_id=222,
                    work_package_id=package_id,
                    message_role=WORK_PACKAGE_PLAN_ROLE,
                )
            )

        async with factory() as session:
            prepared = await prepare_delivery(
                session,
                _outbox(
                    destination=WORK_PACKAGE_PROJECTION_DESTINATION,
                    operation_type="repost_plan",
                    linked_entity_type="work_package",
                    linked_entity_id=package_id,
                ),
            )

        assert prepared.action is DeliveryAction.SEND_STATUS
        assert prepared.message_id is None
        assert prepared.message_role == WORK_PACKAGE_PLAN_ROLE
        assert prepared.work_package_id == package_id
        assert prepared.revision is not None

        async with factory() as session:
            remaining = await session.scalars(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.work_package_id == package_id,
                    TelegramMessageLink.message_role == WORK_PACKAGE_PLAN_ROLE,
                )
            )
            assert remaining.all() == []
        await engine.dispose()

    asyncio.run(scenario())
