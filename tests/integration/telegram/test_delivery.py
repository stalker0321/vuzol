import asyncio
import uuid
from typing import Any

import pytest
from sqlalchemy import func, select, update
from telegram.error import NetworkError

from tests.integration.storage.helpers import storage
from vuzol.storage.models import (
    Interpretation,
    ProjectNamingRequest,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    DeliveryStatus,
    IntakeStatus,
    ProjectNamingStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.projections import FakeTelegramClient, LostTelegramResponse

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
) -> TelegramDeliveryService:
    return TelegramDeliveryService(
        factory,
        client,
        owner=owner,
        lease_seconds=30,
        max_attempts=max_attempts,
        retry_min_seconds=1,
        retry_max_seconds=10,
    )


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
        _, outbox_id = await seed_delivery(factory, message_id=30)
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


def test_unknown_send_is_ambiguous_and_clarification_has_no_task_link(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _, unknown_id = await seed_delivery(factory, message_id=40)
        unknown = service(factory, FakeTelegramClient(fail=LostTelegramResponse("lost")))
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
        assert "Project status" in html
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
        assert "No active tasks right now." in client.edited[0][2]
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
