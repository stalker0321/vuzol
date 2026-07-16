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
