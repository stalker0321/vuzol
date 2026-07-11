import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update

from vuzol.storage.errors import LeaseLost, StorageError
from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item, mark_outbox_ambiguous
from vuzol.storage.models import (
    Approval,
    Artifact,
    ExternalInbox,
    Task,
    TelegramMessageLink,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import ApprovalStatus, DeliveryStatus
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import seed_task_run_step, storage


@pytest.mark.postgresql
def test_inbox_duplicate_creates_one_canonical_record(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            first_id, first_created = await uow.inbox.receive_once(
                source="telegram",
                consumer="bot:main",
                external_event_id="update-1",
                payload_hash="a" * 64,
            )
        async with UnitOfWork(factory) as uow:
            second_id, second_created = await uow.inbox.receive_once(
                source="telegram",
                consumer="bot:main",
                external_event_id="update-1",
                payload_hash="a" * 64,
            )
        assert first_created and not second_created and first_id == second_id
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_outbox_delivery_uses_single_fenced_lease(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, _, _ = await seed_task_run_step(factory)
        async with UnitOfWork(factory) as uow:
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send",
                entity_type="task",
                entity_id=task.id,
                idempotency_key="task-result",
                payload={"text": "done"},
            )

        async def claim(owner: str) -> OutboxLeaseToken | None:
            async with factory.begin() as session:
                return await claim_outbox_item(
                    session,
                    owner=owner,
                    lease_seconds=60,
                    allowed_destinations=frozenset({"telegram"}),
                )

        claims = await asyncio.gather(claim("delivery-a"), claim("delivery-b"))
        tokens = [token for token in claims if token is not None]
        assert len(tokens) == 1
        async with factory.begin() as session:
            await complete_outbox_item(session, tokens[0])
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_ambiguous_outbox_delivery_is_not_automatically_retried(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, _, _ = await seed_task_run_step(factory)
        async with UnitOfWork(factory) as uow:
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send",
                entity_type="task",
                entity_id=task.id,
                idempotency_key="unknown-send",
                payload={"text": "status"},
            )
        async with factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner="delivery-a",
                lease_seconds=60,
                allowed_destinations=frozenset({"telegram"}),
            )
            assert token is not None
            await mark_outbox_ambiguous(session, token)
        async with factory.begin() as session:
            assert (
                await claim_outbox_item(
                    session,
                    owner="delivery-b",
                    lease_seconds=60,
                    allowed_destinations=frozenset({"telegram"}),
                )
                is None
            )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_outbox_claim_filters_destination_and_reclaims_expired_lease(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, _, _ = await seed_task_run_step(factory)
        async with UnitOfWork(factory) as uow:
            for destination in ("workflow_control", "telegram_file", "telegram"):
                await uow.outbox.enqueue(
                    destination=destination,
                    operation_type="test",
                    entity_type="task",
                    entity_id=task.id,
                    idempotency_key=destination,
                    payload={},
                )
        async with factory.begin() as session:
            first = await claim_outbox_item(
                session,
                owner="delivery-a",
                lease_seconds=60,
                allowed_destinations=frozenset({"telegram"}),
            )
        assert first is not None
        async with factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.id == first.item_id)
                .values(lease_expires_at=func.now() - timedelta(seconds=1))
            )
        async with factory.begin() as session:
            second = await claim_outbox_item(
                session,
                owner="delivery-b",
                lease_seconds=60,
                allowed_destinations=frozenset({"telegram"}),
            )
        assert second is not None and second.item_id == first.item_id
        assert second.generation == first.generation + 1
        with pytest.raises(LeaseLost):
            async with factory.begin() as session:
                await complete_outbox_item(session, first)
        async with factory.begin() as session:
            await complete_outbox_item(session, second)
        async with factory() as session:
            untouched = (
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination.in_(["workflow_control", "telegram_file"])
                    )
                )
            ).all()
            assert all(item.status == DeliveryStatus.PENDING for item in untouched)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_approval_is_single_use_and_projection_delete_preserves_task(
    postgres_dsn: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, _, step = await seed_task_run_step(factory)
        approval = Approval(
            step_id=step.id,
            action_envelope_hash="a" * 64,
            requested_action="apply patch",
            normalized_target="repository:main",
            human_summary="Apply validated patch",
            token_hash="b" * 64,
            status=ApprovalStatus.PENDING,
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        async with UnitOfWork(factory) as uow:
            approval_id = await uow.approvals.add(approval)
            link_id = await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=1,
                    message_id=50,
                    task_id=task.id,
                    approval_id=approval_id,
                    message_role="approval_card",
                )
            )
        async with UnitOfWork(factory) as uow:
            await uow.approvals.consume(
                approval_id=approval_id, token_hash="b" * 64, deciding_user_id=1
            )
            await uow.telegram_links.delete_projection(link_id)
        with pytest.raises(StorageError, match="already consumed"):
            async with UnitOfWork(factory) as uow:
                await uow.approvals.consume(
                    approval_id=approval_id, token_hash="b" * 64, deciding_user_id=1
                )
        async with factory() as session:
            assert await session.get(Task, task.id) is not None
            assert await session.get(TelegramMessageLink, link_id) is None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_and_usage_remain_traceable_to_task(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, run_id, step = await seed_task_run_step(factory)
        async with UnitOfWork(factory) as uow:
            artifact_id = await uow.evidence.add(
                Artifact(
                    task_id=task.id,
                    run_id=run_id,
                    step_id=step.id,
                    artifact_type="diff",
                    content_uri="sha256/abc",
                    size_bytes=10,
                    content_hash="c" * 64,
                    media_type="text/x-diff",
                    sensitivity="internal",
                    visibility="user",
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                    metadata_json={},
                )
            )
            usage_id = await uow.evidence.add(
                UsageRecord(
                    provider="provider",
                    profile_id="profile-a",
                    model="model",
                    task_id=task.id,
                    run_id=run_id,
                    step_id=step.id,
                    duration_ms=100,
                    outcome="success",
                )
            )
        async with factory() as session:
            artifact = await session.get(Artifact, artifact_id)
            usage = await session.get(UsageRecord, usage_id)
            assert artifact is not None and artifact.task_id == task.id
            assert usage is not None and usage.step_id == step.id
        await engine.dispose()

    asyncio.run(scenario())
