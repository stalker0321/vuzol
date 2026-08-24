"""Integration tests for the dead-letter outbox replay tooling."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.integration.storage.helpers import storage

from vuzol.ops.outbox_replay import list_dead_letter_items, requeue_dead_letter_items
from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.types import DeliveryStatus

pytestmark = pytest.mark.postgresql


def _outbox(
    *,
    idempotency_key: str,
    destination: str = "discussion_classify",
    status: DeliveryStatus = DeliveryStatus.DEAD_LETTER,
    attempt_count: int = 3,
    error_category: str | None = "provider_unavailable",
) -> TransactionalOutbox:
    return TransactionalOutbox(
        destination=destination,
        operation_type="classify_intake",
        linked_entity_type="telegram_intake",
        linked_entity_id=uuid.uuid4(),
        idempotency_key=idempotency_key,
        status=status,
        attempt_count=attempt_count,
        available_at=datetime.now(UTC) - timedelta(minutes=5),
        last_error_category=error_category,
    )


async def _seed(factory: async_sessionmaker[AsyncSession], *items: TransactionalOutbox) -> None:
    async with factory() as session:
        session.add_all(items)
        await session.commit()


async def _statuses(
    factory: async_sessionmaker[AsyncSession],
) -> dict[uuid.UUID, DeliveryStatus]:
    async with factory() as session:
        rows = (await session.scalars(select(TransactionalOutbox))).all()
        return {row.id: row.status for row in rows}


def test_dead_letter_listing_filters_and_requeue_round_trip(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        dead_discussion = _outbox(idempotency_key="dl-discussion-1")
        dead_telegram = _outbox(
            idempotency_key="dl-telegram-1",
            destination="telegram",
            error_category="context_mismatch",
        )
        delivered = _outbox(idempotency_key="delivered-1", status=DeliveryStatus.DELIVERED)
        pending = _outbox(idempotency_key="pending-1", status=DeliveryStatus.PENDING)
        await _seed(factory, dead_discussion, dead_telegram, delivered, pending)

        async with factory() as session:
            all_dead = await list_dead_letter_items(session)
            assert {item.item_id for item in all_dead} == {
                dead_discussion.id,
                dead_telegram.id,
            }
            discussion_only = await list_dead_letter_items(
                session, destinations=frozenset({"discussion_classify"})
            )
            assert [item.item_id for item in discussion_only] == [dead_discussion.id]
            empty_filter = await list_dead_letter_items(session, destinations=frozenset())
            assert empty_filter == ()
            by_id = await list_dead_letter_items(session, item_ids=(dead_telegram.id,))
            assert [item.item_id for item in by_id] == [dead_telegram.id]
            assert by_id[0].attempt_count == 3
            assert by_id[0].error_category == "context_mismatch"

        async with factory.begin() as session:
            requeued = await requeue_dead_letter_items(session, items=discussion_only)
        assert requeued == 1

        statuses = await _statuses(factory)
        assert statuses[dead_discussion.id] is DeliveryStatus.PENDING
        assert statuses[dead_telegram.id] is DeliveryStatus.DEAD_LETTER
        assert statuses[delivered.id] is DeliveryStatus.DELIVERED
        assert statuses[pending.id] is DeliveryStatus.PENDING

        # Replaying an already requeued item is a no-op.
        async with factory.begin() as session:
            again = await requeue_dead_letter_items(session, items=discussion_only)
            current = await list_dead_letter_items(session)
        assert again == 0
        assert all(item.item_id != dead_discussion.id for item in current)
        await engine.dispose()

    asyncio.run(scenario())


def test_requeue_resets_attempt_budget_and_clears_lease(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        item = _outbox(
            idempotency_key="discussion:classify:lost-message",
            attempt_count=3,
            error_category="context_mismatch",
        )
        item.lease_owner = "stale-worker"
        item.lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)
        await _seed(factory, item)

        async with factory() as session:
            selected = await list_dead_letter_items(session)
            assert len(selected) == 1
            assert selected[0].attempt_count == 3

        async with factory.begin() as session:
            requeued = await requeue_dead_letter_items(session, items=selected)
        assert requeued == 1

        async with factory() as session:
            row = await session.get(TransactionalOutbox, item.id)
            assert row is not None
            assert row.status is DeliveryStatus.PENDING
            assert row.attempt_count == 0
            assert row.last_error_category == "context_mismatch"
            assert row.last_error_ambiguous is False
            assert row.lease_owner is None and row.lease_expires_at is None
        await engine.dispose()

    asyncio.run(scenario())
