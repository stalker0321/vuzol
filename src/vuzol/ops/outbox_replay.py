"""Operator tooling for reviewing and requeueing dead-lettered outbox work.

Dead-lettered pipeline items are user requests that failed permanently after
exhausting their retry budget. This module powers the read-only review and the
explicit requeue action behind ``vuzol-outbox-replay`` so operators no longer
need raw ``psql`` access to recover them.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.types import DeliveryStatus


@dataclass(frozen=True)
class DeadLetterItem:
    item_id: uuid.UUID
    destination: str
    operation_type: str
    linked_entity_type: str
    linked_entity_id: uuid.UUID
    attempt_count: int
    error_category: str | None
    created_at: datetime


async def list_dead_letter_items(
    session: AsyncSession,
    *,
    destinations: frozenset[str] | None = None,
    item_ids: tuple[uuid.UUID, ...] = (),
) -> tuple[DeadLetterItem, ...]:
    """Return dead-lettered outbox items, oldest first, optionally filtered."""

    statement = (
        select(TransactionalOutbox)
        .where(TransactionalOutbox.status == DeliveryStatus.DEAD_LETTER)
        .order_by(TransactionalOutbox.created_at, TransactionalOutbox.id)
    )
    if destinations is not None:
        if not destinations:
            return ()
        statement = statement.where(TransactionalOutbox.destination.in_(sorted(destinations)))
    if item_ids:
        statement = statement.where(TransactionalOutbox.id.in_(item_ids))
    rows = (await session.scalars(statement)).all()
    return tuple(
        DeadLetterItem(
            item_id=row.id,
            destination=row.destination,
            operation_type=row.operation_type,
            linked_entity_type=row.linked_entity_type,
            linked_entity_id=row.linked_entity_id,
            attempt_count=row.attempt_count,
            error_category=row.last_error_category,
            created_at=row.created_at,
        )
        for row in rows
    )


async def requeue_dead_letter_items(
    session: AsyncSession,
    *,
    items: tuple[DeadLetterItem, ...],
) -> int:
    """Requeue dead-lettered items with a fresh attempt budget.

    The attempt counter resets to zero because the operator explicitly decided
    to retry; without the reset the next transient failure would immediately
    dead-letter again. ``last_error_category`` is preserved until the next
    outcome overwrites it.
    """

    ids = [item.item_id for item in items]
    if not ids:
        return 0
    result = await session.execute(
        update(TransactionalOutbox)
        .where(
            TransactionalOutbox.id.in_(ids),
            TransactionalOutbox.status == DeliveryStatus.DEAD_LETTER,
        )
        .values(
            status=DeliveryStatus.PENDING,
            attempt_count=0,
            available_at=func.now(),
            last_error_ambiguous=False,
            lease_owner=None,
            lease_expires_at=None,
        )
    )
    return int(getattr(result, "rowcount", 0) or 0)
