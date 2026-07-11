"""PostgreSQL SKIP LOCKED step claims with fencing generations."""

from datetime import timedelta
from typing import Any, cast

from sqlalchemy import CursorResult, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import Step, TransactionalOutbox
from vuzol.storage.records import LeaseToken, OutboxLeaseToken, StepRecord
from vuzol.storage.repositories.core import step_record
from vuzol.storage.types import DeliveryStatus, StepStatus


async def claim_step(
    session: AsyncSession,
    *,
    owner: str,
    lease_seconds: int,
    capabilities: frozenset[str],
) -> LeaseToken | None:
    statement = (
        select(Step)
        .where(
            Step.status == StepStatus.QUEUED,
            Step.available_at <= func.now(),
            Step.required_capabilities.contained_by(sorted(capabilities)),
        )
        .order_by(Step.priority, Step.available_at, Step.created_at)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    step = await session.scalar(statement)
    if step is None:
        return None
    step.status = StepStatus.LEASED
    step.lease_owner = owner
    step.lease_generation += 1
    step.heartbeat_at = func.now()
    step.lease_expires_at = func.now() + timedelta(seconds=lease_seconds)
    step.attempt_count += 1
    await session.flush()
    await session.refresh(step, attribute_names=["heartbeat_at", "lease_expires_at"])
    return LeaseToken(step=step_record(step), owner=owner, generation=step.lease_generation)


async def heartbeat_step(
    session: AsyncSession,
    token: LeaseToken,
    *,
    lease_seconds: int,
) -> None:
    statement = (
        update(Step)
        .where(
            Step.id == token.step.id,
            Step.lease_owner == token.owner,
            Step.lease_generation == token.generation,
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
        )
        .values(
            heartbeat_at=func.now(),
            lease_expires_at=func.now() + timedelta(seconds=lease_seconds),
        )
    )
    result = cast(CursorResult[Any], await session.execute(statement))
    if result.rowcount != 1:
        raise LeaseLost(f"step lease lost: {token.step.id}")


async def complete_step(
    session: AsyncSession,
    token: LeaseToken,
    *,
    result_payload: dict[str, object],
) -> None:
    statement = (
        update(Step)
        .where(
            Step.id == token.step.id,
            Step.lease_owner == token.owner,
            Step.lease_generation == token.generation,
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
        )
        .values(
            status=StepStatus.COMPLETED,
            result=result_payload,
            lease_owner=None,
            lease_expires_at=None,
        )
    )
    result = cast(CursorResult[Any], await session.execute(statement))
    if result.rowcount != 1:
        raise LeaseLost(f"step lease lost: {token.step.id}")


async def find_expired_leases(session: AsyncSession) -> tuple[StepRecord, ...]:
    statement = (
        select(Step)
        .where(
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
            Step.lease_expires_at < func.now(),
        )
        .order_by(Step.lease_expires_at, Step.id)
    )
    steps = (await session.scalars(statement)).all()
    return tuple(step_record(step) for step in steps)


async def claim_outbox_item(
    session: AsyncSession,
    *,
    owner: str,
    lease_seconds: int,
    allowed_destinations: frozenset[str],
) -> OutboxLeaseToken | None:
    if not allowed_destinations:
        return None
    statement = (
        select(TransactionalOutbox)
        .where(
            TransactionalOutbox.destination.in_(sorted(allowed_destinations)),
            (
                (TransactionalOutbox.status == DeliveryStatus.PENDING)
                | (
                    (TransactionalOutbox.status == DeliveryStatus.LEASED)
                    & (TransactionalOutbox.lease_expires_at < func.now())
                )
            ),
            TransactionalOutbox.available_at <= func.now(),
        )
        .order_by(TransactionalOutbox.available_at, TransactionalOutbox.created_at)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    item = await session.scalar(statement)
    if item is None:
        return None
    item.status = DeliveryStatus.LEASED
    item.lease_owner = owner
    item.lease_generation += 1
    item.lease_expires_at = func.now() + timedelta(seconds=lease_seconds)
    item.attempt_count += 1
    await session.flush()
    await session.refresh(item, attribute_names=["lease_expires_at"])
    assert item.lease_expires_at is not None
    return OutboxLeaseToken(
        item_id=item.id,
        status=item.status,
        owner=owner,
        generation=item.lease_generation,
        lease_expires_at=item.lease_expires_at,
    )


async def complete_outbox_item(session: AsyncSession, token: OutboxLeaseToken) -> None:
    statement = (
        update(TransactionalOutbox)
        .where(
            TransactionalOutbox.id == token.item_id,
            TransactionalOutbox.lease_owner == token.owner,
            TransactionalOutbox.lease_generation == token.generation,
            TransactionalOutbox.status == DeliveryStatus.LEASED,
        )
        .values(
            status=DeliveryStatus.DELIVERED,
            delivered_at=func.now(),
            lease_owner=None,
            lease_expires_at=None,
        )
    )
    result = cast(CursorResult[Any], await session.execute(statement))
    if result.rowcount != 1:
        raise LeaseLost(f"outbox lease lost: {token.item_id}")


async def mark_outbox_ambiguous(session: AsyncSession, token: OutboxLeaseToken) -> None:
    """Quarantine an unknown external outcome until explicit reconciliation."""

    statement = (
        update(TransactionalOutbox)
        .where(
            TransactionalOutbox.id == token.item_id,
            TransactionalOutbox.lease_owner == token.owner,
            TransactionalOutbox.lease_generation == token.generation,
            TransactionalOutbox.status == DeliveryStatus.LEASED,
        )
        .values(
            status=DeliveryStatus.AMBIGUOUS,
            last_error_category="ambiguous_external_outcome",
            last_error_ambiguous=True,
            lease_owner=None,
            lease_expires_at=None,
        )
    )
    result = cast(CursorResult[Any], await session.execute(statement))
    if result.rowcount != 1:
        raise LeaseLost(f"outbox lease lost: {token.item_id}")


async def retry_outbox_item(
    session: AsyncSession,
    token: OutboxLeaseToken,
    *,
    delay_seconds: float,
    error_category: str,
) -> None:
    """Return a transient failure to the queue while preserving fencing metadata."""

    statement = (
        update(TransactionalOutbox)
        .where(
            TransactionalOutbox.id == token.item_id,
            TransactionalOutbox.lease_owner == token.owner,
            TransactionalOutbox.lease_generation == token.generation,
            TransactionalOutbox.status == DeliveryStatus.LEASED,
        )
        .values(
            status=DeliveryStatus.PENDING,
            available_at=func.now() + timedelta(seconds=delay_seconds),
            last_error_category=error_category,
            last_error_ambiguous=False,
            lease_owner=None,
            lease_expires_at=None,
        )
    )
    result = cast(CursorResult[Any], await session.execute(statement))
    if result.rowcount != 1:
        raise LeaseLost(f"outbox lease lost: {token.item_id}")


async def dead_letter_outbox_item(
    session: AsyncSession,
    token: OutboxLeaseToken,
    *,
    error_category: str,
) -> None:
    statement = (
        update(TransactionalOutbox)
        .where(
            TransactionalOutbox.id == token.item_id,
            TransactionalOutbox.lease_owner == token.owner,
            TransactionalOutbox.lease_generation == token.generation,
            TransactionalOutbox.status == DeliveryStatus.LEASED,
        )
        .values(
            status=DeliveryStatus.DEAD_LETTER,
            last_error_category=error_category,
            last_error_ambiguous=False,
            lease_owner=None,
            lease_expires_at=None,
        )
    )
    result = cast(CursorResult[Any], await session.execute(statement))
    if result.rowcount != 1:
        raise LeaseLost(f"outbox lease lost: {token.item_id}")
