"""Exclusive device slot leases with generation fencing (WP12).

Same mechanism family as ``storage.leasing`` (one advisory lock, single-row
claim, fencing generations) — not a second lock mechanism: the advisory key
is new, the pattern is shared. A stale holder can never release or reuse a
slot taken over by a newer generation; restart reconciles expired slots
explicitly before any reuse.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import NodeSlot

SLOT_CLAIM_LOCK_KEY = 8_946_527_106


@dataclass(frozen=True, slots=True)
class SlotToken:
    node_id: str
    slot_name: str
    owner: str
    generation: int


@dataclass(frozen=True, slots=True)
class SlotRecord:
    node_id: str
    slot_name: str
    claimed_by: str | None
    generation: int
    lease_expires_at: datetime | None


async def claim_slot(
    session: AsyncSession,
    *,
    node_id: str,
    slot_name: str,
    owner: str,
    lease_seconds: int,
    now: datetime | None = None,
) -> SlotToken | None:
    """Claim a free slot; held slots return None (contention).

    Expired-but-held slots are NOT taken here: a restart must reconcile the
    old writer explicitly (``reconcile_slot``) before any reuse, so a new
    claim alone can never pick up a previous owner's slot.
    """

    if not slot_name or lease_seconds < 1:
        return None
    await session.execute(select(func.pg_advisory_xact_lock(SLOT_CLAIM_LOCK_KEY)))
    moment = now or datetime.now(UTC)
    row = await session.scalar(
        select(NodeSlot)
        .where(NodeSlot.node_id == node_id, NodeSlot.slot_name == slot_name)
        .with_for_update()
    )
    if row is None:
        row = NodeSlot(node_id=node_id, slot_name=slot_name)
        session.add(row)
        await session.flush()
    if row.claimed_by is not None:
        return None
    row.claimed_by = owner
    row.claim_generation += 1
    row.claimed_at = moment
    row.lease_expires_at = moment + timedelta(seconds=lease_seconds)
    await session.flush()
    return SlotToken(
        node_id=node_id, slot_name=slot_name, owner=owner, generation=row.claim_generation
    )


async def release_slot(session: AsyncSession, token: SlotToken) -> None:
    """Release only with a matching owner+generation; stale holders fail."""

    await session.execute(select(func.pg_advisory_xact_lock(SLOT_CLAIM_LOCK_KEY)))
    row = await session.scalar(
        select(NodeSlot)
        .where(NodeSlot.node_id == token.node_id, NodeSlot.slot_name == token.slot_name)
        .with_for_update()
    )
    if row is None or row.claimed_by != token.owner or row.claim_generation != token.generation:
        raise LeaseLost(f"slot lease lost: {token.node_id}/{token.slot_name}")
    row.claimed_by = None
    row.lease_expires_at = None
    await session.flush()


async def find_expired_slots(session: AsyncSession) -> tuple[SlotRecord, ...]:
    statement = (
        select(NodeSlot)
        .where(
            NodeSlot.claimed_by.is_not(None),
            NodeSlot.lease_expires_at < func.now(),
        )
        .order_by(NodeSlot.lease_expires_at, NodeSlot.node_id, NodeSlot.slot_name)
    )
    rows = (await session.scalars(statement)).all()
    return tuple(
        SlotRecord(
            node_id=row.node_id,
            slot_name=row.slot_name,
            claimed_by=row.claimed_by,
            generation=row.claim_generation,
            lease_expires_at=row.lease_expires_at,
        )
        for row in rows
    )


async def reconcile_slot(session: AsyncSession, *, node_id: str, slot_name: str) -> str:
    """Restart reconciliation: release expired slots, never live ones.

    Returns "released", "held" or "free". A slot is reused only after this
    explicit reconcile frees it — never by a new claim alone.
    """

    await session.execute(select(func.pg_advisory_xact_lock(SLOT_CLAIM_LOCK_KEY)))
    row = await session.scalar(
        select(NodeSlot)
        .where(NodeSlot.node_id == node_id, NodeSlot.slot_name == slot_name)
        .with_for_update()
    )
    if row is None or row.claimed_by is None:
        return "free"
    if row.lease_expires_at is not None and row.lease_expires_at < datetime.now(UTC):
        row.claimed_by = None
        row.lease_expires_at = None
        await session.flush()
        return "released"
    return "held"
