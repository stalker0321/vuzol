"""Server-issued turn claim, checked in the same transaction as each command."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import LeadershipTurn, ManagedGoal
from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.types import DeliveryStatus


@dataclass(frozen=True)
class TurnClaim:
    goal_id: uuid.UUID
    wakeup_id: uuid.UUID
    owner: str
    lease_epoch: int
    outbox_generation: int


async def claim_current(
    session: AsyncSession, goal: ManagedGoal, claim: TurnClaim, turn: LeadershipTurn | None = None
) -> bool:
    now = datetime.now(UTC)
    if (
        goal.id != claim.goal_id
        or goal.lease_owner != claim.owner
        or goal.lease_epoch != claim.lease_epoch
        or goal.lease_until is None
        or goal.lease_until <= now
    ):
        return False
    if turn is not None and (
        turn.lease_epoch != claim.lease_epoch
        or turn.owner != claim.owner
        or turn.wakeup_id != claim.wakeup_id
    ):
        return False
    item = await session.get(TransactionalOutbox, claim.wakeup_id, with_for_update=True)
    return (
        item is not None
        and item.status is DeliveryStatus.LEASED
        and item.lease_owner == claim.owner
        and item.lease_generation == claim.outbox_generation
        and item.lease_expires_at is not None
        and item.lease_expires_at > now
    )
