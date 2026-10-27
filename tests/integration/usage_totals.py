"""Usage-ledger projections the accounting tests check their rows against."""

import uuid
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import UsageRecord
from vuzol.storage.types import AttemptKind


async def usage_totals_by_purpose(
    session: AsyncSession, *, task_id: uuid.UUID
) -> list[tuple[str | None, Decimal, int]]:
    statement = (
        select(
            UsageRecord.purpose,
            func.coalesce(func.sum(UsageRecord.cost_units), 0),
            func.count(),
        )
        .where(UsageRecord.task_id == task_id)
        .group_by(UsageRecord.purpose)
    )
    rows = (await session.execute(statement)).all()
    return [
        (str(row[0]) if row[0] is not None else None, Decimal(row[1]), int(row[2])) for row in rows
    ]


async def usage_retry_subtotal(session: AsyncSession, *, task_id: uuid.UUID) -> tuple[Decimal, int]:
    statement = select(
        func.coalesce(func.sum(UsageRecord.cost_units), 0),
        func.count(),
    ).where(
        UsageRecord.task_id == task_id,
        UsageRecord.attempt_kind.is_not(None),
        UsageRecord.attempt_kind != AttemptKind.INITIAL.value,
    )
    row = (await session.execute(statement)).one()
    return Decimal(row[0]), int(row[1])
