"""PostgreSQL session advisory locks for backup capture (B2)."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

# Distinct from retention RETENTION_SWEEP_LOCK_KEY = 8_946_527_105
BACKUP_CAPTURE_LOCK_KEY = 8_946_527_110
RETENTION_SWEEP_LOCK_KEY = 8_946_527_105


async def try_advisory_lock(connection: AsyncConnection, key: int) -> bool:
    result = await connection.execute(
        text("SELECT pg_try_advisory_lock(:key)"),
        {"key": key},
    )
    value = result.scalar()
    return value is True


async def advisory_unlock(connection: AsyncConnection, key: int) -> None:
    await connection.execute(
        text("SELECT pg_advisory_unlock(:key)"),
        {"key": key},
    )
