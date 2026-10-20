"""Durable account slot tickets. No expiry or automatic uncertain-call replay."""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import Coroutine
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import CheckConstraint, DateTime, Integer, String, delete, or_, select, text
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column

from vuzol.storage.base import Base
from vuzol.storage.models import SupervisedProcess
from vuzol.storage.types import ProcessStatus

if TYPE_CHECKING:
    from vuzol.providers.domain import ProviderRequest


class ProviderAccountLease(Base):
    __tablename__ = "provider_account_leases"
    __table_args__ = (CheckConstraint("slot BETWEEN 0 AND 99", name="account_slot_range"),)

    account_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    slot: Mapped[int] = mapped_column(Integer, primary_key=True)
    token: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    profile_id: Mapped[str] = mapped_column(String(100), nullable=False)
    # Diagnostic identities, not foreign keys: turn requests have no task rows.
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    request_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    provider_attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    invocation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    binding_kind: Mapped[str | None] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


def account_slot_key(account_id: str, slot: int) -> int:
    """Stable signed bigint; never depends on profile, model, role, or hash()."""
    digest = hashlib.sha256(f"vuzol:provider-account:v1:{account_id}:{slot}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


async def finish_account_cleanup(operation: Coroutine[Any, Any, None]) -> None:
    """Drain cleanup even under repeated task cancellation, then propagate it."""
    task = asyncio.create_task(operation)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError


async def create_account_ticket(
    connection: AsyncConnection,
    *,
    account_id: str,
    slot: int,
    token: uuid.UUID,
    profile_id: str,
    request: ProviderRequest,
    binding_kind: str,
) -> bool:
    """Caller holds the session lock; a successful commit must precede execute."""
    inserted = await connection.scalar(
        insert(ProviderAccountLease)
        .values(
            account_id=account_id,
            slot=slot,
            token=token,
            profile_id=profile_id,
            task_id=request.task_id,
            run_id=request.run_id,
            request_id=request.step_id,
            provider_attempt=request.provider_attempt,
            invocation_id=token,
            binding_kind=binding_kind,
        )
        .on_conflict_do_nothing(index_elements=["account_id", "slot"])
        .returning(ProviderAccountLease.token)
    )
    # Commit the connection, NOT its owning session: session.commit() returns
    # the locked connection to the pool. The connection stays pinned to close.
    await connection.commit()
    return inserted == token


async def clear_account_ticket(
    connection: AsyncConnection, *, account_id: str, slot: int, token: uuid.UUID
) -> bool:
    """Caller must hold the slot lock and have authoritative cleanup evidence."""
    removed = await connection.scalar(
        delete(ProviderAccountLease)
        .where(
            ProviderAccountLease.account_id == account_id,
            ProviderAccountLease.slot == slot,
            ProviderAccountLease.token == token,
        )
        .returning(ProviderAccountLease.token)
    )
    await connection.commit()
    return removed == token


async def account_slot_reserved(
    connection: AsyncConnection,
    *,
    account_id: str,
    slot: int,
    account_profile_ids: tuple[str, ...],
    concurrency_limit: int = 100,
) -> bool:
    records = await connection.scalars(
        select(SupervisedProcess.runtime_metadata).where(
            or_(
                SupervisedProcess.profile_id.in_(account_profile_ids),
                SupervisedProcess.runtime_metadata["account_id"].as_string() == account_id,
            ),
            or_(
                SupervisedProcess.status != ProcessStatus.EXITED,
                SupervisedProcess.reaped_at.is_(None),
            ),
        )
    )
    for metadata in records:
        reserved = metadata.get("account_slot")
        if (
            metadata.get("account_id") != account_id
            or type(reserved) is not int
            or not 0 <= reserved < concurrency_limit
            or reserved == slot
        ):
            return True
    return False


async def unlock_account_slot(connection: AsyncConnection, account_id: str, slot: int) -> None:
    try:
        released = await connection.scalar(
            text("SELECT pg_advisory_unlock(:key)"),
            {"key": account_slot_key(account_id, slot)},
        )
        if released is not True:
            raise RuntimeError("provider account advisory lock was lost")
    except BaseException:
        await connection.invalidate()
        raise


async def release_account_ticket_after_cleanup(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    account_id: str,
    slot: int,
    token: uuid.UUID,
    account_profile_ids: tuple[str, ...],
    cleanup_confirmed: bool,
) -> bool:
    """Reaper hook, never a timeout-based recovery policy.

    The caller must prove the original invocation cannot still execute (worker
    fenced/stopped AND runtime reaped), and persist terminal process evidence.
    Absence of a process row alone is NOT proof. A held lock, unresolved process,
    or different ticket prevents deletion. API effects require their own proof.
    """
    if not cleanup_confirmed:
        return False
    async with session_factory() as session:
        connection = await session.connection()
        try:
            acquired = await connection.scalar(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": account_slot_key(account_id, slot)},
            )
        except BaseException:
            await finish_account_cleanup(connection.invalidate())
            raise
        if acquired is not True:
            return False
        try:
            if await account_slot_reserved(
                connection,
                account_id=account_id,
                slot=slot,
                account_profile_ids=account_profile_ids,
            ):
                return False
            return await clear_account_ticket(
                connection, account_id=account_id, slot=slot, token=token
            )
        finally:
            await finish_account_cleanup(unlock_account_slot(connection, account_id, slot))
