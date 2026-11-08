"""The durable queue: a service submits a call, an executor leases and settles it.

A leased call is the trusted authority for its one sandboxed CLI process, the way
a claimed leadership turn is for the lead's.
"""

from __future__ import annotations

import uuid
from collections.abc import Collection
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import ModelCall

# A failed call is settled, not retried: the caller falls back to its next model.
# Only a call whose executor died is leased again, and not forever.
MAX_ATTEMPTS = 3
# Time beyond the call's own timeout for the sandbox to start and be cleaned up.
LEASE_MARGIN_SECONDS = 120


@dataclass(frozen=True, slots=True)
class ModelCallClaim:
    call_id: uuid.UUID
    owner: str
    generation: int

    @property
    def sandbox_reference(self) -> str:
        return f"model-call:{self.call_id}"


current_model_call: ContextVar[ModelCallClaim | None] = ContextVar("model_call", default=None)


async def submit_call(
    session: AsyncSession,
    *,
    purpose: str,
    profile_id: str,
    system: str,
    user: str,
    output_schema: dict[str, Any],
    timeout_seconds: int,
    wait_seconds: int,
    web_search: bool = False,
    mcp_servers: dict[str, str] | None = None,
) -> uuid.UUID:
    call = ModelCall(
        id=uuid.uuid4(),
        purpose=purpose,
        profile_id=profile_id,
        request={
            "system": system,
            "user": user,
            "output_schema": output_schema,
            "web_search": web_search,
            "mcp_servers": dict(mcp_servers or {}),
        },
        timeout_seconds=timeout_seconds,
        state="queued",
        lease_generation=0,
        deadline_at=datetime.now(UTC) + timedelta(seconds=wait_seconds),
    )
    session.add(call)
    await session.flush()
    return call.id


async def claim_call(
    session: AsyncSession, *, owner: str, profile_ids: Collection[str] | None = None
) -> ModelCallClaim | None:
    """profile_ids narrows a lane, e.g. a node that can run only some profiles."""
    now = datetime.now(UTC)
    while True:
        call = await session.scalar(
            select(ModelCall)
            .where(
                or_(
                    ModelCall.state == "queued",
                    (ModelCall.state == "leased") & (ModelCall.lease_expires_at < now),
                ),
                *(() if profile_ids is None else (ModelCall.profile_id.in_(profile_ids),)),
            )
            .order_by(ModelCall.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if call is None:
            return None
        if call.deadline_at <= now:
            _settle(call, error="deadline_passed")
        elif call.lease_generation >= MAX_ATTEMPTS:
            _settle(call, error="attempts_exhausted")
        else:
            call.state = "leased"
            call.lease_owner = owner
            call.lease_generation += 1
            call.lease_expires_at = now + timedelta(
                seconds=call.timeout_seconds + LEASE_MARGIN_SECONDS
            )
            await session.flush()
            return ModelCallClaim(call.id, owner, call.lease_generation)
        await session.flush()


async def call_current(session: AsyncSession, claim: ModelCallClaim, *, lock: bool = True) -> bool:
    call = await session.get(ModelCall, claim.call_id, with_for_update=lock)
    return (
        call is not None
        and call.state == "leased"
        and call.lease_owner == claim.owner
        and call.lease_generation == claim.generation
        and call.lease_expires_at is not None
        and call.lease_expires_at > datetime.now(UTC)
    )


async def finish_call(
    session: AsyncSession,
    claim: ModelCallClaim,
    *,
    output: dict[str, Any] | None = None,
    error: str | None = None,
    usage: dict[str, Any] | None = None,
) -> bool:
    """Settle the call; False when its lease is gone (expired, or claimed again)."""
    if not await call_current(session, claim):
        return False
    call = await session.get(ModelCall, claim.call_id)
    assert call is not None
    _settle(call, output=output, error=error)
    call.usage = usage or {}
    await session.flush()
    return True


def _settle(
    call: ModelCall, *, output: dict[str, Any] | None = None, error: str | None = None
) -> None:
    call.state = "succeeded" if output is not None else "failed"
    call.output = output
    call.error = None if output is not None else (error or "failed")[:200]
    call.lease_owner = None
    call.lease_expires_at = None
