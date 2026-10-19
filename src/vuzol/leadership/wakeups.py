"""Durable project-lead wakeups. The same key is a no-op once it exists."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.types import DeliveryStatus


def enabled_wakeup_key(goal_id: uuid.UUID) -> str:
    return f"lead:wakeup:{goal_id}:enabled"


def completed_wakeup_key(goal_id: uuid.UUID, package_id: uuid.UUID) -> str:
    return f"lead:wakeup:{goal_id}:{package_id}:completed"


async def enqueue_goal_wakeup(
    session: AsyncSession,
    *,
    goal_id: uuid.UUID,
    idempotency_key: str,
    package_id: uuid.UUID | None,
    kind: str,
) -> None:
    """Insert one wakeup unless this destination already has the key."""

    existing = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == "project_lead",
            TransactionalOutbox.idempotency_key == idempotency_key,
        )
    )
    if existing is not None:
        return
    payload: dict[str, Any] = {"goal_id": str(goal_id), "wakeup_kind": kind}
    if package_id is not None:
        payload["package_id"] = str(package_id)
    session.add(
        TransactionalOutbox(
            destination="project_lead",
            operation_type="lead_wakeup",
            linked_entity_type="managed_goal",
            linked_entity_id=goal_id,
            idempotency_key=idempotency_key,
            payload=payload,
            status=DeliveryStatus.PENDING,
        )
    )
    await session.flush()
