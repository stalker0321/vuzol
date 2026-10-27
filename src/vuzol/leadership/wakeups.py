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


def continuation_wakeup_key(goal_id: uuid.UUID, version: int) -> str:
    """Never reopen a consumed wakeup; one continuation per authoritative version."""
    return f"lead:wakeup:{goal_id}:continue:{version}"


def resume_wakeup_key(goal_id: uuid.UUID, fence_epoch: int) -> str:
    """One wakeup per resume generation. The eternal enabled key is not reopened."""

    return f"lead:wakeup:{goal_id}:resume:{fence_epoch}"


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


async def enqueue_legacy_plan_request(
    session: AsyncSession,
    *,
    project_id: str,
    user_id: int,
    source_id: str,
    text: str,
    package_id: uuid.UUID | None = None,
) -> None:
    """Retired plan/replan inputs become durable, owner-scoped lead events.

    This only appends input. Do not acquire a goal lock after the caller's
    discussion/package lock; the lead rechecks authority before any action.
    """
    from vuzol.discussion.domain import DomainError
    from vuzol.leadership.persistence import ManagedGoal

    goal = await session.scalar(
        select(ManagedGoal).where(
            ManagedGoal.project_id == project_id,
            ManagedGoal.lifecycle.in_(("draft", "active", "waiting", "paused")),
        )
    )
    if goal is None:
        raise DomainError("managed_goal_required")
    if goal.owner_user_id != user_id:
        raise DomainError("outside_scope")
    key = f"lead:legacy-plan:{source_id}"
    existing = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == "project_lead",
            TransactionalOutbox.idempotency_key == key,
        )
    )
    if existing is not None:
        return
    session.add(
        TransactionalOutbox(
            destination="project_lead",
            operation_type="lead_wakeup",
            linked_entity_type="managed_goal",
            linked_entity_id=goal.id,
            idempotency_key=key,
            payload={
                "goal_id": str(goal.id),
                "wakeup_kind": "planning_requested",
                "text": text,
                "owner_user_id": user_id,
                "source_id": source_id,
                "package_id": str(package_id) if package_id else None,
            },
        )
    )
    await session.flush()
