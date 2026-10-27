"""Durable corrective-job trace for blocked work."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Event, TransactionalOutbox
from vuzol.telegram.attention import AttentionEvent, should_notify


async def record_corrective_signal(
    session: AsyncSession,
    *,
    scope: str,
    reason: str,
    package_id: uuid.UUID | None = None,
    task_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
    step_id: uuid.UUID | None = None,
    notify: AttentionEvent = AttentionEvent.PACKAGE_ATTENTION,
) -> bool:
    """Durable corrective job trace (D2 L6, Q3): Event + projection outbox.

    The job itself reuses the existing bounded controls (retry/skip/repair
    within lifetime budget); this record makes "evaluating without a job"
    impossible to lose: the Event row is the durable trace, the outbox row
    refreshes the visible card, and the ``should_notify`` policy decision is
    persisted in the payload. Returns the notify decision.
    """

    decision = should_notify(notify)
    payload: dict[str, Any] = {
        "scope": scope,
        "reason": reason,
        "notify": decision,
        "notify_event": notify.value,
        "package_id": None if package_id is None else str(package_id),
        "task_id": None if task_id is None else str(task_id),
        "run_id": None if run_id is None else str(run_id),
        "step_id": None if step_id is None else str(step_id),
    }
    entity_id = package_id or task_id or run_id or step_id or uuid.uuid4()
    session.add(
        Event(
            entity_type="work_package" if package_id is not None else "task",
            entity_id=entity_id,
            event_type="work_package.correction_required"
            if package_id is not None
            else "task.correction_required",
            actor_type="system",
            payload=payload,
        )
    )
    if package_id is not None:
        session.add(
            TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_status",
                linked_entity_type="work_package",
                linked_entity_id=package_id,
                idempotency_key=(
                    f"wp:corrective:{package_id}:{payload['step_id'] or payload['task_id']}"
                ),
                payload={"package_id": str(package_id)},
            )
        )
    await session.flush()
    return decision
