"""Durable effect intent/receipt store for the Git CAS apply path (WP05).

The stable ``operation_key`` is written before launch and reused on retry. An
effect marked ``uncertain`` must never be launched blindly again; callers refuse
to start and defer to the reconciler (or a human).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Effect

EFFECT_SCHEMA_VERSION = "effect.v1"
APPLY_OPERATION_PREFIX = "apply"
EFFECT_CLASS_ISOLATED_MUTATION = "isolated_mutation"
TARGET_KIND_GIT_REF = "git_ref"
IDEMPOTENCY_RECONCILABLE = "reconcilable"
STATUS_INTENT_RECORDED = "intent_recorded"
STATUS_DISPATCHED = "dispatched"
STATUS_SETTLED = "settled"
STATUS_UNCERTAIN = "uncertain"
STATUS_FAILED = "failed"
RECONCILE_NOT_STARTED = "not_started"
RECONCILE_CONFIRMED = "confirmed"
RECONCILE_DENIED = "denied"
RECONCILE_UNCERTAIN = "uncertain"
RECEIPT_APPLIED = "applied"
RECEIPT_FAILED = "failed"
METHOD_READ_GIT_REF = "read_git_ref"

_ACTIVE_STATUSES = (STATUS_INTENT_RECORDED, STATUS_DISPATCHED, STATUS_UNCERTAIN)


class EffectUncertainError(ValueError):
    """An existing effect outcome is unproven; launching again is forbidden."""


def apply_operation_key(
    *, approval_id: uuid.UUID, result_commit: str, target_branch: str
) -> str:
    """Stable key for one approved local apply; reused across retries."""

    return f"{APPLY_OPERATION_PREFIX}:{approval_id}:{result_commit}:{target_branch}"


@dataclass(frozen=True, slots=True)
class EffectIntent:
    operation_key: str
    step_id: uuid.UUID
    effect_class: str
    target_kind: str
    target_reference: str
    idempotency: str
    payload_hash: str
    lease_generation: int
    task_id: uuid.UUID | None = None
    run_id: uuid.UUID | None = None
    horizon_id: uuid.UUID | None = None
    attempt_id: uuid.UUID | None = None
    permission_envelope_hash: str | None = None
    approval_id: uuid.UUID | None = None
    approval_envelope_hash: str | None = None
    context: dict[str, Any] = field(default_factory=dict)


async def record_intent(session: AsyncSession, intent: EffectIntent) -> Effect:
    """Get-or-create the intent and mark it dispatched before the side effect.

    Raises :class:`EffectUncertainError` when a previous attempt left the same
    operation in an unproven state.
    """

    existing = await session.scalar(
        select(Effect).where(Effect.operation_key == intent.operation_key).with_for_update()
    )
    if existing is not None:
        if existing.status == STATUS_UNCERTAIN or existing.reconcile_status == RECONCILE_UNCERTAIN:
            raise EffectUncertainError(
                f"effect {intent.operation_key} is uncertain; manual reconciliation required"
            )
        if existing.status in (STATUS_SETTLED, STATUS_FAILED):
            return existing
        existing.status = STATUS_DISPATCHED
        existing.launch_started_at = existing.launch_started_at or datetime.now(UTC)
        existing.launch_generation = existing.lease_generation
        return existing
    effect = Effect(
        schema_version=EFFECT_SCHEMA_VERSION,
        operation_key=intent.operation_key,
        step_id=intent.step_id,
        task_id=intent.task_id,
        run_id=intent.run_id,
        horizon_id=intent.horizon_id,
        attempt_id=intent.attempt_id,
        effect_class=intent.effect_class,
        target_kind=intent.target_kind,
        target_reference=intent.target_reference,
        idempotency=intent.idempotency,
        permission_envelope_hash=intent.permission_envelope_hash,
        approval_id=intent.approval_id,
        approval_envelope_hash=intent.approval_envelope_hash,
        payload_hash=intent.payload_hash,
        lease_generation=intent.lease_generation,
        status=STATUS_DISPATCHED,
        launch_started_at=datetime.now(UTC),
        launch_generation=intent.lease_generation,
        reconcile_status=RECONCILE_NOT_STARTED,
        context=dict(intent.context),
    )
    session.add(effect)
    await session.flush()
    return effect


def settle_applied(
    effect: Effect,
    *,
    external_ref: str,
    output_hash: str | None = None,
    method: str = METHOD_READ_GIT_REF,
) -> None:
    effect.status = STATUS_SETTLED
    effect.receipt_status = RECEIPT_APPLIED
    effect.receipt_observed_at = datetime.now(UTC)
    effect.receipt_external_ref = external_ref[:500]
    effect.receipt_output_hash = output_hash
    effect.reconcile_status = RECONCILE_CONFIRMED
    effect.reconcile_reconciled_at = datetime.now(UTC)
    effect.reconcile_method = method


def mark_denied(effect: Effect) -> None:
    effect.status = STATUS_FAILED
    effect.receipt_status = RECEIPT_FAILED
    effect.receipt_observed_at = datetime.now(UTC)
    effect.reconcile_status = RECONCILE_DENIED
    effect.reconcile_reconciled_at = datetime.now(UTC)
    effect.reconcile_method = METHOD_READ_GIT_REF


def mark_uncertain(effect: Effect) -> None:
    effect.status = STATUS_UNCERTAIN
    effect.reconcile_status = RECONCILE_UNCERTAIN
    effect.reconcile_reconciled_at = datetime.now(UTC)
    effect.reconcile_method = METHOD_READ_GIT_REF


async def active_effects(
    session: AsyncSession, *, limit: int = 100
) -> tuple[Effect, ...]:
    rows = await session.scalars(
        select(Effect)
        .where(Effect.status.in_(_ACTIVE_STATUSES))
        .order_by(Effect.created_at, Effect.id)
        .limit(limit)
    )
    return tuple(rows.all())
