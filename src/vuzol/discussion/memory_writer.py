"""Async derived-memory writer (D5).

Consumes the ``memory_extract`` outbox destination off the task/plan
completion path: completion never waits for the writer and never reads its
rows. Jobs dedup on (trigger_event_id, extractor_version, scope); units
dedup on the unique extraction identity. Delayed writers resolve
supersession from source revisions, never from job completion order.
Templates are deterministic; no model, config, Git, or environment access.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory import ensure_memory_safe
from vuzol.discussion.memory_units import (
    EXTRACTOR_VERSION,
    MEMORY_DESTINATION,
    decision_template,
    extraction_identity,
    extraction_scope,
    job_idempotency_key,
    outcome_template,
    should_supersede,
)
from vuzol.observability import get_logger
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
)
from vuzol.storage.models import (
    AcceptedDecision,
    Artifact,
    Event,
    TransactionalOutbox,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.repositories.core import EventRepository
from vuzol.storage.types import MemoryUnitStatus
from vuzol.storage.unit_of_work import UnitOfWork

if TYPE_CHECKING:  # pragma: no cover
    from vuzol.storage.repositories.memory_units import MemoryUnitRepository

DECISION_OPERATION = "extract_decision"
OUTCOME_OPERATION = "extract_outcome"
RETRACT_OPERATION = "retract_units"


def _units(session: AsyncSession) -> MemoryUnitRepository:
    # Local import: storage.repositories.memory_units is also reachable from
    # vuzol.storage.repositories, which must not cycle through this module.
    from vuzol.storage.repositories.memory_units import MemoryUnitRepository

    return MemoryUnitRepository(session)


class MemoryWriterError(RuntimeError):
    pass


async def enqueue_memory_extraction(
    uow: UnitOfWork,
    *,
    trigger_event_id: uuid.UUID,
    project_id: str | None,
    session_id: uuid.UUID | None,
    operation: str,
    entity_type: str,
    entity_id: uuid.UUID,
    payload: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Enqueue a writer job in the source transaction (same txn as the event)."""

    scope = extraction_scope(project_id=project_id, session_id=session_id)
    return await uow.outbox.enqueue(
        destination=MEMORY_DESTINATION,
        operation_type=operation,
        entity_type=entity_type,
        entity_id=entity_id,
        idempotency_key=job_idempotency_key(
            trigger_event_id=trigger_event_id, scope=scope, operation=operation
        ),
        payload={
            "trigger_event_id": str(trigger_event_id),
            "extractor_version": EXTRACTOR_VERSION,
            "scope": scope,
            "project_id": project_id,
            "session_id": None if session_id is None else str(session_id),
            **(payload or {}),
        },
    )


async def record_hypothesis(
    uow: UnitOfWork,
    *,
    project_id: str | None,
    session_id: uuid.UUID | None,
    body: str,
    source_turn_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Explicitly record a hypothesis. It can never become verified in code."""

    text = ensure_memory_safe(body)[:4_000]
    scope = extraction_scope(project_id=project_id, session_id=session_id)
    content_hash = hashlib.sha256(f"hypothesis:{scope}:{text}".encode()).hexdigest()
    trigger = uuid.uuid5(uuid.NAMESPACE_URL, f"hypothesis:{content_hash}")
    unit = await uow.memory_units.create_unit(
        text=text,
        unit_type="observation",
        status=MemoryUnitStatus.HYPOTHESIS,
        extractor_version=EXTRACTOR_VERSION,
        extraction_identity=extraction_identity(
            trigger_event_id=trigger,
            scope=scope,
            unit_type="observation",
            unit_key=f"hypothesis:{content_hash[:16]}",
        ),
        effective_at=datetime.now(UTC),
        project_id=project_id,
        session_id=session_id,
        source_turn_id=source_turn_id,
    )
    return unit.id


async def retract_unit(
    uow: UnitOfWork,
    *,
    unit_id: uuid.UUID,
    actor: str,
) -> uuid.UUID:
    """Retract a unit: excluded from recall, row and provenance survive."""

    unit = await uow.memory_units.get(unit_id)
    if unit is None:
        raise MemoryWriterError("memory unit not found")
    event_id = await uow.events.append(
        entity_type="memory_unit",
        entity_id=unit.id,
        event_type="memory_unit.retracted",
        actor_type=actor,
        payload={"extraction_identity": unit.extraction_identity},
    )
    await uow.memory_units.set_status(unit, MemoryUnitStatus.RETRACTED)
    return event_id


async def tombstone_unit(
    uow: UnitOfWork,
    *,
    unit_id: uuid.UUID,
    actor: str,
    reason: str,
) -> uuid.UUID:
    """Tombstone sensitive unit text; the row, refs and events survive."""

    unit = await uow.memory_units.get(unit_id)
    if unit is None:
        raise MemoryWriterError("memory unit not found")
    event_id = await uow.events.append(
        entity_type="memory_unit",
        entity_id=unit.id,
        event_type="memory_unit.tombstoned",
        actor_type=actor,
        payload={"extraction_identity": unit.extraction_identity, "reason": reason},
    )
    await uow.memory_units.set_status(
        unit,
        MemoryUnitStatus.TOMBSTONED,
        text="[tombstoned]",
        tombstone_event_id=event_id,
    )
    return event_id


async def redact_artifact_for_memory(
    uow: UnitOfWork,
    *,
    artifact_id: uuid.UUID,
    actor: str,
    reason: str,
) -> uuid.UUID:
    """Post-hoc redaction marker for a sensitive artifact (D5).

    Only the redaction revision pointer moves; content bytes and hashes are
    untouched, so operational history is never forged. Units referencing the
    artifact stay pinned and readable with provenance.
    """

    assert uow.session is not None
    artifact = await uow.session.get(Artifact, artifact_id)
    if artifact is None:
        raise MemoryWriterError("artifact not found")
    revision = f"redaction-{uuid.uuid4().hex[:16]}"
    artifact.redaction_revision = revision
    await uow.session.flush()
    return await uow.events.append(
        entity_type="artifact",
        entity_id=artifact.id,
        event_type="artifact.redacted",
        actor_type=actor,
        payload={
            "redaction_revision": revision,
            "reason": reason,
            "content_hash": artifact.content_hash,
        },
    )


class MemoryWriterService:
    """Outbox consumer for derived-memory extraction (never on completion path)."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        lease_seconds: int = 60,
    ) -> None:
        self._factory = session_factory
        self._owner = owner
        self._lease_seconds = lease_seconds
        self._logger = get_logger(__name__)

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._lease_seconds,
                allowed_destinations=frozenset({MEMORY_DESTINATION}),
            )
        if token is None:
            return False
        try:
            async with self._factory.begin() as session:
                await self._dispatch(session, token)
                await complete_outbox_item(session, token)
        except (ValueError, DomainError, MemoryWriterError):
            async with self._factory.begin() as session:
                await dead_letter_outbox_item(
                    session, token, error_category="invalid_memory_extraction"
                )
        return True

    async def _dispatch(self, session: AsyncSession, token: OutboxLeaseToken) -> None:
        item = await session.get(TransactionalOutbox, token.item_id)
        if item is None:
            raise MemoryWriterError("memory job item is invalid")
        payload = item.payload if isinstance(item.payload, dict) else {}
        if payload.get("extractor_version") != EXTRACTOR_VERSION:
            raise MemoryWriterError("stale memory extractor version")
        if item.operation_type == DECISION_OPERATION:
            await _extract_decision(session, payload)
        elif item.operation_type == OUTCOME_OPERATION:
            await _extract_outcome(session, payload)
        elif item.operation_type == RETRACT_OPERATION:
            await _retract_decision_units(session, payload)
        else:
            raise MemoryWriterError(f"unsupported memory operation: {item.operation_type}")
        self._logger.info(
            "Memory extraction completed",
            extra={
                "event": "memory.extraction.completed",
                "operation": item.operation_type,
                "outbox_item_id": str(item.id),
            },
        )


def _scope_parts(payload: dict[str, Any]) -> tuple[str | None, uuid.UUID | None, str]:
    project_id = payload.get("project_id")
    raw_session = payload.get("session_id")
    session_id = uuid.UUID(str(raw_session)) if raw_session is not None else None
    scope = str(payload.get("scope") or "")
    return (
        str(project_id) if project_id is not None else None,
        session_id,
        scope,
    )


async def _extract_decision(session: AsyncSession, payload: dict[str, Any]) -> uuid.UUID:
    """Verified template unit for one explicit accepted decision (refs, no copy)."""

    units = _units(session)
    events = EventRepository(session)
    raw_decision = payload.get("decision_id")
    raw_trigger = payload.get("trigger_event_id")
    if raw_decision is None or raw_trigger is None:
        raise MemoryWriterError("decision extraction requires decision and trigger refs")
    decision = await session.get(AcceptedDecision, uuid.UUID(str(raw_decision)))
    if decision is None:
        raise MemoryWriterError("source decision is missing")
    project_id, session_id, scope = _scope_parts(payload)
    unit_key = f"decision:{decision.key}"
    identity = extraction_identity(
        trigger_event_id=uuid.UUID(str(raw_trigger)),
        scope=scope,
        unit_type="decision_template",
        unit_key=unit_key,
    )
    effective_at = decision.created_at
    for current in await units.active_chain(
        project_id=project_id,
        session_id=session_id,
        unit_type="decision_template",
        source_key=decision.key,
    ):
        if current.extraction_identity == identity:
            return current.id
        if not should_supersede(
            current_effective_at=current.effective_at, incoming_effective_at=effective_at
        ):
            # Delayed writer with an older source revision: no-op, never
            # вытесняет current by job order.
            return current.id
    text = decision_template(
        key=decision.key,
        statement=decision.statement,
        accepted_by_user_id=decision.accepted_by_user_id,
    )
    unit = await units.create_unit(
        text=text,
        unit_type="decision_template",
        status=MemoryUnitStatus.VERIFIED,
        extractor_version=EXTRACTOR_VERSION,
        extraction_identity=identity,
        effective_at=effective_at,
        project_id=project_id,
        session_id=session_id,
        trigger_event_id=uuid.UUID(str(raw_trigger)),
        source_event_id=_optional_uuid(payload.get("source_event_id")),
        source_decision_id=decision.id,
        source_turn_id=decision.source_turn_id,
        source_key=decision.key,
    )
    for current in await units.active_chain(
        project_id=project_id,
        session_id=session_id,
        unit_type="decision_template",
        source_key=decision.key,
    ):
        if current.id != unit.id:
            await units.mark_superseded(
                current, superseded_by=unit.id, superseded_at=datetime.now(UTC)
            )
    await events.append(
        entity_type="memory_unit",
        entity_id=unit.id,
        event_type="memory_unit.written",
        actor_type="memory_writer",
        payload={"extraction_identity": identity, "source_decision_id": str(decision.id)},
    )
    return unit.id


async def _extract_outcome(session: AsyncSession, payload: dict[str, Any]) -> uuid.UUID:
    """Verified template unit for a goal-acceptance outcome."""

    from vuzol.storage.models import AcceptanceEvidence

    units = _units(session)
    events = EventRepository(session)
    raw_trigger = payload.get("trigger_event_id")
    raw_package = payload.get("package_id")
    if raw_trigger is None or raw_package is None:
        raise MemoryWriterError("outcome extraction requires trigger and package refs")
    package_id = uuid.UUID(str(raw_package))
    revision_number = int(payload.get("revision_number") or 0)
    accepted_by = int(payload.get("accepted_by_user_id") or 0)
    raw_artifact = payload.get("artifact_id")
    artifact_id = uuid.UUID(str(raw_artifact)) if raw_artifact is not None else None
    evidence_id: uuid.UUID | None = None
    evidence_hash: str | None = None
    if artifact_id is not None:
        artifact = await session.get(Artifact, artifact_id)
        if artifact is None or artifact.verified_at is None:
            raise MemoryWriterError("outcome evidence artifact is missing or unverified")
        evidence = await session.scalar(
            select(AcceptanceEvidence)
            .where(
                AcceptanceEvidence.package_id == package_id,
                AcceptanceEvidence.artifact_id == artifact_id,
            )
            .order_by(AcceptanceEvidence.created_at.desc())
            .limit(1)
        )
        if evidence is not None:
            evidence_id = evidence.id
            evidence_hash = evidence.evidence_hash
    project_id, session_id, scope = _scope_parts(payload)
    unit_key = f"outcome:{package_id}:{revision_number}"
    identity = extraction_identity(
        trigger_event_id=uuid.UUID(str(raw_trigger)),
        scope=scope,
        unit_type="outcome_template",
        unit_key=unit_key,
    )
    trigger_event = await session.get(Event, uuid.UUID(str(raw_trigger)))
    effective_at = trigger_event.created_at if trigger_event is not None else datetime.now(UTC)
    text = outcome_template(
        package_id=package_id,
        revision_number=revision_number,
        accepted_by_user_id=accepted_by,
        evidence_hash=evidence_hash,
    )
    unit = await units.create_unit(
        text=text,
        unit_type="outcome_template",
        status=MemoryUnitStatus.VERIFIED,
        extractor_version=EXTRACTOR_VERSION,
        extraction_identity=identity,
        effective_at=effective_at,
        project_id=project_id,
        session_id=session_id,
        trigger_event_id=uuid.UUID(str(raw_trigger)),
        source_event_id=uuid.UUID(str(raw_trigger)),
        source_artifact_id=artifact_id,
        source_acceptance_evidence_id=evidence_id,
        source_key=f"package:{package_id}",
    )
    await events.append(
        entity_type="memory_unit",
        entity_id=unit.id,
        event_type="memory_unit.written",
        actor_type="memory_writer",
        payload={"extraction_identity": identity, "package_id": str(package_id)},
    )
    return unit.id


async def _retract_decision_units(session: AsyncSession, payload: dict[str, Any]) -> None:
    """Retract units derived from one decision; rows and provenance survive."""

    units = _units(session)
    events = EventRepository(session)
    raw_decision = payload.get("decision_id")
    if raw_decision is None:
        raise MemoryWriterError("retraction requires a decision ref")
    decision_id = uuid.UUID(str(raw_decision))
    for unit in await units.active_units_for_decision(decision_id):
        await units.set_status(unit, MemoryUnitStatus.RETRACTED)
        await events.append(
            entity_type="memory_unit",
            entity_id=unit.id,
            event_type="memory_unit.retracted",
            actor_type="memory_writer",
            payload={"source_decision_id": str(decision_id)},
        )


def _optional_uuid(value: Any) -> uuid.UUID | None:  # noqa: ANN401
    if value is None:
        return None
    return uuid.UUID(str(value))
