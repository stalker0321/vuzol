"""Persistence for derived memory units (D5).

Units are writer output only: completed task/plan paths never query this
repository. Redelivery dedups on the unique extraction identity; delayed
writers resolve supersession from source revisions, never job order.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import MemoryUnit
from vuzol.storage.types import MemoryUnitStatus

if TYPE_CHECKING:  # pragma: no cover
    from vuzol.discussion.memory_units import RecallQuery


class MemoryUnitRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_unit(
        self,
        *,
        text: str,
        unit_type: str,
        status: MemoryUnitStatus,
        extractor_version: str,
        extraction_identity: str,
        effective_at: datetime,
        project_id: str | None = None,
        session_id: uuid.UUID | None = None,
        trigger_event_id: uuid.UUID | None = None,
        source_event_id: uuid.UUID | None = None,
        source_decision_id: uuid.UUID | None = None,
        source_artifact_id: uuid.UUID | None = None,
        source_acceptance_evidence_id: uuid.UUID | None = None,
        source_turn_id: uuid.UUID | None = None,
        source_summary_id: uuid.UUID | None = None,
        source_key: str | None = None,
    ) -> MemoryUnit:
        """Insert a unit; a redelivered extraction identity returns the row."""

        statement = (
            postgres_insert(MemoryUnit)
            .values(
                project_id=project_id,
                session_id=session_id,
                unit_type=unit_type,
                status=status.value,
                text=text,
                trigger_event_id=trigger_event_id,
                extractor_version=extractor_version,
                source_event_id=source_event_id,
                source_decision_id=source_decision_id,
                source_artifact_id=source_artifact_id,
                source_acceptance_evidence_id=source_acceptance_evidence_id,
                source_turn_id=source_turn_id,
                source_summary_id=source_summary_id,
                source_key=source_key,
                effective_at=effective_at,
                extraction_identity=extraction_identity,
            )
            .on_conflict_do_nothing(index_elements=["extraction_identity"])
            .returning(MemoryUnit.id)
        )
        inserted_id = (await self._session.execute(statement)).scalar_one_or_none()
        await self._session.flush()
        if inserted_id is not None:
            row = await self._session.get(MemoryUnit, inserted_id)
            assert row is not None
            return row
        existing = await self._session.scalar(
            select(MemoryUnit).where(MemoryUnit.extraction_identity == extraction_identity)
        )
        assert existing is not None
        return existing

    async def get(self, unit_id: uuid.UUID) -> MemoryUnit | None:
        return await self._session.get(MemoryUnit, unit_id)

    async def active_chain(
        self,
        *,
        project_id: str | None,
        session_id: uuid.UUID | None,
        unit_type: str,
        source_key: str | None,
    ) -> tuple[MemoryUnit, ...]:
        """Live (recallable) units sharing one supersession chain key."""

        from vuzol.discussion.memory_units import RECALLABLE_STATUSES

        statement = select(MemoryUnit).where(
            MemoryUnit.project_id == project_id,
            MemoryUnit.session_id == session_id,
            MemoryUnit.unit_type == unit_type,
            MemoryUnit.source_key == source_key,
            MemoryUnit.status.in_(tuple(member.value for member in RECALLABLE_STATUSES)),
        )
        return tuple((await self._session.scalars(statement)).all())

    async def mark_superseded(
        self, unit: MemoryUnit, *, superseded_by: uuid.UUID, superseded_at: datetime
    ) -> None:
        from vuzol.discussion.memory_units import check_status_transition

        check_status_transition(source=unit.status, target=MemoryUnitStatus.SUPERSEDED)
        unit.status = MemoryUnitStatus.SUPERSEDED
        unit.superseded_by = superseded_by
        unit.superseded_at = superseded_at
        await self._session.flush()

    async def set_status(
        self,
        unit: MemoryUnit,
        status: MemoryUnitStatus,
        *,
        text: str | None = None,
        tombstone_event_id: uuid.UUID | None = None,
        redaction_revision: str | None = None,
    ) -> None:
        """Retract or tombstone a unit; the row and provenance survive."""

        from vuzol.discussion.memory_units import check_status_transition

        check_status_transition(source=unit.status, target=status)
        unit.status = status
        if text is not None:
            unit.text = text
        if tombstone_event_id is not None:
            unit.tombstone_event_id = tombstone_event_id
        if redaction_revision is not None:
            unit.redaction_revision = redaction_revision
        await self._session.flush()

    async def recall(self, query: RecallQuery) -> tuple[MemoryUnit, ...]:
        """Bounded recall over active statuses only (retracted/superseded/
        tombstoned/hypothesis rows are never returned)."""

        from vuzol.discussion.memory_units import RECALLABLE_STATUSES, clamp_recall_limit

        conditions: list[Any] = [
            MemoryUnit.status.in_(tuple(member.value for member in RECALLABLE_STATUSES))
        ]
        if query.project_id is not None:
            conditions.append(MemoryUnit.project_id == query.project_id)
        if query.unit_types:
            conditions.append(MemoryUnit.unit_type.in_(sorted(query.unit_types)))
        if query.query is not None:
            conditions.append(
                MemoryUnit.text_search.op("@@")(
                    func.plainto_tsquery("simple", query.query)
                )
            )
        statement = (
            select(MemoryUnit)
            .where(*conditions)
            .order_by(MemoryUnit.effective_at.desc(), MemoryUnit.id.desc())
            .limit(clamp_recall_limit(query.limit))
        )
        return tuple((await self._session.scalars(statement)).all())

    async def active_units_for_decision(
        self, decision_id: uuid.UUID
    ) -> tuple[MemoryUnit, ...]:
        """Live units derived from one accepted decision (retraction fan-out)."""

        from vuzol.discussion.memory_units import RECALLABLE_STATUSES

        statement = select(MemoryUnit).where(
            MemoryUnit.source_decision_id == decision_id,
            MemoryUnit.status.in_(tuple(member.value for member in RECALLABLE_STATUSES)),
        )
        return tuple((await self._session.scalars(statement)).all())

    async def units_referencing_artifact(
        self, artifact_id: uuid.UUID
    ) -> tuple[MemoryUnit, ...]:
        """Provenance refs pinning an artifact against retention sweep."""

        statement = select(MemoryUnit).where(MemoryUnit.source_artifact_id == artifact_id)
        return tuple((await self._session.scalars(statement)).all())

    async def describe(self, unit: MemoryUnit) -> Mapping[str, Any]:
        return {
            "id": str(unit.id),
            "project_id": unit.project_id,
            "session_id": None if unit.session_id is None else str(unit.session_id),
            "unit_type": unit.unit_type,
            "status": unit.status.value,
            "text": unit.text,
            "extraction_identity": unit.extraction_identity,
            "effective_at": unit.effective_at.isoformat(),
            "superseded_by": None if unit.superseded_by is None else str(unit.superseded_by),
        }

    async def count_all(self) -> int:
        return int(
            await self._session.scalar(select(func.count()).select_from(MemoryUnit)) or 0
        )

    async def units_by_status(
        self, statuses: Sequence[MemoryUnitStatus]
    ) -> tuple[MemoryUnit, ...]:
        statement = select(MemoryUnit).where(
            MemoryUnit.status.in_(tuple(member.value for member in statuses))
        )
        return tuple((await self._session.scalars(statement)).all())
