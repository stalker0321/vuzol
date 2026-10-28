"""Persistence for derived memory and the lead's explicitly scoped notebooks.

Redelivery dedups on extraction identity. Notebook writers serialize budget
checks; derived writers resolve supersession from source revisions.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from vuzol.storage.models import MemoryUnit, ProjectProvisioning
from vuzol.storage.types import MemoryScope, MemoryUnitStatus

if TYPE_CHECKING:  # pragma: no cover
    from vuzol.discussion.memory_units import RecallQuery


@dataclass(frozen=True, slots=True)
class Notebook:
    notes: tuple[MemoryUnit, ...]
    text_bytes: int


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
        scope: MemoryScope | None = None,
        goal_id: uuid.UUID | None = None,
        details: dict[str, Any] | None = None,
        session_id: uuid.UUID | None = None,
        trigger_event_id: uuid.UUID | None = None,
        source_event_id: uuid.UUID | None = None,
        source_decision_id: uuid.UUID | None = None,
        source_artifact_id: uuid.UUID | None = None,
        source_turn_id: uuid.UUID | None = None,
        source_summary_id: uuid.UUID | None = None,
        source_key: str | None = None,
    ) -> MemoryUnit:
        """Insert a unit; a redelivered extraction identity returns the row."""

        statement = (
            postgres_insert(MemoryUnit)
            .values(
                project_id=project_id,
                scope=scope or (MemoryScope.PROJECT if project_id else MemoryScope.SESSION),
                goal_id=goal_id,
                details=details,
                session_id=session_id,
                unit_type=unit_type,
                status=status.value,
                text=text,
                trigger_event_id=trigger_event_id,
                extractor_version=extractor_version,
                source_event_id=source_event_id,
                source_decision_id=source_decision_id,
                source_artifact_id=source_artifact_id,
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

    async def lock_note(self, note_id: uuid.UUID) -> MemoryUnit | None:
        return cast(
            MemoryUnit | None,
            await self._session.scalar(
                select(MemoryUnit)
                .where(MemoryUnit.id == note_id, MemoryUnit.unit_type == "notebook_note")
                .with_for_update()
                .execution_options(populate_existing=True)
            ),
        )

    async def lock_notebook(self, scope: MemoryScope, project_id: str | None) -> None:
        # Serialize budget checks across goals, including an empty/global notebook.
        key = f"lead-notebook:{scope.value}:{project_id or '-'}"
        await self._session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(key, 0)))
        )

    async def notebook(self, *, scope: MemoryScope, project_id: str | None = None) -> Notebook:
        if scope not in {MemoryScope.PROJECT, MemoryScope.GLOBAL} or (
            (scope == MemoryScope.PROJECT) != (project_id is not None)
        ):
            raise ValueError("notebook requires project or global scope")
        notes = tuple(
            await self._session.scalars(
                select(MemoryUnit)
                .where(
                    MemoryUnit.scope == scope,
                    MemoryUnit.project_id == project_id,
                    MemoryUnit.unit_type == "notebook_note",
                    MemoryUnit.status == MemoryUnitStatus.OBSERVATION,
                )
                .order_by(MemoryUnit.created_at, MemoryUnit.id)
            )
        )
        return Notebook(notes, sum(len(note.text.encode("utf-8")) for note in notes))

    async def create_note(
        self,
        *,
        scope: MemoryScope,
        project_id: str | None,
        goal_id: uuid.UUID,
        text: str,
        extraction_identity: str,
    ) -> MemoryUnit:
        return await self.create_unit(
            scope=scope,
            project_id=project_id,
            goal_id=goal_id,
            text=text,
            unit_type="notebook_note",
            status=MemoryUnitStatus.OBSERVATION,
            extractor_version="lead-notebook.v1",
            extraction_identity=extraction_identity,
            effective_at=datetime.now(UTC),
        )

    async def create_decision(
        self,
        *,
        project_id: str,
        goal_id: uuid.UUID,
        text: str,
        details: dict[str, Any],
        extraction_identity: str,
    ) -> MemoryUnit:
        return await self.create_unit(
            scope=MemoryScope.PROJECT,
            project_id=project_id,
            goal_id=goal_id,
            text=text,
            details=details,
            unit_type="decision_entry",
            status=MemoryUnitStatus.OBSERVATION,
            extractor_version="lead-retrospective.v1",
            extraction_identity=extraction_identity,
            effective_at=datetime.now(UTC),
        )

    async def decisions_for_goal(
        self, *, project_id: str, goal_id: uuid.UUID
    ) -> tuple[MemoryUnit, ...]:
        return tuple(
            await self._session.scalars(
                select(MemoryUnit)
                .where(
                    MemoryUnit.unit_type == "decision_entry",
                    MemoryUnit.scope == MemoryScope.PROJECT,
                    MemoryUnit.project_id == project_id,
                    MemoryUnit.goal_id == goal_id,
                )
                .order_by(MemoryUnit.created_at, MemoryUnit.id)
            )
        )

    async def retract_note(self, note: MemoryUnit) -> bool:
        """Caller holds the note lock; repeated Undo is a no-op, including superseded notes."""
        if note.unit_type != "notebook_note" or note.status != MemoryUnitStatus.OBSERVATION:
            return False
        await self.set_status(note, MemoryUnitStatus.RETRACTED)
        return True

    async def restore_replaced_note(self, note: MemoryUnit) -> MemoryUnit | None:
        """Undoing a replacement brings back the note it replaced.

        The only path from superseded back to active: it is an owner's Undo of the
        replacing note, so the lifecycle check that forbids it elsewhere does not apply.
        """
        replaced = await self._session.scalar(
            select(MemoryUnit)
            .where(
                MemoryUnit.superseded_by == note.id,
                MemoryUnit.unit_type == "notebook_note",
                MemoryUnit.status == MemoryUnitStatus.SUPERSEDED,
            )
            .with_for_update()
        )
        if replaced is None:
            return None
        replaced.status = MemoryUnitStatus.OBSERVATION
        replaced.superseded_by = None
        replaced.superseded_at = None
        await self._session.flush()
        return replaced

    async def supersede_note(self, note: MemoryUnit, *, superseded_by: uuid.UUID) -> bool:
        """Caller holds the note lock; supersession history is written only once."""
        if note.unit_type != "notebook_note" or note.status != MemoryUnitStatus.OBSERVATION:
            return False
        await self.mark_superseded(
            note, superseded_by=superseded_by, superseded_at=datetime.now(UTC)
        )
        return True

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
            MemoryUnit.status.in_(tuple(member.value for member in RECALLABLE_STATUSES)),
            MemoryUnit.scope == query.scope,
            MemoryUnit.project_id == query.project_id,
        ]
        if query.scope == MemoryScope.SESSION:
            conditions.append(MemoryUnit.session_id == query.session_id)
        if query.unit_types:
            conditions.append(MemoryUnit.unit_type.in_(sorted(query.unit_types)))
        if query.query is not None:
            conditions.append(
                MemoryUnit.text_search.op("@@")(func.plainto_tsquery("simple", query.query))
            )
        statement = (
            select(MemoryUnit)
            .where(*conditions)
            .order_by(MemoryUnit.effective_at.desc(), MemoryUnit.id.desc())
            .limit(clamp_recall_limit(query.limit))
        )
        return tuple((await self._session.scalars(statement)).all())

    async def recall_lead(
        self,
        *,
        goal_id: uuid.UUID,
        query: str,
        project: str = "this",
        limit: int = 8,
    ) -> tuple[MemoryUnit, ...]:
        """Search stored lead memory, scoped by server-owned goal ownership.

        Topic mappings have no owner. Managed goals and provisioning requests do;
        use those records rather than trusting a model-supplied project/user id.
        Unlike discussion recall, an empty search never returns recent rows.
        """
        from vuzol.leadership.persistence import ManagedGoal

        if not query.strip():
            return ()
        if project not in {"this", "all"} or len(query) > 200 or not 1 <= limit <= 8:
            raise ValueError("invalid lead recall scope or bounds")
        current = aliased(ManagedGoal)
        source = aliased(ManagedGoal)
        owner = select(current.owner_user_id).where(current.id == goal_id).scalar_subquery()
        owned = (
            select(ManagedGoal.project_id)
            .where(ManagedGoal.owner_user_id == owner)
            .union(
                select(ProjectProvisioning.project_id).where(
                    ProjectProvisioning.requested_by_user_id == owner
                )
            )
        )
        # The index has no stemming and the lead asks in its own words, so any
        # query word may match and each word matches its other forms by prefix.
        words = [word for word in re.findall(r"\w+", query.lower()) if len(word) >= 3]
        if not words:
            return ()
        search = func.to_tsquery(
            "simple",
            " | ".join(sorted({f"{word[: max(4, len(word) - 2)]}:*" for word in words})),
        )
        conditions = [
            MemoryUnit.scope == MemoryScope.PROJECT,
            MemoryUnit.project_id.in_(owned),
            MemoryUnit.unit_type.in_(("decision_entry", "notebook_note")),
            MemoryUnit.status.in_((MemoryUnitStatus.OBSERVATION, MemoryUnitStatus.VERIFIED)),
            MemoryUnit.superseded_by.is_(None),
            MemoryUnit.text_search.op("@@")(search),
            # Do not expose another owner's memory after a project's ownership changes.
            or_(
                MemoryUnit.goal_id.is_(None),
                select(source.id)
                .where(
                    source.id == MemoryUnit.goal_id,
                    source.owner_user_id == owner,
                    source.project_id == MemoryUnit.project_id,
                )
                .exists(),
            ),
        ]
        if project == "this":
            conditions.append(
                MemoryUnit.project_id
                == select(current.project_id).where(current.id == goal_id).scalar_subquery()
            )
        statement = (
            select(MemoryUnit)
            .where(*conditions)
            .order_by(
                func.ts_rank(MemoryUnit.text_search, search).desc(),
                MemoryUnit.created_at.desc(),
                MemoryUnit.id.desc(),
            )
            .limit(limit)
        )
        return tuple((await self._session.scalars(statement)).all())

    async def active_units_for_decision(self, decision_id: uuid.UUID) -> tuple[MemoryUnit, ...]:
        """Live units derived from one accepted decision (retraction fan-out)."""

        from vuzol.discussion.memory_units import RECALLABLE_STATUSES

        statement = select(MemoryUnit).where(
            MemoryUnit.source_decision_id == decision_id,
            MemoryUnit.status.in_(tuple(member.value for member in RECALLABLE_STATUSES)),
        )
        return tuple((await self._session.scalars(statement)).all())

    async def units_referencing_artifact(self, artifact_id: uuid.UUID) -> tuple[MemoryUnit, ...]:
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
        return int(await self._session.scalar(select(func.count()).select_from(MemoryUnit)) or 0)

    async def units_by_status(self, statuses: Sequence[MemoryUnitStatus]) -> tuple[MemoryUnit, ...]:
        statement = select(MemoryUnit).where(
            MemoryUnit.status.in_(tuple(member.value for member in statuses))
        )
        return tuple((await self._session.scalars(statement)).all())
