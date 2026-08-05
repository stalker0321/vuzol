"""Work-package identity and fence persistence primitives."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    EditSession,
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    WorkItemDraft,
    WorkPackage,
    WorkPackageOpenDetail,
)
from vuzol.storage.types import EditSessionStatus, WorkPackageStatus

if TYPE_CHECKING:
    from vuzol.discussion.domain import PlanItemDraft


class WorkPackageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_package(self, package: WorkPackage) -> uuid.UUID:
        self._session.add(package)
        await self._session.flush()
        return package.id

    async def get_package(self, package_id: uuid.UUID, *, for_update: bool = False) -> WorkPackage:
        statement = select(WorkPackage).where(WorkPackage.id == package_id)
        if for_update:
            statement = statement.with_for_update()
        package = await self._session.scalar(statement)
        if package is None:
            raise LookupError(f"work package {package_id} does not exist")
        return package

    async def get_revision(self, revision_id: uuid.UUID) -> PlanRevision:
        revision = await self._session.get(PlanRevision, revision_id)
        if revision is None:
            raise LookupError(f"plan revision {revision_id} does not exist")
        return revision

    async def get_head_revision(self, package: WorkPackage) -> PlanRevision | None:
        if package.head_revision_id is None:
            return None
        return await self.get_revision(package.head_revision_id)

    async def get_fenced_revision(
        self, *, package_id: uuid.UUID, revision_number: int, h8: str
    ) -> PlanRevision:
        revision = await self._session.scalar(
            select(PlanRevision).where(
                PlanRevision.work_package_id == package_id,
                PlanRevision.revision_number == revision_number,
                func.substr(PlanRevision.content_hash, 1, 8) == h8,
            )
        )
        if revision is None:
            raise LookupError("revision fence does not identify a plan revision")
        return revision

    async def add_revision(self, revision: PlanRevision) -> uuid.UUID:
        self._session.add(revision)
        await self._session.flush()
        return revision.id

    async def add_item(self, item: WorkItemDraft) -> uuid.UUID:
        self._session.add(item)
        await self._session.flush()
        return item.id

    async def resolve_item_identities(
        self, package_id: uuid.UUID, items: Sequence[PlanItemDraft]
    ) -> tuple[uuid.UUID, ...]:
        """Resolve stable IDs, reusing an explicit UUID or package-local slug when present."""
        resolved: list[uuid.UUID] = []
        for item in items:
            item_id = item.item_id
            local_id = item.local_id
            row: WorkItemDraft | None = None
            if item_id is not None:
                row = await self._session.scalar(
                    select(WorkItemDraft).where(
                        WorkItemDraft.id == item_id,
                        WorkItemDraft.work_package_id == package_id,
                    )
                )
                if row is None:
                    raise ValueError("item identity does not belong to work package")
            elif local_id is not None:
                row = await self._session.scalar(
                    select(WorkItemDraft).where(
                        WorkItemDraft.work_package_id == package_id,
                        WorkItemDraft.local_id == local_id,
                    )
                )
            if row is None:
                row = WorkItemDraft(work_package_id=package_id, local_id=local_id)
                self._session.add(row)
                await self._session.flush()
            resolved.append(row.id)
        return tuple(resolved)

    async def add_revision_item(self, item: PlanRevisionItem) -> uuid.UUID:
        self._session.add(item)
        await self._session.flush()
        return item.id

    async def add_materialization(self, link: MaterializationLink) -> uuid.UUID:
        self._session.add(link)
        await self._session.flush()
        return link.id

    async def add_edit_session(self, edit_session: EditSession) -> uuid.UUID:
        fenced_item = await self._session.scalar(
            select(PlanRevisionItem.id)
            .join(PlanRevision, PlanRevision.id == PlanRevisionItem.plan_revision_id)
            .where(
                PlanRevisionItem.work_package_id == edit_session.package_id,
                PlanRevisionItem.plan_revision_id == edit_session.plan_revision_id,
                PlanRevisionItem.item_id == edit_session.item_id,
                PlanRevisionItem.ordinal == edit_session.ordinal,
                PlanRevision.revision_number == edit_session.plan_revision_number,
                PlanRevision.content_hash == edit_session.content_hash,
            )
        )
        if fenced_item is None:
            raise ValueError("edit-session fence does not identify a revision item")
        self._session.add(edit_session)
        await self._session.flush()
        return edit_session.id

    async def get_edit_session(
        self, edit_session_id: uuid.UUID, *, for_update: bool = False
    ) -> EditSession:
        statement = select(EditSession).where(EditSession.id == edit_session_id)
        if for_update:
            statement = statement.with_for_update()
        edit_session = await self._session.scalar(statement)
        if edit_session is None:
            raise LookupError(f"edit session {edit_session_id} does not exist")
        return edit_session

    async def close_open_edit_sessions(
        self, *, package_id: uuid.UUID, opened_by_user_id: int | None = None
    ) -> tuple[uuid.UUID, ...]:
        conditions = [
            EditSession.package_id == package_id,
            EditSession.status == EditSessionStatus.OPEN,
        ]
        if opened_by_user_id is not None:
            conditions.append(EditSession.opened_by_user_id == opened_by_user_id)
        edit_session_ids = tuple(
            (
                await self._session.scalars(
                    select(EditSession.id).where(*conditions).with_for_update()
                )
            ).all()
        )
        if not edit_session_ids:
            return ()
        await self._session.execute(
            update(EditSession)
            .where(EditSession.id.in_(edit_session_ids))
            .values(status=EditSessionStatus.CLOSED, updated_at=func.now())
        )
        return edit_session_ids

    async def active_package_id(self, *, session_id: uuid.UUID) -> uuid.UUID | None:
        return cast(
            uuid.UUID | None,
            await self._session.scalar(
                select(WorkPackage.id).where(
                    WorkPackage.session_id == session_id,
                    WorkPackage.status.in_(
                        {
                            WorkPackageStatus.DRAFT,
                            WorkPackageStatus.APPROVED,
                            WorkPackageStatus.RUNNING,
                            WorkPackageStatus.PAUSED,
                        }
                    ),
                )
            ),
        )

    async def completed_revision_bodies(
        self, *, session_id: uuid.UUID, project_id: str, limit: int = 10
    ) -> tuple[dict[str, object], ...]:
        rows = await self._session.scalars(
            select(PlanRevision.immutable_body)
            .join(WorkPackage, WorkPackage.head_revision_id == PlanRevision.id)
            .where(
                WorkPackage.session_id == session_id,
                WorkPackage.project_id == project_id,
                WorkPackage.status == WorkPackageStatus.COMPLETED,
            )
            .order_by(PlanRevision.created_at.desc())
            .limit(limit)
        )
        return tuple(rows.all())

    async def resolve_fenced_item(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        ordinal: int,
    ) -> tuple[uuid.UUID, uuid.UUID] | None:
        row = (
            await self._session.execute(
                select(PlanRevision.id, PlanRevisionItem.item_id)
                .join(PlanRevisionItem, PlanRevisionItem.plan_revision_id == PlanRevision.id)
                .where(
                    PlanRevision.work_package_id == package_id,
                    PlanRevision.revision_number == revision_number,
                    func.substr(PlanRevision.content_hash, 1, 8) == h8,
                    PlanRevisionItem.work_package_id == package_id,
                    PlanRevisionItem.ordinal == ordinal,
                )
            )
        ).one_or_none()
        if row is None:
            return None
        return cast(uuid.UUID, row.id), cast(uuid.UUID, row.item_id)

    async def set_open_detail(
        self,
        *,
        package_id: uuid.UUID,
        plan_revision_id: uuid.UUID,
        item_id: uuid.UUID,
        plan_revision_number: int,
        h8: str,
        ordinal: int | None,
    ) -> None:
        fenced_item = await self._session.scalar(
            select(PlanRevisionItem.id)
            .join(PlanRevision, PlanRevision.id == PlanRevisionItem.plan_revision_id)
            .where(
                PlanRevisionItem.work_package_id == package_id,
                PlanRevisionItem.plan_revision_id == plan_revision_id,
                PlanRevisionItem.item_id == item_id,
                PlanRevision.revision_number == plan_revision_number,
                func.substr(PlanRevision.content_hash, 1, 8) == h8,
            )
        )
        if fenced_item is None:
            raise ValueError("open-detail fence does not identify a revision item")
        statement = (
            insert(WorkPackageOpenDetail)
            .values(
                package_id=package_id,
                plan_revision_id=plan_revision_id,
                item_id=item_id,
                plan_revision_number=plan_revision_number,
                h8=h8,
                ordinal=ordinal,
            )
            .on_conflict_do_update(
                index_elements=["package_id"],
                set_={
                    "plan_revision_id": plan_revision_id,
                    "item_id": item_id,
                    "plan_revision_number": plan_revision_number,
                    "h8": h8,
                    "ordinal": ordinal,
                    "updated_at": func.now(),
                },
            )
        )
        await self._session.execute(statement)

    async def clear_open_detail(self, *, package_id: uuid.UUID) -> None:
        await self._session.execute(
            delete(WorkPackageOpenDetail).where(WorkPackageOpenDetail.package_id == package_id)
        )
