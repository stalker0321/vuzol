"""Work-package identity and fence persistence primitives."""

import uuid
from typing import cast

from sqlalchemy import delete, func, select
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
from vuzol.storage.types import WorkPackageStatus


class WorkPackageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_package(self, package: WorkPackage) -> uuid.UUID:
        self._session.add(package)
        await self._session.flush()
        return package.id

    async def add_revision(self, revision: PlanRevision) -> uuid.UUID:
        self._session.add(revision)
        await self._session.flush()
        return revision.id

    async def add_item(self, item: WorkItemDraft) -> uuid.UUID:
        self._session.add(item)
        await self._session.flush()
        return item.id

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
