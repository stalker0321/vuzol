"""Transactional work-package lifecycle built on the storage repositories."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from vuzol.discussion.domain import (
    DomainError,
    PackageControlAction,
    PlanDraft,
    WorkPackageEvent,
    canonical_plan_body,
    canonical_plan_hash,
    control_transition_target,
    require_generation,
    require_mutable,
)
from vuzol.storage.models import (
    EditSession,
    PlanRevision,
    PlanRevisionItem,
    WorkPackage,
)
from vuzol.storage.types import (
    EditSessionStatus,
    PlanRevisionCreatedBy,
    PlanRevisionState,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork


@dataclass(frozen=True, slots=True)
class RevisionResult:
    package_id: uuid.UUID
    revision_id: uuid.UUID
    revision_number: int
    content_hash: str
    status_generation: int


class WorkPackageService:
    """Domain operations only; no Telegram, model, Task, or materialization dependency."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def create_draft(
        self,
        *,
        session_id: uuid.UUID,
        project_id: str,
        plan: PlanDraft,
        created_by: PlanRevisionCreatedBy,
        actor_type: str,
        planner_profile: str | None = None,
        prompt_version: str | None = None,
    ) -> RevisionResult:
        discussion = await self._uow.discussions.get_session(session_id, for_update=True)
        if discussion.project_id != project_id:
            raise DomainError("project_mismatch")
        if await self._uow.work_packages.active_package_id(session_id=session_id) is not None:
            raise DomainError("active_package_exists")
        package = WorkPackage(
            session_id=session_id,
            project_id=project_id,
            status=WorkPackageStatus.DRAFT,
            title=plan.title.strip(),
        )
        package_id = await self._uow.work_packages.add_package(package)
        await self._event(
            package_id,
            WorkPackageEvent.PACKAGE_CREATED,
            actor_type,
            new_state=WorkPackageStatus.DRAFT.value,
            payload={"title": package.title, "status_generation": package.version},
        )
        result = await self._create_revision(
            package=package,
            plan=plan,
            created_by=created_by,
            actor_type=actor_type,
            planner_profile=planner_profile,
            prompt_version=prompt_version,
        )
        discussion.active_work_package_id = package_id
        return result

    async def revise_draft(
        self,
        *,
        package_id: uuid.UUID,
        expected_status_generation: int,
        plan: PlanDraft,
        created_by: PlanRevisionCreatedBy,
        actor_type: str,
        planner_profile: str | None = None,
        prompt_version: str | None = None,
    ) -> RevisionResult:
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_mutable(package.status)
        require_generation(package.version, expected_status_generation)
        previous = await self._uow.work_packages.get_head_revision(package)
        if previous is None:
            raise DomainError("revision_not_found")
        previous.state = PlanRevisionState.SUPERSEDED
        package.approved_revision_id = None
        package.status = WorkPackageStatus.DRAFT
        package.version += 1
        result = await self._create_revision(
            package=package,
            plan=plan,
            created_by=created_by,
            actor_type=actor_type,
            planner_profile=planner_profile,
            prompt_version=prompt_version,
            parent=previous,
        )
        await self._close_open_edits(package_id, actor_type="system")
        await self._uow.work_packages.clear_open_detail(package_id=package_id)
        await self._detail_event(package_id, None, None, None, None, True)
        await self._event(
            previous.id,
            WorkPackageEvent.REVISION_SUPERSEDED,
            actor_type,
            entity_type="plan_revision",
            payload={"package_id": str(package_id), "revision_number": previous.revision_number},
        )
        return result

    async def approve(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> int:
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        if package.status is WorkPackageStatus.APPROVED:
            revision = await self._fenced_revision(package_id, revision_number, h8)
            if package.approved_revision_id == revision.id:
                return package.version
        if package.status is not WorkPackageStatus.DRAFT:
            raise DomainError("invalid_transition")
        control_transition_target(package.status, PackageControlAction.APPROVE)
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if package.head_revision_id != revision.id or revision.state is not PlanRevisionState.DRAFT:
            raise DomainError("stale_revision")
        revision.state = PlanRevisionState.APPROVED
        revision.approved_at = datetime.now(UTC)
        revision.approved_by_user_id = user_id
        package.approved_revision_id = revision.id
        package.status = WorkPackageStatus.APPROVED
        package.version += 1
        await self._event(
            revision.id,
            WorkPackageEvent.REVISION_APPROVED,
            "user",
            entity_type="plan_revision",
            previous_state=WorkPackageStatus.DRAFT.value,
            new_state=WorkPackageStatus.APPROVED.value,
            payload={
                "package_id": str(package_id),
                "revision_number": revision.revision_number,
                "content_hash": revision.content_hash,
                "status_generation": package.version,
                "approved_by_user_id": user_id,
            },
        )
        return package.version

    async def discard(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> int:
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        control_transition_target(package.status, PackageControlAction.DISCARD)
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if package.head_revision_id != revision.id:
            raise DomainError("stale_revision")
        previous = package.status
        revision.state = PlanRevisionState.DISCARDED
        package.status = WorkPackageStatus.DISCARDED
        package.approved_revision_id = None
        package.version += 1
        await self._close_open_edits(package_id, actor_type="system")
        await self._uow.work_packages.clear_open_detail(package_id=package_id)
        await self._detail_event(package_id, None, None, None, None, True)
        await self._event(
            package_id,
            WorkPackageEvent.PACKAGE_DISCARDED,
            "user",
            previous_state=previous.value,
            new_state=WorkPackageStatus.DISCARDED.value,
            payload={"revision_id": str(revision.id), "discarded_by_user_id": user_id},
        )
        return package.version

    async def open_edit_session(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        ordinal: int,
        user_id: int,
        expires_in: timedelta = timedelta(minutes=20),
    ) -> EditSession:
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_mutable(package.status)
        revision_id, item_id = await self._resolve_item(package_id, revision_number, h8, ordinal)
        if package.head_revision_id != revision_id:
            raise DomainError("stale_revision")
        closed = await self._uow.work_packages.close_open_edit_sessions(
            package_id=package_id,
            opened_by_user_id=user_id,
        )
        for closed_id in closed:
            await self._event(
                closed_id,
                WorkPackageEvent.EDIT_SESSION_CLOSED,
                "system",
                entity_type="edit_session",
                payload={"package_id": str(package_id), "reason": "replaced"},
            )
        edit_session = EditSession(
            package_id=package_id,
            plan_revision_id=revision_id,
            plan_revision_number=revision_number,
            content_hash=(await self._uow.work_packages.get_revision(revision_id)).content_hash,
            item_id=item_id,
            ordinal=ordinal,
            opened_by_user_id=user_id,
            status=EditSessionStatus.OPEN,
            session_generation=package.version,
            expires_at=datetime.now(UTC) + expires_in,
        )
        await self._uow.work_packages.add_edit_session(edit_session)
        await self._event(
            edit_session.id,
            WorkPackageEvent.EDIT_SESSION_OPENED,
            "user",
            payload={"package_id": str(package_id), "item_id": str(item_id), "ordinal": ordinal},
            entity_type="edit_session",
        )
        return edit_session

    async def close_edit_session(
        self, *, edit_session_id: uuid.UUID, user_id: int | None, expired: bool = False
    ) -> None:
        edit = await self._uow.work_packages.get_edit_session(edit_session_id, for_update=True)
        if user_id is not None and edit.opened_by_user_id != user_id:
            raise DomainError("edit_session_owner_mismatch")
        if edit.status is not EditSessionStatus.OPEN:
            return
        edit.status = EditSessionStatus.EXPIRED if expired else EditSessionStatus.CLOSED
        await self._event(
            edit.id,
            (
                WorkPackageEvent.EDIT_SESSION_EXPIRED
                if expired
                else WorkPackageEvent.EDIT_SESSION_CLOSED
            ),
            "system" if user_id is None else "user",
            entity_type="edit_session",
            payload={"package_id": str(edit.package_id)},
        )

    async def apply_item_edit(
        self,
        *,
        edit_session_id: uuid.UUID,
        expected_session_generation: int,
        replacement: PlanDraft,
        user_id: int,
    ) -> RevisionResult:
        edit = await self._uow.work_packages.get_edit_session(edit_session_id, for_update=True)
        if edit.opened_by_user_id != user_id:
            raise DomainError("edit_session_owner_mismatch")
        if edit.status is not EditSessionStatus.OPEN:
            raise DomainError("edit_session_closed")
        if edit.expires_at <= datetime.now(UTC):
            raise DomainError("edit_session_expired")
        if edit.session_generation != expected_session_generation:
            raise DomainError("edit_session_generation_stale")
        package = await self._uow.work_packages.get_package(edit.package_id, for_update=True)
        if (
            package.version != edit.session_generation
            or package.head_revision_id != edit.plan_revision_id
        ):
            raise DomainError("stale_revision")
        edit.status = EditSessionStatus.ACCEPTED
        result = await self.revise_draft(
            package_id=package.id,
            expected_status_generation=package.version,
            plan=replacement,
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
        )
        await self._event(
            edit.id,
            WorkPackageEvent.EDIT_SESSION_ACCEPTED,
            "user",
            entity_type="edit_session",
            payload={"package_id": str(package.id), "new_revision_id": str(result.revision_id)},
        )
        return result

    async def set_open_detail(
        self, *, package_id: uuid.UUID, revision_number: int, h8: str, ordinal: int
    ) -> None:
        await self._uow.work_packages.get_package(package_id, for_update=True)
        revision_id, item_id = await self._resolve_item(package_id, revision_number, h8, ordinal)
        await self._uow.work_packages.set_open_detail(
            package_id=package_id,
            plan_revision_id=revision_id,
            item_id=item_id,
            plan_revision_number=revision_number,
            h8=h8,
            ordinal=ordinal,
        )
        await self._detail_event(package_id, revision_number, h8, item_id, ordinal, False)

    async def clear_open_detail(self, *, package_id: uuid.UUID) -> None:
        await self._uow.work_packages.get_package(package_id, for_update=True)
        await self._uow.work_packages.clear_open_detail(package_id=package_id)
        await self._detail_event(package_id, None, None, None, None, True)

    async def validate_startable(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
    ) -> uuid.UUID:
        """Validate approval binding without starting or creating a canonical Task."""
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        if package.status is not WorkPackageStatus.APPROVED:
            raise DomainError("invalid_transition")
        control_transition_target(package.status, PackageControlAction.START)
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if (
            revision.state is not PlanRevisionState.APPROVED
            or package.approved_revision_id != revision.id
        ):
            raise DomainError("approval_binding_mismatch")
        return revision.id

    async def validate_control_fence(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
    ) -> tuple[uuid.UUID, int]:
        """Validate a not-yet-wired control without applying a state transition."""

        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_mutable(package.status)
        require_generation(package.version, expected_status_generation)
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if package.head_revision_id != revision.id:
            raise DomainError("stale_revision")
        return revision.id, package.version

    async def _create_revision(
        self,
        *,
        package: WorkPackage,
        plan: PlanDraft,
        created_by: PlanRevisionCreatedBy,
        actor_type: str,
        planner_profile: str | None,
        prompt_version: str | None,
        parent: PlanRevision | None = None,
    ) -> RevisionResult:
        item_ids = await self._uow.work_packages.resolve_item_identities(package.id, plan.items)
        body = canonical_plan_body(plan, item_ids)
        content_hash = canonical_plan_hash(body)
        revision = PlanRevision(
            work_package_id=package.id,
            revision_number=1 if parent is None else parent.revision_number + 1,
            parent_revision_id=None if parent is None else parent.id,
            state=PlanRevisionState.DRAFT,
            content_hash=content_hash,
            created_by=created_by,
            planner_profile=planner_profile,
            prompt_version=prompt_version,
            immutable_body=body,
        )
        await self._uow.work_packages.add_revision(revision)
        for ordinal, (item, item_id) in enumerate(zip(plan.items, item_ids, strict=True), 1):
            await self._uow.work_packages.add_revision_item(
                PlanRevisionItem(
                    work_package_id=package.id,
                    plan_revision_id=revision.id,
                    item_id=item_id,
                    ordinal=ordinal,
                    summary=item.summary.strip(),
                    goal=item.goal.strip(),
                    expected_outcome=item.expected_outcome.strip(),
                    completion_criteria=list(item.completion_criteria),
                    allowed_scope=item.allowed_scope.strip(),
                    out_of_scope=list(item.out_of_scope),
                    dependencies=list(item.dependencies),
                    trusted_checks=list(item.trusted_checks),
                    suggested_risk=item.suggested_risk,
                    needs_approval=item.needs_approval,
                    estimated_complexity=item.estimated_complexity,
                )
            )
        package.title = plan.title.strip()
        package.head_revision_id = revision.id
        await self._event(
            revision.id,
            WorkPackageEvent.REVISION_CREATED,
            actor_type,
            entity_type="plan_revision",
            payload={
                "package_id": str(package.id),
                "revision_number": revision.revision_number,
                "parent_revision_id": None if parent is None else str(parent.id),
                "content_hash": content_hash,
                "status_generation": package.version,
            },
        )
        return RevisionResult(
            package.id, revision.id, revision.revision_number, content_hash, package.version
        )

    async def _resolve_item(
        self, package_id: uuid.UUID, revision_number: int, h8: str, ordinal: int
    ) -> tuple[uuid.UUID, uuid.UUID]:
        resolved = await self._uow.work_packages.resolve_fenced_item(
            package_id=package_id, revision_number=revision_number, h8=h8, ordinal=ordinal
        )
        if resolved is None:
            raise DomainError("stale_revision")
        return resolved

    async def _fenced_revision(
        self, package_id: uuid.UUID, revision_number: int, h8: str
    ) -> PlanRevision:
        try:
            return await self._uow.work_packages.get_fenced_revision(
                package_id=package_id,
                revision_number=revision_number,
                h8=h8,
            )
        except LookupError as error:
            raise DomainError("stale_revision") from error

    async def _detail_event(
        self,
        package_id: uuid.UUID,
        revision_number: int | None,
        h8: str | None,
        item_id: uuid.UUID | None,
        ordinal: int | None,
        cleared: bool,
    ) -> None:
        await self._event(
            package_id,
            WorkPackageEvent.DETAIL_POINTER_CHANGED,
            "user",
            payload={
                "item_id": None if item_id is None else str(item_id),
                "revision_number": revision_number,
                "h8": h8,
                "ordinal": ordinal,
                "cleared": cleared,
            },
        )

    async def _close_open_edits(self, package_id: uuid.UUID, *, actor_type: str) -> None:
        closed = await self._uow.work_packages.close_open_edit_sessions(package_id=package_id)
        for edit_session_id in closed:
            await self._event(
                edit_session_id,
                WorkPackageEvent.EDIT_SESSION_CLOSED,
                actor_type,
                entity_type="edit_session",
                payload={"package_id": str(package_id), "reason": "package_changed"},
            )

    async def _event(
        self,
        entity_id: uuid.UUID,
        event_type: WorkPackageEvent,
        actor_type: str,
        *,
        entity_type: str = "work_package",
        previous_state: str | None = None,
        new_state: str | None = None,
        payload: dict[str, object] | None = None,
    ) -> None:
        await self._uow.events.append(
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type.value,
            actor_type=actor_type,
            previous_state=previous_state,
            new_state=new_state,
            payload=payload,
        )
