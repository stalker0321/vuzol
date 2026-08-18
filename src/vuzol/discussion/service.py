"""Transactional work-package lifecycle built on the storage repositories."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from vuzol.discussion.domain import (
    CapabilityProvisioning,
    CapabilityRequirementDraft,
    ComponentKind,
    DomainError,
    EnvironmentComponentDraft,
    EnvironmentDeltaDraft,
    PackageControlAction,
    PlanDraft,
    PlanItemDraft,
    WorkPackageEvent,
    canonical_plan_body,
    canonical_plan_hash,
    control_transition_target,
    plan_outline_hash,
    require_generation,
    require_mutable,
    revision_outline_hash,
    semantic_plan_hash,
    semantic_revision_hash,
)
from vuzol.project_environment import apply_approved_environment_delta
from vuzol.storage.models import (
    EditSession,
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    ProjectDiscussionSession,
    Run,
    Step,
    Task,
    WorkPackage,
)
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    EditSessionStatus,
    EstimatedComplexity,
    PlanRevisionCreatedBy,
    PlanRevisionState,
    RiskLevel,
    StepStatus,
    TaskStatus,
    WorkPackagePauseReason,
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
        if await self.is_duplicate_terminal_plan(
            session_id=session_id, project_id=project_id, plan=plan
        ):
            raise DomainError(
                "duplicate_terminal_plan",
                "the proposed plan repeats a terminal plan",
            )
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

    async def is_duplicate_terminal_plan(
        self, *, session_id: uuid.UUID, project_id: str, plan: PlanDraft
    ) -> bool:
        bodies = await self._uow.work_packages.terminal_revision_bodies(
            session_id=session_id, project_id=project_id
        )
        semantic = semantic_plan_hash(plan)
        outline = plan_outline_hash(plan)
        return any(
            semantic_revision_hash(body) == semantic or revision_outline_hash(body) == outline
            for body in bodies
        )

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
        _allow_stopped: bool = False,
    ) -> RevisionResult:
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        if not (_allow_stopped and package.status is WorkPackageStatus.STOPPED):
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

    async def restart_plan(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> RevisionResult:
        """Clone a stopped approved revision so a fresh task can be materialized safely."""

        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        control_transition_target(package.status, PackageControlAction.RESTART_PACKAGE)
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if package.head_revision_id != revision.id or package.approved_revision_id != revision.id:
            raise DomainError("approval_binding_mismatch")
        result = await self.revise_draft(
            package_id=package_id,
            expected_status_generation=expected_status_generation,
            plan=_plan_from_revision_body(revision.immutable_body),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
            _allow_stopped=True,
        )
        package.pause_reason = None
        package.last_failure_task_id = None
        await self._event(
            package.id,
            WorkPackageEvent.PACKAGE_REPLAN_REQUESTED,
            "user",
            previous_state=WorkPackageStatus.STOPPED.value,
            new_state=WorkPackageStatus.DRAFT.value,
            payload={
                "previous_revision_id": str(revision.id),
                "new_revision_id": str(result.revision_id),
                "requested_by_user_id": user_id,
                "restart": True,
                "status_generation": result.status_generation,
            },
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
        assert self._uow.session is not None
        await apply_approved_environment_delta(
            self._uow.session,
            project_id=package.project_id,
            plan_revision=revision,
            approved_by_user_id=user_id,
        )
        package.approved_revision_id = revision.id
        # Every accepted revision starts from the then-current project target.
        # Old integration refs remain immutable evidence but cannot leak changes
        # into a replanned package execution.
        package.integration_branch = None
        package.integration_target_branch = None
        package.integration_base_commit = None
        package.integration_head_commit = None
        package.preview_url = None
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
        await self._release_discussion(package)
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

    async def finish_package(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> int:
        """Abandon a live/stopped chain and release its project topic for new work."""

        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        control_transition_target(package.status, PackageControlAction.FINISH_PACKAGE)
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if package.head_revision_id != revision.id:
            raise DomainError("stale_revision")
        previous = package.status
        cancelled_task_id = await self._cancel_current_task(package, actor_id=str(user_id))
        package.status = WorkPackageStatus.DISCARDED
        package.pause_reason = None
        package.approved_revision_id = None
        revision.state = PlanRevisionState.DISCARDED
        package.version += 1
        await self._release_discussion(package)
        await self._close_open_edits(package_id, actor_type="system")
        await self._uow.work_packages.clear_open_detail(package_id=package_id)
        await self._detail_event(package_id, None, None, None, None, True)
        await self._event(
            package.id,
            WorkPackageEvent.PACKAGE_DISCARDED,
            "user",
            previous_state=previous.value,
            new_state=WorkPackageStatus.DISCARDED.value,
            payload={
                "revision_id": str(revision.id),
                "finished_by_user_id": user_id,
                "cancelled_task_id": (
                    None if cancelled_task_id is None else str(cancelled_task_id)
                ),
                "status_generation": package.version,
            },
        )
        return package.version

    async def _release_discussion(self, package: WorkPackage) -> None:
        session = self._uow.session
        if session is None:
            raise RuntimeError("unit of work is not active")
        discussion = await session.get(
            ProjectDiscussionSession, package.session_id, with_for_update=True
        )
        if discussion is not None and discussion.active_work_package_id == package.id:
            discussion.active_work_package_id = None

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

    async def pause_for_item_outcome(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        ordinal: int,
        blocked: bool,
        failure_task_id: uuid.UUID | None = None,
    ) -> int:
        """Record a sequencer-owned failed/blocked outcome without advancing the queue."""

        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        if package.status is not WorkPackageStatus.RUNNING:
            raise DomainError("invalid_transition")
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if package.running_revision_id != revision.id or package.head_revision_id != revision.id:
            raise DomainError("stale_revision")
        resolved_revision_id, _ = await self._resolve_item(package_id, revision_number, h8, ordinal)
        if resolved_revision_id != revision.id or (
            package.cursor_ordinal is not None and package.cursor_ordinal != ordinal
        ):
            raise DomainError("stale_cursor")
        package.status = WorkPackageStatus.PAUSED
        package.pause_reason = (
            WorkPackagePauseReason.ITEM_BLOCKED if blocked else WorkPackagePauseReason.ITEM_FAILED
        )
        package.cursor_ordinal = ordinal
        package.last_failure_task_id = failure_task_id
        package.version += 1
        await self._event(
            package.id,
            WorkPackageEvent.PACKAGE_PAUSED,
            "system",
            previous_state=WorkPackageStatus.RUNNING.value,
            new_state=WorkPackageStatus.PAUSED.value,
            payload={
                "revision_id": str(revision.id),
                "ordinal": ordinal,
                "reason": package.pause_reason.value,
                "failure_task_id": None if failure_task_id is None else str(failure_task_id),
                "status_generation": package.version,
            },
        )
        await self._enqueue_plan_projection(package.id, package.version, "pause")
        return package.version

    async def retry_item(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> int:
        package, revision = await self._queue_control_package(
            package_id, revision_number, h8, expected_status_generation
        )
        control_transition_target(package.status, PackageControlAction.RETRY_ITEM)
        self._require_failure_pause(package)
        if package.cursor_ordinal is None:
            raise DomainError("failure_context_missing")
        if (
            package.pause_reason is not WorkPackagePauseReason.ITEM_BLOCKED
            or package.last_failure_task_id is None
        ):
            raise DomainError("item_not_safely_retryable")
        assert self._uow.session is not None
        step_id = await self._uow.session.scalar(
            select(Step.id)
            .join(Run, Run.id == Step.run_id)
            .where(
                Run.task_id == package.last_failure_task_id,
                Step.status == StepStatus.BLOCKED,
            )
            .order_by(Step.ordinal.desc())
            .limit(1)
        )
        if step_id is None:
            raise DomainError("item_not_safely_retryable")
        from vuzol.workflows.controls import retry_blocked_step

        try:
            await retry_blocked_step(self._uow.session, step_id, actor_id=str(user_id))
        except ValueError as exc:
            raise DomainError("item_not_safely_retryable") from exc
        package.status = WorkPackageStatus.RUNNING
        package.pause_reason = None
        package.version += 1
        await self._event(
            package.id,
            WorkPackageEvent.PACKAGE_RETRIED,
            "user",
            previous_state=WorkPackageStatus.PAUSED.value,
            new_state=WorkPackageStatus.RUNNING.value,
            payload={
                "revision_id": str(revision.id),
                "ordinal": package.cursor_ordinal,
                "requested_by_user_id": user_id,
                "status_generation": package.version,
            },
        )
        return package.version

    async def skip_item(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> int:
        package, revision = await self._queue_control_package(
            package_id, revision_number, h8, expected_status_generation
        )
        control_transition_target(package.status, PackageControlAction.SKIP_ITEM)
        self._require_failure_pause(package)
        if package.cursor_ordinal is None:
            raise DomainError("failure_context_missing")
        skipped_ordinal = package.cursor_ordinal
        package.cursor_ordinal += 1
        package.status = WorkPackageStatus.RUNNING
        package.pause_reason = None
        package.last_failure_task_id = None
        package.version += 1
        await self._event(
            package.id,
            WorkPackageEvent.PACKAGE_ITEM_SKIPPED,
            "user",
            previous_state=WorkPackageStatus.PAUSED.value,
            new_state=WorkPackageStatus.RUNNING.value,
            payload={
                "revision_id": str(revision.id),
                "ordinal": skipped_ordinal,
                "next_ordinal": package.cursor_ordinal,
                "requested_by_user_id": user_id,
                "status_generation": package.version,
            },
        )
        return package.version

    async def stop_package(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> int:
        package, revision = await self._queue_control_package(
            package_id, revision_number, h8, expected_status_generation
        )
        control_transition_target(package.status, PackageControlAction.STOP_PACKAGE)
        previous = package.status
        cancelled_task_id = await self._cancel_current_task(package, actor_id=str(user_id))
        package.status = WorkPackageStatus.STOPPED
        package.pause_reason = None
        package.version += 1
        await self._close_open_edits(package_id, actor_type="system")
        await self._uow.work_packages.clear_open_detail(package_id=package_id)
        await self._detail_event(package_id, None, None, None, None, True)
        await self._event(
            package.id,
            WorkPackageEvent.PACKAGE_STOPPED,
            "user",
            previous_state=previous.value,
            new_state=WorkPackageStatus.STOPPED.value,
            payload={
                "revision_id": str(revision.id),
                "stopped_by_user_id": user_id,
                "cancelled_task_id": (
                    None if cancelled_task_id is None else str(cancelled_task_id)
                ),
                "status_generation": package.version,
            },
        )
        return package.version

    async def _cancel_current_task(
        self, package: WorkPackage, *, actor_id: str
    ) -> uuid.UUID | None:
        """Fence stop against duplicate execution before a later resume."""

        if package.running_revision_id is None or package.cursor_ordinal is None:
            return None
        assert self._uow.session is not None
        link = await self._uow.session.scalar(
            select(MaterializationLink).where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == package.running_revision_id,
                MaterializationLink.ordinal == package.cursor_ordinal,
            )
        )
        if link is None:
            return None
        task = await self._uow.session.get(Task, link.task_id, with_for_update=True)
        if task is None or task.status in USER_TERMINAL_TASK_STATUSES:
            return None
        run_id = await self._uow.session.scalar(
            select(Run.id).where(Run.task_id == task.id).order_by(Run.created_at.desc()).limit(1)
        )
        if run_id is None:
            from vuzol.workflows.transitions import transition_task

            await transition_task(
                self._uow.session,
                task,
                TaskStatus.CANCELLED,
                actor_type="user",
                actor_id=actor_id,
            )
        else:
            from vuzol.workflows.controls import cancel_task

            await cancel_task(self._uow.session, task.id, actor_id=actor_id)
        return task.id

    async def request_replan(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> int:
        """Pause the queue until the next model-backed replacement plan is ready."""

        package, revision = await self._queue_control_package(
            package_id, revision_number, h8, expected_status_generation
        )
        control_transition_target(package.status, PackageControlAction.REQUEST_REPLAN)
        previous_status = package.status
        package.status = WorkPackageStatus.PAUSED
        package.pause_reason = WorkPackagePauseReason.REPLAN_REQUIRED
        package.version += 1
        await self._event(
            package.id,
            WorkPackageEvent.PACKAGE_REPLAN_REQUESTED,
            "user",
            previous_state=previous_status.value,
            new_state=WorkPackageStatus.PAUSED.value,
            payload={
                "previous_revision_id": str(revision.id),
                "requested_by_user_id": user_id,
                "status_generation": package.version,
            },
        )
        return package.version

    async def _queue_control_package(
        self,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
    ) -> tuple[WorkPackage, PlanRevision]:
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_mutable(package.status)
        require_generation(package.version, expected_status_generation)
        revision = await self._fenced_revision(package_id, revision_number, h8)
        if package.head_revision_id != revision.id:
            raise DomainError("stale_revision")
        return package, revision

    @staticmethod
    def _require_failure_pause(package: WorkPackage) -> None:
        if package.pause_reason not in {
            WorkPackagePauseReason.ITEM_FAILED,
            WorkPackagePauseReason.ITEM_BLOCKED,
        }:
            raise DomainError("failure_context_missing")

    async def _enqueue_plan_projection(
        self, package_id: uuid.UUID, generation: int, reason: str
    ) -> None:
        await self._uow.outbox.enqueue(
            destination="work_package_projection",
            operation_type="render_plan",
            entity_type="work_package",
            entity_id=package_id,
            idempotency_key=f"wp:projection:{reason}:{package_id}:{generation}",
            payload={"package_id": str(package_id)},
        )

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
        event_id = await self._event(
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
        await self._uow.outbox.enqueue(
            destination="work_package_projection",
            operation_type="clear_detail" if cleared else "render_detail",
            entity_type="work_package",
            entity_id=package_id,
            idempotency_key=f"wp:projection:detail:{event_id}",
            payload={"package_id": str(package_id)},
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
    ) -> uuid.UUID:
        return await self._uow.events.append(
            entity_type=entity_type,
            entity_id=entity_id,
            event_type=event_type.value,
            actor_type=actor_type,
            previous_state=previous_state,
            new_state=new_state,
            payload=payload,
        )


def _plan_from_revision_body(body: dict[str, object]) -> PlanDraft:
    title = body.get("title")
    items = body.get("items")
    if not isinstance(title, str) or not isinstance(items, list):
        raise DomainError("invalid_plan")
    drafts: list[PlanItemDraft] = []
    try:
        for raw in items:
            if not isinstance(raw, dict):
                raise DomainError("invalid_plan")
            raw_local_id = raw.get("local_id")
            if raw_local_id is not None and not isinstance(raw_local_id, str):
                raise DomainError("invalid_plan")
            drafts.append(
                PlanItemDraft(
                    item_id=uuid.UUID(str(raw["item_id"])),
                    local_id=raw_local_id,
                    summary=str(raw["summary"]),
                    goal=str(raw["goal"]),
                    expected_outcome=str(raw["expected_outcome"]),
                    completion_criteria=tuple(str(value) for value in raw["completion_criteria"]),
                    allowed_scope=str(raw["allowed_scope"]),
                    out_of_scope=tuple(str(value) for value in raw.get("out_of_scope", [])),
                    dependencies=tuple(str(value) for value in raw.get("dependencies", [])),
                    trusted_checks=tuple(str(value) for value in raw.get("trusted_checks", [])),
                    suggested_risk=RiskLevel(str(raw["suggested_risk"])),
                    needs_approval=bool(raw["needs_approval"]),
                    estimated_complexity=EstimatedComplexity(str(raw["estimated_complexity"])),
                )
            )
    except (KeyError, TypeError, ValueError) as error:
        raise DomainError("invalid_plan") from error
    return PlanDraft(
        title=title,
        items=tuple(drafts),
        environment_delta=_environment_delta_from_revision_body(body),
    )


def _environment_delta_from_revision_body(body: dict[str, object]) -> EnvironmentDeltaDraft:
    raw_delta = body.get("environment_delta")
    if raw_delta is None:
        return EnvironmentDeltaDraft()
    if not isinstance(raw_delta, dict):
        raise DomainError("invalid_environment")
    try:
        raw_components = raw_delta.get("upsert_components", [])
        raw_removed = raw_delta.get("remove_components", [])
        raw_capabilities = raw_delta.get("required_capabilities", [])
        if not all(
            isinstance(value, list) for value in (raw_components, raw_removed, raw_capabilities)
        ):
            raise DomainError("invalid_environment")
        components = tuple(
            EnvironmentComponentDraft(
                key=str(component["key"]),
                label=str(component["label"]),
                kind=ComponentKind(str(component["kind"])),
                technology=str(component["technology"]),
                version=(None if component.get("version") is None else str(component["version"])),
                run_command=tuple(str(value) for value in component.get("run_command", [])),
                port=None if component.get("port") is None else int(component["port"]),
                healthcheck_path=(
                    None
                    if component.get("healthcheck_path") is None
                    else str(component["healthcheck_path"])
                ),
                artifact_patterns=tuple(
                    str(value) for value in component.get("artifact_patterns", [])
                ),
            )
            for component in raw_components
            if isinstance(component, dict)
        )
        capabilities = tuple(
            CapabilityRequirementDraft(
                key=str(capability["key"]),
                label=str(capability["label"]),
                provisioning=CapabilityProvisioning(str(capability["provisioning"])),
                reason=str(capability.get("reason", "")),
            )
            for capability in raw_capabilities
            if isinstance(capability, dict)
        )
        return EnvironmentDeltaDraft(
            upsert_components=components,
            remove_components=tuple(str(value) for value in raw_removed),
            required_capabilities=capabilities,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise DomainError("invalid_environment") from error
