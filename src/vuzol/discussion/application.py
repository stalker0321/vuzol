"""P5 application boundaries for draft mutation and authoritative package controls."""

from __future__ import annotations

import uuid
from enum import StrEnum

from pydantic import Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.discussion.domain import (
    DomainError,
    PackageControlAction,
    PlanDraft,
    canonical_plan_body,
)
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.discussion.service import RevisionResult, WorkPackageService
from vuzol.interpretation.discussion import (
    DiscussionInterpretation,
    DiscussionInterpretRequest,
    enforce_discussion_policy,
)
from vuzol.interpretation.domain import FrozenModel
from vuzol.leadership.fence import legacy_package_is_fenced
from vuzol.storage.models import TelegramControlAction
from vuzol.storage.types import (
    ControlActionStatus,
    InteractionMode,
    WorkPackagePauseReason,
)
from vuzol.storage.unit_of_work import UnitOfWork


class PackageControlSource(StrEnum):
    TELEGRAM_CALLBACK = "telegram_callback_wp_cb_v1"
    EXPLICIT_COMMAND = "explicit_command"


class PackageControlResultCode(StrEnum):
    APPLIED = "applied"
    QUEUED = "queued"
    START_NOT_WIRED = "start_not_wired"
    ACTION_NOT_WIRED = "action_not_wired"


class AuthoritativeControlCommand(FrozenModel):
    action: PackageControlAction
    package_id: uuid.UUID
    plan_revision_number: int = Field(ge=1)
    h8: str = Field(pattern=r"^[0-9a-f]{8}$")
    content_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    scope_revision: int | None = Field(default=None, ge=1)
    expected_status_generation: int = Field(ge=1)
    user_id: int
    item_ordinal: int | None = Field(default=None, ge=1, le=20)
    source: PackageControlSource
    external_idempotency_key: str = Field(min_length=1, max_length=255)


class PackageControlResult(FrozenModel):
    action_id: uuid.UUID
    code: PackageControlResultCode
    status_generation: int
    revision_id: uuid.UUID | None = None
    duplicate: bool = False


class DiscussionPlanApplicationService:
    """Apply only policy-tightened draft hints; model controls have no edge here."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        enabled: bool,
    ) -> None:
        self._factory = session_factory
        self._enabled = enabled

    async def apply_item_edit(
        self,
        *,
        request: DiscussionInterpretRequest,
        interpretation: DiscussionInterpretation,
        replacement: PlanDraft,
    ) -> RevisionResult:
        self._require_enabled()
        result = enforce_discussion_policy(request, interpretation)
        context = request.edit_session
        if (
            result.interaction_mode is not InteractionMode.ITEM_EDIT
            or result.item_edit is None
            or not result.should_mutate_plan
            or context is None
            or context.opened_by_user_id != request.user_id
        ):
            raise DomainError("item_edit_not_authorized")
        async with UnitOfWork(self._factory) as uow:
            edit = await uow.work_packages.get_edit_session(
                context.edit_session_id, for_update=True
            )
            if (
                edit.package_id != context.package_id
                or edit.plan_revision_id != context.revision_id
                or edit.plan_revision_number != context.revision_number
                or edit.content_hash != context.revision_hash
                or edit.item_id != context.item_id
                or edit.session_generation != context.session_generation
                or edit.opened_by_user_id != request.user_id
            ):
                raise DomainError("edit_session_context_mismatch")
            revision = await uow.work_packages.get_revision(edit.plan_revision_id)
            _require_item_scoped_replacement(
                replacement,
                current_body=revision.immutable_body,
                target_item_id=edit.item_id,
            )
            revision_result = await WorkPackageService(uow).apply_item_edit(
                edit_session_id=edit.id,
                expected_session_generation=context.session_generation,
                replacement=replacement,
                user_id=request.user_id,
            )
            await enqueue_plan_projection(uow, revision_result)
            return revision_result

    def _require_enabled(self) -> None:
        if not self._enabled:
            raise DomainError("project_discussion_disabled")


async def enqueue_plan_projection(uow: UnitOfWork, result: RevisionResult) -> None:
    await uow.outbox.enqueue(
        destination="work_package_projection",
        operation_type="render_plan",
        entity_type="work_package",
        entity_id=result.package_id,
        idempotency_key=f"wp:projection:revision:{result.revision_id}",
        payload={"package_id": str(result.package_id)},
    )
    await uow.outbox.enqueue(
        destination="work_package_projection",
        operation_type="render_status",
        entity_type="work_package",
        entity_id=result.package_id,
        idempotency_key=f"wp:projection:revision-status:{result.revision_id}",
        payload={"package_id": str(result.package_id)},
    )


class PackageControlIngress:
    """Sole P5 authority for callback/command package mutations."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        enabled: bool,
        authorized_user_ids: frozenset[int],
    ) -> None:
        self._factory = session_factory
        self._enabled = enabled
        self._authorized_user_ids = authorized_user_ids

    async def apply(self, command: AuthoritativeControlCommand) -> PackageControlResult:
        if not self._enabled:
            raise DomainError("project_discussion_disabled")
        if command.user_id not in self._authorized_user_ids:
            raise DomainError("control_unauthorized")
        command_payload = command.model_dump(mode="json", exclude={"external_idempotency_key"})
        async with UnitOfWork(self._factory) as uow:
            action = TelegramControlAction(
                external_action_id=command.external_idempotency_key,
                action_kind=f"work_package.{command.action.value}",
                requested_by_user_id=command.user_id,
                task_id=None,
                step_id=None,
                approval_id=None,
                payload={"command": command_payload},
            )
            action_id, created = await uow.telegram_actions.queue_once(action)
            assert uow.session is not None
            persisted = await uow.session.get(
                TelegramControlAction, action_id, with_for_update=True
            )
            assert persisted is not None
            if not created:
                return _duplicate_result(persisted, command_payload, command)
            if command.action in {
                PackageControlAction.APPROVE,
                PackageControlAction.START,
                PackageControlAction.RESTART_PACKAGE,
            }:
                from vuzol.leadership.fence import lock_project_admission
                from vuzol.storage.models import WorkPackage

                project_id = await uow.session.scalar(
                    select(WorkPackage.project_id).where(WorkPackage.id == command.package_id)
                )
                if project_id is not None:
                    await lock_project_admission(uow.session, project_id)
                managed_goal = await WorkPackageService(uow)._lock_managed_goal(command.package_id)
                watched = await uow.work_packages.get_package(command.package_id, for_update=True)
                if await legacy_package_is_fenced(uow.session, watched) and not (
                    command.action is PackageControlAction.APPROVE
                    and managed_goal is not None
                    and managed_goal.current_package_id == watched.id
                ):
                    outcome = {
                        "code": PackageControlResultCode.QUEUED.value,
                        "status_generation": watched.version,
                        "revision_id": None,
                    }
                    persisted.payload = {"command": command_payload, "outcome": outcome}
                    persisted.status = ControlActionStatus.PROCESSED
                    return PackageControlResult(
                        action_id=action_id,
                        code=PackageControlResultCode.QUEUED,
                        status_generation=watched.version,
                        revision_id=None,
                    )
            service = WorkPackageService(uow)
            if command.action is PackageControlAction.APPROVE:
                approved_generation = await service.approve(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                    content_hash=command.content_hash,
                    scope_revision=command.scope_revision,
                )
                package = await uow.work_packages.get_package(command.package_id)
                if package.managed_goal_id is not None:
                    generation = approved_generation
                else:
                    sequence = await WorkPackageSequencer(uow).start(
                        package_id=command.package_id,
                        revision_number=command.plan_revision_number,
                        h8=command.h8,
                        expected_status_generation=approved_generation,
                        user_id=command.user_id,
                    )
                    generation = sequence.status_generation
                code = PackageControlResultCode.APPLIED
                revision_id = None
            elif command.action is PackageControlAction.DISCARD:
                generation = await service.discard(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                )
                code = PackageControlResultCode.APPLIED
                revision_id = None
            elif command.action is PackageControlAction.START:
                sequence = await WorkPackageSequencer(uow).start(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                )
                revision_id = None
                generation = sequence.status_generation
                code = PackageControlResultCode.APPLIED
            elif command.action is PackageControlAction.RETRY_ITEM:
                package = await uow.work_packages.get_package(command.package_id)
                if package.pause_reason is WorkPackagePauseReason.ITEM_FAILED:
                    sequence = await WorkPackageSequencer(uow).retry_failed_item(
                        package_id=command.package_id,
                        revision_number=command.plan_revision_number,
                        h8=command.h8,
                        expected_status_generation=command.expected_status_generation,
                        user_id=command.user_id,
                    )
                    generation = sequence.status_generation
                else:
                    generation = await service.retry_item(
                        package_id=command.package_id,
                        revision_number=command.plan_revision_number,
                        h8=command.h8,
                        expected_status_generation=command.expected_status_generation,
                        user_id=command.user_id,
                    )
                code = PackageControlResultCode.APPLIED
                revision_id = None
            elif command.action is PackageControlAction.SKIP_ITEM:
                generation = await service.skip_item(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                )
                await WorkPackageSequencer(uow).materialize_running(package_id=command.package_id)
                code = PackageControlResultCode.APPLIED
                revision_id = None
            elif command.action is PackageControlAction.STOP_PACKAGE:
                generation = await service.stop_package(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                )
                code = PackageControlResultCode.APPLIED
                revision_id = None
            elif command.action is PackageControlAction.FINISH_PACKAGE:
                generation = await service.finish_package(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                )
                code = PackageControlResultCode.APPLIED
                revision_id = None
            elif command.action is PackageControlAction.RESTART_PACKAGE:
                restart = await service.restart_plan(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                )
                restarted_revision = await uow.work_packages.get_revision(restart.revision_id)
                approved_generation = await service.approve(
                    package_id=command.package_id,
                    revision_number=restarted_revision.revision_number,
                    h8=restarted_revision.content_hash[:8],
                    expected_status_generation=restart.status_generation,
                    user_id=command.user_id,
                )
                sequence = await WorkPackageSequencer(uow).start(
                    package_id=command.package_id,
                    revision_number=restarted_revision.revision_number,
                    h8=restarted_revision.content_hash[:8],
                    expected_status_generation=approved_generation,
                    user_id=command.user_id,
                )
                generation = sequence.status_generation
                revision_id = restarted_revision.id
                code = PackageControlResultCode.APPLIED
            elif command.action is PackageControlAction.REQUEST_REPLAN:
                generation = await service.request_replan(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                    user_id=command.user_id,
                )
                code = PackageControlResultCode.APPLIED
                revision_id = None
            else:
                revision_id, generation = await service.validate_control_fence(
                    package_id=command.package_id,
                    revision_number=command.plan_revision_number,
                    h8=command.h8,
                    expected_status_generation=command.expected_status_generation,
                )
                code = PackageControlResultCode.ACTION_NOT_WIRED
            outcome = {
                "code": code.value,
                "status_generation": generation,
                "revision_id": None if revision_id is None else str(revision_id),
            }
            persisted.payload = {"command": command_payload, "outcome": outcome}
            persisted.status = ControlActionStatus.PROCESSED
            if code is PackageControlResultCode.APPLIED:
                await uow.outbox.enqueue(
                    destination="work_package_projection",
                    operation_type=(
                        "render_plan"
                        if command.action is PackageControlAction.APPROVE
                        else "render_status"
                    ),
                    entity_type="work_package",
                    entity_id=command.package_id,
                    idempotency_key=f"wp:projection:control:{action_id}:{generation}",
                    payload={"package_id": str(command.package_id)},
                )
            return PackageControlResult(
                action_id=action_id,
                code=code,
                status_generation=generation,
                revision_id=revision_id,
            )


def _duplicate_result(
    persisted: TelegramControlAction,
    command_payload: dict[str, object],
    command: AuthoritativeControlCommand,
) -> PackageControlResult:
    if (
        persisted.action_kind != f"work_package.{command.action.value}"
        or persisted.requested_by_user_id != command.user_id
        or persisted.payload.get("command") != command_payload
    ):
        raise DomainError("idempotency_conflict")
    outcome = persisted.payload.get("outcome")
    if persisted.status is not ControlActionStatus.PROCESSED or not isinstance(outcome, dict):
        raise DomainError("idempotency_incomplete")
    try:
        code = PackageControlResultCode(str(outcome["code"]))
        generation = int(outcome["status_generation"])
        raw_revision_id = outcome.get("revision_id")
        revision_id = None if raw_revision_id is None else uuid.UUID(str(raw_revision_id))
    except (KeyError, TypeError, ValueError) as error:
        raise DomainError("idempotency_outcome_invalid") from error
    return PackageControlResult(
        action_id=persisted.id,
        code=code,
        status_generation=generation,
        revision_id=revision_id,
        duplicate=True,
    )


def _require_item_scoped_replacement(
    replacement: PlanDraft,
    *,
    current_body: dict[str, object],
    target_item_id: uuid.UUID,
) -> None:
    current_title = current_body.get("title")
    current_items = current_body.get("items")
    if current_title != replacement.title.strip() or not isinstance(current_items, list):
        raise DomainError("item_edit_scope_violation")
    item_ids = tuple(item.item_id for item in replacement.items)
    if any(item_id is None for item_id in item_ids):
        raise DomainError("item_edit_scope_violation")
    resolved_ids = tuple(item_id for item_id in item_ids if item_id is not None)
    replacement_body = canonical_plan_body(replacement, resolved_ids)
    replacement_items = replacement_body["items"]
    if len(current_items) != len(replacement_items):
        raise DomainError("item_edit_scope_violation")
    target_seen = False
    for current, updated in zip(current_items, replacement_items, strict=True):
        if not isinstance(current, dict) or current.get("item_id") != updated.get("item_id"):
            raise DomainError("item_edit_scope_violation")
        if current.get("item_id") == str(target_item_id):
            target_seen = True
        elif current != updated:
            raise DomainError("item_edit_scope_violation")
    if not target_seen:
        raise DomainError("item_edit_scope_violation")
