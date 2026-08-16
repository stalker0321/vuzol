"""Persisted idempotent Telegram callback handling."""

import hashlib

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    PackageControlIngress,
    PackageControlSource,
)
from vuzol.discussion.domain import DomainError, PackageControlAction
from vuzol.discussion.service import WorkPackageService
from vuzol.interpretation.discussion import ControlOverrideKind
from vuzol.projects.executor_preference import ExecutorPreferenceError
from vuzol.projects.naming import ProjectNamingControlError, ProjectNamingController
from vuzol.security.secret_ingress import cancel_request
from vuzol.storage.errors import EntityNotFound
from vuzol.storage.models import Approval, EditSession, TelegramControlAction
from vuzol.storage.types import EditSessionStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import (
    ControlUpdate,
    IngressResult,
    IngressStatus,
    WorkPackageControlUpdate,
)
from vuzol.telegram.model_command import ProjectModelController
from vuzol.telegram.policy import TelegramPolicyError, authorize
from vuzol.telegram.work_package_projections import WORK_PACKAGE_PROJECTION_DESTINATION
from vuzol.telegram.work_packages import (
    ContinueDiscussionOverrides,
    WorkPackageCallback,
    WorkPackageCallbackError,
    WorkPackageCallbackKind,
    parse_work_package_callback,
)

_MUTATING_PACKAGE_KINDS = {
    WorkPackageCallbackKind.APPROVE: PackageControlAction.APPROVE,
    WorkPackageCallbackKind.START: PackageControlAction.START,
    WorkPackageCallbackKind.DISCARD: PackageControlAction.DISCARD,
    WorkPackageCallbackKind.RETRY_ITEM: PackageControlAction.RETRY_ITEM,
    WorkPackageCallbackKind.SKIP_ITEM: PackageControlAction.SKIP_ITEM,
    WorkPackageCallbackKind.STOP_PACKAGE: PackageControlAction.STOP_PACKAGE,
    WorkPackageCallbackKind.FINISH_PACKAGE: PackageControlAction.FINISH_PACKAGE,
    WorkPackageCallbackKind.RESTART_PACKAGE: PackageControlAction.RESTART_PACKAGE,
}


class TelegramControlService:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
        continue_discussion_overrides: ContinueDiscussionOverrides | None = None,
    ) -> None:
        self._runtime = runtime
        self._session_factory = session_factory
        self._project_naming = ProjectNamingController(runtime)
        self._project_model = ProjectModelController(runtime)
        self._continue_discussion_overrides = continue_discussion_overrides

    async def accept(self, update: ControlUpdate | WorkPackageControlUpdate) -> IngressResult:
        if isinstance(update, WorkPackageControlUpdate):
            return await self._accept_work_package(update)
        try:
            authorize(
                self._runtime.settings,
                chat_id=update.chat_id,
                user_id=update.user_id,
            )
            naming_action = update.action_kind.startswith("project_name_")
            model_action = update.action_kind.startswith("project_model_")
            secret_action = update.action_kind == "secret_cancel"
            if (
                update.task_id is None
                and update.approval_id is None
                and not naming_action
                and not model_action
                and not secret_action
            ):
                raise TelegramPolicyError("control action requires a persisted target")
            if update.action_kind in {"approve", "redo", "reject"} and update.approval_id is None:
                raise TelegramPolicyError("approval action requires approval_id")
        except TelegramPolicyError as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        payload_hash = hashlib.sha256(update.model_dump_json().encode()).hexdigest()
        try:
            async with UnitOfWork(self._session_factory) as uow:
                inbox_id, update_created = await uow.inbox.receive_once(
                    source="telegram_callback",
                    consumer=f"bot:{update.bot_id}",
                    external_event_id=str(update.update_id),
                    payload_hash=payload_hash,
                )
                if not update_created:
                    return IngressResult(status=IngressStatus.DUPLICATE)
                if naming_action:
                    assert uow.session is not None
                    outcome = await self._project_naming.apply(uow.session, update)
                    await uow.inbox.mark_processed(
                        inbox_id,
                        entity_type="project_naming",
                        entity_id=outcome.request_id,
                    )
                    return IngressResult(status=IngressStatus.CREATED)
                if model_action:
                    assert uow.session is not None
                    action = TelegramControlAction(
                        external_action_id=update.callback_query_id,
                        action_kind=update.action_kind,
                        requested_by_user_id=update.user_id,
                        task_id=None,
                        step_id=None,
                        approval_id=None,
                        payload={
                            "preference_revision": update.preference_revision,
                            "preference_worker": update.preference_worker,
                            "preference_profile_id": update.preference_profile_id,
                            "preference_effort": update.preference_effort,
                            "message_thread_id": update.message_thread_id,
                        },
                    )
                    action_id, action_created = await uow.telegram_actions.queue_once(action)
                    if action_created:
                        await self._project_model.apply(uow.session, update, action_id=action_id)
                    await uow.inbox.mark_processed(
                        inbox_id,
                        entity_type="telegram_control_action",
                        entity_id=action_id,
                    )
                    return IngressResult(
                        status=IngressStatus.CREATED if action_created else IngressStatus.DUPLICATE,
                        action_id=action_id,
                    )
                if secret_action:
                    assert uow.session is not None
                    if update.secret_request_id is None or not await cancel_request(
                        uow.session, update.secret_request_id, user_id=update.user_id
                    ):
                        raise TelegramPolicyError("secret request is unavailable")
                    await uow.inbox.mark_processed(
                        inbox_id,
                        entity_type="secret_ingress_request",
                        entity_id=update.secret_request_id,
                    )
                    if update.message_id is not None:
                        await uow.outbox.enqueue(
                            destination="telegram",
                            operation_type="delete_message",
                            entity_type="secret_ingress_request",
                            entity_id=update.secret_request_id,
                            idempotency_key=f"telegram:secret_cancel_delete:{update.callback_query_id}",
                            payload={
                                "role": "user_command_delete",
                                "chat_id": update.chat_id,
                                "message_thread_id": update.message_thread_id,
                                "message_id": update.message_id,
                            },
                        )
                    return IngressResult(status=IngressStatus.HANDLED, reason="secret_cancelled")
                if update.task_id is not None:
                    await uow.tasks.get(update.task_id, for_update=True)
                elif update.approval_id is not None:
                    assert uow.session is not None
                    approval = await uow.session.get(Approval, update.approval_id)
                    if approval is None:
                        raise EntityNotFound(f"approval not found: {update.approval_id}")
                action = TelegramControlAction(
                    external_action_id=update.callback_query_id,
                    action_kind=update.action_kind,
                    requested_by_user_id=update.user_id,
                    task_id=update.task_id,
                    step_id=update.step_id,
                    approval_id=update.approval_id,
                    payload={},
                )
                action_id, action_created = await uow.telegram_actions.queue_once(action)
                if action_created:
                    await uow.outbox.enqueue(
                        destination="workflow_control",
                        operation_type=update.action_kind,
                        entity_type="telegram_control_action",
                        entity_id=action_id,
                        idempotency_key=f"telegram:control:{update.callback_query_id}",
                        payload=update.model_dump(mode="json"),
                    )
                await uow.inbox.mark_processed(
                    inbox_id,
                    entity_type="telegram_control_action",
                    entity_id=action_id,
                )
        except (
            EntityNotFound,
            ProjectNamingControlError,
            ExecutorPreferenceError,
            TelegramPolicyError,
        ) as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        return IngressResult(
            status=IngressStatus.CREATED if action_created else IngressStatus.DUPLICATE,
            action_id=action_id,
        )

    async def _accept_work_package(self, update: WorkPackageControlUpdate) -> IngressResult:
        try:
            authorize(self._runtime.settings, chat_id=update.chat_id, user_id=update.user_id)
            if not self._runtime.settings.project_discussion_enabled:
                raise TelegramPolicyError("project discussion is disabled")
            callback = parse_work_package_callback(update.callback_data)
            async with UnitOfWork(self._session_factory) as uow:
                binding = await uow.telegram_links.resolve_work_package_control(
                    update.chat_id, update.message_id
                )
                if binding is None:
                    raise DomainError("control_binding_missing")
                package_id, revision_id, generation, message_role = binding
                revision = await uow.work_packages.get_revision(revision_id)
                package = await uow.work_packages.get_package(package_id)
                if (
                    callback.package_id != package_id
                    or revision.work_package_id != package_id
                    or callback.revision_number != revision.revision_number
                    or callback.h8 != revision.content_hash[:8]
                ):
                    raise DomainError("stale_revision")
                if message_role == "work_package_plan":
                    if package.head_revision_id != revision.id:
                        raise DomainError("stale_revision")
                    generation = package.version
            action = _MUTATING_PACKAGE_KINDS.get(callback.kind)
            if action is not None:
                result = await PackageControlIngress(
                    self._session_factory,
                    enabled=True,
                    authorized_user_ids=frozenset(self._runtime.settings.allowed_user_ids),
                ).apply(
                    AuthoritativeControlCommand(
                        action=action,
                        package_id=callback.package_id,
                        plan_revision_number=callback.revision_number,
                        h8=callback.h8,
                        expected_status_generation=generation,
                        user_id=update.user_id,
                        item_ordinal=callback.ordinal,
                        source=PackageControlSource.TELEGRAM_CALLBACK,
                        external_idempotency_key=update.callback_query_id,
                    )
                )
                return IngressResult(
                    status=IngressStatus.DUPLICATE if result.duplicate else IngressStatus.HANDLED,
                    action_id=result.action_id,
                    reason=result.code.value,
                )
            return await self._apply_non_mutating_package_callback(update, callback)
        except (TelegramPolicyError, WorkPackageCallbackError, DomainError, LookupError) as error:
            reason = error.code if isinstance(error, DomainError) else str(error)
            return IngressResult(status=IngressStatus.REJECTED, reason=reason)

    async def _apply_non_mutating_package_callback(
        self, update: WorkPackageControlUpdate, callback: WorkPackageCallback
    ) -> IngressResult:
        payload_hash = hashlib.sha256(update.model_dump_json().encode()).hexdigest()
        async with UnitOfWork(self._session_factory) as uow:
            inbox_id, created = await uow.inbox.receive_once(
                source="telegram_callback",
                consumer=f"bot:{update.bot_id}",
                external_event_id=str(update.update_id),
                payload_hash=payload_hash,
            )
            if not created:
                return IngressResult(status=IngressStatus.DUPLICATE)
            binding = await uow.telegram_links.resolve_work_package_control(
                update.chat_id, update.message_id
            )
            if binding is None:
                raise DomainError("control_binding_missing")
            package_id, revision_id, generation, message_role = binding
            package = await uow.work_packages.get_package(package_id, for_update=True)
            revision = await uow.work_packages.get_revision(revision_id)
            if (
                callback.package_id != package_id
                or revision.work_package_id != package_id
                or callback.revision_number != revision.revision_number
                or callback.h8 != revision.content_hash[:8]
                or package.head_revision_id != revision.id
                or (
                    message_role != "work_package_plan" and package.version != generation
                )
            ):
                raise DomainError("stale_projection")
            if message_role == "work_package_plan":
                generation = package.version
            service = WorkPackageService(uow)
            operation: str | None = None
            enqueue_projection = False
            payload: dict[str, object] = {"package_id": str(callback.package_id)}
            if callback.kind is WorkPackageCallbackKind.OPEN_ITEM:
                assert callback.ordinal is not None
                await service.set_open_detail(
                    package_id=callback.package_id,
                    revision_number=callback.revision_number,
                    h8=callback.h8,
                    ordinal=callback.ordinal,
                )
                operation = "render_detail"
            elif callback.kind is WorkPackageCallbackKind.OPEN_EDIT:
                assert callback.ordinal is not None
                await service.open_edit_session(
                    package_id=callback.package_id,
                    revision_number=callback.revision_number,
                    h8=callback.h8,
                    ordinal=callback.ordinal,
                    user_id=update.user_id,
                )
                operation = "open_edit"
            elif callback.kind is WorkPackageCallbackKind.CLOSE_DETAIL:
                await service.clear_open_detail(package_id=callback.package_id)
                operation = "clear_detail"
            elif callback.kind is WorkPackageCallbackKind.SET_PAGE:
                assert callback.page is not None
                operation = "render_plan"
                enqueue_projection = True
                payload["page"] = callback.page
            elif callback.kind is WorkPackageCallbackKind.CONTINUE_DISCUSSION:
                assert uow.session is not None
                edit_ids = tuple(
                    (
                        await uow.session.scalars(
                            select(EditSession.id).where(
                                EditSession.package_id == callback.package_id,
                                EditSession.opened_by_user_id == update.user_id,
                                EditSession.status == EditSessionStatus.OPEN,
                            )
                        )
                    ).all()
                )
                for edit_id in edit_ids:
                    await service.close_edit_session(
                        edit_session_id=edit_id, user_id=update.user_id
                    )
                operation = "continue_discussion"
            elif callback.kind is WorkPackageCallbackKind.REQUEST_REPLAN:
                await service.request_replan(
                    package_id=callback.package_id,
                    revision_number=callback.revision_number,
                    h8=callback.h8,
                    expected_status_generation=generation,
                    user_id=update.user_id,
                )
                operation = "request_replan"
                enqueue_projection = True
            else:
                raise DomainError("unsupported_control")
            if enqueue_projection:
                await uow.outbox.enqueue(
                    destination=WORK_PACKAGE_PROJECTION_DESTINATION,
                    operation_type=operation,
                    entity_type="work_package",
                    entity_id=callback.package_id,
                    idempotency_key=f"wp:projection:{update.callback_query_id}",
                    payload=payload,
                )
            await uow.inbox.mark_processed(
                inbox_id, entity_type="work_package", entity_id=callback.package_id
            )
        if (
            operation in {"continue_discussion", "request_replan"}
            and self._continue_discussion_overrides is not None
            and update.message_thread_id is not None
        ):
            await self._continue_discussion_overrides.arm(
                chat_id=update.chat_id,
                thread_id=update.message_thread_id,
                user_id=update.user_id,
                kind=(
                    ControlOverrideKind.REPLAN
                    if operation == "request_replan"
                    else ControlOverrideKind.CONTINUE_DISCUSSION
                ),
            )
        return IngressResult(status=IngressStatus.HANDLED, reason=operation)
