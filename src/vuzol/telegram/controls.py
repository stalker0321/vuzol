"""Persisted idempotent Telegram callback handling."""

import hashlib
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    PackageControlIngress,
    PackageControlSource,
)
from vuzol.discussion.context_budget import ChatContextError
from vuzol.discussion.domain import DomainError, PackageControlAction
from vuzol.discussion.service import WorkPackageService
from vuzol.interpretation.discussion import ControlOverrideKind
from vuzol.interpretation.documents import DocumentError
from vuzol.projects.executor_preference import ExecutorPreferenceError
from vuzol.projects.naming import ProjectNamingControlError, ProjectNamingController
from vuzol.security.secret_ingress import cancel_request
from vuzol.storage.errors import EntityNotFound
from vuzol.storage.models import (
    Approval,
    EditSession,
    MaterializationLink,
    Run,
    Step,
    Task,
    TelegramControlAction,
    TelegramMessageLink,
    WorkPackage,
)
from vuzol.storage.types import EditSessionStatus, RunStatus, StepStatus, WorkPackageStatus
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


async def _status_card_revision(session: AsyncSession, task: Task) -> int:
    """Revision stamped on the delivered card. Package cards combine both versions."""

    from vuzol.telegram.projections import task_card_revision

    materialization = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task.id)
    )
    package_version = None
    if materialization is not None:
        package = await session.get(WorkPackage, materialization.work_package_id)
        if package is not None:
            package_version = package.version
    return task_card_revision(task.version, package_version)


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
            if update.action_kind == "think_handoff":
                from vuzol.discussion.handoff import accept_think_handoff

                async with UnitOfWork(self._session_factory) as uow:
                    return await accept_think_handoff(uow, self._runtime, update)
            if update.action_kind in {"lead_resume", "lead_cancel", "lead_accept", "lead_rework"}:
                from vuzol.leadership.telegram_commands import apply_goal_button

                if update.message_id is None or update.message_thread_id is None:
                    raise TelegramPolicyError("control_binding_missing")
                topic = self._runtime.registries.topics.resolve(
                    update.chat_id, update.message_thread_id
                )
                if not topic.enabled or topic.project_id is None:
                    raise TelegramPolicyError("outside_scope")
                return await apply_goal_button(
                    session_factory=self._session_factory,
                    settings=self._runtime.settings,
                    project_id=topic.project_id,
                    update=update,
                )
            if update.action_kind == "lead_answer":
                from vuzol.leadership.telegram_commands import answer_question_message

                if update.message_id is None or update.message_thread_id is None:
                    raise TelegramPolicyError("control_binding_missing")
                topic = self._runtime.registries.topics.resolve(
                    update.chat_id, update.message_thread_id
                )
                if not topic.enabled or topic.project_id is None:
                    raise TelegramPolicyError("outside_scope")
                if update.question_version is None or update.answer_option is None:
                    raise TelegramPolicyError("answer_required")
                result = await answer_question_message(
                    session_factory=self._session_factory,
                    settings=self._runtime.settings,
                    project_id=topic.project_id,
                    chat_id=update.chat_id,
                    thread_id=update.message_thread_id,
                    message_id=update.message_id,
                    user_id=update.user_id,
                    key=f"telegram:lead-answer:{update.callback_query_id}",
                    question_version=update.question_version,
                    option=update.answer_option,
                )
                return result or IngressResult(
                    status=IngressStatus.REJECTED, reason="control_binding_missing"
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
        except (TelegramPolicyError, RegistryError) as error:
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
                if update.action_kind == "chat_retry":
                    from vuzol.discussion.chat_recovery import retry_chat_turn

                    result = await retry_chat_turn(uow, self._runtime, update)
                    await uow.inbox.mark_processed(
                        inbox_id,
                        entity_type="task",
                        entity_id=update.task_id,
                    )
                    return result
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
                action_payload: dict[str, Any] = {}
                target_step = update.step_id
                if (
                    update.action_kind in {"start", "pause", "resume", "cancel"}
                    and update.expected_task_version is not None
                ):
                    assert uow.session is not None
                    current = await uow.session.get(Task, update.task_id)
                    if (
                        current is None
                        or current.source_thread_id is None
                        or (
                            current.user_id,
                            current.source_chat_id,
                            current.source_thread_id,
                        )
                        != (update.user_id, update.chat_id, update.message_thread_id)
                    ):
                        raise TelegramPolicyError("task_retry_scope_mismatch")
                    link = await uow.session.scalar(
                        select(TelegramMessageLink).where(
                            TelegramMessageLink.chat_id == update.chat_id,
                            TelegramMessageLink.message_id == update.message_id,
                            TelegramMessageLink.task_id == current.id,
                            TelegramMessageLink.message_role.in_(
                                ("task_status", "task_control_snapshot", "task_status_archived")
                            ),
                        )
                    )
                    if link is None or (
                        current.version == update.expected_task_version
                        and link.projection_revision != update.expected_task_version
                    ):
                        raise TelegramPolicyError("task_retry_message_mismatch")
                    if current.version != update.expected_task_version:
                        from vuzol.telegram.projections import enqueue_task_control_snapshot

                        assert update.message_thread_id is not None
                        await enqueue_task_control_snapshot(
                            uow.session,
                            task=current,
                            chat_id=update.chat_id,
                            thread_id=update.message_thread_id,
                            user_id=update.user_id,
                            key=f"task:control-stale:{update.callback_query_id}",
                        )
                        await uow.inbox.mark_processed(
                            inbox_id, entity_type="task", entity_id=current.id
                        )
                        return IngressResult(
                            status=IngressStatus.REJECTED, reason="task_retry_stale"
                        )
                    action_payload = {"expected_task_version": update.expected_task_version}
                if update.action_kind in {"retry", "admit", "uncertain"}:
                    assert uow.session is not None
                    task = await uow.session.get(Task, update.task_id)
                    if (
                        task is None
                        or task.source_thread_id is None
                        or (
                            task.user_id,
                            task.source_chat_id,
                            task.source_thread_id,
                        )
                        != (update.user_id, update.chat_id, update.message_thread_id)
                    ):
                        raise TelegramPolicyError("task_retry_scope_mismatch")
                    button_revision = update.expected_task_version
                    assert update.message_thread_id is not None
                    if button_revision is None:
                        raise TelegramPolicyError("task_retry_version_required")
                    # Package cards stamp task.version * 10000 + package.version.
                    # The button carries that stamp; the command CAS uses task.version.
                    card_revision = await _status_card_revision(uow.session, task)
                    link = await uow.session.scalar(
                        select(TelegramMessageLink).where(
                            TelegramMessageLink.chat_id == update.chat_id,
                            TelegramMessageLink.message_id == update.message_id,
                            TelegramMessageLink.task_id == task.id,
                            TelegramMessageLink.message_role.in_(
                                ("task_status", "task_control_snapshot", "task_status_archived")
                            ),
                        )
                    )
                    if link is None or (
                        card_revision == button_revision
                        and link.projection_revision != button_revision
                    ):
                        raise TelegramPolicyError("task_retry_message_mismatch")
                    run = await uow.session.scalar(
                        select(Run)
                        .where(Run.task_id == task.id)
                        .order_by(Run.created_at.desc())
                        .limit(1)
                    )
                    step = (
                        None
                        if run is None
                        else await uow.session.scalar(
                            select(Step).where(
                                Step.run_id == run.id,
                                Step.ordinal == update.retry_step_ordinal,
                            )
                        )
                    )
                    from vuzol.workflows.retry_policy import blocked_step_is_retryable

                    if (
                        card_revision != button_revision
                        or run is None
                        or run.status is not RunStatus.BLOCKED
                        or step is None
                        or step.status is not StepStatus.BLOCKED
                        or not blocked_step_is_retryable(step)
                    ):
                        from vuzol.telegram.projections import enqueue_task_control_snapshot

                        await enqueue_task_control_snapshot(
                            uow.session,
                            task=task,
                            chat_id=update.chat_id,
                            thread_id=update.message_thread_id,
                            user_id=update.user_id,
                            key=f"task:retry-stale:{update.callback_query_id}",
                        )
                        await uow.inbox.mark_processed(
                            inbox_id, entity_type="task", entity_id=task.id
                        )
                        return IngressResult(
                            status=IngressStatus.REJECTED, reason="task_retry_stale"
                        )
                    target_step = step.id
                    action_payload = {"expected_task_version": task.version}
                action = TelegramControlAction(
                    external_action_id=update.callback_query_id,
                    action_kind=update.action_kind,
                    requested_by_user_id=update.user_id,
                    task_id=update.task_id,
                    step_id=target_step,
                    approval_id=update.approval_id,
                    payload=action_payload,
                )
                action_id, action_created = await uow.telegram_actions.queue_once(action)
                prior_outcome: dict[str, Any] | None = None
                if action_created:
                    await uow.outbox.enqueue(
                        destination="workflow_control",
                        operation_type=update.action_kind,
                        entity_type="telegram_control_action",
                        entity_id=action_id,
                        idempotency_key=f"telegram:control:{update.callback_query_id}",
                        payload=update.model_dump(mode="json"),
                    )
                else:
                    # D1 L5: duplicate delivery returns the prior receipt
                    # instead of "unknown" (same outcome as the first apply).
                    assert uow.session is not None
                    prior_action = await uow.session.get(TelegramControlAction, action_id)
                    if prior_action is not None and isinstance(prior_action.payload, dict):
                        stored = prior_action.payload.get("outcome")
                        if isinstance(stored, dict):
                            prior_outcome = stored
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
            DocumentError,
            ChatContextError,
        ) as error:
            return IngressResult(status=IngressStatus.REJECTED, reason=str(error))

        return IngressResult(
            status=IngressStatus.CREATED if action_created else IngressStatus.DUPLICATE,
            action_id=action_id,
            outcome=prior_outcome,
        )

    async def _accept_work_package(self, update: WorkPackageControlUpdate) -> IngressResult:
        try:
            authorize(self._runtime.settings, chat_id=update.chat_id, user_id=update.user_id)
            settings = self._runtime.settings
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
                # The lead's plan cards use these controls too; only legacy
                # discussion packages depend on the discussion feature flag.
                managed = package.managed_goal_id is not None and settings.project_lead.enabled
                if not managed and not settings.project_discussion_enabled:
                    raise TelegramPolicyError("project discussion is disabled")
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
                        content_hash=revision.content_hash,
                        scope_revision=revision.immutable_body.get("scope_revision"),
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
            if isinstance(error, DomainError) and await self._repost_stale_card(update):
                reason = "stale_card_reposted"
            return IngressResult(status=IngressStatus.REJECTED, reason=reason)

    async def _repost_stale_card(self, update: WorkPackageControlUpdate) -> bool:
        """A press on an outdated card brings the current one to the bottom of the topic."""
        try:
            package_id = parse_work_package_callback(update.callback_data).package_id
        except WorkPackageCallbackError:
            return False
        async with UnitOfWork(self._session_factory) as uow:
            try:
                package = await uow.work_packages.get_package(package_id)
            except LookupError:
                return False
            if package.status is WorkPackageStatus.DISCARDED:
                return False
            await uow.outbox.enqueue(
                destination="work_package_projection",
                operation_type="repost_plan",
                entity_type="work_package",
                entity_id=package_id,
                idempotency_key=f"wp:repost:stale:{update.callback_query_id}",
                payload={"package_id": str(package_id)},
            )
        return True

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
                or (message_role != "work_package_plan" and package.version != generation)
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
                if package.managed_goal_id is not None:
                    from vuzol.leadership.persistence import ManagedGoal

                    goal = await uow.session.get(ManagedGoal, package.managed_goal_id)
                    if goal is None or goal.owner_user_id != update.user_id:
                        raise DomainError("outside_scope")
                    operation = "change_plan"
                else:
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
            operation == "continue_discussion"
            and self._continue_discussion_overrides is not None
            and update.message_thread_id is not None
        ):
            await self._continue_discussion_overrides.arm(
                chat_id=update.chat_id,
                thread_id=update.message_thread_id,
                user_id=update.user_id,
                kind=ControlOverrideKind.CONTINUE_DISCUSSION,
            )
        return IngressResult(status=IngressStatus.HANDLED, reason=operation)
