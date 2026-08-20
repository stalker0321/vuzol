"""PostgreSQL-backed plan and item-detail Telegram projections."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.projects.executor_preference import load_preference
from vuzol.storage.models import (
    Approval,
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    ProjectDiscussionSession,
    Run,
    Step,
    Task,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
    UsageRecord,
    WorkPackage,
    WorkPackageOpenDetail,
)
from vuzol.storage.types import (
    ApprovalStatus,
    DeliveryStatus,
    StepStatus,
    TaskStatus,
    WorkPackagePauseReason,
    WorkPackageStatus,
)
from vuzol.telegram.projections import TELEGRAM_TEXT_LIMIT, telegram_html
from vuzol.telegram.work_packages import (
    WorkPackageCallback,
    WorkPackageCallbackKind,
    encode_work_package_callback,
)
from vuzol.workflows.retry_policy import (
    blocked_step_is_retryable,
    failed_item_is_rematerializable,
)

WORK_PACKAGE_PLAN_ROLE = "work_package_plan"
WORK_PACKAGE_STATUS_ROLE = "work_package_status"
WORK_PACKAGE_ACTION_ROLE = "work_package_action"
WORK_PACKAGE_DETAIL_ROLE = "work_package_detail"
WORK_PACKAGE_PROJECTION_DESTINATION = "work_package_projection"
PLAN_PAGE_SIZE = 8


async def enqueue_project_topic_status(
    session: AsyncSession,
    *,
    chat_id: int,
    thread_id: int,
    project_id: str,
    revision: int,
    idle: bool = False,
) -> None:
    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == chat_id,
            TopicMapping.message_thread_id == thread_id,
            TopicMapping.project_id == project_id,
            TopicMapping.enabled.is_(True),
        )
    )
    if mapping is None:
        return
    existing_link = await session.scalar(
        select(TelegramMessageLink.id).where(
            TelegramMessageLink.chat_id == chat_id,
            TelegramMessageLink.message_thread_id == thread_id,
            TelegramMessageLink.message_role == WORK_PACKAGE_STATUS_ROLE,
        )
    )
    blocking_bootstrap = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == WORK_PACKAGE_PROJECTION_DESTINATION,
            TransactionalOutbox.operation_type.in_(("render_topic_status", "render_topic_idle")),
            TransactionalOutbox.status != DeliveryStatus.DEAD_LETTER,
            TransactionalOutbox.payload["chat_id"].astext == str(chat_id),
            TransactionalOutbox.payload["thread_id"].astext == str(thread_id),
        )
    )
    if existing_link is not None or blocking_bootstrap is not None:
        return
    dead_bootstrap = await session.scalar(
        select(TransactionalOutbox.id)
        .where(
            TransactionalOutbox.destination == WORK_PACKAGE_PROJECTION_DESTINATION,
            TransactionalOutbox.operation_type.in_(("render_topic_status", "render_topic_idle")),
            TransactionalOutbox.status == DeliveryStatus.DEAD_LETTER,
            TransactionalOutbox.payload["chat_id"].astext == str(chat_id),
            TransactionalOutbox.payload["thread_id"].astext == str(thread_id),
        )
        .order_by(TransactionalOutbox.created_at.desc(), TransactionalOutbox.id.desc())
        .limit(1)
    )
    retry_suffix = "" if dead_bootstrap is None else f":retry:{dead_bootstrap}"
    session.add(
        TransactionalOutbox(
            destination=WORK_PACKAGE_PROJECTION_DESTINATION,
            operation_type="render_topic_idle" if idle else "render_topic_status",
            linked_entity_type="topic_mapping",
            linked_entity_id=mapping.id,
            idempotency_key=(
                f"topic-idle-bootstrap:{chat_id}:{thread_id}:{revision}{retry_suffix}"
                if idle
                else f"topic-status:{chat_id}:{thread_id}:{revision}"
            ),
            payload={
                "chat_id": chat_id,
                "thread_id": thread_id,
                "project_id": project_id,
                "revision": revision,
            },
        )
    )


async def enqueue_project_topic_idle(
    session: AsyncSession,
    *,
    chat_id: int,
    thread_id: int,
    project_id: str,
    source_outbox_id: uuid.UUID,
) -> None:
    """Return an existing topic card to idle after a confirmed discussion reply."""

    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == chat_id,
            TopicMapping.message_thread_id == thread_id,
            TopicMapping.project_id == project_id,
            TopicMapping.enabled.is_(True),
        )
    )
    link = await session.scalar(
        select(TelegramMessageLink).where(
            TelegramMessageLink.chat_id == chat_id,
            TelegramMessageLink.message_thread_id == thread_id,
            TelegramMessageLink.message_role == WORK_PACKAGE_STATUS_ROLE,
        )
    )
    if mapping is None or link is None:
        return
    session.add(
        TransactionalOutbox(
            destination=WORK_PACKAGE_PROJECTION_DESTINATION,
            operation_type="render_topic_idle",
            linked_entity_type="topic_mapping",
            linked_entity_id=mapping.id,
            idempotency_key=f"topic-idle:{source_outbox_id}",
            payload={
                "chat_id": chat_id,
                "thread_id": thread_id,
                "project_id": project_id,
                "revision": link.projection_revision + 1,
            },
        )
    )


class WorkPackageProjectionError(RuntimeError):
    """A reconstructable projection cannot be built from canonical state."""


@dataclass(frozen=True, slots=True)
class WorkPackageCard:
    package_id: uuid.UUID
    revision_id: uuid.UUID
    revision_number: int
    status_generation: int
    chat_id: int
    thread_id: int
    role: str
    html: str
    callback_buttons: tuple[tuple[tuple[str, str], ...], ...]
    page: int = 1


def _callback(
    kind: WorkPackageCallbackKind,
    package: WorkPackage,
    revision: PlanRevision,
    value: int | None = None,
) -> str:
    return encode_work_package_callback(
        WorkPackageCallback(
            kind=kind,
            package_id=package.id,
            revision_number=revision.revision_number,
            h8=revision.content_hash[:8],
            value=value,
        )
    )


def _environment_delta_lines(body: dict[str, object]) -> tuple[str, ...]:
    delta = body.get("environment_delta")
    if not isinstance(delta, dict):
        return ()
    lines: list[str] = []
    components = delta.get("upsert_components", [])
    if isinstance(components, list):
        for component in components[:12]:
            if not isinstance(component, dict):
                continue
            label = telegram_html(component.get("label", component.get("key", "Component")))
            technology = telegram_html(component.get("technology", "unspecified"))
            version = component.get("version")
            suffix = f" {telegram_html(version)}" if isinstance(version, str) and version else ""
            lines.append(f"+ {label} · {technology}{suffix}")
    removed = delta.get("remove_components", [])
    if isinstance(removed, list):
        lines.extend(f"- {telegram_html(key)}" for key in removed[:8])
    capabilities = delta.get("required_capabilities", [])
    if isinstance(capabilities, list):
        external: list[str] = []
        privileged: list[str] = []
        for capability in capabilities:
            if not isinstance(capability, dict):
                continue
            label = str(capability.get("label", capability.get("key", "capability")))
            provisioning = capability.get("provisioning")
            if provisioning == "external_setup":
                external.append(label)
            elif provisioning == "approval_required":
                privileged.append(label)
        if privileged:
            lines.append("Approval: " + telegram_html(", ".join(privileged[:6])))
        if external:
            lines.append("Setup: " + telegram_html(", ".join(external[:6])))
    return tuple(lines)


async def build_work_package_plan_card(
    session: AsyncSession,
    package_id: uuid.UUID,
    *,
    page: int = 1,
    _status_card: bool = False,
    _action_card: bool = False,
) -> WorkPackageCard:
    if page < 1 or page > 999:
        raise WorkPackageProjectionError("invalid_page")
    package = await session.get(WorkPackage, package_id)
    if package is None or package.head_revision_id is None:
        raise WorkPackageProjectionError("package_missing")
    revision = await session.get(PlanRevision, package.head_revision_id)
    discussion = await session.get(ProjectDiscussionSession, package.session_id)
    if revision is None or discussion is None or revision.work_package_id != package.id:
        raise WorkPackageProjectionError("projection_source_missing")
    items = tuple(
        (
            await session.scalars(
                select(PlanRevisionItem)
                .where(PlanRevisionItem.plan_revision_id == revision.id)
                .order_by(PlanRevisionItem.ordinal)
            )
        ).all()
    )
    page_count = max(1, math.ceil(len(items) / PLAN_PAGE_SIZE))
    if page > page_count:
        raise WorkPackageProjectionError("page_out_of_range")
    visible = items[(page - 1) * PLAN_PAGE_SIZE : page * PLAN_PAGE_SIZE]
    status = {
        WorkPackageStatus.DRAFT: "Plan approval",
        WorkPackageStatus.APPROVED: "Queued",
        WorkPackageStatus.RUNNING: "Working",
        WorkPackageStatus.PAUSED: "Paused",
        WorkPackageStatus.COMPLETED: "Done",
        WorkPackageStatus.STOPPED: "Stopped",
        WorkPackageStatus.DISCARDED: "Cancelled",
    }[package.status]
    released = (
        package.status in {WorkPackageStatus.COMPLETED, WorkPackageStatus.DISCARDED}
        and discussion.active_work_package_id != package.id
    )
    if released:
        status = "Ready"
    progress = None
    worker = None
    current_task_status = None
    if package.status in {WorkPackageStatus.RUNNING, WorkPackageStatus.PAUSED}:
        ordinal = package.cursor_ordinal or 1
        current_task_status = await session.scalar(
            select(Task.status)
            .join(MaterializationLink, MaterializationLink.task_id == Task.id)
            .where(
                MaterializationLink.plan_revision_id == revision.id,
                MaterializationLink.ordinal == ordinal,
            )
            .limit(1)
        )
        executor_profile_id = await session.scalar(
            select(Step.executor_profile_id)
            .join(Run, Run.id == Step.run_id)
            .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
            .where(
                MaterializationLink.plan_revision_id == revision.id,
                MaterializationLink.ordinal == ordinal,
                Step.executor_profile_id.is_not(None),
            )
            .order_by(Run.created_at.desc(), Step.ordinal.desc())
            .limit(1)
        )
        route = await session.scalar(
            select(Run.selected_route)
            .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
            .where(
                MaterializationLink.plan_revision_id == revision.id,
                MaterializationLink.ordinal == ordinal,
            )
            .order_by(Run.created_at.desc())
            .limit(1)
        )
        worker = _route_provider_label(
            {"profile_id": executor_profile_id} if executor_profile_id is not None else route
        )
        progress = f"{ordinal}/{len(items)}"
        if current_task_status is TaskStatus.WAITING_APPROVAL:
            requested_action = await session.scalar(
                select(Approval.requested_action)
                .join(Step, Step.id == Approval.step_id)
                .join(Run, Run.id == Step.run_id)
                .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
                .where(
                    MaterializationLink.plan_revision_id == revision.id,
                    MaterializationLink.ordinal == ordinal,
                    Approval.status == ApprovalStatus.PENDING,
                )
                .order_by(Approval.created_at.desc())
                .limit(1)
            )
            approval_statuses: dict[str | None, str] = {
                "install_capabilities": "Tool installation approval",
                "install_dependencies": "Dependency installation approval",
            }
            status = approval_statuses.get(requested_action, "Result approval")
    if _status_card or _action_card:
        preference = await load_preference(session, discussion.project_id)
        if package.status is WorkPackageStatus.PAUSED and preference.worker_key is not None:
            worker = preference.worker_key.value.title()
        if worker is None:
            worker = (
                "Auto" if preference.worker_key is None else preference.worker_key.value.title()
            )
        current_item = (
            next((item for item in items if item.ordinal == (package.cursor_ordinal or 1)), None)
            if package.status in {WorkPackageStatus.RUNNING, WorkPackageStatus.PAUSED}
            else None
        )
        title = (
            "Send a new task"
            if released
            else current_item.summary
            if current_item is not None
            else package.title
        )
        if progress is not None:
            title = f"{progress} · {title}"
        lines = [
            f"<b>{status} | {telegram_html(worker)}</b>",
            telegram_html(title),
            "",
        ]
    else:
        lines = [
            f"<b>{telegram_html(package.title)}</b>",
            f"Статус: <b>{status}</b> · версия плана {revision.revision_number}",
            "",
        ]
    if _action_card and package.status is WorkPackageStatus.PAUSED:
        failed_step = (
            None
            if package.last_failure_task_id is None
            else await session.scalar(
                select(Step)
                .join(Run, Run.id == Step.run_id)
                .where(
                    Run.task_id == package.last_failure_task_id,
                    Step.status.in_((StepStatus.BLOCKED, StepStatus.FAILED)),
                )
                .order_by(Step.updated_at.desc())
                .limit(1)
            )
        )
        reason_key = "unknown" if package.pause_reason is None else package.pause_reason.value
        reason = {
            "item_failed": "пункт завершился ошибкой",
            "item_blocked": "пункт заблокирован",
            "replan_required": "требуется новый план",
            "policy": "остановлено политикой безопасности",
            "user": "остановлено пользователем",
        }.get(reason_key, "причина неизвестна")
        current = "не определён" if package.cursor_ordinal is None else str(package.cursor_ordinal)
        if failed_step is not None and failed_step.failure_category == "environment_setup_required":
            lines[0] = f"<b>Needs setup | {telegram_html(worker)}</b>"
            lines.extend(
                (
                    "Для продолжения не хватает возможности окружения.",
                    telegram_html(failed_step.failure_summary or "Environment setup is required"),
                    f"Пункт {current} сохранён; после настройки его можно повторить.",  # noqa: RUF001
                    "",
                )
            )
        else:
            lines.extend(
                (
                    f"Очередь остановлена: <b>{reason}</b>.",
                    f"Текущий пункт: {current}. Автоматического перехода дальше не будет.",
                    "",
                )
            )
    token_line = None
    if _action_card:
        input_tokens, output_tokens, cached_tokens = await _work_package_token_totals(
            session, package.id
        )
        if input_tokens or output_tokens or cached_tokens:
            token_line = (
                "Токены: "
                f"{_format_count(input_tokens)} вх / "
                f"{_format_count(output_tokens)} вых / "
                f"{_format_count(cached_tokens)} кэш"
            )
    package_approval = None
    package_result_complete = _action_card and package.status is WorkPackageStatus.COMPLETED
    package_delivery_failure = None
    if _action_card and package.status is WorkPackageStatus.PAUSED:
        package_delivery_failure = await session.scalar(
            select(Step)
            .join(Run, Run.id == Step.run_id)
            .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
            .where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == revision.id,
                MaterializationLink.ordinal == len(items),
                Step.step_type == "approval",
                Step.status == StepStatus.BLOCKED,
            )
            .order_by(Step.updated_at.desc())
            .limit(1)
        )
    if _action_card and current_task_status is TaskStatus.WAITING_APPROVAL:
        package_approval = await session.scalar(
            select(Approval)
            .join(Step, Step.id == Approval.step_id)
            .join(Run, Run.id == Step.run_id)
            .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
            .where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == revision.id,
                MaterializationLink.ordinal == len(items),
                Approval.status == ApprovalStatus.PENDING,
                Approval.requested_action == "apply_result",
            )
            .order_by(Approval.created_at.desc())
            .limit(1)
        )
        if package_approval is not None:
            lines = []
            lines.extend(
                (
                    "<b>Plan completed · approval required</b>",
                    *(f"✅ {item.ordinal}. {telegram_html(item.summary)}" for item in items),
                    "",
                    "Все пункты и настроенные проверки завершены.",  # noqa: RUF001
                )
            )
            if token_line is not None:
                lines.extend((token_line, ""))
            if package.preview_url is not None:
                preview = telegram_html(package.preview_url)
                lines.extend((f'<b>Preview:</b> <a href="{preview}">{preview}</a>', ""))
            lines.extend(("Применить итоговый результат плана?", ""))
    if package_result_complete:
        lines = []
        lines.extend(
            (
                "<b>Plan approved · deployed</b>",
                *(f"✅ {item.ordinal}. {telegram_html(item.summary)}" for item in items),
                "",
                "Итоговый результат принят и применён.",
                "",
            )
        )
        if token_line is not None:
            lines.extend((token_line, ""))
        production_url = await session.scalar(
            select(Step.result["public_url"].astext)
            .join(Run, Run.id == Step.run_id)
            .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
            .where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == revision.id,
                Step.step_type == "publish_static",
                Step.status == StepStatus.COMPLETED,
                Step.result["status"].astext == "published",
            )
            .order_by(MaterializationLink.ordinal.desc(), Step.updated_at.desc())
            .limit(1)
        )
        if isinstance(production_url, str) and production_url:
            production = telegram_html(production_url)
            lines.extend((f'<b>Production:</b> <a href="{production}">{production}</a>', ""))
    if package_delivery_failure is not None:
        lines = ["<b>Plan completed · delivery failed</b>"]
        lines.extend(f"✅ {item.ordinal}. {telegram_html(item.summary)}" for item in items)
        lines.append("")
        if package.preview_url is not None:
            preview = telegram_html(package.preview_url)
            lines.extend((f'<b>Preview:</b> <a href="{preview}">{preview}</a>', ""))
        lines.extend(
            (
                "<b>Production apply failed</b>",
                telegram_html(package_delivery_failure.failure_summary or "unknown failure"),
                "",
            )
        )
    if (
        _action_card
        and package_approval is None
        and not package_result_complete
        and token_line is not None
    ):
        lines.extend((token_line, ""))
    if (
        not _status_card
        and package_approval is None
        and not package_result_complete
        and package_delivery_failure is None
    ):
        lines.extend(f"<b>{item.ordinal}.</b> {telegram_html(item.summary)}" for item in visible)
        if page == page_count:
            environment_lines = _environment_delta_lines(revision.immutable_body)
            if environment_lines:
                lines.extend(("", "<b>Stack</b>", *environment_lines))
    if not _status_card and page_count > 1:
        lines.extend(("", f"Страница {page}/{page_count}"))
    html = "\n".join(lines)
    if len(html) > TELEGRAM_TEXT_LIMIT:
        raise WorkPackageProjectionError("plan_card_too_long")
    buttons: list[tuple[tuple[str, str], ...]] = []
    if not _status_card:
        for offset in range(0, len(visible), 4):
            buttons.append(
                tuple(
                    (
                        str(item.ordinal),
                        _callback(
                            WorkPackageCallbackKind.OPEN_ITEM, package, revision, item.ordinal
                        ),
                    )
                    for item in visible[offset : offset + 4]
                )
            )
    if not _status_card and page_count > 1:
        navigation: list[tuple[str, str]] = []
        if page > 1:
            navigation.append(
                ("←", _callback(WorkPackageCallbackKind.SET_PAGE, package, revision, page - 1))
            )
        if page < page_count:
            navigation.append(
                ("→", _callback(WorkPackageCallbackKind.SET_PAGE, package, revision, page + 1))
            )
        buttons.append(tuple(navigation))
    controls: list[tuple[str, str]] = []
    if package_delivery_failure is not None:
        if await _package_retry_available(session, package):
            controls.append(
                ("Повторить", _callback(WorkPackageCallbackKind.RETRY_ITEM, package, revision))
            )
        controls.append(
            (
                "Внести правки",
                _callback(WorkPackageCallbackKind.CONTINUE_DISCUSSION, package, revision),
            )
        )
    elif package_approval is not None:
        controls.extend(
            (
                ("Принять", f"v1:approve:{package_approval.id}"),
                ("Изменить", f"v1:redo:{package_approval.id}"),
                ("Отклонить", f"v1:reject:{package_approval.id}"),
            )
        )
    elif not _status_card and package.status is WorkPackageStatus.DRAFT:
        controls.append(
            ("Принять план", _callback(WorkPackageCallbackKind.APPROVE, package, revision))
        )
        controls.append(("Отменить", _callback(WorkPackageCallbackKind.DISCARD, package, revision)))
    elif not _status_card and package.status is WorkPackageStatus.RUNNING:
        controls.append(
            (
                "Остановить",
                _callback(WorkPackageCallbackKind.STOP_PACKAGE, package, revision),
            )
        )
        controls.append(
            ("Завершить", _callback(WorkPackageCallbackKind.FINISH_PACKAGE, package, revision))
        )
        controls.append(
            (
                "Перепланировать",
                _callback(WorkPackageCallbackKind.REQUEST_REPLAN, package, revision),
            )
        )
    elif not _status_card and package.status is WorkPackageStatus.PAUSED:
        if await _package_retry_available(session, package):
            controls.append(
                ("Повторить", _callback(WorkPackageCallbackKind.RETRY_ITEM, package, revision))
            )
        controls.extend(
            (
                (
                    "Перепланировать",
                    _callback(WorkPackageCallbackKind.REQUEST_REPLAN, package, revision),
                ),
                (
                    "Завершить",
                    _callback(WorkPackageCallbackKind.FINISH_PACKAGE, package, revision),
                ),
            )
        )
    elif not _status_card and (
        package.status is WorkPackageStatus.STOPPED and package.approved_revision_id == revision.id
    ):
        controls.append(
            (
                "Возобновить",
                _callback(WorkPackageCallbackKind.RESTART_PACKAGE, package, revision),
            )
        )
        controls.append(
            ("Завершить", _callback(WorkPackageCallbackKind.FINISH_PACKAGE, package, revision))
        )
    if controls:
        buttons.append(tuple(controls))
    if not _status_card and not package_result_complete and package_delivery_failure is None:
        buttons.append(
            (
                (
                    "Обсудить",
                    _callback(WorkPackageCallbackKind.CONTINUE_DISCUSSION, package, revision),
                ),
            )
        )
    return WorkPackageCard(
        package_id=package.id,
        revision_id=revision.id,
        revision_number=revision.revision_number,
        status_generation=package.version,
        chat_id=discussion.chat_id,
        thread_id=discussion.message_thread_id,
        role=(
            WORK_PACKAGE_STATUS_ROLE
            if _status_card
            else WORK_PACKAGE_ACTION_ROLE
            if _action_card
            else WORK_PACKAGE_PLAN_ROLE
        ),
        html=html,
        callback_buttons=tuple(buttons),
        page=page,
    )


async def build_work_package_status_card(
    session: AsyncSession, package_id: uuid.UUID
) -> WorkPackageCard:
    return await build_work_package_plan_card(session, package_id, _status_card=True)


async def build_work_package_action_card(
    session: AsyncSession, package_id: uuid.UUID
) -> WorkPackageCard:
    return await build_work_package_plan_card(session, package_id, _action_card=True)


def _route_provider_label(route: object) -> str | None:
    if not isinstance(route, dict):
        return None
    values = (
        route.get("executor_worker_key"),
        route.get("model_override"),
        route.get("trusted_profile_id"),
        route.get("profile_id"),
        route.get("executor"),
    )
    value = next((candidate for candidate in values if isinstance(candidate, str)), None)
    if value is None:
        return None
    lowered = value.lower()
    if "terra" in lowered:
        return "Terra"
    if "luna" in lowered:
        return "Luna"
    if "sol" in lowered:
        return "Sol"
    if "grok" in lowered:
        return "Grok"
    if "kimi" in lowered:
        return "Kimi"
    if "codex" in lowered or "openai" in lowered:
        return "Codex"
    return value


async def _work_package_token_totals(
    session: AsyncSession, package_id: uuid.UUID
) -> tuple[int, int, int]:
    row = (
        await session.execute(
            select(
                func.coalesce(func.sum(UsageRecord.input_tokens), 0),
                func.coalesce(func.sum(UsageRecord.output_tokens), 0),
                func.coalesce(func.sum(UsageRecord.cached_tokens), 0),
            )
            .join(Run, Run.id == UsageRecord.run_id)
            .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
            .where(MaterializationLink.work_package_id == package_id)
        )
    ).one()
    return int(row[0]), int(row[1]), int(row[2])


async def _package_retry_available(session: AsyncSession, package: WorkPackage) -> bool:
    if package.last_failure_task_id is None:
        return False
    expected_status = (
        StepStatus.BLOCKED
        if package.pause_reason is WorkPackagePauseReason.ITEM_BLOCKED
        else StepStatus.FAILED
        if package.pause_reason is WorkPackagePauseReason.ITEM_FAILED
        else None
    )
    if expected_status is None:
        return False
    step = await session.scalar(
        select(Step)
        .join(Run, Run.id == Step.run_id)
        .where(
            Run.task_id == package.last_failure_task_id,
            Step.status == expected_status,
        )
        .order_by(Step.ordinal.desc())
        .limit(1)
    )
    return step is not None and (
        blocked_step_is_retryable(step)
        if expected_status is StepStatus.BLOCKED
        else failed_item_is_rematerializable(step)
    )


def _format_count(value: int) -> str:
    return f"{value:,}".replace(",", " ")


async def build_work_package_detail_card(
    session: AsyncSession, package_id: uuid.UUID
) -> WorkPackageCard | None:
    pointer = await session.get(WorkPackageOpenDetail, package_id)
    if pointer is None:
        return None
    package = await session.get(WorkPackage, package_id)
    revision = await session.get(PlanRevision, pointer.plan_revision_id)
    discussion = (
        None if package is None else await session.get(ProjectDiscussionSession, package.session_id)
    )
    item = await session.scalar(
        select(PlanRevisionItem).where(
            PlanRevisionItem.plan_revision_id == pointer.plan_revision_id,
            PlanRevisionItem.item_id == pointer.item_id,
            PlanRevisionItem.work_package_id == pointer.package_id,
        )
    )
    if (
        package is None
        or revision is None
        or discussion is None
        or item is None
        or revision.work_package_id != package.id
        or revision.revision_number != pointer.plan_revision_number
        or revision.content_hash[:8] != pointer.h8
    ):
        raise WorkPackageProjectionError("stale_detail_pointer")
    criteria = "\n".join(f"• {telegram_html(value)}" for value in item.completion_criteria)
    html = "\n".join(
        (
            f"<b>{item.ordinal}. {telegram_html(item.summary)}</b>",
            "",
            f"<b>Цель</b>\n{telegram_html(item.goal)}",
            f"<b>Результат</b>\n{telegram_html(item.expected_outcome)}",
            f"<b>Готово, когда</b>\n{criteria}",
            f"<b>Область</b>\n{telegram_html(item.allowed_scope)}",
        )
    )
    if len(html) > TELEGRAM_TEXT_LIMIT:
        raise WorkPackageProjectionError("detail_card_too_long")
    buttons: list[tuple[str, str]] = []
    if package.status in {WorkPackageStatus.DRAFT, WorkPackageStatus.APPROVED}:
        buttons.append(
            (
                "Изменить",
                _callback(WorkPackageCallbackKind.OPEN_EDIT, package, revision, item.ordinal),
            )
        )
    buttons.append(("Закрыть", _callback(WorkPackageCallbackKind.CLOSE_DETAIL, package, revision)))
    return WorkPackageCard(
        package_id=package.id,
        revision_id=revision.id,
        revision_number=revision.revision_number,
        status_generation=package.version,
        chat_id=discussion.chat_id,
        thread_id=discussion.message_thread_id,
        role=WORK_PACKAGE_DETAIL_ROLE,
        html=html,
        callback_buttons=(tuple(buttons),),
    )
