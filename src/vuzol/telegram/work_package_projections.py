"""PostgreSQL-backed plan and item-detail Telegram projections."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    ProjectDiscussionSession,
    Run,
    Step,
    UsageRecord,
    WorkPackage,
    WorkPackageOpenDetail,
)
from vuzol.storage.types import StepStatus, WorkPackagePauseReason, WorkPackageStatus
from vuzol.telegram.projections import TELEGRAM_TEXT_LIMIT, telegram_html
from vuzol.telegram.work_packages import (
    WorkPackageCallback,
    WorkPackageCallbackKind,
    encode_work_package_callback,
)
from vuzol.workflows.retry_policy import blocked_step_is_retryable

WORK_PACKAGE_PLAN_ROLE = "work_package_plan"
WORK_PACKAGE_STATUS_ROLE = "work_package_status"
WORK_PACKAGE_DETAIL_ROLE = "work_package_detail"
WORK_PACKAGE_PROJECTION_DESTINATION = "work_package_projection"
PLAN_PAGE_SIZE = 8


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


async def build_work_package_plan_card(
    session: AsyncSession,
    package_id: uuid.UUID,
    *,
    page: int = 1,
    _status_card: bool = False,
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
        WorkPackageStatus.DRAFT: "Черновик",
        WorkPackageStatus.APPROVED: "План принят",
        WorkPackageStatus.RUNNING: "Выполняется",
        WorkPackageStatus.PAUSED: "Приостановлен",
        WorkPackageStatus.COMPLETED: "Завершён",
        WorkPackageStatus.STOPPED: "Остановлен",
        WorkPackageStatus.DISCARDED: "Отменён",
    }[package.status]
    progress = None
    if package.status in {WorkPackageStatus.RUNNING, WorkPackageStatus.PAUSED}:
        ordinal = package.cursor_ordinal or 1
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
        worker = _route_provider_label(route)
        action = "выполняется" if package.status is WorkPackageStatus.RUNNING else "пауза"
        progress = f"{ordinal}/{len(items)} {action}"
        if worker is not None:
            progress += f" | {worker}"
    if _status_card and progress is not None:
        current_item = next(
            (item for item in items if item.ordinal == (package.cursor_ordinal or 1)), None
        )
        lines = [
            f"<b>{telegram_html(progress)}</b>",
            (
                telegram_html(current_item.summary)
                if current_item is not None
                else telegram_html(package.title)
            ),
            "",
            f"<b>{telegram_html(package.title)}</b>",
            "",
        ]
    else:
        lines = [
            f"<b>{telegram_html(package.title)}</b>",
            f"Статус: <b>{status}</b> · версия плана {revision.revision_number}",
            "",
        ]
    if _status_card and package.status is WorkPackageStatus.PAUSED:
        reason_key = "unknown" if package.pause_reason is None else package.pause_reason.value
        reason = {
            "item_failed": "пункт завершился ошибкой",
            "item_blocked": "пункт заблокирован",
            "replan_required": "требуется новый план",
            "policy": "остановлено политикой безопасности",
            "user": "остановлено пользователем",
        }.get(reason_key, "причина неизвестна")
        current = "не определён" if package.cursor_ordinal is None else str(package.cursor_ordinal)
        lines.extend(
            (
                f"Очередь остановлена: <b>{reason}</b>.",
                f"Текущий пункт: {current}. Автоматического перехода дальше не будет.",
                "",
            )
        )
    if not _status_card:
        lines.extend(f"<b>{item.ordinal}.</b> {telegram_html(item.summary)}" for item in visible)
    tokens = await _work_package_token_totals(session, package.id)
    if _status_card and any(tokens):
        lines.extend(
            (
                "",
                f"Токены: {_format_count(tokens[0])} вх / {_format_count(tokens[1])} вых / "
                f"{_format_count(tokens[2])} кэш",
            )
        )
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
    if not _status_card and package.status is WorkPackageStatus.DRAFT:
        controls.append(
            ("Принять план", _callback(WorkPackageCallbackKind.APPROVE, package, revision))
        )
        controls.append(("Отменить", _callback(WorkPackageCallbackKind.DISCARD, package, revision)))
    elif _status_card and package.status is WorkPackageStatus.RUNNING:
        controls.append(
            (
                "Завершить цепочку",
                _callback(WorkPackageCallbackKind.STOP_PACKAGE, package, revision),
            )
        )
        controls.append(
            (
                "Перепланировать",
                _callback(WorkPackageCallbackKind.REQUEST_REPLAN, package, revision),
            )
        )
    elif _status_card and package.status is WorkPackageStatus.PAUSED:
        if await _package_retry_available(session, package):
            controls.append(
                ("Повторить", _callback(WorkPackageCallbackKind.RETRY_ITEM, package, revision))
            )
        controls.extend(
            (
                ("Пропустить", _callback(WorkPackageCallbackKind.SKIP_ITEM, package, revision)),
                (
                    "Перепланировать",
                    _callback(WorkPackageCallbackKind.REQUEST_REPLAN, package, revision),
                ),
                (
                    "Завершить цепочку",
                    _callback(WorkPackageCallbackKind.STOP_PACKAGE, package, revision),
                ),
            )
        )
    elif _status_card and (
        package.status is WorkPackageStatus.STOPPED and package.approved_revision_id == revision.id
    ):
        controls.append(
            (
                "Возобновить",
                _callback(WorkPackageCallbackKind.RESTART_PACKAGE, package, revision),
            )
        )
    if controls:
        buttons.append(tuple(controls))
    buttons.append(
        (("Обсудить", _callback(WorkPackageCallbackKind.CONTINUE_DISCUSSION, package, revision)),)
    )
    return WorkPackageCard(
        package_id=package.id,
        revision_id=revision.id,
        revision_number=revision.revision_number,
        status_generation=package.version,
        chat_id=discussion.chat_id,
        thread_id=discussion.message_thread_id,
        role=WORK_PACKAGE_STATUS_ROLE if _status_card else WORK_PACKAGE_PLAN_ROLE,
        html=html,
        callback_buttons=tuple(buttons),
        page=page,
    )


async def build_work_package_status_card(
    session: AsyncSession, package_id: uuid.UUID
) -> WorkPackageCard:
    return await build_work_package_plan_card(session, package_id, _status_card=True)


def _route_provider_label(route: object) -> str | None:
    if not isinstance(route, dict):
        return None
    value = route.get("trusted_profile_id") or route.get("profile_id") or route.get("executor")
    if not isinstance(value, str):
        return None
    lowered = value.lower()
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
    if (
        package.pause_reason is not WorkPackagePauseReason.ITEM_BLOCKED
        or package.last_failure_task_id is None
    ):
        return False
    step = await session.scalar(
        select(Step)
        .join(Run, Run.id == Step.run_id)
        .where(
            Run.task_id == package.last_failure_task_id,
            Step.status == StepStatus.BLOCKED,
        )
        .order_by(Step.ordinal.desc())
        .limit(1)
    )
    return step is not None and blocked_step_is_retryable(step)


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
