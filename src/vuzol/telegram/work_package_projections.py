"""PostgreSQL-backed plan and item-detail Telegram projections."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    PlanRevision,
    PlanRevisionItem,
    ProjectDiscussionSession,
    WorkPackage,
    WorkPackageOpenDetail,
)
from vuzol.storage.types import WorkPackagePauseReason, WorkPackageStatus
from vuzol.telegram.projections import TELEGRAM_TEXT_LIMIT, telegram_html
from vuzol.telegram.work_packages import (
    WorkPackageCallback,
    WorkPackageCallbackKind,
    encode_work_package_callback,
)

WORK_PACKAGE_PLAN_ROLE = "work_package_plan"
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
    session: AsyncSession, package_id: uuid.UUID, *, page: int = 1
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
    lines = [
        f"<b>{telegram_html(package.title)}</b>",
        f"Статус: <b>{status}</b> · версия плана {revision.revision_number}",
        "",
    ]
    if package.status is WorkPackageStatus.PAUSED:
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
    lines.extend(f"<b>{item.ordinal}.</b> {telegram_html(item.summary)}" for item in visible)
    if page_count > 1:
        lines.extend(("", f"Страница {page}/{page_count}"))
    html = "\n".join(lines)
    if len(html) > TELEGRAM_TEXT_LIMIT:
        raise WorkPackageProjectionError("plan_card_too_long")
    buttons: list[tuple[tuple[str, str], ...]] = []
    for offset in range(0, len(visible), 4):
        buttons.append(
            tuple(
                (
                    str(item.ordinal),
                    _callback(WorkPackageCallbackKind.OPEN_ITEM, package, revision, item.ordinal),
                )
                for item in visible[offset : offset + 4]
            )
        )
    if page_count > 1:
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
    if package.status is WorkPackageStatus.DRAFT:
        controls.append(
            ("Принять план", _callback(WorkPackageCallbackKind.APPROVE, package, revision))
        )
        controls.append(("Отменить", _callback(WorkPackageCallbackKind.DISCARD, package, revision)))
    elif package.status is WorkPackageStatus.APPROVED:
        controls.append(("Начать", _callback(WorkPackageCallbackKind.START, package, revision)))
        controls.append(("Отменить", _callback(WorkPackageCallbackKind.DISCARD, package, revision)))
    elif package.status is WorkPackageStatus.RUNNING:
        controls.append(
            ("Остановить", _callback(WorkPackageCallbackKind.STOP_PACKAGE, package, revision))
        )
        controls.append(
            (
                "Перепланировать",
                _callback(WorkPackageCallbackKind.REQUEST_REPLAN, package, revision),
            )
        )
    elif package.status is WorkPackageStatus.PAUSED:
        if package.pause_reason is WorkPackagePauseReason.ITEM_BLOCKED:
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
                ("Остановить", _callback(WorkPackageCallbackKind.STOP_PACKAGE, package, revision)),
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
        role=WORK_PACKAGE_PLAN_ROLE,
        html=html,
        callback_buttons=tuple(buttons),
        page=page,
    )


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
