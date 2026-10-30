"""The lead's memory as the owner sees it in Telegram: /lead_memory and 🗑 buttons.

Owner 2026-10-30 (memory step 4): look at what the lead keeps and drop what is
wrong without asking the lead.
"""

from __future__ import annotations

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.storage.models import MemoryUnit, TelegramMessageLink, TransactionalOutbox
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import MemoryScope, MemoryUnitStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import ControlUpdate, IngressResult, IngressStatus

_NOTE_CHARS = 160
_DECISIONS = 5
_BUTTONS_PER_ROW = 4


def memory_role(chat_id: int, thread_id: int | None, project_id: str) -> str:
    return "lm:" + uuid.uuid5(uuid.NAMESPACE_URL, f"{chat_id}:{thread_id}:{project_id}").hex


async def enqueue_memory_view(
    session: AsyncSession, *, project_id: str, chat_id: int, thread_id: int | None, key: str
) -> None:
    """One message per topic, edited in place after every 🗑."""
    from vuzol.telegram.projections import enqueue_chat_notice

    units = MemoryUnitRepository(session)
    project = (await units.notebook(scope=MemoryScope.PROJECT, project_id=project_id)).notes
    owner = (await units.notebook(scope=MemoryScope.GLOBAL)).notes
    decisions = list(
        await session.scalars(
            select(MemoryUnit)
            .where(
                MemoryUnit.project_id == project_id,
                MemoryUnit.unit_type == "decision_entry",
                MemoryUnit.status == MemoryUnitStatus.OBSERVATION,
            )
            .order_by(MemoryUnit.created_at.desc(), MemoryUnit.id)
            .limit(_DECISIONS)
        )
    )
    decision_count = await session.scalar(
        select(func.count())
        .select_from(MemoryUnit)
        .where(
            MemoryUnit.project_id == project_id,
            MemoryUnit.unit_type == "decision_entry",
            MemoryUnit.status == MemoryUnitStatus.OBSERVATION,
        )
    )
    lines = [f"🧠 Память лида · {project_id}"]
    buttons: list[tuple[str, str]] = []
    number = 0
    for title, notes in (("📌 Заметки проекта", project), ("🌐 Общие заметки", owner)):
        lines.append("")
        lines.append(f"{title}: {len(notes) or 'нет'}")
        for note in notes:
            number += 1
            lines.append(f"{number}. {_short(note.text)}")
            buttons.append((f"🗑 {number}", f"v1:mf:{note.id.hex}"))
    lines.append("")
    lines.append(f"📒 Решений в журнале: {decision_count or 0}")
    lines.extend(f"• {_short(item.text.splitlines()[0])}" for item in decisions)
    if buttons:
        lines.append("")
        lines.append("🗑 N — забыть заметку N. Новое лиду можно сказать: «запомни …».")
    role = memory_role(chat_id, thread_id, project_id)
    # Each render is newer than every earlier one, delivered or still queued.
    renders = await session.scalar(
        select(func.count())
        .select_from(TransactionalOutbox)
        .where(
            TransactionalOutbox.destination == "telegram",
            TransactionalOutbox.payload["projection_role"].astext == role,
        )
    )
    await enqueue_chat_notice(
        session,
        chat_id=chat_id,
        thread_id=thread_id,
        key=key,
        text="\n".join(lines),
        projection_role=role,
        revision=int(renders or 0) + 1,
        callback_buttons=tuple(
            tuple(buttons[index : index + _BUTTONS_PER_ROW])
            for index in range(0, len(buttons), _BUTTONS_PER_ROW)
        ),
    )


async def forget_from_memory_view(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    project_id: str,
    update: ControlUpdate,
) -> IngressResult:
    """Forget one note from the delivered /lead_memory message, then redraw it."""
    if update.note_id is None or update.message_id is None:
        return IngressResult(status=IngressStatus.REJECTED, reason="control_binding_missing")
    role = memory_role(update.chat_id, update.message_thread_id, project_id)
    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        link = await uow.session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.chat_id == update.chat_id,
                TelegramMessageLink.message_thread_id == update.message_thread_id,
                TelegramMessageLink.message_id == update.message_id,
                TelegramMessageLink.message_role == role,
            )
        )
        if link is None:
            return IngressResult(status=IngressStatus.REJECTED, reason="control_binding_missing")
        note = await uow.memory_units.lock_note(update.note_id)
        if (
            note is None
            or note.unit_type != "notebook_note"
            or not (
                (note.scope == MemoryScope.GLOBAL and note.project_id is None)
                or (note.scope == MemoryScope.PROJECT and note.project_id == project_id)
            )
        ):
            return IngressResult(status=IngressStatus.REJECTED, reason="outside_scope")
        changed = await uow.memory_units.retract_note(note)
        await enqueue_memory_view(
            uow.session,
            project_id=project_id,
            chat_id=update.chat_id,
            thread_id=update.message_thread_id,
            key=f"lead:memory:forget:{update.callback_query_id}",
        )
        return IngressResult(
            status=IngressStatus.HANDLED,
            reason="memory_forgotten" if changed else "memory_already_undone",
        )


def _short(text: str) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= _NOTE_CHARS else flat[: _NOTE_CHARS - 1] + "…"
