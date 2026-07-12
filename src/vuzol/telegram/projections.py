"""Reconstructable and revision-safe Telegram projections."""

import asyncio
import html
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Event, Run, Step, Task, TelegramMessageLink

TELEGRAM_TEXT_LIMIT = 4096


def telegram_html(value: object) -> str:
    """Escape all externally supplied text before using Telegram HTML mode."""

    return html.escape(str(value), quote=True)


def split_message(text: str, limit: int = TELEGRAM_TEXT_LIMIT) -> tuple[str, ...]:
    if limit < 1:
        raise ValueError("message limit must be positive")
    return tuple(text[offset : offset + limit] for offset in range(0, len(text), limit)) or ("",)


@dataclass(frozen=True, slots=True)
class StatusCard:
    task_id: uuid.UUID
    revision: int
    html: str
    buttons: tuple[str, ...] = ()


async def build_status_card(session: AsyncSession, task_id: uuid.UUID) -> StatusCard:
    """Build presentation solely from canonical database state."""

    task = await session.get(Task, task_id)
    if task is None:
        raise LookupError(f"task not found: {task_id}")
    run = await session.scalar(
        select(Run).where(Run.task_id == task_id).order_by(Run.created_at.desc()).limit(1)
    )
    step = None
    if run is not None:
        step = await session.scalar(
            select(Step).where(Step.run_id == run.id).order_by(Step.ordinal.desc()).limit(1)
        )
    event = await session.scalar(
        select(Event).where(Event.entity_id == task_id).order_by(Event.created_at.desc()).limit(1)
    )
    title = str(task.task_draft.get("title") or task.original_text).strip()[:120]
    scope = task.project_id or "personal"
    lines = [
        f"<b>{telegram_html(title)}</b>",
        f"<code>{task.id}</code>",
        f"Scope: {telegram_html(scope)}",
        f"Status: <b>{telegram_html(task.status.value)}</b>",
    ]
    if step is not None:
        lines.append(f"Step: {telegram_html(step.step_type)} ({telegram_html(step.status.value)})")
    if run is not None and run.selected_route:
        executor = run.selected_route.get("executor") or run.selected_route.get("profile_id")
        if executor:
            lines.append(f"Executor: {telegram_html(executor)}")
    elapsed = max(0, int((datetime.now(UTC) - task.created_at).total_seconds()))
    lines.append(f"Elapsed: {elapsed}s")
    if event is not None:
        lines.append(f"Latest: {telegram_html(event.event_type)}")
    buttons = (
        ("start",)
        if run is not None and run.status.value == "created"
        else tuple(status_buttons(task.status.value))
    )
    return StatusCard(
        task_id=task.id,
        revision=task.version,
        html="\n".join(lines),
        buttons=buttons,
    )


class TelegramClient(Protocol):
    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
    ) -> int: ...

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
    ) -> None: ...


class LostTelegramResponse(RuntimeError):
    """Telegram may have accepted a send, but no message ID was received."""


@dataclass(slots=True)
class FakeTelegramClient:
    fail: Exception | None = None
    next_message_id: int = 1
    sent: list[tuple[int, int | None, str]] = field(default_factory=list, init=False)
    edited: list[tuple[int, int, str]] = field(default_factory=list, init=False)

    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
    ) -> int:
        del buttons, task_id
        if self.fail:
            raise self.fail
        self.sent.append((chat_id, thread_id, html))
        message_id = self.next_message_id
        self.next_message_id += 1
        return message_id

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
    ) -> None:
        del buttons, task_id
        if self.fail:
            raise self.fail
        self.edited.append((chat_id, message_id, html))


async def apply_status_projection(
    session: AsyncSession,
    client: TelegramClient,
    *,
    card: StatusCard,
    chat_id: int,
    thread_id: int | None,
) -> bool:
    """Apply only a newer desired revision; persist link after a confirmed send."""

    link = await session.scalar(
        select(TelegramMessageLink)
        .where(
            TelegramMessageLink.task_id == card.task_id,
            TelegramMessageLink.message_role == "task_status",
        )
        .with_for_update()
    )
    if link is not None and card.revision <= link.projection_revision:
        return False
    if link is None:
        message_id = await client.send_message(
            chat_id=chat_id,
            thread_id=thread_id,
            html=card.html,
            buttons=card.buttons,
            task_id=card.task_id,
        )
        session.add(
            TelegramMessageLink(
                chat_id=chat_id,
                message_thread_id=thread_id,
                message_id=message_id,
                task_id=card.task_id,
                message_role="task_status",
                projection_revision=card.revision,
            )
        )
    else:
        await client.edit_message(
            chat_id=chat_id,
            message_id=link.message_id,
            html=card.html,
            buttons=card.buttons,
            task_id=card.task_id,
        )
        link.projection_revision = card.revision
    await session.flush()
    return True


class EditRateLimiter:
    """Per-projection gate; callers naturally coalesce to the latest desired card."""

    def __init__(self, minimum_interval_seconds: float) -> None:
        self._interval = timedelta(seconds=minimum_interval_seconds)
        self._next: dict[uuid.UUID, datetime] = {}
        self._lock = asyncio.Lock()

    async def reserve(self, task_id: uuid.UUID, now: datetime) -> datetime:
        async with self._lock:
            available = max(now, self._next.get(task_id, now))
            self._next[task_id] = available + self._interval
            return available


def status_buttons(status: str) -> Sequence[str]:
    if status == "paused":
        return ("resume", "cancel")
    if status in {
        "received",
        "context_prepared",
        "planned",
        "waiting_approval",
        "executing",
        "validating",
        "reviewing",
        "retrying",
    }:
        return ("pause", "cancel")
    return ()
