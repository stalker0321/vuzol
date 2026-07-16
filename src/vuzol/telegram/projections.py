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

from vuzol.storage.models import (
    Approval,
    Event,
    Run,
    Step,
    Task,
    TelegramMessageLink,
    UsageRecord,
    Worktree,
)
from vuzol.storage.types import ApprovalStatus
from vuzol.workflows.result_approval import verified_envelope

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
    approval_id: uuid.UUID | None = None


def task_title(task: Task) -> str:
    if task.public_task_number is not None:
        return f"Задача №{task.public_task_number}"
    return str(
        task.task_draft.get("normalized_title")
        or task.task_draft.get("title")
        or task.original_text
    ).strip()[:120]


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
    title = task_title(task)
    scope = task.project_id or "personal"
    lines = [
        f"<b>{telegram_html(title)}</b>",
        f"<code>{task.id}</code>",
        f"Scope: {telegram_html(scope)}",
        f"Status: <b>{telegram_html(task.status.value)}</b>",
    ]
    if step is not None:
        lines.append(f"Step: {telegram_html(step.step_type)} ({telegram_html(step.status.value)})")
    approval = None
    if step is not None and step.status.value == "waiting_approval":
        approval = await session.scalar(
            select(Approval).where(
                Approval.step_id == step.id,
                Approval.status == ApprovalStatus.PENDING,
            )
        )
    if run is not None and run.selected_route:
        executor = (
            run.selected_route.get("trusted_profile_id")
            or run.selected_route.get("executor")
            or run.selected_route.get("profile_id")
        )
        if executor:
            lines.append(f"Executor: {telegram_html(executor)}")
        worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
        if worktree is not None and worktree.result_commit and approval is None:
            lines.append(f"Delivery: {telegram_html(worktree.delivery_state.value)}")
        usage = await session.scalar(
            select(UsageRecord)
            .where(UsageRecord.run_id == run.id)
            .order_by(UsageRecord.created_at.desc())
            .limit(1)
        )
        if usage is not None and usage.input_tokens is not None:
            lines.append(
                f"Usage: {telegram_html(usage.input_tokens)} in / "
                f"{telegram_html(usage.output_tokens or 0)} out"
            )
    elapsed = max(0, int((datetime.now(UTC) - task.created_at).total_seconds()))
    lines.append(f"Elapsed: {elapsed}s")
    if event is not None:
        lines.append(f"Latest: {telegram_html(event.event_type)}")
        if event.event_type == "result.redo_requested":
            lines.append("Send a new bounded /sol request with the corrected instructions.")
    if approval is not None and step is not None:
        envelope = verified_envelope(step, approval)
        lines.extend(("", "<b>What was done</b>", telegram_html(approval.human_summary)))
        lines.extend(("", "<b>Checks</b>"))
        for gate in envelope["gates"]:
            duration = int(gate.get("duration_ms", 0)) / 1000
            lines.append(
                f"✅ {telegram_html(gate.get('name', 'check'))} — passed ({duration:.1f}s)"
            )
        lines.extend(("", "Approve this result for safe local apply?"))
        buttons: tuple[str, ...] = ("approve", "redo", "reject")
    else:
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
        approval_id=approval.id if approval is not None else None,
    )


async def build_approval_card(session: AsyncSession, task_id: uuid.UUID) -> StatusCard:
    """Build the global approval projection for the latest exact result."""

    task = await session.get(Task, task_id)
    if task is None:
        raise LookupError(f"task not found: {task_id}")
    approval = await session.scalar(
        select(Approval)
        .join(Step, Approval.step_id == Step.id)
        .join(Run, Step.run_id == Run.id)
        .where(Run.task_id == task_id)
        .order_by(Approval.requested_at.desc())
        .limit(1)
    )
    if approval is None:
        raise LookupError(f"approval not found for task: {task_id}")
    step = await session.get(Step, approval.step_id)
    assert step is not None
    envelope = verified_envelope(step, approval)
    title = task_title(task)
    lines = [
        f"<b>{telegram_html(task.project_id or 'personal')} · {telegram_html(title)}</b>",
        f"<code>{task.id}</code>",
        "",
        "<b>Что сделано</b>",
        telegram_html(approval.human_summary),
        "",
        "<b>Проверки</b>",
    ]
    for gate in envelope["gates"]:
        duration = int(gate.get("duration_ms", 0)) / 1000
        lines.append(f"✅ {telegram_html(gate.get('name', 'check'))} — {duration:.1f}s")
    buttons: tuple[str, ...]
    if approval.status is ApprovalStatus.PENDING:
        lines.extend(("", "Применить этот результат локально?"))
        buttons = ("approve", "redo", "reject")
    else:
        lines.extend(("", f"Решение: <b>{telegram_html(approval.status.value)}</b>"))
        buttons = ()
    return StatusCard(
        task_id=task.id,
        revision=task.version,
        html="\n".join(lines),
        buttons=buttons,
        approval_id=approval.id,
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
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> int: ...

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> None: ...

    async def delete_message(self, *, chat_id: int, message_id: int) -> None: ...


class LostTelegramResponse(RuntimeError):
    """Telegram may have accepted a send, but no message ID was received."""


@dataclass(slots=True)
class FakeTelegramClient:
    fail: Exception | None = None
    next_message_id: int = 1
    sent: list[tuple[int, int | None, str]] = field(default_factory=list, init=False)
    edited: list[tuple[int, int, str]] = field(default_factory=list, init=False)
    deleted: list[tuple[int, int]] = field(default_factory=list, init=False)
    sent_keyboards: list[tuple[tuple[tuple[str, str], ...], ...]] = field(
        default_factory=list, init=False
    )

    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> int:
        del buttons, task_id, approval_id
        if self.fail:
            raise self.fail
        self.sent.append((chat_id, thread_id, html))
        self.sent_keyboards.append(callback_buttons)
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
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> None:
        del buttons, task_id, approval_id, callback_buttons
        if self.fail:
            raise self.fail
        self.edited.append((chat_id, message_id, html))

    async def delete_message(self, *, chat_id: int, message_id: int) -> None:
        if self.fail:
            raise self.fail
        self.deleted.append((chat_id, message_id))


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
            approval_id=card.approval_id,
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
            approval_id=card.approval_id,
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
