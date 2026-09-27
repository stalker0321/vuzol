"""Transport-neutral task application service (WP09).

Identical user actions over Telegram and CLI/API: commands run against the
domain operations with an explicit principal (no fake chat/user IDs) and a
task-level CAS (stale revision never applies a control). Inspect is strictly
read-only: no locks, no writes, no outbox rows.

Both Telegram ingress (`workflows/controls._apply`, `workflows/dispatch`)
and the operator CLI route through `apply_task_command` here.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.storage.models import Task
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.controls import _locked_context, cancel_task, pause_task, resume_task
from vuzol.workflows.service import start_run

INGRESS_SOURCES = frozenset({"telegram", "cli", "api"})


class TaskCommand(StrEnum):
    START = "start"
    PAUSE = "pause"
    CANCEL = "cancel"
    RESUME = "resume"
    INSPECT = "inspect"


@dataclass(frozen=True, slots=True)
class Principal:
    """Explicit caller identity. Zero IDs are rejected, never defaulted."""

    user_id: int
    ingress_source: str


@dataclass(frozen=True, slots=True)
class TaskCommandResult:
    task_id: uuid.UUID
    version: int
    status: str
    applied: bool


def validate_principal(principal: Principal) -> None:
    if principal.user_id == 0:
        raise ValueError("principal_invalid: refusing fake user ID 0")
    if principal.ingress_source not in INGRESS_SOURCES:
        raise ValueError(f"principal_invalid: unknown ingress {principal.ingress_source}")


def validate_command(command: object) -> TaskCommand:
    try:
        return TaskCommand(command)  # type: ignore[arg-type]
    except ValueError as error:
        raise ValueError(f"unknown task command: {command}") from error


async def _read_version(session: AsyncSession, task_id: uuid.UUID) -> int:
    task = await session.get(Task, task_id)
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    return task.version


async def _start(
    session: AsyncSession,
    task_id: uuid.UUID,
    actor_id: str,
    expected_task_version: int | None,
) -> bool:
    task, run, _ = await _locked_context(session, task_id)
    if expected_task_version is not None and task.version != expected_task_version:
        raise ValueError(f"stale task version: expected {expected_task_version}")
    before = task.version
    await start_run(session, run, task=task, actor_type="user", actor_id=actor_id)
    return task.version != before


async def apply_task_command(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    command: TaskCommand | str,
    principal: Principal,
    expected_task_version: int | None = None,
) -> TaskCommandResult:
    """Session-level core shared by Telegram ingress, dispatch and the CLI."""

    action = validate_command(command)
    validate_principal(principal)
    actor_id = str(principal.user_id)
    if action is TaskCommand.INSPECT:
        task = await session.get(Task, task_id)
        if task is None:
            raise ValueError(f"task not found: {task_id}")
        return TaskCommandResult(
            task_id=task.id,
            version=task.version,
            status=task.status.value,
            applied=False,
        )
    if action is TaskCommand.START:
        applied = await _start(session, task_id, actor_id, expected_task_version)
    elif action is TaskCommand.PAUSE:
        before = await _read_version(session, task_id)
        await pause_task(
            session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
        )
        applied = (await _read_version(session, task_id)) != before
    elif action is TaskCommand.RESUME:
        before = await _read_version(session, task_id)
        await resume_task(
            session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
        )
        applied = (await _read_version(session, task_id)) != before
    else:
        before = await _read_version(session, task_id)
        await cancel_task(
            session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
        )
        applied = (await _read_version(session, task_id)) != before
    task = await session.get(Task, task_id)
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    return TaskCommandResult(
        task_id=task.id,
        version=task.version,
        status=task.status.value,
        applied=applied,
    )


class TaskControlService:
    """Application boundary for task commands; Telegram-free, CLI-ready."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._factory = session_factory

    async def execute(
        self,
        *,
        task_id: uuid.UUID,
        command: TaskCommand | str,
        principal: Principal,
        expected_task_version: int | None = None,
    ) -> TaskCommandResult:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            return await apply_task_command(
                uow.session,
                task_id=task_id,
                command=command,
                principal=principal,
                expected_task_version=expected_task_version,
            )
