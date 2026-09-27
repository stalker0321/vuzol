"""Transport-neutral task application service (WP09).

Identical user actions over Telegram and CLI/API: commands run against the
domain operations with an explicit principal (no fake chat/user IDs) and a
task-level CAS (stale revision never applies a control). Inspect is strictly
read-only: no locks, no writes, no outbox rows.
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
        action = validate_command(command)
        validate_principal(principal)
        if action is TaskCommand.INSPECT:
            return await self._inspect(task_id)
        actor_id = str(principal.user_id)
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            session = uow.session
            if action is TaskCommand.START:
                applied = await self._start(session, task_id, actor_id, expected_task_version)
            elif action is TaskCommand.PAUSE:
                before = await self._version(session, task_id)
                await pause_task(
                    session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
                )
                applied = await self._changed(session, task_id, before)
            elif action is TaskCommand.RESUME:
                before = await self._version(session, task_id)
                await resume_task(
                    session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
                )
                applied = await self._changed(session, task_id, before)
            else:
                before = await self._version(session, task_id)
                await cancel_task(
                    session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
                )
                applied = await self._changed(session, task_id, before)
            task = await session.get(Task, task_id)
            assert task is not None
            return TaskCommandResult(
                task_id=task.id,
                version=task.version,
                status=task.status.value,
                applied=applied,
            )

    async def _inspect(self, task_id: uuid.UUID) -> TaskCommandResult:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            task = await uow.session.get(Task, task_id)
            assert task is not None
            return TaskCommandResult(
                task_id=task.id,
                version=task.version,
                status=task.status.value,
                applied=False,
            )

    @staticmethod
    async def _version(session: AsyncSession, task_id: uuid.UUID) -> int:
        task = await session.get(Task, task_id)
        assert task is not None
        return task.version

    @staticmethod
    async def _changed(session: AsyncSession, task_id: uuid.UUID, before: int) -> bool:
        return (await TaskControlService._version(session, task_id)) != before

    @staticmethod
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
