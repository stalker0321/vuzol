"""Human-readable messages for system-owned result commits."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.storage.models import Task

_SPACE = re.compile(r"\s+")
_COMMIT_TYPES = {
    "create": "feat",
    "fix": "fix",
    "inspect": "chore",
    "explain": "docs",
    "modify": "chore",
    "deploy": "chore",
    "monitor": "chore",
}


@dataclass(frozen=True, slots=True)
class CommitMessageContext:
    task_id: uuid.UUID
    task_number: int | None
    operation: str | None
    normalized_title: str


class CommitMessageResolver(Protocol):
    async def resolve(self, *, task_id: uuid.UUID, run_id: uuid.UUID, project_id: str) -> str: ...


class DatabaseCommitMessageResolver:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self._factory = factory

    async def resolve(self, *, task_id: uuid.UUID, run_id: uuid.UUID, project_id: str) -> str:
        del run_id
        async with self._factory() as session:
            task = await session.get(Task, task_id)
            if task is None or task.project_id != project_id:
                raise LookupError("result commit task context is missing")
            operation = task.task_draft.get("operation")
            return build_commit_message(
                CommitMessageContext(
                    task_id=task_id,
                    task_number=task.topic_task_number,
                    operation=operation if isinstance(operation, str) else None,
                    normalized_title=_task_title(task),
                )
            )


def build_commit_message(context: CommitMessageContext) -> str:
    """Render a complete title and one short reverse link into Vuzol."""

    commit_type = _COMMIT_TYPES.get(context.operation or "", "chore")
    title = _one_line(context.normalized_title) or "apply requested changes"
    task_reference = (
        str(context.task_number) if context.task_number is not None else str(context.task_id)
    )
    return f"{commit_type}: {title}\n\nVuzol-Task: {task_reference}"


def _task_title(task: Task) -> str:
    value = task.task_draft.get("normalized_title")
    if isinstance(value, str) and value.strip():
        return value
    summary = task.task_draft.get("task_summary")
    if isinstance(summary, str) and summary.strip():
        return summary
    return task.original_text


def _one_line(value: str) -> str:
    return _SPACE.sub(" ", value).strip()
