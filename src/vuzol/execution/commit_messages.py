"""Deterministic, provenance-rich messages for system-owned result commits."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.storage.models import MaterializationLink, PlanRevisionItem, Task

_SPACE = re.compile(r"\s+")
_SCOPE_UNSAFE = re.compile(r"[^a-z0-9-]+")
_HEADER_LIMIT = 72


@dataclass(frozen=True, slots=True)
class CommitMessageContext:
    task_id: uuid.UUID
    run_id: uuid.UUID
    project_id: str
    normalized_title: str
    work_package_id: uuid.UUID | None = None
    plan_revision_id: uuid.UUID | None = None
    plan_ordinal: int | None = None
    plan_item_count: int | None = None


class CommitMessageResolver(Protocol):
    async def resolve(
        self, *, task_id: uuid.UUID, run_id: uuid.UUID, project_id: str
    ) -> str: ...


class DatabaseCommitMessageResolver:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self._factory = factory

    async def resolve(
        self, *, task_id: uuid.UUID, run_id: uuid.UUID, project_id: str
    ) -> str:
        async with self._factory() as session:
            task = await session.get(Task, task_id)
            if task is None or task.project_id != project_id:
                raise LookupError("result commit task context is missing")
            link = await session.scalar(
                select(MaterializationLink).where(MaterializationLink.task_id == task_id)
            )
            item_count: int | None = None
            if link is not None:
                item_count = int(
                    await session.scalar(
                        select(func.count())
                        .select_from(PlanRevisionItem)
                        .where(PlanRevisionItem.plan_revision_id == link.plan_revision_id)
                    )
                    or 0
                )
            return build_commit_message(
                CommitMessageContext(
                    task_id=task_id,
                    run_id=run_id,
                    project_id=project_id,
                    normalized_title=_task_title(task),
                    work_package_id=link.work_package_id if link is not None else None,
                    plan_revision_id=link.plan_revision_id if link is not None else None,
                    plan_ordinal=link.ordinal if link is not None else None,
                    plan_item_count=item_count,
                )
            )


def build_commit_message(context: CommitMessageContext) -> str:
    """Render one bounded header plus machine-readable provenance trailers."""

    scope = _commit_scope(context.project_id)
    prefix = f"task({scope}): "
    title = _one_line(context.normalized_title) or "apply requested changes"
    header = prefix + title[: max(1, _HEADER_LIMIT - len(prefix))].rstrip(" .")
    trailers = [
        f"Vuzol-Task: {context.task_id}",
        f"Vuzol-Run: {context.run_id}",
    ]
    if (
        context.work_package_id is not None
        and context.plan_revision_id is not None
        and context.plan_ordinal is not None
        and context.plan_item_count is not None
        and context.plan_item_count >= context.plan_ordinal
    ):
        trailers.extend(
            (
                f"Vuzol-Work-Package: {context.work_package_id}",
                f"Vuzol-Plan-Revision: {context.plan_revision_id}",
                f"Vuzol-Plan-Item: {context.plan_ordinal}/{context.plan_item_count}",
            )
        )
    return f"{header}\n\n" + "\n".join(trailers)


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


def _commit_scope(project_id: str) -> str:
    normalized = project_id.lower().replace("_", "-")
    return _SCOPE_UNSAFE.sub("-", normalized).strip("-") or "project"
