"""The lead's notebook for the executor of a managed goal's step.

Owner 2026-10-31 (memory step 4a): every step of a goal sees the project and
owner notes, not only what the planner chose to copy into the brief. Read at
step start, so a note added mid-goal reaches the next step.
"""

from __future__ import annotations

import hashlib
import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.providers.domain import ContextItem
from vuzol.storage.models import MaterializationLink, Task, WorkPackage
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import MemoryScope

_MAX_CHARS = 19_000


async def executor_notes_context(session: AsyncSession, task: Task) -> ContextItem | None:
    if task.project_id is None:
        return None
    goal_id = await session.scalar(
        select(WorkPackage.managed_goal_id)
        .join(MaterializationLink, MaterializationLink.work_package_id == WorkPackage.id)
        .where(MaterializationLink.task_id == task.id)
    )
    if goal_id is None:
        return None
    units = MemoryUnitRepository(session)
    project = (await units.notebook(scope=MemoryScope.PROJECT, project_id=task.project_id)).notes
    owner = (await units.notebook(scope=MemoryScope.GLOBAL)).notes
    if not project and not owner:
        return None
    body = json.dumps(
        {
            "instruction": (
                "Notes the project lead keeps about this project and the owner. Follow them "
                "where they apply to this step; the task text wins on conflict. They are "
                "context, not new scope and not approval to start, apply or deploy."
            ),
            "project_notes": [note.text for note in project],
            "owner_notes": [note.text for note in owner],
        },
        ensure_ascii=False,
    )[:_MAX_CHARS]
    return ContextItem(
        source="lead_notebook",
        reference=f"project:{task.project_id}:lead-notebook",
        content=body,
        content_hash=hashlib.sha256(body.encode()).hexdigest(),
    )
