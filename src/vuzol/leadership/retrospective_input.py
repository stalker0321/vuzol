"""Bounded historical evidence for a terminal goal, never model instructions."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.feedback import _text
from vuzol.leadership.notebooks import NOTEBOOK_BYTES, note_view
from vuzol.leadership.persistence import ManagedGoal
from vuzol.review.domain import ReviewVerdictKind
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    PlanRevision,
    ReviewOutcomeHistory,
    Run,
    Step,
    Task,
    WorkPackage,
)
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import MemoryScope

MAX_INPUT_BYTES = 40_000
MAX_ROWS = 24


def _size(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))


def _bounded(value: object, *, rows: int = MAX_ROWS, chars: int = 1200, depth: int = 6) -> object:
    if isinstance(value, str):
        return _text(value, chars)
    if isinstance(value, (dict, list, tuple)) and depth <= 0:
        return {"truncated": True}
    if isinstance(value, dict):
        result = {
            str(key)[:100]: _bounded(item, rows=rows, chars=chars, depth=depth - 1)
            for key, item in list(value.items())[:rows]
        }
        if len(value) > rows:
            result["truncated"] = True
        return result
    if isinstance(value, (list, tuple)):
        return [
            _bounded(item, rows=rows, chars=chars, depth=depth - 1) for item in value[:rows]
        ] + ([{"truncated": True}] if len(value) > rows else [])
    return value


def bound_input(context: dict[str, Any]) -> dict[str, Any]:
    """Keep valid JSON and recent evidence; explicitly mark every omitted section."""
    result = {key: _bounded(value) for key, value in context.items()}
    # No single large history may crowd every other kind of evidence out.
    for key, limit in (
        ("reviews", 10_000),
        ("plans", 5000),
        ("results", 4000),
        ("events", 3000),
        ("constraints", 3000),
        ("tasks", 4000),
    ):
        if key not in result:
            continue
        original = result[key]
        for rows, chars in ((12, 600), (4, 240), (1, 120)):
            if _size(result[key]) <= limit:
                break
            result[key] = _bounded(original, rows=rows, chars=chars)
            result["truncated"] = True
    # Notebook texts have their own limits. Keep complete notebooks whenever possible.
    for key in ("project_notebook", "global_notebook"):
        if key in context:
            result[key] = context[key]
    # Lose old step details before review rejections and the notebooks they may revise.
    for key in (
        "results",
        "events",
        "plans",
        "tasks",
        "packages",
        "constraints",
        "goal",
        "project_notebook",
        "global_notebook",
        "reviews",
    ):
        if _size(result) <= MAX_INPUT_BYTES:
            break
        if key not in result:
            continue
        result["truncated"] = True
        original = result[key]
        for rows, chars in ((12, 600), (4, 240), (1, 120)):
            result[key] = _bounded(original, rows=rows, chars=chars)
            if _size(result) <= MAX_INPUT_BYTES:
                break
        if _size(result) > MAX_INPUT_BYTES:
            result[key] = {"truncated": True}
    assert _size(result) <= MAX_INPUT_BYTES
    return result


async def retrospective_input(
    session: AsyncSession, goal: ManagedGoal, *, outcome: str, reason: str | None
) -> dict[str, Any]:
    packages = select(WorkPackage.id).where(
        WorkPackage.managed_goal_id == goal.id, WorkPackage.project_id == goal.project_id
    )
    tasks = select(MaterializationLink.task_id).where(
        MaterializationLink.work_package_id.in_(packages)
    )
    context: dict[str, Any] = {
        "goal": {
            "id": str(goal.id),
            "project_id": goal.project_id,
            "request": goal.original_request,
            "scope": goal.scope_text,
            "criteria": goal.criteria,
            "scope_revision": goal.scope_revision,
        },
        "outcome": {
            "status": outcome,
            "reason": _text(reason),
            "acceptance": goal.constraints.get("owner_acceptance"),
            "integration_branch": goal.integration_branch,
        },
        "constraints": goal.constraints,
        "notebook_budgets": {scope.value: size for scope, size in NOTEBOOK_BYTES.items()},
        "truncated": False,
    }
    units = MemoryUnitRepository(session)
    for scope in (MemoryScope.PROJECT, MemoryScope.GLOBAL):
        notebook = await units.notebook(
            scope=scope, project_id=goal.project_id if scope == MemoryScope.PROJECT else None
        )
        context[f"{scope.value}_notebook"] = {
            "text_bytes": notebook.text_bytes,
            "notes": [note_view(note) for note in reversed(notebook.notes)],
        }
    rows = list(
        await session.scalars(
            select(WorkPackage)
            .where(WorkPackage.id.in_(packages))
            .order_by(WorkPackage.created_at.desc(), WorkPackage.id.desc())
            .limit(MAX_ROWS + 1)
        )
    )
    context["packages"] = [
        {
            "id": str(row.id),
            "title": row.title,
            "status": row.status.value,
            "branch": row.integration_branch,
            "result_commit": row.integration_head_commit,
        }
        for row in rows
    ]
    revisions = await session.scalars(
        select(PlanRevision)
        .where(PlanRevision.work_package_id.in_(packages))
        .order_by(PlanRevision.created_at.desc(), PlanRevision.id.desc())
        .limit(MAX_ROWS + 1)
    )
    context["plans"] = [
        {
            "id": str(row.id),
            "package_id": str(row.work_package_id),
            "revision": row.revision_number,
            "parent_revision_id": str(row.parent_revision_id) if row.parent_revision_id else None,
            "state": row.state.value,
            "plan": row.immutable_body,
        }
        for row in revisions
    ]
    task_rows = await session.execute(
        select(Task, MaterializationLink)
        .join(MaterializationLink, MaterializationLink.task_id == Task.id)
        .where(
            MaterializationLink.work_package_id.in_(packages), Task.project_id == goal.project_id
        )
        .order_by(MaterializationLink.materialized_at.desc(), Task.id.desc())
        .limit(MAX_ROWS + 1)
    )
    context["tasks"] = [
        {
            "id": str(task.id),
            "package_id": str(link.work_package_id),
            "ordinal": link.ordinal,
            "status": task.status.value,
            "request": task.original_text,
            "draft": task.task_draft,
        }
        for task, link in task_rows
    ]
    reviews = await session.scalars(
        select(ReviewOutcomeHistory)
        .where(ReviewOutcomeHistory.task_id.in_(tasks))
        # Rejections survive even when many later successful reviews exist.
        .order_by(
            ReviewOutcomeHistory.verdict.in_(
                (ReviewVerdictKind.PASSED.value, ReviewVerdictKind.PASSED_WITH_WARNINGS.value)
            ),
            ReviewOutcomeHistory.created_at.desc(),
            ReviewOutcomeHistory.id.desc(),
        )
        .limit(MAX_ROWS + 1)
    )
    context["reviews"] = [
        {
            "task_id": str(row.task_id),
            "verdict": row.verdict,
            "summary": row.summary,
            "findings": row.findings,
            "result_commit": row.result_commit,
        }
        for row in reviews
    ]
    results = await session.execute(
        select(Step, Run.task_id)
        .join(Run, Run.id == Step.run_id)
        .where(Run.task_id.in_(tasks), Step.result.is_not(None))
        .order_by(Step.updated_at.desc(), Step.id.desc())
        .limit(MAX_ROWS + 1)
    )
    context["results"] = [
        {
            "task_id": str(task_id),
            "step": row.step_type,
            "status": row.status.value,
            "result": row.result,
        }
        for row, task_id in results
    ]
    events = await session.scalars(
        select(Event)
        .where(Event.entity_type == "managed_goal", Event.entity_id == goal.id)
        .order_by(Event.created_at.desc(), Event.id.desc())
        .limit(MAX_ROWS + 1)
    )
    context["events"] = [
        {"event": row.event_type, "actor": row.actor_type, "payload": row.payload} for row in events
    ]
    return bound_input(context)
