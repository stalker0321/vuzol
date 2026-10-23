"""Bounded factual execution snapshot shared by status and read-only chat."""

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.execution.identity import step_execution_identity
from vuzol.storage.models import MaterializationLink, Run, Step, SupervisedProcess, Task


async def execution_snapshot(
    session: AsyncSession, package_id: uuid.UUID | None
) -> list[dict[str, Any]]:
    if package_id is None:
        return []
    tasks = list(
        await session.scalars(
            select(Task)
            .join(MaterializationLink, MaterializationLink.task_id == Task.id)
            .where(MaterializationLink.work_package_id == package_id)
            .order_by(Task.created_at.desc(), Task.id)
            .limit(8)
        )
    )
    result = []
    now = datetime.now(UTC)
    for task in tasks:
        run = await session.scalar(
            select(Run)
            .where(Run.task_id == task.id)
            .order_by(Run.created_at.desc(), Run.id.desc())
            .limit(1)
        )
        step = (
            await session.scalar(
                select(Step)
                .where(
                    Step.run_id == run.id,
                    Step.status.in_(("leased", "running", "blocked", "queued")),
                )
                .order_by(Step.ordinal)
                .limit(1)
            )
            if run
            else None
        )
        process = (
            await session.scalar(
                select(SupervisedProcess)
                .where(SupervisedProcess.step_id == step.id)
                .order_by(SupervisedProcess.created_at.desc())
                .limit(1)
            )
            if step
            else None
        )
        metadata = process.runtime_metadata if process else {}
        identity = await step_execution_identity(session, step) if step else None
        result.append(
            {
                "task_id": str(task.id),
                "task_status": task.status.value,
                "run_id": str(run.id) if run else None,
                "step_id": str(step.id) if step else None,
                "step": step.step_type if step else None,
                "step_status": step.status.value if step else None,
                "failure_category": step.failure_category if step else None,
                "profile": step.executor_profile_id if step else None,
                "account": identity.account_id if identity else None,
                "model": identity.model if identity else None,
                "process_status": process.status.value if process else None,
                "elapsed_seconds": int(
                    ((process.ended_at or now) - process.started_at).total_seconds()
                )
                if process and process.started_at
                else None,
                "last_tool": metadata.get("last_provider_tool_name"),
                "cleanup_confirmed": metadata.get("cleanup_confirmed") is True,
                "runtime_state_known": process is None or process.status.value != "unknown",
            }
        )
    return result
