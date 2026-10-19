"""Lock Task, then Run, then Step so control and recovery cannot deadlock."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Run, Step, Task


async def lock_task_run_step(
    session: AsyncSession, step_id: uuid.UUID
) -> tuple[Task, Run, Step] | None:
    """Lock the workflow triple in parent-first order.

    The locating read takes no locks. Callers re-check lease and status after
    this returns, because the step may have moved while they waited for Task.
    """

    located = (
        await session.execute(
            select(Run.task_id, Step.run_id)
            .join(Run, Run.id == Step.run_id)
            .where(Step.id == step_id)
        )
    ).one_or_none()
    if located is None:
        return None
    task_id, run_id = located
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        return None
    run = await session.scalar(select(Run).where(Run.id == run_id).with_for_update())
    if run is None:
        return None
    step = await session.scalar(select(Step).where(Step.id == step_id).with_for_update())
    if step is None:
        return None
    return task, run, step
