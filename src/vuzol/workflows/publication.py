"""Short, locked admission boundary for publisher effects."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.leadership.evidence import new_effect_blocker
from vuzol.storage.models import Run, Step, Task
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
from vuzol.workflows.locking import lock_task_run_step
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class PublicationDenied(RuntimeError):
    """The pending publication no longer has authority."""


@asynccontextmanager
async def publication_authority(
    factory: async_sessionmaker[AsyncSession],
    request: StepExecutionRequest,
    cancellation: CancellationContext,
    *,
    feature_enabled: bool,
    restoring: dict[str, object] | None = None,
) -> AsyncIterator[None]:
    # Goal -> Task -> Run -> Step is also the owner-control lock order.
    async with factory.begin() as session:
        blocker = await new_effect_blocker(
            session,
            request.task_id,
            feature_enabled=feature_enabled,
            restoring_publication=restoring is not None,
        )
        if blocker is not None:
            raise PublicationDenied(blocker)
        locked = await lock_task_run_step(session, request.step_id)
        if locked is None:
            raise PublicationDenied("publication_workflow_missing")
        task, run, step = locked
        if restoring is not None:
            # Recovery of an already published artifact is not a new step lease.
            # Revalidate the exact receipt and newest publication under control locks.
            latest = await session.scalar(
                select(Step.id)
                .join(Run, Run.id == Step.run_id)
                .join(Task, Task.id == Run.task_id)
                .where(
                    Task.project_id == task.project_id,
                    Step.step_type == "publish_preview",
                    Step.status == StepStatus.COMPLETED,
                )
                .order_by(Step.updated_at.desc(), Step.id.desc())
                .limit(1)
            )
            if (
                step.id != latest
                or step.result != restoring
                or step.status is not StepStatus.COMPLETED
                or task.status in {TaskStatus.PAUSED, TaskStatus.CANCELLED, TaskStatus.ROLLED_BACK}
                or run.status in {RunStatus.PAUSED, RunStatus.CANCELLED}
                or task.id != request.task_id
                or run.id != request.run_id
                or cancellation.requested
            ):
                raise PublicationDenied("preview_restore_stale")
            yield
            return
        if (
            task.id != request.task_id
            or run.id != request.run_id
            or run.status is not RunStatus.RUNNING
            or task.status
            not in {TaskStatus.EXECUTING, TaskStatus.VALIDATING, TaskStatus.REVIEWING}
            or step.status is not StepStatus.RUNNING
            or step.lease_owner != request.lease.owner
            or step.lease_generation != request.lease.generation
            or step.lease_expires_at is None
            or step.lease_expires_at <= datetime.now(UTC)
        ):
            raise PublicationDenied("publication_lease_lost")
        if cancellation.requested:
            raise PublicationDenied("cancelled")
        yield
