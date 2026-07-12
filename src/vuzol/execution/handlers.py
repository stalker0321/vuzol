"""Workflow handlers for worktree preparation."""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.worktrees import WorktreeService
from vuzol.storage.models import Task
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class PrepareWorktreeHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        registries: ConfigurationBundle,
        worktrees: WorktreeService,
        *,
        owner: str,
    ) -> None:
        self._factory = factory
        self._registries = registries
        self._worktrees = worktrees
        self._owner = owner

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(
                kind=OutcomeKind.CANCELLED,
                result={},
                category="cancelled_before_worktree",
            )
        async with self._factory.begin() as session:
            task = await session.get(Task, request.task_id)
            if task is None or task.project_id is None:
                return StepOutcome(
                    kind=OutcomeKind.PERMANENT_FAILURE,
                    result={},
                    category="project_required",
                )
            reference = await self._worktrees.prepare(
                session,
                task_id=task.id,
                run_id=request.run_id,
                project=self._registries.projects.get(task.project_id),
                owner=self._owner,
            )
        return StepOutcome.succeeded(
            {
                "worktree_id": str(reference.id),
                "base_commit": reference.base_commit,
                "branch": reference.branch,
            }
        )
