"""Workflow handlers for worktree preparation."""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings
from vuzol.execution.worktrees import WorktreeService
from vuzol.observability import get_logger
from vuzol.ops.disk_pressure import (
    DISK_PRESSURE_CATEGORY,
    DISK_PRESSURE_SUMMARY,
    FreeSpaceProbe,
    assess_heavy_claim_gate,
)
from vuzol.storage.models import Task
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

_LOGGER = get_logger(__name__)


class PrepareWorktreeHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        registries: ConfigurationBundle,
        worktrees: WorktreeService,
        *,
        owner: str,
        settings: Settings | None = None,
        free_space_probe: FreeSpaceProbe | None = None,
    ) -> None:
        self._factory = factory
        self._registries = registries
        self._worktrees = worktrees
        self._owner = owner
        self._settings = settings
        self._free_space_probe = free_space_probe

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(
                kind=OutcomeKind.CANCELLED,
                result={},
                category="cancelled_before_worktree",
            )
        # Last practical re-check before worktree materialization (claim-time race residual).
        assessment = assess_heavy_claim_gate(self._settings, probe=self._free_space_probe)
        if assessment.blocked:
            _LOGGER.warning(
                "worktree preparation deferred due to disk pressure",
                extra={
                    "event": "ops.disk_pressure.deferred",
                    "reason": assessment.reason,
                    "required_bytes": assessment.required_bytes,
                    "free_bytes": assessment.free_bytes,
                    "step_id": str(request.step_id),
                },
            )
            return StepOutcome(
                kind=OutcomeKind.TRANSIENT_FAILURE,
                result={},
                category=DISK_PRESSURE_CATEGORY,
                summary=DISK_PRESSURE_SUMMARY,
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
