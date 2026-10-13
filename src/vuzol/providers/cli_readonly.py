"""Read-only CLI roles over the existing supervised sandbox transport."""

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, RuntimeConfiguration
from vuzol.config.models import LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.config.revision import content_revision
from vuzol.execution.access import RootlessIdentityResolver, WorktreeAccessManager
from vuzol.providers.domain import (
    EffectiveProfileState,
    ProviderErrorCategory,
    ProviderRequest,
    ProviderResult,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import ProviderAdapter
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import STEP_CLAIM_LOCK_KEY
from vuzol.storage.models import Run, Step, Task, Worktree
from vuzol.storage.types import RunStatus, StepStatus, WorktreeDeliveryState
from vuzol.workflows.ports import CancellationContext


class ReadOnlyCliAdapter:
    def __init__(
        self,
        adapter: ProviderAdapter,
        runtime: RuntimeConfiguration,
        factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._adapter = adapter
        self._runtime = runtime
        self._factory = factory
        self._access = WorktreeAccessManager(
            runtime.settings.worktree_root,
            RootlessIdentityResolver(runtime.settings.execution.rootless_docker_socket),
        )

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        if request.role is not ProviderRole.REVIEWER:
            raise ValueError("read-only CLI reviewer required")
        if (
            not profile.enabled
            or profile.provider != "grok"
            or profile.launch_mode is not LaunchMode.CLI
            or ProviderRole.REVIEWER not in profile.roles
        ):
            raise ValueError("enabled Grok CLI reviewer profile required")
        if cancellation.requested:
            raise LeaseLost("read-only call cancelled before launch")
        async with self._factory.begin() as session:
            await session.execute(select(func.pg_advisory_xact_lock(STEP_CLAIM_LOCK_KEY)))
            step = await session.get(Step, request.step_id, with_for_update=True)
            task = await session.get(Task, request.task_id)
            run = await session.get(Run, request.run_id)
            if (
                step is None
                or task is None
                or run is None
                or run.task_id != request.task_id
                or run.status is not RunStatus.RUNNING
                or task.project_id is None
                or step.run_id != request.run_id
                or step.step_type != "review"
                or step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_generation != request.lease_generation
                or step.lease_expires_at is None
                or step.lease_expires_at <= datetime.now(UTC)
            ):
                raise LeaseLost("read-only CLI lease is stale")
            pin = {
                "profile_id": profile.id,
                "profile_revision": content_revision(profile),
            }
            existing_pin = step.payload.get("cli_reviewer_pin")
            if existing_pin is not None and existing_pin != pin:
                raise LeaseLost("CLI reviewer assignment changed during review")
            active = await session.scalar(
                select(func.count())
                .select_from(Step)
                .where(
                    Step.executor_profile_id == profile.id,
                    Step.id != step.id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_expires_at >= func.now(),
                )
            )
            if int(active or 0) >= profile.concurrency_limit:
                raise ProviderFailure(
                    ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                    retryable=True,
                    request_sent=False,
                    safe_summary="selected CLI account is busy",
                )
            step.executor_profile_id = profile.id
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.task_id == task.id, Worktree.run_id == request.run_id
                )
            )
            if (
                worktree is None
                or worktree.project_id != task.project_id
                or worktree.delivery_state is not WorktreeDeliveryState.WORKTREE_RETAINED
                or worktree.result_commit is None
            ):
                raise LeaseLost("CLI reviewer has no retained worktree")
            reference_id, reference_path = worktree.id, Path(worktree.path)
            # The envelope factory reads claim-bound overrides for canonical argv.
            step.payload = {
                **step.payload,
                "cli_reviewer_pin": pin,
                "executor_model_override": profile.model,
                "executor_reasoning_effort": profile.model_reasoning_effort,
            }
        bounded = request.model_copy(
            update={
                "sandbox_reference": f"worktree:{reference_id}",
                "required_capabilities": frozenset({Capability.REPOSITORY_READ}),
            }
        )
        project = self._runtime.registries.projects.get(task.project_id)
        sandbox = self._runtime.registries.sandboxes.get(project.sandbox_profile)
        access = await self._access.grant(
            reference_path,
            sandbox_uid=sandbox.uid,
            sandbox_gid=sandbox.gid,
        )
        try:
            return await self._adapter.execute(bounded, profile, cancellation)
        finally:
            await access.revoke()

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        return await self._adapter.health(profile)
