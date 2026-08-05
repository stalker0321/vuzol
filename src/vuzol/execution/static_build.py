"""Trusted static-site build in the rootless validation sandbox."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.access import WorktreeAccessLease, WorktreeAccessManager
from vuzol.execution.finalization import GateExecutionContext, GateRunner
from vuzol.execution.git import LocalGit
from vuzol.execution.paths import contained, trusted_root
from vuzol.experiments.domain import RequiredGate
from vuzol.ops.static_publish import StaticPublishError, measure_static_tree
from vuzol.storage.models import Run, Step, Task, Worktree
from vuzol.storage.types import StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class StaticBuildHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        registries: ConfigurationBundle,
        git: LocalGit,
        gates: GateRunner,
        access: WorktreeAccessManager,
        *,
        worktree_root: Path,
    ) -> None:
        self._factory = factory
        self._registries = registries
        self._git = git
        self._gates = gates
        self._access = access
        self._worktree_root = trusted_root(worktree_root, create=False)

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        lease: WorktreeAccessLease | None = None
        try:
            task, worktree, step, path = await self._load(request)
            project_id = task.project_id
            if project_id is None:
                raise LookupError("static build task has no project")
            project = self._registries.projects.get(project_id)
            deployment = project.static_deployment
            if deployment is None or not deployment.enabled:
                return StepOutcome.succeeded({"status": "skipped", "reason": "not_configured"})
            if deployment.build_command is None:
                return StepOutcome(
                    kind=OutcomeKind.BLOCKED,
                    result={},
                    category="static_build_not_configured",
                    summary="static delivery requires a trusted build command",
                )
            sandbox = self._registries.sandboxes.get(project.sandbox_profile)
            lease = await self._access.grant(path, sandbox_uid=sandbox.uid, sandbox_gid=sandbox.gid)
            before = await self._git.inspect(path, worktree.base_commit)
            runs = await self._gates.run(
                path,
                (RequiredGate(name="static-build", command_id=deployment.build_command),),
                timeout_seconds=step.timeout_seconds,
                context=GateExecutionContext(
                    task_id=request.task_id,
                    run_id=request.run_id,
                    step_id=request.step_id,
                    worktree_id=worktree.id,
                    profile_id="static-build",
                    provider_attempt=1,
                    lease_generation=request.lease.generation,
                ),
                cancellation=cancellation,
            )
            gate = runs[0].evidence
            if gate.exit_code != 0:
                return StepOutcome(
                    kind=OutcomeKind.BLOCKED,
                    result={"gate": gate.model_dump(mode="json")},
                    category="static_build_failed",
                    summary="trusted static build command failed",
                )
            after = await self._git.inspect(path, worktree.base_commit)
            if (before.head, before.diff_hash, before.changed_files) != (
                after.head,
                after.diff_hash,
                after.changed_files,
            ):
                raise ValueError("static build changed tracked Git facts")
            source = contained(path, path / deployment.source_directory)
            evidence = measure_static_tree(source, entrypoint=deployment.entrypoint)
            return StepOutcome.succeeded(
                {
                    "status": "built",
                    "source_commit": worktree.result_commit,
                    "source_directory": deployment.source_directory.as_posix(),
                    "entrypoint": deployment.entrypoint.as_posix(),
                    "artifact_hash": evidence.digest,
                    "files": evidence.files,
                    "bytes": evidence.bytes,
                    "gate": gate.model_dump(mode="json"),
                }
            )
        except (LookupError, ValueError, StaticPublishError) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="static_build_invalid",
                summary=str(error)[:500],
            )
        finally:
            if lease is not None:
                await lease.revoke()

    async def _load(self, request: StepExecutionRequest) -> tuple[Task, Worktree, Step, Path]:
        async with self._factory() as session:
            task = await session.get(Task, request.task_id)
            run = await session.get(Run, request.run_id)
            step = await session.get(Step, request.step_id)
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.task_id == request.task_id,
                    Worktree.run_id == request.run_id,
                )
            )
            if (
                task is None
                or task.project_id is None
                or run is None
                or step is None
                or worktree is None
            ):
                raise LookupError("static build state is incomplete")
            if step.status not in {StepStatus.LEASED, StepStatus.RUNNING}:
                raise ValueError("static build step is not active")
            path = contained(self._worktree_root, Path(worktree.path))
            return task, worktree, step, path
