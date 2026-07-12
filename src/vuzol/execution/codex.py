"""Codex process transport over the externally enforced sandbox runtime."""

import hashlib
import uuid
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.domain import (
    MountMode,
    ProcessEnvelope,
    SandboxMount,
    SandboxSpec,
)
from vuzol.execution.paths import contained, trusted_root
from vuzol.execution.ports import SandboxRuntime
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.storage.models import Step, SupervisedProcess, Worktree
from vuzol.storage.types import ProcessOutcome, ProcessStatus, StepStatus, TerminationStage
from vuzol.workflows.ports import CancellationContext


class ExecutionEnvelopeFactory:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        registries: ConfigurationBundle,
    ) -> None:
        self._factory = factory
        self._settings = settings
        self._registries = registries
        self._worktree_root = trusted_root(settings.worktree_root, create=True)
        self._artifact_root = trusted_root(settings.artifact_root, create=True)

    async def build(self, invocation: CodexInvocation) -> tuple[ProcessEnvelope, uuid.UUID]:
        _require_invocation_identity(invocation)
        assert invocation.sandbox_reference is not None
        assert invocation.task_id is not None
        assert invocation.run_id is not None
        assert invocation.step_id is not None
        assert invocation.profile_id is not None
        assert invocation.provider_attempt is not None
        assert invocation.lease_generation is not None
        worktree_id = uuid.UUID(invocation.sandbox_reference.removeprefix("worktree:"))
        async with self._factory.begin() as session:
            worktree = await session.get(Worktree, worktree_id)
            step = await session.get(Step, invocation.step_id)
            if worktree is None or step is None:
                raise LookupError("sandbox worktree or step is missing")
            if (
                step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_generation != invocation.lease_generation
                or worktree.run_id != invocation.run_id
                or worktree.task_id != invocation.task_id
            ):
                raise ValueError("sandbox invocation is not bound to the current fenced lease")
            profile = self._registries.profiles.get(invocation.profile_id)
            project = self._registries.projects.get(worktree.project_id)
            sandbox = self._registries.sandboxes.get(project.sandbox_profile)
            if not sandbox.enabled or profile.state_directory is None:
                raise ValueError("sandbox or CLI profile is disabled")
            worktree_path = contained(self._worktree_root, Path(worktree.path))
            state_path = profile.state_directory.resolve(strict=True)
            staging = (
                self._artifact_root
                / "execution"
                / str(invocation.step_id)
                / str(invocation.provider_attempt)
            )
            staging.mkdir(parents=True, exist_ok=True)
            contained(self._artifact_root, staging)
            spec = SandboxSpec(
                image=sandbox.image,
                uid=sandbox.uid,
                gid=sandbox.gid,
                working_directory=Path("/workspace"),
                mounts=(
                    SandboxMount(
                        source=worktree_path,
                        target=Path("/workspace"),
                        mode=MountMode.READ_WRITE,
                        purpose="task-worktree",
                    ),
                    SandboxMount(
                        source=staging,
                        target=Path("/artifacts"),
                        mode=MountMode.READ_WRITE,
                        purpose="task-artifacts",
                    ),
                    SandboxMount(
                        source=state_path,
                        target=Path("/codex-home"),
                        mode=MountMode.READ_WRITE,
                        purpose="provider-state",
                    ),
                ),
                cpu_count=sandbox.cpu_count,
                memory_bytes=sandbox.memory_bytes,
                pids_limit=sandbox.pids_limit,
                tmpfs_bytes=sandbox.tmpfs_bytes,
                open_files_limit=sandbox.open_files_limit,
                output_bytes=sandbox.output_bytes,
                timeout_seconds=min(sandbox.timeout_seconds, int(invocation.timeout_seconds)),
                stop_grace_seconds=sandbox.stop_grace_seconds,
                network_disabled=sandbox.network_mode.value == "none",
                environment={
                    "CODEX_HOME": "/codex-home",
                    "HOME": "/tmp/home",  # noqa: S108 - container-scoped bounded tmpfs
                },
            )
            envelope = ProcessEnvelope(
                task_id=invocation.task_id,
                run_id=invocation.run_id,
                step_id=invocation.step_id,
                profile_id=invocation.profile_id,
                provider_attempt=invocation.provider_attempt,
                lease_generation=invocation.lease_generation,
                worktree_id=worktree.id,
                argv=invocation.argv,
                stdin=invocation.stdin,
                sandbox=spec,
            )
            idempotency_key = hashlib.sha256(
                f"{invocation.step_id}:{invocation.provider_attempt}".encode()
            ).hexdigest()
            existing = await session.scalar(
                select(SupervisedProcess).where(
                    SupervisedProcess.idempotency_key == idempotency_key
                )
            )
            if existing is not None:
                raise ValueError("supervised process attempt already exists")
            process = SupervisedProcess(
                task_id=invocation.task_id,
                run_id=invocation.run_id,
                step_id=invocation.step_id,
                profile_id=invocation.profile_id,
                provider_attempt=invocation.provider_attempt,
                lease_generation=invocation.lease_generation,
                worktree_id=worktree.id,
                idempotency_key=idempotency_key,
                command_envelope_hash=envelope.stable_hash,
                command_envelope=envelope.redacted,
                sandbox_spec_hash=spec.stable_hash,
                container_runtime="rootless-docker",
                image_digest=sandbox.image,
                working_directory="/workspace",
                status=ProcessStatus.STARTING,
                termination_stage=TerminationStage.NONE,
            )
            session.add(process)
            await session.flush()
            return envelope, process.id

    async def complete(
        self,
        process_id: uuid.UUID,
        result: CodexProcessResult,
        artifacts: ArtifactStore,
    ) -> None:
        async with self._factory.begin() as session:
            process = await session.get(SupervisedProcess, process_id, with_for_update=True)
            if process is None:
                raise LookupError("supervised process disappeared")
            stdout = await artifacts.persist(
                session,
                task_id=process.task_id,
                run_id=process.run_id,
                step_id=process.step_id,
                artifact_type="stdout",
                content=result.stdout.encode(),
                media_type="text/plain",
                producer_process_id=process.id,
            )
            stderr = await artifacts.persist(
                session,
                task_id=process.task_id,
                run_id=process.run_id,
                step_id=process.step_id,
                artifact_type="stderr",
                content=result.stderr.encode(),
                media_type="text/plain",
                producer_process_id=process.id,
            )
            process.stdout_artifact_id = stdout.id
            process.stderr_artifact_id = stderr.id
            process.exit_code = result.exit_code
            process.outcome = (
                ProcessOutcome.SUCCEEDED if result.exit_code == 0 else ProcessOutcome.FAILED
            )
            process.status = ProcessStatus.EXITED
            process.ended_at = func.now()
            process.reaped_at = func.now()

    async def fail_unknown(self, process_id: uuid.UUID) -> None:
        async with self._factory.begin() as session:
            process = await session.get(SupervisedProcess, process_id, with_for_update=True)
            if process is None:
                raise LookupError("supervised process disappeared")
            process.status = ProcessStatus.UNKNOWN
            process.outcome = ProcessOutcome.UNKNOWN
            process.ended_at = func.now()


class SandboxCodexTransport:
    def __init__(
        self,
        runtime: SandboxRuntime,
        envelopes: ExecutionEnvelopeFactory,
        artifacts: ArtifactStore,
    ) -> None:
        self._runtime = runtime
        self._envelopes = envelopes
        self._artifacts = artifacts

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        envelope, process_id = await self._envelopes.build(invocation)
        try:
            result = await self._runtime.run(envelope, cancellation)
        except RuntimeError:
            await self._envelopes.fail_unknown(process_id)
            raise
        await self._envelopes.complete(process_id, result, self._artifacts)
        return result


def _require_invocation_identity(invocation: CodexInvocation) -> None:
    required = (
        invocation.task_id,
        invocation.run_id,
        invocation.step_id,
        invocation.profile_id,
        invocation.provider_attempt,
        invocation.lease_generation,
    )
    if invocation.sandbox_reference is None or any(value is None for value in required):
        raise ValueError("Codex invocation lacks fenced execution identity")
