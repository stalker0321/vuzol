"""Codex process transport over the externally enforced sandbox runtime."""

import asyncio
import hashlib
import uuid
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import SandboxNetworkMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.domain import (
    MountMode,
    ProcessEnvelope,
    SandboxMount,
    SandboxSpec,
)
from vuzol.execution.egress import AllowedConnectTarget, compile_proxy_allowlist
from vuzol.execution.paths import contained, trusted_root
from vuzol.execution.ports import SandboxRuntime
from vuzol.execution.proxy_service import ProxyServiceLease, ProxyServiceManager
from vuzol.providers.codex import canonical_codex_argv
from vuzol.providers.grok import canonical_grok_argv
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

    async def proxy_targets(self, invocation: CodexInvocation) -> tuple[AllowedConnectTarget, ...]:
        _require_invocation_identity(invocation)
        assert invocation.step_id is not None
        assert invocation.run_id is not None
        assert invocation.task_id is not None
        assert invocation.lease_generation is not None
        assert invocation.profile_id is not None
        assert invocation.sandbox_reference is not None
        async with self._factory() as session:
            worktree_id = uuid.UUID(invocation.sandbox_reference.removeprefix("worktree:"))
            worktree = await session.get(Worktree, worktree_id)
            step = await session.get(Step, invocation.step_id)
            if worktree is None or step is None:
                raise LookupError("sandbox worktree or step is missing")
            _validate_fenced_binding(invocation, worktree, step)
            profile = self._registries.profiles.get(invocation.profile_id)
            project = self._registries.projects.get(worktree.project_id)
            sandbox = self._registries.sandboxes.get(project.sandbox_profile)
            if sandbox.network_mode is SandboxNetworkMode.NONE:
                return ()
            project_targets = compile_proxy_allowlist(project.network)
            profile_targets = compile_proxy_allowlist(profile.runtime_network)
            project_keys = {(target.hostname, target.port) for target in project_targets}
            if any(
                (target.hostname, target.port) not in project_keys for target in profile_targets
            ):
                raise ValueError("CLI profile egress exceeds the project network policy")
            return profile_targets

    async def build(
        self,
        invocation: CodexInvocation,
        *,
        proxy_network: str | None = None,
        https_proxy_url: str | None = None,
    ) -> tuple[ProcessEnvelope, uuid.UUID]:
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
            _validate_fenced_binding(invocation, worktree, step)
            profile = self._registries.profiles.get(invocation.profile_id)
            _require_provider_command(invocation.argv, profile.provider, profile.model)
            project = self._registries.projects.get(worktree.project_id)
            sandbox = self._registries.sandboxes.get(project.sandbox_profile)
            if not sandbox.enabled or profile.state_directory is None:
                raise ValueError("sandbox or CLI profile is disabled")
            seccomp_profile = self._settings.execution.sandbox_seccomp_profile
            seccomp_digest = self._settings.execution.sandbox_seccomp_profile_sha256
            if seccomp_profile is None or seccomp_digest is None:
                raise ValueError("sandbox seccomp profile is not configured")
            networked = sandbox.network_mode is SandboxNetworkMode.HTTPS_PROXY
            if networked != (proxy_network is not None and https_proxy_url is not None):
                raise ValueError("sandbox proxy materialization does not match network policy")
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
            state_target, environment = _provider_state_runtime(profile.provider)
            spec = SandboxSpec(
                image=sandbox.image,
                uid=sandbox.uid,
                gid=sandbox.gid,
                seccomp_profile=seccomp_profile,
                seccomp_profile_sha256=seccomp_digest,
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
                        target=state_target,
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
                network_disabled=not networked,
                proxy_network=proxy_network,
                https_proxy_url=https_proxy_url,
                environment=environment,
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

    async def mark_running(self, process_id: uuid.UUID, container_name: str) -> None:
        async with self._factory.begin() as session:
            process = await session.get(SupervisedProcess, process_id, with_for_update=True)
            if process is not None:
                process.status = ProcessStatus.RUNNING
                process.container_id = container_name
                process.started_at = func.now()

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
        proxy: ProxyServiceManager | None = None,
    ) -> None:
        self._runtime = runtime
        self._envelopes = envelopes
        self._artifacts = artifacts
        self._proxy = proxy

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        targets = await self._envelopes.proxy_targets(invocation)
        proxy_lease: ProxyServiceLease | None = None
        process_id: uuid.UUID | None = None
        primary: BaseException | None = None
        try:
            if targets:
                if self._proxy is None:
                    raise RuntimeError("controlled proxy runtime is unavailable")
                assert invocation.task_id is not None
                assert invocation.run_id is not None
                assert invocation.step_id is not None
                assert invocation.lease_generation is not None
                proxy_lease = await self._proxy.create(
                    invocation.task_id,
                    invocation.run_id,
                    invocation.step_id,
                    invocation.lease_generation,
                    targets,
                )
            envelope, process_id = await self._envelopes.build(
                invocation,
                proxy_network=(proxy_lease.networks.internal_name if proxy_lease else None),
                https_proxy_url=(proxy_lease.proxy_url if proxy_lease else None),
            )
            container_name = f"vuzol-{str(envelope.step_id)[:12]}-{envelope.lease_generation}"
            await self._envelopes.mark_running(process_id, container_name)
            result = await self._run_monitored(envelope, cancellation, proxy_lease)
            await self._envelopes.complete(process_id, result, self._artifacts)
            return result
        except BaseException as error:
            primary = error
            if process_id is not None and isinstance(error, RuntimeError):
                await self._envelopes.fail_unknown(process_id)
            raise
        finally:
            if proxy_lease is not None:
                try:
                    assert self._proxy is not None
                    await self._proxy.cleanup(proxy_lease)
                except BaseException:
                    if primary is None:
                        raise
                    raise RuntimeError(
                        "execution failed and proxy cleanup was incomplete"
                    ) from primary

    async def _run_monitored(
        self,
        envelope: ProcessEnvelope,
        cancellation: CancellationContext,
        proxy_lease: ProxyServiceLease | None,
    ) -> CodexProcessResult:
        run_task = asyncio.create_task(self._runtime.run(envelope, cancellation))
        if proxy_lease is None:
            return await run_task
        assert self._proxy is not None
        proxy_task = asyncio.create_task(self._proxy.wait_until_dead(proxy_lease))
        try:
            done, _pending = await asyncio.wait(
                {run_task, proxy_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if proxy_task in done and run_task not in done:
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
                raise RuntimeError("controlled proxy exited during sandbox execution")
            return await run_task
        finally:
            if not proxy_task.done():
                proxy_task.cancel()
            await asyncio.gather(proxy_task, return_exceptions=True)


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


def _validate_fenced_binding(invocation: CodexInvocation, worktree: Worktree, step: Step) -> None:
    if (
        step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
        or step.lease_generation != invocation.lease_generation
        or worktree.run_id != invocation.run_id
        or worktree.task_id != invocation.task_id
    ):
        raise ValueError("sandbox invocation is not bound to the current fenced lease")


def _require_provider_command(argv: tuple[str, ...], provider: str, model: str) -> None:
    expected = {
        "codex": canonical_codex_argv(),
        "grok": canonical_grok_argv(model),
    }.get(provider)
    if expected is None or argv != expected:
        raise ValueError("sandbox rejected a non-canonical provider command")


def _provider_state_runtime(provider: str) -> tuple[Path, dict[str, str]]:
    if provider == "codex":
        return Path("/codex-home"), {
            "CODEX_HOME": "/codex-home",
            "HOME": "/tmp/home",  # noqa: S108 - container-scoped bounded tmpfs
        }
    if provider == "grok":
        return Path("/grok-home"), {"HOME": "/grok-home"}
    raise ValueError("sandbox rejected an unsupported CLI provider")
