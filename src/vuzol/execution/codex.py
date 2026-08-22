"""Codex process transport over the externally enforced sandbox runtime."""

import asyncio
import contextlib
import hashlib
import json
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import SandboxNetworkMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.diagnostics import (
    build_cli_forensic_diagnostics,
    should_capture_cli_forensics,
    summarize_cli_process,
)
from vuzol.execution.domain import (
    MountMode,
    ProcessEnvelope,
    SandboxMount,
    SandboxSpec,
)
from vuzol.execution.egress import AllowedConnectTarget, compile_proxy_allowlist
from vuzol.execution.finalization import (
    TRUSTED_GATE_COMMANDS,
    TRUSTED_PYTHON_FORMATTER,
    GateExecutionContext,
)
from vuzol.execution.paths import PathViolation, contained, trusted_root
from vuzol.execution.ports import SandboxRuntime
from vuzol.execution.proxy_service import ProxyServiceLease, ProxyServiceManager
from vuzol.project_environment import current_environment
from vuzol.projects.custom_sources import CustomDependencySource
from vuzol.projects.dependencies import (
    DependencyEnvironment,
    inspect_dependency_requests,
    load_dependency_environment,
)
from vuzol.projects.executor_preference import apply_profile_overrides
from vuzol.projects.source_catalog import SourceCatalog
from vuzol.projects.toolchains import ToolchainRuntime, toolchain_runtime
from vuzol.providers.codex import canonical_codex_argv
from vuzol.providers.grok import (
    GROK_DIAGNOSTIC_FILE_MAX_BYTES,
    build_grok_forensic_diagnostics,
    canonical_grok_argv,
    staged_grok_diagnostic_paths,
    summarize_grok_events,
)
from vuzol.providers.kimi import canonical_kimi_argv
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.storage.models import ProjectDependencySource, Step, SupervisedProcess, Worktree
from vuzol.storage.types import ProcessOutcome, ProcessStatus, StepStatus, TerminationStage
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

_ARTIFACT_EXECUTABLES = {
    "python": "/opt/vuzol-validation/bin/python",
    "python3": "/opt/vuzol-validation/bin/python",
    "pytest": "/opt/vuzol-validation/bin/pytest",
    "alembic": "/opt/vuzol-validation/bin/alembic",
    "uv": "/usr/local/bin/uv",
    "make": "/usr/bin/make",
    "node": "/usr/bin/node",
    "npm": "/usr/bin/npm",
    "./gradlew": "/workspace/gradlew",
}


@dataclass(frozen=True, slots=True)
class _DependencyRuntime:
    mounts: tuple[SandboxMount, ...] = ()
    environment: tuple[tuple[str, str], ...] = ()


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
            profile = apply_profile_overrides(
                self._registries.profiles.get(invocation.profile_id),
                dict(step.payload) if isinstance(step.payload, dict) else {},
            )
            _require_provider_command(
                invocation.argv,
                profile.provider,
                profile.model,
                reasoning_effort=profile.model_reasoning_effort,
            )
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
            git_metadata_path = _worktree_git_metadata(worktree_path)
            state_path = profile.state_directory.resolve(strict=True)
            state_target, environment = _provider_state_runtime(profile.provider)
            mounts = [
                SandboxMount(
                    source=worktree_path,
                    target=Path("/workspace"),
                    mode=(
                        MountMode.READ_ONLY
                        if step.step_type == "execute_agent"
                        else MountMode.READ_WRITE
                    ),
                    purpose="task-worktree",
                ),
                SandboxMount(
                    source=git_metadata_path,
                    target=Path("/workspace/.git"),
                    mode=MountMode.READ_ONLY,
                    purpose="worktree-git-metadata",
                ),
            ]
            dependency_runtime = self._dependency_runtime(
                worktree_path,
                project_id=worktree.project_id,
                custom_sources=(
                    await _project_dependency_sources(session, worktree.project_id)
                    if self._settings.dependency_provisioning.enabled is True
                    else ()
                ),
            )
            mounts.extend(dependency_runtime.mounts)
            environment.update(dict(dependency_runtime.environment))
            if profile.provider == "grok":
                staging = (
                    self._artifact_root
                    / "execution"
                    / str(invocation.step_id)
                    / str(invocation.provider_attempt)
                )
                staging.mkdir(parents=True, exist_ok=True)
                contained(self._artifact_root, staging)
                mounts.append(
                    SandboxMount(
                        source=staging,
                        target=Path("/artifacts"),
                        mode=MountMode.READ_WRITE,
                        purpose="task-artifacts",
                    )
                )
            mounts.append(
                SandboxMount(
                    source=state_path,
                    target=state_target,
                    mode=MountMode.READ_WRITE,
                    purpose="provider-state",
                )
            )
            spec = SandboxSpec(
                image=sandbox.image,
                uid=sandbox.uid,
                gid=sandbox.gid,
                seccomp_profile=seccomp_profile,
                seccomp_profile_sha256=seccomp_digest,
                working_directory=Path("/workspace"),
                mounts=tuple(mounts),
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
                runtime_metadata={
                    "configured_deadline_seconds": spec.timeout_seconds,
                    "cancellation_classification": None,
                    "cancellation_initiator": None,
                    "cleanup_initiator": "sandbox_transport_finally",
                },
            )
            session.add(process)
            await session.flush()
            return envelope, process.id

    async def build_gate(
        self,
        context: GateExecutionContext,
        argv: tuple[str, ...],
        *,
        timeout_seconds: int,
    ) -> ProcessEnvelope:
        if argv not in TRUSTED_GATE_COMMANDS.values():
            raise ValueError("gate command is absent from the trusted registry")
        managed_runtime = await self._managed_artifact_runtime(context.worktree_id)
        normalized = (
            _artifact_argv(argv, managed_executables=dict(managed_runtime.executables))
            if argv and argv[0] in {"node", "npm"}
            else argv
        )
        return await self._build_validation_envelope(
            context,
            normalized,
            timeout_seconds,
            managed_runtime=managed_runtime,
        )

    async def build_artifact(
        self,
        request: StepExecutionRequest,
        *,
        worktree_id: uuid.UUID,
        argv: tuple[str, ...],
        timeout_seconds: int,
    ) -> ProcessEnvelope:
        """Build a no-network validation envelope for an approved component command."""

        managed_runtime = await self._managed_artifact_runtime(worktree_id)
        normalized = _artifact_argv(argv, managed_executables=dict(managed_runtime.executables))
        context = GateExecutionContext(
            task_id=request.task_id,
            run_id=request.run_id,
            step_id=request.step_id,
            worktree_id=worktree_id,
            profile_id="artifact-producer",
            provider_attempt=1,
            lease_generation=request.lease.generation,
        )
        return await self._build_validation_envelope(
            context,
            normalized,
            timeout_seconds,
            managed_runtime=managed_runtime,
        )

    async def _managed_artifact_runtime(self, worktree_id: uuid.UUID) -> ToolchainRuntime:
        root = self._settings.capability_provisioning.toolchain_root
        if not root.is_dir():
            return ToolchainRuntime()
        async with self._factory() as session:
            worktree = await session.get(Worktree, worktree_id)
            environment = (
                None
                if worktree is None
                else await current_environment(session, worktree.project_id)
            )
        if worktree is None or environment is None:
            return ToolchainRuntime()
        raw = environment.contract.get("capabilities")
        capability_keys = (
            tuple(
                key
                for key, value in raw.items()
                if isinstance(key, str)
                and isinstance(value, dict)
                and value.get("provisioning", "automatic") == "automatic"
            )
            if isinstance(raw, dict)
            else ()
        )
        return toolchain_runtime(trusted_root(root, create=False), capability_keys)

    async def build_canonicalizer(
        self,
        context: GateExecutionContext,
        paths: tuple[str, ...],
        *,
        timeout_seconds: int,
    ) -> ProcessEnvelope:
        if not paths or len(paths) > 100:
            raise ValueError("canonicalization requires a bounded file set")
        normalized: list[str] = []
        for path in paths:
            candidate = Path(path)
            if (
                candidate.is_absolute()
                or ".." in candidate.parts
                or candidate.suffix != ".py"
                or str(candidate) != path
            ):
                raise ValueError("canonicalization path is unsafe")
            normalized.append(f"/workspace/{path}")
        argv = (*TRUSTED_PYTHON_FORMATTER, *normalized)
        return await self._build_validation_envelope(context, argv, timeout_seconds)

    async def _build_validation_envelope(
        self,
        context: GateExecutionContext,
        argv: tuple[str, ...],
        timeout_seconds: int,
        *,
        managed_runtime: ToolchainRuntime | None = None,
    ) -> ProcessEnvelope:
        async with self._factory() as session:
            worktree = await session.get(Worktree, context.worktree_id)
            step = await session.get(Step, context.step_id)
            if worktree is None or step is None:
                raise LookupError("sandbox worktree or step is missing")
            if (
                step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_generation != context.lease_generation
                or step.run_id != context.run_id
                or worktree.run_id != context.run_id
                or worktree.task_id != context.task_id
            ):
                raise ValueError("gate sandbox is not bound to the current fenced lease")
            project = self._registries.projects.get(worktree.project_id)
            validation_profile = project.validation_sandbox_profile
            if validation_profile is None:
                raise ValueError(f"project {project.id} has no validation sandbox profile")
            sandbox = self._registries.sandboxes.get(validation_profile)
            if not sandbox.enabled:
                raise ValueError("gate sandbox is disabled")
            if sandbox.network_mode is not SandboxNetworkMode.NONE:
                raise ValueError("gate sandbox must disable networking")
            seccomp_profile = self._settings.execution.sandbox_seccomp_profile
            seccomp_digest = self._settings.execution.sandbox_seccomp_profile_sha256
            if seccomp_profile is None or seccomp_digest is None:
                raise ValueError("sandbox seccomp profile is not configured")
            worktree_path = contained(self._worktree_root, Path(worktree.path))
            git_metadata_path = _worktree_git_metadata(worktree_path)
            mounts = [
                SandboxMount(
                    source=worktree_path,
                    target=Path("/workspace"),
                    mode=MountMode.READ_WRITE,
                    purpose="finalizer-worktree",
                ),
                SandboxMount(
                    source=git_metadata_path,
                    target=Path("/workspace/.git"),
                    mode=MountMode.READ_ONLY,
                    purpose="worktree-git-metadata",
                ),
            ]
            environment = {
                "HOME": "/tmp/home",  # noqa: S108 - container-scoped tmpfs
                "CI": "1",
                "PATH": "/opt/vuzol-validation/bin:/usr/local/bin:/usr/bin:/bin",
                "VIRTUAL_ENV": "/opt/vuzol-validation",
                "UV_PROJECT_ENVIRONMENT": "/opt/vuzol-validation",
                "UV_NO_SYNC": "1",
                "UV_OFFLINE": "1",
                "UV_CACHE_DIR": "/tmp/uv-cache",  # noqa: S108
                "PYTHONPATH": "/workspace/src",
                "PYTHONDONTWRITEBYTECODE": "1",
                "COVERAGE_FILE": "/tmp/.coverage",  # noqa: S108
                "RUFF_CACHE_DIR": "/tmp/ruff-cache",  # noqa: S108
                "MYPY_CACHE_DIR": "/tmp/mypy-cache",  # noqa: S108
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "safe.directory",
                "GIT_CONFIG_VALUE_0": "/workspace",
            }
            dependency_runtime = self._dependency_runtime(
                worktree_path,
                project_id=worktree.project_id,
                custom_sources=(
                    await _project_dependency_sources(session, worktree.project_id)
                    if self._settings.dependency_provisioning.enabled is True
                    else ()
                ),
            )
            mounts.extend(dependency_runtime.mounts)
            environment.update(dict(dependency_runtime.environment))
            toolchain_root = self._settings.capability_provisioning.toolchain_root
            if managed_runtime is not None and managed_runtime.executables:
                mounts.append(
                    SandboxMount(
                        source=trusted_root(toolchain_root, create=False),
                        target=Path("/toolchains"),
                        mode=MountMode.READ_ONLY,
                        purpose="approved-capability-toolchains",
                    )
                )
                environment.update(dict(managed_runtime.environment))
                if managed_runtime.path_entries:
                    environment["PATH"] = (
                        ":".join(managed_runtime.path_entries) + ":" + environment["PATH"]
                    )
            spec = SandboxSpec(
                image=sandbox.image,
                uid=sandbox.uid,
                gid=sandbox.gid,
                seccomp_profile=seccomp_profile,
                seccomp_profile_sha256=seccomp_digest,
                working_directory=Path("/workspace"),
                mounts=tuple(mounts),
                cpu_count=sandbox.cpu_count,
                memory_bytes=sandbox.memory_bytes,
                pids_limit=sandbox.pids_limit,
                tmpfs_bytes=sandbox.tmpfs_bytes,
                open_files_limit=sandbox.open_files_limit,
                output_bytes=sandbox.output_bytes,
                timeout_seconds=min(sandbox.timeout_seconds, timeout_seconds),
                stop_grace_seconds=sandbox.stop_grace_seconds,
                network_disabled=True,
                environment=environment,
            )
            return ProcessEnvelope(
                task_id=context.task_id,
                run_id=context.run_id,
                step_id=context.step_id,
                worktree_id=context.worktree_id,
                profile_id=context.profile_id,
                provider_attempt=context.provider_attempt,
                lease_generation=context.lease_generation,
                argv=argv,
                stdin="",
                sandbox=spec,
            )

    def _dependency_runtime(
        self,
        worktree: Path,
        *,
        project_id: str,
        custom_sources: tuple[CustomDependencySource, ...],
    ) -> _DependencyRuntime:
        settings = self._settings.dependency_provisioning
        if settings.enabled is not True:
            return _DependencyRuntime()
        requests = inspect_dependency_requests(
            worktree,
            SourceCatalog.builtin(),
            maximum_direct_dependencies=settings.maximum_direct_dependencies,
            custom_sources=custom_sources,
        )
        environments = tuple(
            dependency_environment
            for request in requests
            if (
                dependency_environment := load_dependency_environment(
                    settings.environment_root, project_id, request
                )
            )
            is not None
        )
        mounts: list[SandboxMount] = []
        environment: dict[str, str] = {}
        for dependency in environments:
            target = Path("/dependencies") / dependency.request.ecosystem
            mounts.append(
                SandboxMount(
                    source=dependency.root,
                    target=target,
                    mode=MountMode.READ_ONLY,
                    purpose="approved-project-dependencies",
                )
            )
            _apply_dependency_environment(environment, dependency, target)
        return _DependencyRuntime(tuple(mounts), tuple(sorted(environment.items())))

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
            argv = _diagnostic_argv(process.command_envelope.get("argv", []))
            provider = _diagnostic_provider(argv)
            generic_summary = summarize_cli_process(
                provider=provider,
                argv=argv,
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.exit_code,
            )
            event_summary: dict[str, object] | None = None
            provider_details: dict[str, object] | None = None
            forensic_artifact = None
            if provider == "grok":
                event_summary, forensic_diagnostics = _collect_grok_process_evidence(
                    result.stdout,
                    self._attempt_staging(process),
                )
                if forensic_diagnostics is not None:
                    try:
                        decoded_details = json.loads(forensic_diagnostics)
                    except (json.JSONDecodeError, TypeError):
                        decoded_details = None
                    if isinstance(decoded_details, dict):
                        provider_details = decoded_details
                stop_reason = event_summary["last_stop_reason"]
                if stop_reason == "Cancelled":
                    signals = generic_summary.get("failure_signals")
                    if isinstance(signals, list) and (
                        "provider_stop_reason:Cancelled" not in signals
                    ):
                        generic_summary["failure_signals"] = [
                            *signals,
                            "provider_stop_reason:Cancelled",
                        ]
            if should_capture_cli_forensics(generic_summary):
                forensic_artifact = await artifacts.persist(
                    session,
                    task_id=process.task_id,
                    run_id=process.run_id,
                    step_id=process.step_id,
                    artifact_type="provider-diagnostics",
                    content=build_cli_forensic_diagnostics(
                        provider=provider,
                        argv=argv,
                        stdout=result.stdout,
                        stderr=result.stderr,
                        exit_code=result.exit_code,
                        provider_details=provider_details,
                    ),
                    media_type="application/json",
                    sensitivity="private",
                    visibility="private",
                    producer_process_id=process.id,
                )
            if event_summary is not None:
                provider_events = await artifacts.persist(
                    session,
                    task_id=process.task_id,
                    run_id=process.run_id,
                    step_id=process.step_id,
                    artifact_type="provider-event-summary",
                    content=(
                        json.dumps(event_summary, sort_keys=True, separators=(",", ":")) + "\n"
                    ).encode(),
                    media_type="application/json",
                    producer_process_id=process.id,
                )
                process.provider_events_artifact_id = provider_events.id
                stop_reason = event_summary["last_stop_reason"]
                metadata = dict(process.runtime_metadata)
                metadata.update(
                    {
                        "actual_elapsed_ms": result.duration_ms,
                        "process_exit_code": result.exit_code,
                        "process_signal": None,
                        "last_provider_event_type": event_summary["last_event_type"],
                        "last_provider_stop_reason": stop_reason,
                        "last_provider_tool_name": event_summary["last_provider_tool_name"],
                        "last_tool_call_id": event_summary["last_tool_call_id"],
                    }
                )
                if stop_reason == "Cancelled":
                    classification = event_summary["cancellation_evidence_category"]
                    metadata.update(
                        {
                            "cancellation_classification": classification,
                            "cancellation_initiator": _cancellation_initiator(classification),
                            "cancellation_stage": event_summary["cancellation_stage"],
                            "cancellation_evidence_completeness": event_summary[
                                "evidence_completeness"
                            ],
                            "cancellation_missing_evidence_reason": event_summary[
                                "missing_evidence_reason"
                            ],
                            "last_native_tool_request_sequence": event_summary[
                                "last_native_tool_request_sequence"
                            ],
                            "last_native_tool_result_sequence": event_summary[
                                "last_native_tool_result_sequence"
                            ],
                            "last_permission_event_sequence": event_summary[
                                "last_permission_event_sequence"
                            ],
                            "last_permission_decision": event_summary["last_permission_decision"],
                        }
                    )
            else:
                metadata = dict(process.runtime_metadata)
                metadata.update(
                    {
                        "actual_elapsed_ms": result.duration_ms,
                        "process_exit_code": result.exit_code,
                        "process_signal": None,
                        "last_provider_event_type": generic_summary["last_event_type"],
                        "last_provider_stop_reason": generic_summary["last_stop_reason"],
                        "last_provider_tool_name": generic_summary["last_provider_tool_name"],
                        "last_tool_call_id": generic_summary["last_tool_call_id"],
                    }
                )
            signals = generic_summary.get("failure_signals")
            if isinstance(signals, list) and signals:
                metadata["provider_diagnostic_failure_signals"] = signals
            if forensic_artifact is not None:
                metadata["provider_diagnostics_artifact_id"] = str(forensic_artifact.id)
            process.runtime_metadata = metadata
            process.exit_code = result.exit_code
            process.outcome = (
                ProcessOutcome.SUCCEEDED if result.exit_code == 0 else ProcessOutcome.FAILED
            )
            process.status = ProcessStatus.EXITED
            process.ended_at = func.now()
            process.reaped_at = func.now()

    def _attempt_staging(self, process: SupervisedProcess) -> Path:
        staging = (
            self._artifact_root / "execution" / str(process.step_id) / str(process.provider_attempt)
        )
        return contained(self._artifact_root, staging)

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
            if process_id is not None:
                # The workflow deadline can cancel this transport at the same instant as
                # the sandbox deadline.  CancelledError is not a RuntimeError, so limiting
                # finalization to RuntimeError leaves an already-cleaned container recorded
                # as running forever.  Any exception after the process row is created means
                # the normal, evidence-backed completion path did not finish; close it as
                # unknown so reconciliation can reason about a terminal process record.
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
            if not run_task.done():
                run_task.cancel()
            if not proxy_task.done():
                proxy_task.cancel()
            await asyncio.gather(run_task, proxy_task, return_exceptions=True)


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


def _apply_dependency_environment(
    environment: dict[str, str], dependency: DependencyEnvironment, target: Path
) -> None:
    if dependency.request.ecosystem == "python":
        library = dependency.root / "venv" / "lib"
        candidates = tuple(
            path
            for path in library.glob("python*/site-packages")
            if path.is_dir() and not path.is_symlink()
        )
        if len(candidates) != 1:
            raise ValueError("approved Python environment has no unique site-packages path")
        relative = candidates[0].relative_to(dependency.root)
        environment["PYTHONPATH"] = f"{target.joinpath(relative)}:/workspace/src"
        return
    if dependency.request.ecosystem == "node":
        modules = target / "node_modules"
        environment["NODE_PATH"] = str(modules)
        environment["PATH"] = (
            f"{modules}/.bin:/opt/vuzol-validation/bin:/usr/local/bin:/usr/bin:/bin"
        )
        return
    raise ValueError("approved dependency ecosystem has no runtime mapping")


async def _project_dependency_sources(
    session: AsyncSession, project_id: str
) -> tuple[CustomDependencySource, ...]:
    rows = tuple(
        (
            await session.scalars(
                select(ProjectDependencySource).where(
                    ProjectDependencySource.project_id == project_id,
                    ProjectDependencySource.revoked_at.is_(None),
                )
            )
        ).all()
    )
    return tuple(
        CustomDependencySource(
            id=row.id,
            project_id=row.project_id,
            ecosystem=row.ecosystem,
            package_name=row.package_name,
            source_kind=row.source_kind,
            source_url=row.source_url,
            source_pin=row.source_pin,
        )
        for row in rows
    )


def _worktree_git_metadata(worktree: Path) -> Path:
    path = worktree / ".git"
    try:
        metadata = path.lstat()
    except OSError as error:
        raise PathViolation("worktree Git metadata is unavailable") from error
    if path.is_symlink() or not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
        raise PathViolation("worktree Git metadata is unsafe")
    return path


def _require_provider_command(
    argv: tuple[str, ...],
    provider: str,
    model: str,
    *,
    reasoning_effort: str | None = None,
) -> None:
    effort = reasoning_effort if isinstance(reasoning_effort, str) else None
    if provider == "codex":
        expected = {
            canonical_codex_argv(model=model, reasoning_effort=effort),
            canonical_codex_argv(model=model, reasoning_effort=effort, read_only=True),
        }
    elif provider == "grok":
        expected = {canonical_grok_argv(model), canonical_grok_argv(model, read_only=True)}
    elif provider == "kimi":
        expected = {
            canonical_kimi_argv(model, reasoning_effort=effort or "low"),
            canonical_kimi_argv(model, reasoning_effort=effort or "low", read_only=True),
        }
    else:
        expected = None
    if expected is None or argv not in expected:
        raise ValueError("sandbox rejected a non-canonical provider command")


def _provider_state_runtime(provider: str) -> tuple[Path, dict[str, str]]:
    if provider == "codex":
        return Path("/codex-home"), {
            "CODEX_HOME": "/codex-home",
            "HOME": "/tmp/home",  # noqa: S108 - container-scoped bounded tmpfs
        }
    if provider == "grok":
        return Path("/grok-home"), {"HOME": "/grok-home"}
    if provider == "kimi":
        return Path("/kimi-home"), {
            "HOME": "/tmp/home",  # noqa: S108 - container-scoped bounded tmpfs
            "KIMI_CODE_HOME": "/kimi-home",
        }
    raise ValueError("sandbox rejected an unsupported CLI provider")


def _diagnostic_argv(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return ()
    return tuple(value)


def _diagnostic_provider(argv: tuple[str, ...]) -> str:
    if not argv:
        return "unknown"
    if argv[0] in {"codex", "grok"}:
        return argv[0]
    if argv[0] == "sh" and len(argv) > 2 and "kimi --model" in argv[2]:
        return "kimi"
    return argv[0]


def _summarize_grok_process(stdout: str, staging: Path) -> dict[str, object]:
    summary, _forensic_diagnostics = _collect_grok_process_evidence(stdout, staging)
    return summary


def _collect_grok_process_evidence(
    stdout: str, staging: Path
) -> tuple[dict[str, object], bytes | None]:
    protocol_summary = summarize_grok_events(stdout)
    session_id = protocol_summary["provider_session_id"]
    if not isinstance(session_id, str):
        return protocol_summary, None
    paths = staged_grok_diagnostic_paths(staging, session_id)
    if paths is None:
        return protocol_summary, None
    try:
        with contextlib.ExitStack() as stack:
            diagnostic_stream = _open_staged_diagnostic(stack, staging, paths[0])
            update_stream = _open_staged_diagnostic(stack, staging, paths[1])
            diagnostic_lines = tuple(diagnostic_stream) if diagnostic_stream is not None else None
            update_lines = tuple(update_stream) if update_stream is not None else None
            summary = summarize_grok_events(
                stdout,
                diagnostic_events=diagnostic_lines,
                session_updates=update_lines,
            )
            forensic = (
                build_grok_forensic_diagnostics(
                    diagnostic_lines or (),
                    update_lines or (),
                )
                if diagnostic_lines is not None or update_lines is not None
                else None
            )
            return summary, forensic
    finally:
        _remove_staged_diagnostics(paths)


def _open_staged_diagnostic(
    stack: contextlib.ExitStack, staging: Path, path: Path
) -> TextIO | None:
    try:
        contained(staging, path)
        path_stat = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(path_stat.st_mode):
            return None
        if path_stat.st_size > GROK_DIAGNOSTIC_FILE_MAX_BYTES:
            return None
        return stack.enter_context(path.open(encoding="utf-8", errors="replace"))
    except (OSError, PathViolation):
        return None


def _remove_staged_diagnostics(paths: tuple[Path, Path]) -> None:
    for path in paths:
        try:
            path_stat = path.lstat()
            if stat.S_ISREG(path_stat.st_mode) or path.is_symlink():
                path.unlink()
        except OSError:
            continue
    for directory in (paths[0].parent, paths[0].parent.parent):
        with contextlib.suppress(OSError):
            directory.rmdir()


def _artifact_argv(
    argv: tuple[str, ...], *, managed_executables: dict[str, str] | None = None
) -> tuple[str, ...]:
    if not argv or len(argv) > 32:
        raise ValueError("artifact command must contain 1..32 arguments")
    executable = managed_executables.get(argv[0]) if managed_executables is not None else None
    if executable is None:
        executable = _ARTIFACT_EXECUTABLES.get(argv[0])
    if executable is None:
        raise ValueError(f"artifact executable is not allowed: {argv[0]}")
    if any(not value or len(value) > 500 or "\x00" in value for value in argv):
        raise ValueError("artifact command contains an unsafe argument")
    return (executable, *argv[1:])


def _cancellation_initiator(classification: object) -> str:
    if classification == "PROVIDER_PERMISSION_CANCELLED":
        return "grok_permission_engine"
    if classification in {"PROVIDER_INTERNAL_CANCELLED", "INVALID_TOOL_INVOCATION"}:
        return "grok_cli"
    return "grok_cli_or_provider"
