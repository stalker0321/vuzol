"""Dedicated worktree and rootless sandbox execution worker."""

import asyncio
import os
import signal
import socket
from contextlib import suppress

from vuzol.config import LaunchMode, ScopedSecretResolver, get_runtime_configuration
from vuzol.config.models import SandboxNetworkMode
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.codex import ExecutionEnvelopeFactory, SandboxCodexTransport
from vuzol.execution.finalization import TrustedGateRunner, WorkerFinalizer
from vuzol.execution.git import LocalGit
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.execution.proxy_service import ProxyServiceManager
from vuzol.execution.reconciliation import ProxyStartupReconciler
from vuzol.execution.sandbox import RootlessDockerRuntime, validate_seccomp_profile
from vuzol.execution.worktrees import WorktreeService
from vuzol.observability import configure_logging, get_logger
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.handlers import ProviderStepHandler, executor_provider_handlers
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.ports import ProviderAdapter
from vuzol.providers.registry import AdapterRegistry
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.types import QueueClass
from vuzol.workflows.worker import RoutedWorkflowWorker, WorkflowWorker


class ExecutorChain:
    def __init__(self, worktrees: WorkflowWorker, providers: RoutedWorkflowWorker) -> None:
        self._worktrees = worktrees
        self._providers = providers

    async def process_one(self) -> bool:
        return await self._worktrees.process_one() or await self._providers.process_one()


def main() -> None:
    asyncio.run(run())


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-executor", level=settings.log_level)
    if not settings.execution.enabled:
        raise RuntimeError("execution worker is disabled")
    seccomp_profile = settings.execution.sandbox_seccomp_profile
    seccomp_digest = settings.execution.sandbox_seccomp_profile_sha256
    if seccomp_profile is None or seccomp_digest is None:
        raise RuntimeError("execution worker has no pinned sandbox seccomp profile")
    validate_seccomp_profile(seccomp_profile, seccomp_digest)
    sandbox_runtime = RootlessDockerRuntime(settings.execution.rootless_docker_socket)
    if settings.execution.require_preflight:
        await sandbox_runtime.preflight()
    engine = create_engine(settings, resolve_database_dsn(settings))
    factory = create_session_factory(engine)
    owner = f"{socket.gethostname()}:{os.getpid()}:executor"
    try:
        async with factory.begin() as session:
            await synchronize_profiles(
                session,
                runtime.registries.profiles.items(),
                configuration_revision=runtime.registries.revision,
            )
        resolver = ScopedSecretResolver(
            access_policy={
                profile.credential_reference: frozenset({f"profile:{profile.id}"})
                for profile in runtime.registries.profiles.items()
                if profile.credential_reference is not None
            },
            secret_file_root=settings.secret_file_root,
        )
        artifact_store = ArtifactStore(
            settings.artifact_root,
            max_bytes=settings.limits.artifact_bytes,
            retention_days=settings.retention.artifact_days,
            redaction_patterns=settings.redaction_patterns,
        )
        envelope_factory = ExecutionEnvelopeFactory(factory, settings, runtime.registries)
        networked = any(
            sandbox.enabled and sandbox.network_mode is SandboxNetworkMode.HTTPS_PROXY
            for sandbox in runtime.registries.sandboxes.items()
        )
        if networked and settings.execution.proxy_image is None:
            raise RuntimeError("networked execution requires a pinned proxy image")
        proxy_manager = (
            ProxyServiceManager(
                settings.execution.rootless_docker_socket,
                settings.execution.proxy_runtime_root,
                settings.execution.proxy_image,
            )
            if networked and settings.execution.proxy_image is not None
            else None
        )
        if proxy_manager is not None:
            report = await ProxyStartupReconciler(
                factory,
                proxy_manager,
                owner=owner,
            ).reconcile_startup()
            if not report.lock_acquired:
                get_logger(__name__).warning(
                    "startup reconciliation lock was unavailable; cleanup skipped",
                    extra={"event": "executor.proxy_reconciliation_lock_timeout"},
                )
            if report.removed_count:
                get_logger(__name__).warning(
                    "recovered interrupted controlled-egress executions",
                    extra={
                        "event": "executor.proxy_recovered",
                        "count": report.removed_count,
                    },
                )
        transport = SandboxCodexTransport(
            sandbox_runtime, envelope_factory, artifact_store, proxy_manager
        )
        adapters: dict[str, ProviderAdapter] = {}
        for profile in runtime.registries.profiles.items():
            if not profile.enabled or profile.launch_mode is not LaunchMode.CLI:
                continue
            if profile.provider == "codex":
                adapters[profile.id] = CodexCliAdapter(transport)
            elif profile.provider == "grok":
                adapters[profile.id] = GrokCliAdapter(transport)
        if not adapters:
            raise RuntimeError("execution worker has no enabled CLI profile")
        adapter_registry = AdapterRegistry(runtime.registries.profiles, resolver, adapters=adapters)
        local_git = LocalGit()
        worktree_service = WorktreeService(
            settings.worktree_root,
            local_git,
            retention_days=settings.retention.failed_worktree_days,
        )
        finalizer = WorkerFinalizer(
            local_git,
            gate_runner=TrustedGateRunner(envelope_factory, sandbox_runtime),
            artifacts=artifact_store,
        )
        provider_handler = ProviderStepHandler(
            factory,
            runtime.registries,
            adapter_registry,
            worktrees=worktree_service,
            artifacts=artifact_store,
            finalizer=finalizer,
        )
        worktree_handler = PrepareWorktreeHandler(
            factory,
            runtime.registries,
            worktree_service,
            owner=owner,
        )
        worktree_worker = WorkflowWorker(
            settings,
            factory,
            owner=f"{owner}:worktree",
            handlers={"prepare_worktree": worktree_handler},
            queue_classes=frozenset({QueueClass.HEAVY}),
        )
        provider_worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=runtime.registries,
            owner=f"{owner}:provider",
            handlers=executor_provider_handlers(provider_handler),
            queue_classes=frozenset({QueueClass.HEAVY}),
        )
        await _run_loop(
            ExecutorChain(worktree_worker, provider_worker), settings.workflow.poll_interval_seconds
        )
    finally:
        await engine.dispose()


async def _run_loop(processor: ExecutorChain, poll_interval: float) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signum, stop.set)
    get_logger(__name__).info("executor ready", extra={"event": "executor.ready"})
    while not stop.is_set():
        if not await processor.process_one():
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
    get_logger(__name__).info("executor stopped", extra={"event": "executor.stopped"})


if __name__ == "__main__":
    main()
