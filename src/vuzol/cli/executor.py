"""Dedicated worktree and rootless sandbox execution worker."""

import asyncio
import os
import signal
import socket
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import timedelta
from pathlib import Path
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import LaunchMode, ScopedSecretResolver, get_runtime_configuration
from vuzol.config.models import SandboxNetworkMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import SubscriptionLimitSettings
from vuzol.execution.access import RootlessIdentityResolver, WorktreeAccessManager
from vuzol.execution.artifact_production import (
    ArtifactProductionHandler,
    SandboxedArtifactCommandRunner,
)
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.codex import ExecutionEnvelopeFactory, SandboxCodexTransport
from vuzol.execution.dependency_build import SandboxedDependencyBuilder
from vuzol.execution.domain import ProcessEnvelope, SandboxSpec
from vuzol.execution.finalization import TrustedGateRunner, WorkerFinalizer
from vuzol.execution.git import LocalGit
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.execution.journal import ExecutionJournal
from vuzol.execution.proxy_service import ProxyServiceManager
from vuzol.execution.reconciliation import ProxyStartupReconciler, TurnStartupReconciler
from vuzol.execution.result_validation import ResultValidationHandler
from vuzol.execution.runtime_contract import AgentCertificateStore
from vuzol.execution.sandbox import RootlessDockerRuntime, validate_seccomp_profile
from vuzol.execution.static_build import StaticBuildHandler
from vuzol.execution.worktrees import WorktreeService
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.observability import configure_logging, get_logger
from vuzol.projects.capability_provisioning import (
    CapabilityProvisioningHandler,
    OfflineCapabilityInstaller,
)
from vuzol.projects.dependency_provisioning import DependencyProvisioningHandler
from vuzol.providers.catalog import cli_adapter, credential_access_policy, supported_profile
from vuzol.providers.cli_readonly import ReadOnlyCliAdapter
from vuzol.providers.handlers import ProviderStepHandler, executor_provider_handlers
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.ports import ProviderAdapter
from vuzol.providers.registry import AdapterRegistry
from vuzol.providers.role_cli import RoleBoundCliAdapter
from vuzol.review import ResultReviewHandler
from vuzol.review.independent import DatabaseReviewAccounting, IndependentModelReviewer
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.storage.types import QueueClass
from vuzol.workflows.ports import CancellationContext, StepHandler
from vuzol.workflows.worker import RoutedWorkflowWorker, WorkflowWorker

VALIDATION_IMAGE_PREFLIGHT_COMMANDS: tuple[tuple[str, ...], ...] = (
    ("/usr/bin/make", "--version"),
    ("/usr/bin/git", "--version"),
    ("python", "--version"),
    ("uv", "--version"),
)


class ExecutorLane(Protocol):
    async def process_one(self) -> bool: ...


class ExecutorChain:
    def __init__(
        self,
        worktrees: WorkflowWorker,
        providers: RoutedWorkflowWorker,
        reviews: WorkflowWorker | None = None,
        lead: ProjectLeadWorker | None = None,
        recovery: Callable[[], Awaitable[None]] | None = None,
        *,
        control: ExecutorLane | None = None,
        control_worktrees: ExecutorLane | None = None,
        provider_parallelism: int = 1,
    ) -> None:
        self._worktrees = worktrees
        self._providers = providers
        self._reviews = reviews
        self._lead = lead
        self._recovery = recovery
        self._next_recovery = time.monotonic() + 30
        self._control = control
        self._control_worktrees = control_worktrees
        self._provider_parallelism = provider_parallelism

    def lanes(self) -> tuple[ExecutorLane, ...]:
        """Long provider turns must not occupy the lead/control event loop."""
        return tuple(
            worker
            for worker in (
                self._lead,
                self._reviews,
                self._control_worktrees,
                self._control,
                self._worktrees,
                *([self._providers] * self._provider_parallelism),
            )
            if worker is not None
        )

    async def recover_if_due(self) -> None:
        if self._recovery is not None and time.monotonic() >= self._next_recovery:
            await self._recovery()
            self._next_recovery = time.monotonic() + 30

    async def process_one(self) -> bool:
        if self._recovery is not None and time.monotonic() >= self._next_recovery:
            await self._recovery()
            self._next_recovery = time.monotonic() + 30
        if self._lead is not None and await self._lead.process_one():
            return True
        if self._reviews is not None and await self._reviews.process_one():
            return True
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
    journal = (
        ExecutionJournal(settings.execution.journal_root, settings.execution.journal_key_file)
        if settings.execution.journal_key_file is not None
        else None
    )
    sandbox_runtime = RootlessDockerRuntime(
        settings.execution.rootless_docker_socket, journal=journal
    )
    if settings.execution.require_preflight:
        await sandbox_runtime.preflight()
        await _preflight_validation_images(
            sandbox_runtime,
            runtime.registries,
            seccomp_profile=seccomp_profile,
            seccomp_digest=seccomp_digest,
        )
        await _preflight_agent_contracts(
            sandbox_runtime,
            runtime.registries,
            seccomp_profile=seccomp_profile,
            seccomp_digest=seccomp_digest,
        )
    identity_resolver = RootlessIdentityResolver(settings.execution.rootless_docker_socket)
    worktree_access = WorktreeAccessManager(settings.worktree_root, identity_resolver)
    sandbox_identities = tuple(
        sorted(
            {
                (sandbox.uid, sandbox.gid)
                for sandbox in runtime.registries.sandboxes.items()
                if sandbox.enabled
            }
        )
    )
    await worktree_access.preflight(sandbox_identities)
    dependency_access = WorktreeAccessManager(
        settings.dependency_provisioning.environment_root, identity_resolver
    )
    if settings.dependency_provisioning.enabled:
        await dependency_access.preflight(sandbox_identities)
    engine = create_engine(settings, resolve_database_dsn(settings))
    try:
        # S-2.1: fail closed before profile sync, proxy reconcile, ready, or claim loops.
        await require_migration_head(engine)
        factory = create_session_factory(engine)
        owner = f"{socket.gethostname()}:{os.getpid()}:executor"
        async with factory.begin() as session:
            await synchronize_profiles(
                session,
                runtime.registries.profiles.items(),
                configuration_revision=runtime.registries.revision,
            )
        resolver = ScopedSecretResolver(
            access_policy=credential_access_policy(runtime.registries.profiles.items()),
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
        if settings.dependency_provisioning.enabled and proxy_manager is None:
            raise RuntimeError("dependency provisioning requires the controlled proxy runtime")
        turn_reconciler = TurnStartupReconciler(
            factory,
            sandbox_runtime,
            proxy_manager,
            owner=owner,
            enabled=lambda: settings.project_lead.enabled,
        )
        await turn_reconciler.reconcile_startup()
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
        from vuzol.execution.turn_transport import TurnSandboxTransport

        turn_transport = TurnSandboxTransport(
            sandbox_runtime, factory, settings, runtime.registries, proxy_manager
        )
        adapters: dict[str, ProviderAdapter] = {}
        for profile in runtime.registries.profiles.items():
            if (
                not profile.enabled
                or profile.launch_mode is not LaunchMode.CLI
                or not supported_profile(profile)
            ):
                continue
            base = cli_adapter(profile, transport)
            adapters[profile.id] = RoleBoundCliAdapter(
                base,
                ReadOnlyCliAdapter(base, runtime, factory),
                cli_adapter(profile, turn_transport),
            )
        if not adapters:
            raise RuntimeError("execution worker has no enabled CLI profile")
        adapter_registry = AdapterRegistry(
            runtime.registries.profiles, resolver, adapters=adapters, session_factory=factory
        )
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
            worktree_access=worktree_access,
            agent_certificates=AgentCertificateStore(settings.artifact_root / "agent-certificates"),
            redaction_patterns=settings.redaction_patterns,
            dependency_provisioning_enabled=settings.dependency_provisioning.enabled,
        )
        worktree_handler = PrepareWorktreeHandler(
            factory,
            runtime.registries,
            worktree_service,
            owner=owner,
            settings=settings,
        )
        validation_handler = ResultValidationHandler(
            factory,
            runtime.registries,
            local_git,
            worktree_root=settings.worktree_root,
            gate_runner=TrustedGateRunner(envelope_factory, sandbox_runtime),
            worktree_access=worktree_access,
            artifacts=artifact_store,
        )
        static_build_handler = StaticBuildHandler(
            factory,
            runtime.registries,
            local_git,
            TrustedGateRunner(envelope_factory, sandbox_runtime),
            worktree_access,
            worktree_root=settings.worktree_root,
        )
        artifact_handler = ArtifactProductionHandler(
            factory,
            runtime.registries,
            local_git,
            worktree_access,
            SandboxedArtifactCommandRunner(envelope_factory, sandbox_runtime),
            artifact_store,
            worktree_root=settings.worktree_root,
            delivery_uid=settings.artifact_delivery_uid,
        )
        dependency_builder = (
            None
            if proxy_manager is None
            else SandboxedDependencyBuilder(
                settings,
                runtime.registries,
                sandbox_runtime,
                proxy_manager,
                dependency_access,
                factory=factory,
            )
        )
        dependency_handler = DependencyProvisioningHandler(
            factory,
            settings.dependency_provisioning,
            dependency_builder,
            worktree_root=settings.worktree_root,
            capabilities=CapabilityProvisioningHandler(
                factory,
                OfflineCapabilityInstaller(settings.capability_provisioning),
                registries=runtime.registries,
                worktree_root=settings.worktree_root,
            ),
        )
        worktree_handlers: dict[str, StepHandler] = {
            "prepare_worktree": worktree_handler,
            "ensure_dependencies": dependency_handler,
            "validate": validation_handler,
            "produce_artifacts": artifact_handler,
            "build_static": static_build_handler,
        }
        worktree_worker = WorkflowWorker(
            settings,
            factory,
            owner=f"{owner}:worktree",
            handlers=worktree_handlers,
            queue_classes=frozenset({QueueClass.HEAVY}),
        )
        provider_worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=runtime.registries,
            owner=f"{owner}:provider",
            handlers={**executor_provider_handlers(provider_handler), "plan": provider_handler},
            queue_classes=frozenset({QueueClass.HEAVY}),
        )
        control_worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=runtime.registries,
            owner=f"{owner}:conversation",
            handlers={**executor_provider_handlers(provider_handler), "plan": provider_handler},
            queue_classes=frozenset({QueueClass.LIGHT}),
        )
        control_worktree_worker = WorkflowWorker(
            settings,
            factory,
            owner=f"{owner}:conversation-worktree",
            handlers={"prepare_worktree": worktree_handler},
            queue_classes=frozenset({QueueClass.LIGHT}),
        )
        reviewer_id = settings.execution.reviewer_profile_id
        reviewer = IndependentModelReviewer(
            runtime.registries,
            adapter_registry,
            DatabaseReviewAccounting(factory, settings.limits),
            policy_revision=runtime.registries.revision,
            profile_id=reviewer_id,
            input_token_limit=settings.limits.provider_call_input_tokens,
            session_factory=factory,
        )
        review_worker = WorkflowWorker(
            settings,
            factory,
            owner=f"{owner}:review",
            handlers={
                "review": ResultReviewHandler(
                    factory,
                    local_git,
                    worktree_root=settings.worktree_root,
                    independent_reviewer=reviewer,
                )
            },
            queue_classes=frozenset({QueueClass.LIGHT}),
            profile_limits={p.id: p.concurrency_limit for p in runtime.registries.profiles.items()},
        )
        lead_worker = (
            build_project_lead_worker(
                factory,
                settings=settings,
                profiles=runtime.registries.profiles,
                owner=f"{owner}:project-lead",
                default_branches={
                    p.id: p.default_branch for p in runtime.registries.projects.items()
                },
                adapter_registry=adapter_registry,
            )
            if settings.project_lead.enabled
            else None
        )

        async def recover_cli_resources() -> None:
            await turn_reconciler.reconcile_startup()
            if proxy_manager is not None:
                await ProxyStartupReconciler(
                    factory, proxy_manager, owner=owner
                ).reconcile_startup()

        await _run_loop(
            ExecutorChain(
                worktree_worker,
                provider_worker,
                review_worker,
                lead_worker,
                recover_cli_resources,
                control=control_worker,
                control_worktrees=control_worktree_worker,
                provider_parallelism=settings.execution.provider_parallelism,
            ),
            settings.workflow.poll_interval_seconds,
            session_factory=factory,
            registries=runtime.registries,
            subscription_limit_settings=settings.subscription_limits,
        )
    finally:
        await engine.dispose()


async def _refresh_subscription_limits_tick(
    session_factory: async_sessionmaker[AsyncSession],
    registries: object,
    *,
    due_periodic: bool,
    settings: SubscriptionLimitSettings,
) -> bool:
    """Claim optional /update refresh requests, collect host limits, refresh dashboards.

    Returns True when a forced refresh request was claimed (so the loop can skip sleep).
    """

    from sqlalchemy import select

    from vuzol.providers.subscription_limits import (
        SUBSCRIPTION_LIMITS_DESTINATION,
        refresh_and_store_subscription_limits,
    )
    from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item
    from vuzol.storage.models import TopicMapping, TransactionalOutbox
    from vuzol.telegram.projections import enqueue_project_status_dashboard

    try:
        async with session_factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner="vuzol-executor-limits",
                lease_seconds=60,
                allowed_destinations=frozenset({SUBSCRIPTION_LIMITS_DESTINATION}),
            )
            force_chat_ids: list[int] = []
            if token is not None:
                item = await session.get(TransactionalOutbox, token.item_id)
                if item is not None:
                    raw_chat = item.payload.get("chat_id")
                    if raw_chat is not None:
                        force_chat_ids.append(int(raw_chat))
            if token is None and not due_periodic:
                return False
            profiles = registries.profiles.items()  # type: ignore[attr-defined]
            await refresh_and_store_subscription_limits(
                session,
                profiles,
                grok_limit_source=settings.source,
                grok_limit_snapshot_file=settings.snapshot_file,
                grok_limit_snapshot_max_age=timedelta(seconds=settings.snapshot_max_age_seconds),
                opencode_snapshots=settings.opencode_snapshots,
            )
            if force_chat_ids:
                chats: list[int] = force_chat_ids
            else:
                chats = list(
                    (
                        await session.scalars(
                            select(TopicMapping.chat_id)
                            .where(
                                TopicMapping.topic_kind == "task_dashboard",
                                TopicMapping.enabled.is_(True),
                            )
                            .distinct()
                        )
                    ).all()
                )
            for chat_id in chats:
                await enqueue_project_status_dashboard(session, int(chat_id))
            if token is not None:
                await complete_outbox_item(session, token)
            return token is not None
    except Exception as error:  # pragma: no cover - operational best-effort
        get_logger(__name__).warning(
            "subscription limit refresh failed",
            extra={
                "event": "executor.subscription_limits_failed",
                "error_type": type(error).__name__,
            },
        )
        return False


async def _run_loop(
    processor: ExecutorChain,
    poll_interval: float,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    registries: object | None = None,
    subscription_limit_settings: SubscriptionLimitSettings | None = None,
) -> None:
    if (
        session_factory is not None
        and registries is not None
        and subscription_limit_settings is None
    ):
        raise ValueError("subscription limit settings are required when limit refresh is enabled")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signum, stop.set)
    get_logger(__name__).info("executor ready", extra={"event": "executor.ready"})
    limit_refresh_seconds = 120.0
    last_limit_refresh = -limit_refresh_seconds  # refresh once on startup

    async def run_lane(lane: ExecutorLane) -> None:
        while not stop.is_set():
            worked = await lane.process_one()
            if not worked:
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=poll_interval)

    lanes = [asyncio.create_task(run_lane(lane)) for lane in processor.lanes()]
    try:
        while not stop.is_set():
            for task in lanes:
                if task.done():
                    task.result()  # A failed lane must fail visibly, not silently disappear.
            await processor.recover_if_due()
            now = time.monotonic()
            due_periodic = now - last_limit_refresh >= limit_refresh_seconds
            if session_factory is not None and registries is not None:
                assert subscription_limit_settings is not None
                await _refresh_subscription_limits_tick(
                    session_factory,
                    registries,
                    due_periodic=due_periodic,
                    settings=subscription_limit_settings,
                )
                if due_periodic:
                    last_limit_refresh = now
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
    finally:
        for task in lanes:
            task.cancel()
        await asyncio.gather(*lanes, return_exceptions=True)
    get_logger(__name__).info("executor stopped", extra={"event": "executor.stopped"})


async def _preflight_validation_images(
    runtime: RootlessDockerRuntime,
    registries: ConfigurationBundle,
    *,
    seccomp_profile: Path,
    seccomp_digest: str,
) -> None:
    profile_ids = {
        project.validation_sandbox_profile
        for project in registries.projects.items()
        if project.enabled and project.validation_sandbox_profile is not None
    }
    for profile_id in sorted(profile_ids):
        sandbox = registries.sandboxes.get(profile_id)
        for argv in VALIDATION_IMAGE_PREFLIGHT_COMMANDS:
            identity = uuid.uuid4()
            envelope = ProcessEnvelope(
                task_id=identity,
                run_id=uuid.uuid4(),
                step_id=uuid.uuid4(),
                worktree_id=uuid.uuid4(),
                profile_id="validation-preflight",
                provider_attempt=1,
                lease_generation=1,
                argv=argv,
                stdin="",
                sandbox=SandboxSpec(
                    image=sandbox.image,
                    uid=sandbox.uid,
                    gid=sandbox.gid,
                    seccomp_profile=seccomp_profile,
                    seccomp_profile_sha256=seccomp_digest,
                    working_directory=Path("/workspace"),
                    mounts=(),
                    cpu_count=sandbox.cpu_count,
                    memory_bytes=sandbox.memory_bytes,
                    pids_limit=sandbox.pids_limit,
                    tmpfs_bytes=sandbox.tmpfs_bytes,
                    open_files_limit=sandbox.open_files_limit,
                    output_bytes=sandbox.output_bytes,
                    timeout_seconds=min(sandbox.timeout_seconds, 60),
                    stop_grace_seconds=sandbox.stop_grace_seconds,
                    network_disabled=True,
                    environment={
                        "HOME": "/tmp/home",  # noqa: S108
                        "PATH": "/opt/vuzol-validation/bin:/usr/local/bin:/usr/bin:/bin",
                        "UV_NO_SYNC": "1",
                        "UV_OFFLINE": "1",
                    },
                ),
            )
            result = await runtime.run(envelope, CancellationContext())
            if result.exit_code != 0:
                raise RuntimeError(
                    f"validation sandbox {profile_id} failed toolchain preflight: {argv[0]}"
                )


async def _preflight_agent_contracts(
    runtime: RootlessDockerRuntime,
    registries: ConfigurationBundle,
    *,
    seccomp_profile: Path,
    seccomp_digest: str,
) -> None:
    projects = tuple(project for project in registries.projects.items() if project.enabled)
    for profile in registries.profiles.items():
        contract = profile.agent_runtime_contract
        if not profile.enabled or contract is None:
            continue
        sandbox_ids = sorted({project.sandbox_profile for project in projects})
        for sandbox_id in sandbox_ids:
            sandbox = registries.sandboxes.get(sandbox_id)
            identity = uuid.uuid4()
            envelope = ProcessEnvelope(
                task_id=identity,
                run_id=uuid.uuid4(),
                step_id=uuid.uuid4(),
                worktree_id=uuid.uuid4(),
                profile_id=profile.id,
                provider_attempt=1,
                lease_generation=1,
                argv=(profile.provider, "--version"),
                stdin="",
                sandbox=SandboxSpec(
                    image=sandbox.image,
                    uid=sandbox.uid,
                    gid=sandbox.gid,
                    seccomp_profile=seccomp_profile,
                    seccomp_profile_sha256=seccomp_digest,
                    working_directory=contract.working_directory,
                    mounts=(),
                    cpu_count=sandbox.cpu_count,
                    memory_bytes=sandbox.memory_bytes,
                    pids_limit=sandbox.pids_limit,
                    tmpfs_bytes=sandbox.tmpfs_bytes,
                    open_files_limit=sandbox.open_files_limit,
                    output_bytes=sandbox.output_bytes,
                    timeout_seconds=min(sandbox.timeout_seconds, 60),
                    stop_grace_seconds=sandbox.stop_grace_seconds,
                    network_disabled=True,
                    environment={"HOME": "/tmp/home"},  # noqa: S108
                ),
            )
            result = await runtime.run(envelope, CancellationContext())
            if result.exit_code != 0 or result.stdout.strip() != contract.cli_version:
                raise RuntimeError(
                    f"agent runtime contract preflight failed for {profile.id}/{sandbox_id}"
                )


if __name__ == "__main__":
    main()
