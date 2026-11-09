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
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.cli.worker import ProcessorChain
from vuzol.config import LaunchMode, ScopedSecretResolver, get_runtime_configuration
from vuzol.config.models import SandboxNetworkMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import (
    ExecutionNode,
    ProjectLeadSettings,
    Settings,
    SubscriptionLimitSettings,
)
from vuzol.execution.access import RootlessIdentityResolver, WorktreeAccessManager
from vuzol.execution.artifact_production import (
    ArtifactProductionHandler,
    SandboxedArtifactCommandRunner,
)
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.codex import ExecutionEnvelopeFactory, SandboxCodexTransport
from vuzol.execution.dependency_build import SandboxedDependencyBuilder
from vuzol.execution.domain import ProcessEnvelope, SandboxSpec
from vuzol.execution.gates import TrustedGateRunner
from vuzol.execution.git import LocalGit
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.execution.journal import ExecutionJournal
from vuzol.execution.proxy_service import ProxyServiceManager
from vuzol.execution.reconciliation import (
    NodeStepReconciler,
    ProxyStartupReconciler,
    TurnStartupReconciler,
)
from vuzol.execution.result_validation import ResultValidationHandler
from vuzol.execution.sandbox import RootlessDockerRuntime, validate_seccomp_profile
from vuzol.execution.static_build import StaticBuildHandler
from vuzol.execution.worktrees import WorktreeService
from vuzol.leadership.composition import (
    build_dream_worker,
    build_goal_retrospective_worker,
    build_project_lead_worker,
)
from vuzol.leadership.dreaming import enqueue_dreams
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
from vuzol.workflows.upgrade import require_workflow_upgrade
from vuzol.workflows.worker import RoutedWorkflowWorker, WorkflowWorker

VALIDATION_IMAGE_PREFLIGHT_COMMANDS: tuple[tuple[str, ...], ...] = (
    ("/usr/bin/make", "--version"),
    ("/usr/bin/git", "--version"),
    ("python", "--version"),
    ("uv", "--version"),
)


# Kept apart from the local proxy root, whose recovery would claim these manifests.
NODE_PROXY_RUNTIME_ROOT = Path("/var/lib/vuzol-proxy-runtime-nodes")


class ExecutorLane(Protocol):
    async def process_one(self) -> bool: ...


class ExecutorChain:
    def __init__(
        self,
        worktrees: WorkflowWorker,
        providers: RoutedWorkflowWorker,
        reviews: WorkflowWorker | None = None,
        lead: ExecutorLane | None = None,
        recovery: Callable[[], Awaitable[None]] | None = None,
        *,
        control: ExecutorLane | None = None,
        control_worktrees: ExecutorLane | None = None,
        provider_parallelism: int = 1,
        model_calls: tuple[ExecutorLane, ...] = (),
        node_providers: tuple[ExecutorLane, ...] = (),
        leads: tuple[ExecutorLane, ...] = (),
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
        self._model_calls = model_calls
        self._node_providers = node_providers
        self._leads = leads

    def lanes(self) -> tuple[ExecutorLane, ...]:
        """Long provider turns must not occupy the lead/control event loop."""
        return tuple(
            worker
            for worker in (
                self._lead,
                *self._leads,
                self._reviews,
                self._control_worktrees,
                self._control,
                self._worktrees,
                *([self._providers] * self._provider_parallelism),
                *self._model_calls,
                *self._node_providers,
            )
            if worker is not None
        )

    async def recover_if_due(self) -> None:
        if self._recovery is not None and time.monotonic() >= self._next_recovery:
            await self._recovery()
            self._next_recovery = time.monotonic() + 30


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
        await require_workflow_upgrade(engine)
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
        from vuzol.model_calls.transport import ModelCallSandboxTransport
        from vuzol.model_calls.worker import ModelCallReconciler, ModelCallWorker

        call_transport = ModelCallSandboxTransport(
            sandbox_runtime, factory, settings, runtime.registries, proxy_manager
        )
        call_reconciler = ModelCallReconciler(factory, sandbox_runtime, proxy_manager)
        await call_reconciler.reconcile()
        node_calls = _node_model_calls(
            settings, runtime.registries, factory, resolver, journal, proxy_manager
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
                cli_adapter(profile, call_transport),
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
        provider_handler = ProviderStepHandler(
            factory,
            runtime.registries,
            adapter_registry,
            worktrees=worktree_service,
            artifacts=artifact_store,
            worktree_access=worktree_access,
            redaction_patterns=settings.redaction_patterns,
            dependency_provisioning_enabled=settings.dependency_provisioning.enabled,
            worker_mcp_servers={
                name: str(url) for name, url in settings.execution.worker_mcp_servers.items()
            },
        )
        node_providers = _node_providers(
            settings,
            runtime.registries,
            factory,
            resolver,
            journal,
            artifacts=artifact_store,
            worktrees=worktree_service,
            worktree_access=worktree_access,
            owner=owner,
        )
        for _node, _worker, reaper in node_providers:
            with suppress(Exception):  # an unreachable node must not stop us
                await reaper.reconcile()
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
            handlers=executor_provider_handlers(provider_handler),
            queue_classes=frozenset({QueueClass.HEAVY}),
        )
        control_worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=runtime.registries,
            owner=f"{owner}:conversation",
            handlers=executor_provider_handlers(provider_handler),
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

        def project_lead(lane_owner: str) -> ProjectLeadWorker:
            return build_project_lead_worker(
                factory,
                settings=settings,
                profiles=runtime.registries.profiles,
                owner=lane_owner,
                default_branches={
                    p.id: p.default_branch for p in runtime.registries.projects.items()
                },
                repository_paths={
                    p.id: p.repository_path for p in runtime.registries.projects.items()
                },
                adapter_registry=adapter_registry,
            )

        lead_worker = (
            ProcessorChain(
                project_lead(f"{owner}:project-lead"),
                build_goal_retrospective_worker(
                    factory,
                    settings=settings,
                    profiles=runtime.registries.profiles,
                    owner=f"{owner}:retrospective",
                    adapter_registry=adapter_registry,
                ),
                build_dream_worker(
                    factory,
                    settings=settings,
                    profiles=runtime.registries.profiles,
                    owner=f"{owner}:dream",
                    adapter_registry=adapter_registry,
                ),
            )
            if settings.project_lead.enabled
            else None
        )
        # Every lane needs its own owner: a goal lease names the lane that holds it.
        extra_leads = (
            tuple(
                project_lead(f"{owner}:project-lead:{lane}")
                for lane in range(1, settings.project_lead.lead_parallelism)
            )
            if settings.project_lead.enabled
            else ()
        )

        model_call_lanes = (
            tuple(
                ModelCallWorker(
                    factory,
                    runtime.registries.profiles,
                    adapter_registry.get,
                    owner=f"{owner}:model-call",
                )
                for _ in range(settings.execution.model_call_parallelism)
            )
            if settings.execution.model_call_sandbox_profile is not None
            else ()
        ) + tuple(
            ModelCallWorker(
                factory,
                runtime.registries.profiles,
                registry.get,
                owner=f"{owner}:model-call:{node.id}",
                profile_ids=frozenset(registry_ids),
                ready=node_ready,
            )
            for node, registry, registry_ids, node_ready, _reaper in node_calls
            for _ in range(node.model_call_parallelism)
        )

        async def recover_cli_resources() -> None:
            await turn_reconciler.reconcile_startup()
            await call_reconciler.reconcile()
            for *_lane, reaper in node_calls:
                with suppress(Exception):  # an unreachable node must not stop us
                    await reaper.reconcile()
            for _node, _worker, reaper in node_providers:
                with suppress(Exception):
                    await reaper.reconcile()
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
                model_calls=model_call_lanes,
                node_providers=tuple(
                    worker
                    for node, worker, _reaper in node_providers
                    for _ in range(node.provider_parallelism)
                ),
                leads=extra_leads,
            ),
            settings.workflow.poll_interval_seconds,
            session_factory=factory,
            registries=runtime.registries,
            subscription_limit_settings=settings.subscription_limits,
            dream_settings=settings.project_lead,
            timezone=settings.assistant.timezone,
        )
    finally:
        await engine.dispose()


def _node_providers(
    settings: Settings,
    registries: ConfigurationBundle,
    factory: async_sessionmaker[AsyncSession],
    resolver: ScopedSecretResolver,
    journal: ExecutionJournal | None,
    *,
    artifacts: ArtifactStore,
    worktrees: WorktreeService,
    worktree_access: WorktreeAccessManager,
    owner: str,
) -> tuple[tuple[ExecutionNode, RoutedWorkflowWorker, NodeStepReconciler], ...]:
    """Extra coding capacity for pi; OAuth state with refresh owners stays local."""
    from vuzol.execution.node import NodeDockerRuntime, NodeProxyServiceManager

    profiles = tuple(
        p
        for p in registries.profiles.items()
        if p.enabled
        and p.launch_mode is LaunchMode.CLI
        and p.provider == "pi"
        and supported_profile(p)
    )
    if not profiles:
        return ()
    # The helper image must already be on the node; model calls use the same one.
    sandbox_id = settings.execution.model_call_sandbox_profile
    helper_image = registries.sandboxes.get(sandbox_id).image if sandbox_id else None
    groups = []
    for node in settings.execution.nodes:
        if node.provider_parallelism == 0:
            continue
        local = settings.execution.rootless_docker_socket
        node_runtime = NodeDockerRuntime(
            node.id,
            node.docker_socket,
            local,
            journal=journal,
            max_copy_back_bytes=node.max_copy_back_bytes,
            writable_worktree_only=True,
        )
        proxy_image = settings.execution.proxy_image
        node_proxy = (
            NodeProxyServiceManager(
                node.docker_socket,
                local,
                NODE_PROXY_RUNTIME_ROOT / node.id,
                proxy_image,
                helper_image=helper_image,
            )
            if proxy_image is not None and helper_image is not None
            else None
        )
        envelopes = ExecutionEnvelopeFactory(factory, settings, registries, node=node.id)
        transport = SandboxCodexTransport(node_runtime, envelopes, artifacts, node_proxy)
        adapters = {profile.id: cli_adapter(profile, transport) for profile in profiles}
        registry = AdapterRegistry(
            registries.profiles,
            resolver,
            adapters=adapters,
            session_factory=factory,
        )
        handler = ProviderStepHandler(
            factory,
            registries,
            registry,
            worktrees=worktrees,
            artifacts=artifacts,
            worktree_access=worktree_access,
            redaction_patterns=settings.redaction_patterns,
            dependency_provisioning_enabled=settings.dependency_provisioning.enabled,
            worker_mcp_servers={
                name: str(url) for name, url in settings.execution.worker_mcp_servers.items()
            },
        )
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner=f"{owner}:provider:{node.id}",
            handlers={"execute_code": executor_provider_handlers(handler)["execute_code"]},
            queue_classes=frozenset({QueueClass.HEAVY}),
            profile_ids=frozenset(adapters),
            ready=node.docker_socket.is_socket,
        )
        groups.append(
            (
                node,
                worker,
                NodeStepReconciler(
                    factory,
                    node_runtime,
                    node_proxy,
                    node=node.id,
                    owner=owner,
                ),
            )
        )
    return tuple(groups)


def _node_model_calls(
    settings: Settings,
    registries: ConfigurationBundle,
    factory: async_sessionmaker[AsyncSession],
    resolver: ScopedSecretResolver,
    journal: ExecutionJournal | None,
    proxy_manager: ProxyServiceManager | None,
) -> tuple[tuple[ExecutionNode, AdapterRegistry, tuple[str, ...], Callable[[], bool], Any], ...]:
    """Model-call lanes on other machines' Docker; empty `execution.nodes` adds none.

    A node runs pi profiles only: their provider state is read-only, so nothing
    has to come back from the node (vuzol.execution.node).
    """
    from vuzol.execution.node import NodeDockerRuntime, NodeProxyServiceManager
    from vuzol.model_calls.transport import ModelCallSandboxTransport
    from vuzol.model_calls.worker import ModelCallReconciler

    sandbox_id = settings.execution.model_call_sandbox_profile
    proxy_image = settings.execution.proxy_image
    if sandbox_id is None or proxy_manager is None or proxy_image is None:
        return ()
    helper_image = registries.sandboxes.get(sandbox_id).image
    lanes = []
    for node in settings.execution.nodes:
        if node.model_call_parallelism == 0:
            continue
        local = settings.execution.rootless_docker_socket
        node_runtime = NodeDockerRuntime(
            node.id,
            node.docker_socket,
            local,
            journal=journal,
            max_copy_back_bytes=node.max_copy_back_bytes,
        )
        node_proxy = NodeProxyServiceManager(
            node.docker_socket,
            local,
            NODE_PROXY_RUNTIME_ROOT / node.id,
            proxy_image,
            helper_image=helper_image,
        )
        transport = ModelCallSandboxTransport(
            node_runtime, factory, settings, registries, node_proxy, node=node.id
        )
        adapters: dict[str, ProviderAdapter] = {}
        for profile in registries.profiles.items():
            if (
                profile.enabled
                and profile.launch_mode is LaunchMode.CLI
                and profile.provider == "pi"
                and supported_profile(profile)
            ):
                base = cli_adapter(profile, transport)
                adapters[profile.id] = RoleBoundCliAdapter(base, base, base, base)
        if not adapters:
            continue
        registry = AdapterRegistry(
            registries.profiles, resolver, adapters=adapters, session_factory=factory
        )
        socket = node.docker_socket
        lanes.append(
            (
                node,
                registry,
                tuple(sorted(adapters)),
                socket.is_socket,
                ModelCallReconciler(factory, node_runtime, node_proxy, node=node.id),
            )
        )
    return tuple(lanes)


async def _refresh_topic_lines_tick(
    session_factory: async_sessionmaker[AsyncSession], registries: object
) -> None:
    """Keep each project topic's pinned line saying what runs there now."""
    from vuzol.telegram.topic_line import profile_labels, refresh_topic_lines

    try:
        labels = profile_labels(registries.profiles.items())  # type: ignore[attr-defined]
        async with session_factory.begin() as session:
            await refresh_topic_lines(session, labels)
    except Exception:  # a status line must never stop the executor
        get_logger(__name__).warning(
            "topic line refresh failed",
            extra={"event": "executor.topic_line_failed"},
            exc_info=True,
        )


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
    dream_settings: ProjectLeadSettings | None = None,
    timezone: str = "Europe/Amsterdam",
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
    line_refresh_seconds = 10.0
    last_line_refresh = -line_refresh_seconds
    last_dream_tick = -60.0

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
            if (
                session_factory is not None
                and dream_settings is not None
                and now - last_dream_tick >= 60
            ):
                last_dream_tick = now
                try:
                    async with session_factory.begin() as session:
                        await enqueue_dreams(session, settings=dream_settings, timezone=timezone)
                except Exception:  # nightly memory upkeep must never stop the executor
                    get_logger(__name__).warning(
                        "dream scheduling failed",
                        extra={"event": "executor.dream_schedule_failed"},
                        exc_info=True,
                    )
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
                if now - last_line_refresh >= line_refresh_seconds:
                    last_line_refresh = now
                    await _refresh_topic_lines_tick(session_factory, registries)
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
    finally:
        for task in lanes:
            task.cancel()
        await asyncio.gather(*lanes, return_exceptions=True)
        for lane in processor.lanes():
            close = getattr(lane, "aclose", None)
            if close is not None:
                await close()
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


if __name__ == "__main__":
    main()
