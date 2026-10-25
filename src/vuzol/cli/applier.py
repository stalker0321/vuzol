"""Narrow control-plane and privileged approved-result apply worker."""

import asyncio
import os
import signal
import socket
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, DeliveryMode, RuntimeConfiguration, get_runtime_configuration
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.effect_reconciliation import EffectReconciler
from vuzol.execution.git import LocalGit, require_writable_directory
from vuzol.execution.result_apply import ResultApplyHandler
from vuzol.leadership.acceptance import GoalAcceptanceConsumer
from vuzol.observability import configure_logging, get_logger
from vuzol.projects.capability_provisioning import (
    CapabilityProvisioningHandler,
    OfflineCapabilityInstaller,
)
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.storage.types import QueueClass
from vuzol.workflows.acceptance import AcceptanceGateHandler
from vuzol.workflows.controls import WorkflowControlConsumer
from vuzol.workflows.ports import StepHandler
from vuzol.workflows.upgrade import require_workflow_upgrade
from vuzol.workflows.worker import WorkflowWorker


class ApplierChain:
    def __init__(
        self,
        controls: WorkflowControlConsumer,
        worker: WorkflowWorker,
        acceptance: GoalAcceptanceConsumer | None = None,
    ) -> None:
        self._controls = controls
        self._worker = worker
        self._acceptance = acceptance

    async def process_one(self) -> bool:
        return (
            await self._controls.process_one()
            or await self._worker.process_one()
            or (self._acceptance is not None and await self._acceptance.process_one())
        )


def main() -> None:
    asyncio.run(run())


def applier_handlers(
    factory: async_sessionmaker[AsyncSession], runtime: RuntimeConfiguration
) -> dict[str, StepHandler]:
    """Actual narrow service assembly, also exercised by composition tests."""
    settings = runtime.settings
    git = LocalGit()
    return {
        "approval": ResultApplyHandler(
            factory, runtime.registries, git, project_lead_enabled=settings.project_lead.enabled
        ),
        "ensure_capabilities": CapabilityProvisioningHandler(
            factory,
            OfflineCapabilityInstaller(settings.capability_provisioning),
            registries=runtime.registries,
            worktree_root=settings.worktree_root,
            apply_access_check=git.require_apply_access,
        ),
        "acceptance": AcceptanceGateHandler(
            factory,
            artifacts=ArtifactStore(
                settings.artifact_root,
                max_bytes=settings.limits.artifact_bytes,
                retention_days=settings.retention.artifact_days,
                redaction_patterns=settings.redaction_patterns,
            ),
        ),
    }


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-applier", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    try:
        # S-2.1: fail closed before control/apply workers, ready event, or poll loops.
        await require_migration_head(engine)
        await require_workflow_upgrade(engine)
        # Run in this service's real mount namespace, not in the interpreter
        # container. A single-project systemd exception cannot admit new projects.
        if any(
            project.enabled and DeliveryMode.APPLY in project.git_delivery.allowed_modes
            for project in runtime.registries.projects.items()
        ):
            require_writable_directory(settings.repository_root)
        factory = create_session_factory(engine)
        owner = f"{socket.gethostname()}:{os.getpid()}:applier"
        # Reconcile unsettled apply effects before claiming new work (WP05): a
        # crash between the Git CAS and the business-state record must be settled
        # from the observed ref, never re-dispatched.
        report = await EffectReconciler(
            factory,
            LocalGit(),
            runtime.registries,
            owner=f"{owner}:effect-reconcile",
        ).reconcile_startup()
        if not report.lock_acquired:
            get_logger(__name__).warning(
                "effect reconciliation lock was unavailable; settlement skipped",
                extra={"event": "applier.effect_reconciliation_lock_timeout"},
            )
        if report.decisions:
            get_logger(__name__).info(
                "reconciled unsettled apply effects",
                extra={
                    "event": "applier.effect_reconciliation",
                    "confirmed": report.confirmed_count,
                    "denied": report.denied_count,
                    "uncertain": report.uncertain_count,
                },
            )
        controls = WorkflowControlConsumer(settings, factory, owner=f"{owner}:control")
        worker = WorkflowWorker(
            settings,
            factory,
            owner=f"{owner}:apply",
            handlers=applier_handlers(factory, runtime),
            capabilities=frozenset({Capability.GIT, Capability.HOST_ADMIN}),
            queue_classes=frozenset({QueueClass.PRIVILEGED}),
        )
        # The owner's accepted goal fast-forwards main here: this service owns the
        # project repositories (live pilot 2026-10-25: ingress/worker have no git).
        acceptance = GoalAcceptanceConsumer(factory, owner=f"{owner}:goal-acceptance")
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            with suppress(NotImplementedError):
                loop.add_signal_handler(signum, stop.set)
        get_logger(__name__).info("applier ready", extra={"event": "applier.ready"})
        while not stop.is_set():
            if not await ApplierChain(controls, worker, acceptance).process_one():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=settings.workflow.poll_interval_seconds
                    )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    main()
