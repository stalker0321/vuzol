"""Narrow control-plane and privileged approved-result apply worker."""

import asyncio
import os
import signal
import socket
from contextlib import suppress

from vuzol.config import Capability, get_runtime_configuration
from vuzol.execution.git import LocalGit
from vuzol.execution.result_apply import ResultApplyHandler
from vuzol.observability import configure_logging, get_logger
from vuzol.projects.capability_provisioning import (
    CapabilityProvisioningHandler,
    OfflineCapabilityInstaller,
)
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.storage.types import QueueClass
from vuzol.workflows.controls import WorkflowControlConsumer
from vuzol.workflows.worker import WorkflowWorker


class ApplierChain:
    def __init__(self, controls: WorkflowControlConsumer, worker: WorkflowWorker) -> None:
        self._controls = controls
        self._worker = worker

    async def process_one(self) -> bool:
        return await self._controls.process_one() or await self._worker.process_one()


def main() -> None:
    asyncio.run(run())


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-applier", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    try:
        # S-2.1: fail closed before control/apply workers, ready event, or poll loops.
        await require_migration_head(engine)
        factory = create_session_factory(engine)
        owner = f"{socket.gethostname()}:{os.getpid()}:applier"
        controls = WorkflowControlConsumer(settings, factory, owner=f"{owner}:control")
        handler = ResultApplyHandler(factory, runtime.registries, LocalGit())
        capability_handler = CapabilityProvisioningHandler(
            factory,
            OfflineCapabilityInstaller(settings.capability_provisioning),
        )
        worker = WorkflowWorker(
            settings,
            factory,
            owner=f"{owner}:apply",
            handlers={"approval": handler, "ensure_capabilities": capability_handler},
            capabilities=frozenset({Capability.GIT, Capability.HOST_ADMIN}),
            queue_classes=frozenset({QueueClass.PRIVILEGED}),
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            with suppress(NotImplementedError):
                loop.add_signal_handler(signum, stop.set)
        get_logger(__name__).info("applier ready", extra={"event": "applier.ready"})
        while not stop.is_set():
            if not await ApplierChain(controls, worker).process_one():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=settings.workflow.poll_interval_seconds
                    )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    main()
