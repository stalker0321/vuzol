"""Database-backed trusted static publication worker."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from contextlib import suppress

import uvicorn

from vuzol.app.preview_gateway import create_preview_gateway
from vuzol.config import Capability, get_runtime_configuration
from vuzol.observability import configure_logging, get_logger
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.storage.types import QueueClass
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry, RuntimePreviewHandler
from vuzol.workflows.static_publish import StaticPublishHandler
from vuzol.workflows.worker import WorkflowWorker


def main() -> None:
    asyncio.run(run())


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-publisher", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    preview_registry: PreviewRuntimeRegistry | None = None
    gateway: uvicorn.Server | None = None
    gateway_task: asyncio.Task[None] | None = None
    try:
        await require_migration_head(engine)
        factory = create_session_factory(engine)
        owner = f"{socket.gethostname()}:{os.getpid()}:publisher"
        preview_registry = PreviewRuntimeRegistry()
        runtime_preview = RuntimePreviewHandler(factory, runtime, preview_registry)
        static_preview = StaticPublishHandler(factory, runtime, preview=True)
        worker = WorkflowWorker(
            settings,
            factory,
            owner=owner,
            handlers={
                "publish_static": StaticPublishHandler(factory, runtime),
                "publish_preview": _PreviewHandler(runtime_preview, static_preview),
            },
            capabilities=frozenset({Capability.FILESYSTEM_WRITE}),
            queue_classes=frozenset({QueueClass.LIGHT}),
        )
        stop = asyncio.Event()
        gateway = uvicorn.Server(
            uvicorn.Config(
                create_preview_gateway(preview_registry, static_root=settings.preview_site_root),
                host=settings.preview_gateway_host,
                port=settings.preview_gateway_port,
                log_config=None,
            )
        )
        gateway_task = asyncio.create_task(gateway.serve())
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            with suppress(NotImplementedError):
                loop.add_signal_handler(signum, stop.set)
        get_logger(__name__).info("publisher ready", extra={"event": "publisher.ready"})
        while not stop.is_set():
            if not await worker.process_one():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=settings.workflow.poll_interval_seconds
                    )
    finally:
        if gateway is not None:
            gateway.should_exit = True
        if gateway_task is not None:
            with suppress(asyncio.CancelledError):
                await gateway_task
        if preview_registry is not None:
            await preview_registry.close()
        await engine.dispose()


class _PreviewHandler:
    def __init__(self, runtime: RuntimePreviewHandler, static: StaticPublishHandler) -> None:
        self._runtime = runtime
        self._static = static

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        outcome = await self._runtime.execute(request, cancellation)
        if (
            outcome.kind is OutcomeKind.SUCCEEDED
            and outcome.result.get("reason") == "no_web_component"
        ):
            return await self._static.execute(request, cancellation)
        return outcome


if __name__ == "__main__":
    main()
