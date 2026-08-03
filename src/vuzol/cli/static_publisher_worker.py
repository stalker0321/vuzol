"""Database-backed trusted static publication worker."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from contextlib import suppress

from vuzol.config import Capability, get_runtime_configuration
from vuzol.observability import configure_logging, get_logger
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.storage.types import QueueClass
from vuzol.workflows.static_publish import StaticPublishHandler
from vuzol.workflows.worker import WorkflowWorker


def main() -> None:
    asyncio.run(run())


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-publisher", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    try:
        await require_migration_head(engine)
        factory = create_session_factory(engine)
        owner = f"{socket.gethostname()}:{os.getpid()}:publisher"
        worker = WorkflowWorker(
            settings,
            factory,
            owner=owner,
            handlers={"publish_static": StaticPublishHandler(factory, runtime)},
            capabilities=frozenset({Capability.FILESYSTEM_WRITE}),
            queue_classes=frozenset({QueueClass.LIGHT}),
        )
        stop = asyncio.Event()
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
        await engine.dispose()


if __name__ == "__main__":
    main()
