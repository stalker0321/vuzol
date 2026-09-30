"""Dedicated derived-memory writer runtime (D5).

Standalone outbox consumer for the ``memory_extract`` destination, following
the interpreter/applier CLI pattern. It is deliberately NOT embedded in any
existing worker: completion paths never wait for memory extraction, and no
other consumer claims this destination (each claim is fenced by its own
``allowed_destinations``).
"""

import asyncio
import os
import signal
import socket
from contextlib import suppress

from vuzol.config import get_runtime_configuration
from vuzol.discussion.memory_writer import MemoryWriterService
from vuzol.observability import configure_logging, get_logger
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head


def main() -> None:
    asyncio.run(run())


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-memory-writer", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    stop_event = asyncio.Event()

    def request_stop(signum: int, _frame: object) -> None:
        get_logger(__name__).info(
            "Memory writer stop requested",
            extra={"event": "memory_writer.stop_requested", "signal": signum},
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    owner = f"{socket.gethostname()}:{os.getpid()}:memory-writer"
    try:
        # Fail closed before the session factory or poll loop.
        await require_migration_head(engine)
        writer = MemoryWriterService(
            create_session_factory(engine),
            owner=owner,
            lease_seconds=settings.workflow.lease_seconds,
        )
        get_logger(__name__).info("memory writer ready", extra={"event": "memory_writer.ready"})
        while not stop_event.is_set():
            if not await writer.process_one():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=settings.workflow.poll_interval_seconds,
                    )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    main()
