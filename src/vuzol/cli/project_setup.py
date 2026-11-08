"""Root host unit that creates projects the owner approved in the assistant topic."""

import asyncio
import os
import signal
import socket

from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging
from vuzol.projects.setup import ProjectSetupService, run_setup_loop
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-project-setup", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    try:
        await require_migration_head(engine)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop.set)
        if not settings.project_setup.enabled:
            await stop.wait()  # installed everywhere, active only where enabled
            return
        service = ProjectSetupService(
            runtime,
            create_session_factory(engine),
            owner=f"{socket.gethostname()}:{os.getpid()}:project-setup",
        )
        await run_setup_loop(
            service, poll_interval_seconds=settings.worker_poll_interval_seconds, stop=stop
        )
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
