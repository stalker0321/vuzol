"""Foundation worker CLI with graceful process termination."""

import signal
import threading

from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging, get_logger


def run_worker(*, poll_interval_seconds: float, stop_event: threading.Event) -> None:
    """Wait for future workflow work without owning in-memory business state."""

    logger = get_logger(__name__)
    logger.info("worker ready", extra={"event": "worker.ready"})
    while not stop_event.wait(poll_interval_seconds):
        logger.debug("worker idle", extra={"event": "worker.idle"})
    logger.info("worker stopped", extra={"event": "worker.stopped"})


def main() -> None:
    """Load settings and run the worker process."""

    settings = get_runtime_configuration().settings
    configure_logging(service=f"{settings.service_name}-worker", level=settings.log_level)
    stop_event = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        get_logger(__name__).info(
            "worker stop requested",
            extra={"event": "worker.stop_requested", "signal": signum},
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    run_worker(
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        stop_event=stop_event,
    )


if __name__ == "__main__":
    main()
