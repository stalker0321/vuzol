import logging
import threading

from pytest import LogCaptureFixture

from vuzol.cli.worker import run_worker


def test_worker_stops_when_requested(caplog: LogCaptureFixture) -> None:
    stop_event = threading.Event()
    stop_event.set()

    with caplog.at_level(logging.INFO):
        run_worker(poll_interval_seconds=0.01, stop_event=stop_event)

    assert [record.message for record in caplog.records] == ["worker ready", "worker stopped"]
