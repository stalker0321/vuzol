import logging
import signal
import threading
from collections.abc import Callable
from types import FrameType
from typing import cast

import uvicorn
from pytest import LogCaptureFixture, MonkeyPatch

from vuzol.cli import app as app_cli
from vuzol.cli import worker as worker_cli
from vuzol.config import Settings


def test_app_main_composes_server(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test", host="127.0.0.2", port=9001)
    calls: dict[str, object] = {}

    monkeypatch.setattr(app_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(app_cli, "configure_logging", lambda **kwargs: calls.update(kwargs))
    monkeypatch.setattr(
        uvicorn,
        "run",
        lambda application, **kwargs: calls.update(application=application, **kwargs),
    )

    app_cli.main()

    assert calls["service"] == "vuzol-app"
    assert calls["level"] == "INFO"
    assert calls["host"] == "127.0.0.2"
    assert calls["port"] == 9001
    assert calls["log_config"] is None


def test_worker_main_registers_signals_and_runs(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test", worker_poll_interval_seconds=0.25)
    handlers: dict[int, Callable[[int, FrameType | None], None]] = {}
    run_arguments: dict[str, object] = {}

    monkeypatch.setattr(worker_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(worker_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(
        signal,
        "signal",
        lambda signum, handler: handlers.update({signum: handler}),
    )
    monkeypatch.setattr(
        worker_cli,
        "run_worker",
        lambda **kwargs: run_arguments.update(kwargs),
    )

    worker_cli.main()

    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    assert run_arguments["poll_interval_seconds"] == 0.25
    stop_event = cast(threading.Event, run_arguments["stop_event"])
    handlers[signal.SIGTERM](signal.SIGTERM, None)
    assert stop_event.is_set()


def test_worker_stop_handler_logs_signal(
    monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    settings = Settings(environment="test")
    handlers: dict[int, Callable[[int, FrameType | None], None]] = {}

    monkeypatch.setattr(worker_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(worker_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(
        signal,
        "signal",
        lambda signum, handler: handlers.update({signum: handler}),
    )
    monkeypatch.setattr(worker_cli, "run_worker", lambda **_kwargs: None)

    with caplog.at_level(logging.INFO):
        worker_cli.main()
        handlers[signal.SIGINT](signal.SIGINT, None)

    assert caplog.records[-1].__dict__["signal"] == signal.SIGINT
