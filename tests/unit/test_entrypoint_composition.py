import logging
import signal
from collections.abc import Callable
from types import FrameType

import uvicorn
from pytest import LogCaptureFixture, MonkeyPatch

from vuzol.cli import app as app_cli
from vuzol.cli import worker as worker_cli
from vuzol.config import RegistryDocument, RuntimeConfiguration, Settings, build_bundle


def runtime_configuration(settings: Settings) -> RuntimeConfiguration:
    return RuntimeConfiguration(
        settings=settings, registries=build_bundle(RegistryDocument(), settings)
    )


def test_app_main_composes_server(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test", host="127.0.0.2", port=9001)
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        app_cli, "get_runtime_configuration", lambda: runtime_configuration(settings)
    )
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


def test_worker_main_composes_runtime_and_handles_stop(
    monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    settings = Settings(environment="test")
    handlers: dict[int, Callable[[int, FrameType | None], None]] = {}
    calls: dict[str, object] = {}

    class Transaction:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_args: object) -> None:
            return None

    class Factory:
        def begin(self) -> Transaction:
            return Transaction()

    class Engine:
        async def dispose(self) -> None:
            calls["disposed"] = True

    class Dispatcher:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def process_one(self) -> bool:
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return False

    class Controls:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def process_one(self) -> bool:
            return False

    class WorkflowWorker:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def process_one(self) -> bool:
            return False

    monkeypatch.setattr(
        worker_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(worker_cli, "configure_logging", lambda **kwargs: calls.update(kwargs))
    monkeypatch.setattr(
        signal,
        "signal",
        lambda signum, handler: handlers.update({signum: handler}),
    )
    monkeypatch.setattr(worker_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(worker_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(worker_cli, "create_session_factory", lambda _engine: Factory())
    monkeypatch.setattr(worker_cli, "WorkflowDispatcher", Dispatcher)
    monkeypatch.setattr(worker_cli, "WorkflowControlConsumer", Controls)
    monkeypatch.setattr(worker_cli, "WorkflowWorker", WorkflowWorker)

    async def recover(_session: object, *, batch_size: int) -> int:
        calls["recovery_batch_size"] = batch_size
        return 0

    monkeypatch.setattr(worker_cli, "recover_expired_steps", recover)

    with caplog.at_level(logging.INFO):
        worker_cli.main()

    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    assert calls["service"] == "vuzol-worker"
    assert calls["recovery_batch_size"] == 100
    assert calls["disposed"] is True
    assert any(record.__dict__.get("signal") == signal.SIGTERM for record in caplog.records)
