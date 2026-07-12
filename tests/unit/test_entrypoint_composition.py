import logging
import signal
from collections.abc import Callable
from types import FrameType
from unittest.mock import AsyncMock, MagicMock

import pytest
import uvicorn
from pydantic import SecretStr
from pytest import LogCaptureFixture, MonkeyPatch

from vuzol.cli import app as app_cli
from vuzol.cli import telegram as telegram_cli
from vuzol.cli import telegram_delivery as delivery_cli
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


def test_telegram_main_composes_long_polling(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test")
    application = MagicMock()
    callbacks: dict[str, object] = {}
    monkeypatch.setattr(
        telegram_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(telegram_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(telegram_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(telegram_cli, "create_engine", lambda *_args: object())
    monkeypatch.setattr(telegram_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(telegram_cli, "TelegramIngressService", MagicMock())
    monkeypatch.setattr(telegram_cli, "TelegramControlService", MagicMock())
    monkeypatch.setattr(telegram_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))

    def build(_token: str, **kwargs: object) -> object:
        callbacks.update(kwargs)
        return application

    monkeypatch.setattr(telegram_cli, "build_long_polling_application", build)
    telegram_cli.main()
    application.run_polling.assert_called_once()
    assert callbacks["bot_id"] == "main"


@pytest.mark.anyio
async def test_telegram_delivery_composes_and_disposes(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test")
    engine = MagicMock()
    engine.dispose = AsyncMock()
    bot = MagicMock()

    class BotContext:
        async def __aenter__(self) -> object:
            return bot

        async def __aexit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        delivery_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(delivery_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(delivery_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(delivery_cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(delivery_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(delivery_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))
    monkeypatch.setattr(delivery_cli, "Bot", lambda _token: BotContext())
    monkeypatch.setattr(delivery_cli, "TelegramDeliveryService", MagicMock())
    loop = AsyncMock()
    monkeypatch.setattr(delivery_cli, "run_delivery_loop", loop)
    monkeypatch.setattr(signal, "signal", lambda *_args: None)

    await delivery_cli.run()
    loop.assert_awaited_once()
    engine.dispose.assert_awaited_once()
