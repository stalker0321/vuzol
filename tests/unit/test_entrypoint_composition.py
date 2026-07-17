import asyncio
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
from vuzol.cli import applier as applier_cli
from vuzol.cli import project_provisioner as provisioner_cli
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


@pytest.mark.anyio
async def test_project_provisioner_composes_and_disposes(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test")
    runtime = runtime_configuration(settings)
    calls: dict[str, object] = {}
    handlers: dict[int, Callable[[int, FrameType | None], None]] = {}

    class Engine:
        async def dispose(self) -> None:
            calls["disposed"] = True

    class BotContext:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_args: object) -> None:
            return None

    bot_context = BotContext()

    def bot_factory(token: str) -> BotContext:
        calls["token"] = token
        return bot_context

    service = object()
    monkeypatch.setattr(provisioner_cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(provisioner_cli, "configure_logging", lambda **kwargs: calls.update(kwargs))
    monkeypatch.setattr(provisioner_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(provisioner_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(provisioner_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(provisioner_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))
    monkeypatch.setattr(provisioner_cli, "Bot", bot_factory)
    monkeypatch.setattr(provisioner_cli, "PythonTelegramClient", lambda bot: ("client", bot))
    monkeypatch.setattr(provisioner_cli, "FixedSystemdReloader", lambda: "reloader")
    monkeypatch.setattr(
        provisioner_cli,
        "ProjectProvisioningService",
        lambda *_args, **kwargs: calls.update(service_kwargs=kwargs) or service,
    )
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.update({signum: handler})
    )

    async def run_loop(actual: object, **kwargs: object) -> None:
        calls["service"] = actual
        calls.update(kwargs)
        handler = handlers[signal.SIGTERM]
        handler(signal.SIGTERM, None)

    monkeypatch.setattr(provisioner_cli, "run_provisioning_loop", run_loop)

    await provisioner_cli.run()

    assert calls["service"] is service
    service_kwargs = calls["service_kwargs"]
    assert isinstance(service_kwargs, dict)
    assert service_kwargs.keys() == {"owner", "reloader"}
    assert service_kwargs["reloader"] == "reloader"
    assert isinstance(service_kwargs["owner"], str)
    assert ":" in service_kwargs["owner"]
    assert calls["poll_interval_seconds"] == settings.worker_poll_interval_seconds
    stop_event = calls["stop_event"]
    assert isinstance(stop_event, asyncio.Event)
    assert stop_event.is_set()
    assert calls["disposed"] is True
    assert calls["level"] == "INFO"


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
    monkeypatch.setattr(worker_cli, "ResultReviewHandler", MagicMock())
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


@pytest.mark.anyio
async def test_applier_composes_narrow_control_and_privileged_worker(
    monkeypatch: MonkeyPatch,
) -> None:
    settings = Settings(environment="test")
    runtime = runtime_configuration(settings)
    calls: dict[str, object] = {}

    class Engine:
        async def dispose(self) -> None:
            calls["disposed"] = True

    class Stop:
        checks = 0

        def is_set(self) -> bool:
            self.checks += 1
            return self.checks > 1

        def set(self) -> None:
            self.checks = 2

        async def wait(self) -> None:
            return None

    controls = MagicMock()
    controls.process_one = AsyncMock(return_value=False)
    worker = MagicMock()
    worker.process_one = AsyncMock(return_value=False)
    monkeypatch.setattr(applier_cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(applier_cli, "configure_logging", lambda **kwargs: calls.update(kwargs))
    monkeypatch.setattr(applier_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(applier_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(applier_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(applier_cli, "WorkflowControlConsumer", lambda *_args, **_kwargs: controls)
    monkeypatch.setattr(applier_cli, "ResultApplyHandler", MagicMock())
    monkeypatch.setattr(applier_cli, "WorkflowWorker", lambda *_args, **_kwargs: worker)
    monkeypatch.setattr("vuzol.cli.applier.asyncio.Event", Stop)

    await applier_cli.run()

    assert calls["service"] == "vuzol-applier"
    assert calls["disposed"] is True
    controls.process_one.assert_awaited_once()
    worker.process_one.assert_awaited_once()


@pytest.mark.anyio
async def test_applier_chain_prioritizes_controls() -> None:
    controls = MagicMock()
    controls.process_one = AsyncMock(return_value=True)
    worker = MagicMock()
    worker.process_one = AsyncMock(return_value=True)
    chain = applier_cli.ApplierChain(controls, worker)
    assert await chain.process_one()
    worker.process_one.assert_not_awaited()
    controls.process_one.return_value = False
    assert await chain.process_one()
    worker.process_one.assert_awaited_once()


@pytest.mark.anyio
async def test_telegram_main_composes_long_polling(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test")
    application = MagicMock()
    callbacks: dict[str, object] = {}
    ingress = MagicMock()
    ingress.accept_message = AsyncMock()
    dogfood = MagicMock()
    dogfood.accept_message = AsyncMock(return_value=None)
    monkeypatch.setattr(
        telegram_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(telegram_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(telegram_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(telegram_cli, "create_engine", lambda *_args: object())
    monkeypatch.setattr(telegram_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(telegram_cli, "TelegramIngressService", lambda *_args: ingress)
    monkeypatch.setattr(telegram_cli, "TelegramDogfoodIngressService", lambda *_args: dogfood)
    monkeypatch.setattr(telegram_cli, "TelegramControlService", MagicMock())
    monkeypatch.setattr(telegram_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))

    def build(_token: str, **kwargs: object) -> object:
        callbacks.update(kwargs)
        return application

    monkeypatch.setattr(telegram_cli, "build_long_polling_application", build)
    telegram_cli.main()
    application.run_polling.assert_called_once()
    assert callbacks["bot_id"] == "main"
    on_message = callbacks["on_message"]
    assert callable(on_message)
    update = MagicMock()
    await on_message(update)
    dogfood.accept_message.assert_awaited_once_with(update)
    ingress.accept_message.assert_awaited_once_with(update)

    dogfood.accept_message.reset_mock(return_value=True)
    dogfood.accept_message.return_value = object()
    ingress.accept_message.reset_mock()
    await on_message(update)
    ingress.accept_message.assert_not_awaited()


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
