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
        app_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
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
    order: list[str] = []

    class Engine:
        async def dispose(self) -> None:
            calls["disposed"] = True
            order.append("dispose")

    class BotContext:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_args: object) -> None:
            return None

    bot_context = BotContext()

    def bot_factory(token: str) -> BotContext:
        calls["token"] = token
        order.append("bot")
        return bot_context

    service = object()

    async def pass_head(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        return object()

    monkeypatch.setattr(provisioner_cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(provisioner_cli, "configure_logging", lambda **kwargs: calls.update(kwargs))
    monkeypatch.setattr(provisioner_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(provisioner_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(provisioner_cli, "require_migration_head", pass_head)
    monkeypatch.setattr(provisioner_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(
        provisioner_cli, "reconcile_imported_environments", AsyncMock(return_value=0)
    )
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
        order.append("run_provisioning_loop")
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
    assert order == [
        "require_migration_head",
        "bot",
        "run_provisioning_loop",
        "dispose",
    ]


@pytest.mark.anyio
async def test_project_provisioner_fails_closed_before_loop_when_migration_head_refuses(
    monkeypatch: MonkeyPatch,
) -> None:
    """S-2.2c: gate refusal disposes once; no Bot/factory/provision loop."""

    from vuzol.storage.migration_preflight import MigrationHeadError

    settings = Settings(environment="test")
    order: list[str] = []

    class Engine:
        async def dispose(self) -> None:
            order.append("dispose")

    async def refuse(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        raise MigrationHeadError(
            "migration_head_behind",
            "database schema is behind this release",
        )

    def boom_bot(*_args: object, **_kwargs: object) -> object:
        order.append("bot")
        raise AssertionError("Bot must not start after migration head failure")

    def boom_factory(*_args: object, **_kwargs: object) -> object:
        order.append("create_session_factory")
        raise AssertionError("session factory must not run after migration head failure")

    async def never_loop(*_args: object, **_kwargs: object) -> None:
        order.append("run_provisioning_loop")
        raise AssertionError("provision loop must not start after migration head failure")

    monkeypatch.setattr(
        provisioner_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(provisioner_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(provisioner_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(provisioner_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(provisioner_cli, "require_migration_head", refuse)
    monkeypatch.setattr(provisioner_cli, "create_session_factory", boom_factory)
    monkeypatch.setattr(provisioner_cli, "Bot", boom_bot)
    monkeypatch.setattr(provisioner_cli, "run_provisioning_loop", never_loop)
    monkeypatch.setattr(provisioner_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))
    monkeypatch.setattr(signal, "signal", lambda *_args: None)

    with pytest.raises(MigrationHeadError) as excinfo:
        await provisioner_cli.run()

    assert excinfo.value.code == "migration_head_behind"
    assert order == ["require_migration_head", "dispose"]


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
    migration_head = AsyncMock()
    monkeypatch.setattr(worker_cli, "require_migration_head", migration_head)
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
    migration_head.assert_awaited()
    assert any(record.__dict__.get("signal") == signal.SIGTERM for record in caplog.records)


def test_worker_fails_closed_before_profile_sync_when_migration_head_refuses(
    monkeypatch: MonkeyPatch,
) -> None:
    """S-2.1: MigrationHeadError propagates; no synchronize_profiles / ready work."""

    from vuzol.storage.migration_preflight import MigrationHeadError

    settings = Settings(environment="test")
    order: list[str] = []

    class Engine:
        async def dispose(self) -> None:
            order.append("dispose")

    class Factory:
        def begin(self) -> object:
            order.append("factory_begin")
            raise AssertionError("session work must not start after migration head failure")

    async def refuse(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        raise MigrationHeadError("migration_head_behind", "database schema is behind this release")

    async def never_sync(*_args: object, **_kwargs: object) -> None:
        order.append("synchronize_profiles")
        raise AssertionError("synchronize_profiles must not run")

    monkeypatch.setattr(
        worker_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(worker_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(signal, "signal", lambda *_args: None)
    monkeypatch.setattr(worker_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(worker_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(worker_cli, "create_session_factory", lambda _engine: Factory())
    monkeypatch.setattr(worker_cli, "require_migration_head", refuse)
    monkeypatch.setattr(worker_cli, "synchronize_profiles", never_sync)

    with pytest.raises(MigrationHeadError) as excinfo:
        asyncio.run(worker_cli.run())

    assert excinfo.value.code == "migration_head_behind"
    # Gate fails closed before profile sync; engine still disposed in finally.
    assert order == ["require_migration_head", "dispose"]
    assert "synchronize_profiles" not in order


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
    migration_head = AsyncMock()
    monkeypatch.setattr(applier_cli, "require_migration_head", migration_head)
    monkeypatch.setattr(applier_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(applier_cli, "WorkflowControlConsumer", lambda *_args, **_kwargs: controls)
    monkeypatch.setattr(applier_cli, "ResultApplyHandler", MagicMock())
    monkeypatch.setattr(applier_cli, "WorkflowWorker", lambda *_args, **_kwargs: worker)
    monkeypatch.setattr("vuzol.cli.applier.asyncio.Event", Stop)

    await applier_cli.run()

    assert calls["service"] == "vuzol-applier"
    assert calls["disposed"] is True
    migration_head.assert_awaited()
    controls.process_one.assert_awaited_once()
    worker.process_one.assert_awaited_once()


@pytest.mark.anyio
async def test_applier_fails_closed_before_workers_when_migration_head_refuses(
    monkeypatch: MonkeyPatch,
) -> None:
    from vuzol.storage.migration_preflight import MigrationHeadError

    settings = Settings(environment="test")
    runtime = runtime_configuration(settings)
    order: list[str] = []

    class Engine:
        async def dispose(self) -> None:
            order.append("dispose")

    async def refuse(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        raise MigrationHeadError(
            "migration_head_mismatch",
            "database revision set does not match this code tree heads",
        )

    def boom_factory(*_args: object, **_kwargs: object) -> object:
        order.append("worker_or_control")
        raise AssertionError("workers must not be constructed after migration head failure")

    monkeypatch.setattr(applier_cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(applier_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(applier_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(applier_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(applier_cli, "require_migration_head", refuse)
    monkeypatch.setattr(applier_cli, "create_session_factory", boom_factory)
    monkeypatch.setattr(applier_cli, "WorkflowControlConsumer", boom_factory)
    monkeypatch.setattr(applier_cli, "WorkflowWorker", boom_factory)

    with pytest.raises(MigrationHeadError) as excinfo:
        await applier_cli.run()

    assert excinfo.value.code == "migration_head_mismatch"
    # Gate fails closed before workers; engine still disposed in finally.
    assert order == ["require_migration_head", "dispose"]


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


def test_telegram_main_composes_long_polling(monkeypatch: MonkeyPatch) -> None:
    """One owned loop: gate → run_polling(close_loop=False) → dispose."""

    settings = Settings(environment="test")
    application = MagicMock()
    callbacks: dict[str, object] = {}
    polling_kwargs: dict[str, object] = {}
    ingress = MagicMock()
    ingress.accept_message = AsyncMock()
    dogfood = MagicMock()
    dogfood.accept_message = AsyncMock(return_value=None)
    order: list[str] = []
    loop_ids: list[int] = []

    class Engine:
        async def dispose(self) -> None:
            order.append("dispose")
            loop_ids.append(id(asyncio.get_running_loop()))

    async def pass_head(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        loop_ids.append(id(asyncio.get_running_loop()))
        return object()

    monkeypatch.setattr(
        telegram_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(telegram_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(telegram_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(telegram_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(telegram_cli, "require_migration_head", pass_head)
    monkeypatch.setattr(telegram_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(telegram_cli, "TelegramIngressService", lambda *_args: ingress)
    monkeypatch.setattr(telegram_cli, "TelegramDogfoodIngressService", lambda *_args: dogfood)
    monkeypatch.setattr(telegram_cli, "TelegramControlService", MagicMock())
    monkeypatch.setattr(telegram_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))

    def build(_token: str, **kwargs: object) -> object:
        order.append("build_application")
        callbacks.update(kwargs)
        return application

    def run_polling(*_args: object, **_kwargs: object) -> None:
        order.append("run_polling")
        polling_kwargs.update(_kwargs)

    application.run_polling = run_polling
    monkeypatch.setattr(telegram_cli, "build_long_polling_application", build)
    telegram_cli.main()
    assert order == ["require_migration_head", "build_application", "run_polling", "dispose"]
    assert polling_kwargs.get("close_loop") is False
    # Gate and dispose share the same owned loop identity.
    assert len(loop_ids) == 2
    assert loop_ids[0] == loop_ids[1]
    assert callbacks["bot_id"] == "main"
    on_message = callbacks["on_message"]
    assert callable(on_message)
    update = MagicMock()
    asyncio.run(on_message(update))
    dogfood.accept_message.assert_awaited_once_with(update)
    ingress.accept_message.assert_awaited_once_with(update)

    dogfood.accept_message.reset_mock(return_value=True)
    dogfood.accept_message.return_value = object()
    ingress.accept_message.reset_mock()
    asyncio.run(on_message(update))
    ingress.accept_message.assert_not_awaited()


def test_telegram_fails_closed_before_polling_when_migration_head_refuses(
    monkeypatch: MonkeyPatch,
) -> None:
    """S-2.2a: MigrationHeadError before run_polling; engine disposed once on same loop."""

    from vuzol.storage.migration_preflight import MigrationHeadError

    settings = Settings(environment="test")
    order: list[str] = []
    loop_ids: list[int] = []
    application = MagicMock()

    class Engine:
        async def dispose(self) -> None:
            order.append("dispose")
            loop_ids.append(id(asyncio.get_running_loop()))

    async def refuse(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        loop_ids.append(id(asyncio.get_running_loop()))
        raise MigrationHeadError("migration_head_behind", "database schema is behind this release")

    def never_build(*_args: object, **_kwargs: object) -> object:
        order.append("build_application")
        raise AssertionError("application must not be built after migration head failure")

    monkeypatch.setattr(
        telegram_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(telegram_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(telegram_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(telegram_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(telegram_cli, "require_migration_head", refuse)
    monkeypatch.setattr(telegram_cli, "create_session_factory", never_build)
    monkeypatch.setattr(telegram_cli, "build_long_polling_application", never_build)
    monkeypatch.setattr(telegram_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))

    with pytest.raises(MigrationHeadError) as excinfo:
        telegram_cli.main()

    assert excinfo.value.code == "migration_head_behind"
    assert order == ["require_migration_head", "dispose"]
    assert loop_ids[0] == loop_ids[1]
    application.run_polling.assert_not_called()


@pytest.mark.anyio
async def test_telegram_delivery_composes_and_disposes(monkeypatch: MonkeyPatch) -> None:
    settings = Settings(environment="test")
    engine = MagicMock()
    engine.dispose = AsyncMock()
    bot = MagicMock()
    order: list[str] = []

    class BotContext:
        async def __aenter__(self) -> object:
            return bot

        async def __aexit__(self, *_args: object) -> None:
            return None

    async def pass_head(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        return object()

    monkeypatch.setattr(
        delivery_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(delivery_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(delivery_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(delivery_cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(delivery_cli, "require_migration_head", pass_head)
    monkeypatch.setattr(delivery_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(delivery_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))
    monkeypatch.setattr(delivery_cli, "Bot", lambda _token: BotContext())
    delivery_service = MagicMock()
    monkeypatch.setattr(delivery_cli, "TelegramDeliveryService", delivery_service)
    loop = AsyncMock()

    async def run_loop(*_args: object, **_kwargs: object) -> None:
        order.append("run_delivery_loop")
        await loop(*_args, **_kwargs)

    monkeypatch.setattr(delivery_cli, "run_delivery_loop", run_loop)
    monkeypatch.setattr(signal, "signal", lambda *_args: None)

    await delivery_cli.run()
    assert order == ["require_migration_head", "run_delivery_loop"]
    loop.assert_awaited_once()
    assert delivery_service.call_args.kwargs["trace_enabled"] is True
    assert delivery_service.call_args.kwargs["trace_sample_percent"] == 100
    assert delivery_service.call_args.kwargs["trace_always_include_anomalies"] is True
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_telegram_delivery_fails_closed_before_loop_when_migration_head_refuses(
    monkeypatch: MonkeyPatch,
) -> None:
    from vuzol.storage.migration_preflight import MigrationHeadError

    settings = Settings(environment="test")
    order: list[str] = []

    class Engine:
        async def dispose(self) -> None:
            order.append("dispose")

    async def refuse(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        raise MigrationHeadError(
            "migration_head_mismatch",
            "database revision set does not match this code tree heads",
        )

    async def never_loop(*_args: object, **_kwargs: object) -> None:
        order.append("run_delivery_loop")
        raise AssertionError("delivery loop must not start after migration head failure")

    monkeypatch.setattr(
        delivery_cli,
        "get_runtime_configuration",
        lambda **_kwargs: runtime_configuration(settings),
    )
    monkeypatch.setattr(delivery_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(delivery_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(delivery_cli, "create_engine", lambda *_args: Engine())
    monkeypatch.setattr(delivery_cli, "require_migration_head", refuse)
    monkeypatch.setattr(delivery_cli, "run_delivery_loop", never_loop)
    monkeypatch.setattr(delivery_cli, "resolve_bot_token", lambda _settings: SecretStr("token"))
    monkeypatch.setattr(signal, "signal", lambda *_args: None)

    with pytest.raises(MigrationHeadError) as excinfo:
        await delivery_cli.run()

    assert excinfo.value.code == "migration_head_mismatch"
    assert order == ["require_migration_head", "dispose"]
