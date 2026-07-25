import asyncio
import signal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import HttpUrl, SecretStr
from pytest import MonkeyPatch

from vuzol.cli import interpreter as interpreter_cli
from vuzol.config import (
    CostClass,
    InterpretationSettings,
    LaunchMode,
    ProviderProfileConfig,
    RegistryDocument,
    RuntimeConfiguration,
    Settings,
    build_bundle,
)
from vuzol.storage.migration_preflight import MigrationHeadError


class FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class FakeBot:
    def __init__(self, token: str) -> None:
        assert token == "telegram-token"  # noqa: S105  # pragma: allowlist secret

    async def __aenter__(self) -> "FakeBot":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


def runtime(tmp_path: Path) -> RuntimeConfiguration:
    settings = Settings(
        environment="test",
        database_dsn_reference="env:DB_DSN",
        telegram_bot_token_reference="env:BOT_TOKEN",  # noqa: S106  # pragma: allowlist secret
        repository_root=tmp_path / "repositories",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
        interpretation=InterpretationSettings(profile_id="interpreter"),
    )
    settings.repository_root.mkdir()
    document = RegistryDocument(
        profiles=(
            ProviderProfileConfig(
                id="interpreter",
                provider="openai-compatible",
                model="cheap-model",
                api_base_url=HttpUrl("https://provider.example/v1"),
                launch_mode=LaunchMode.API,
                credential_reference="env:MODEL_KEY",
                capabilities=frozenset(),
                concurrency_limit=1,
                cost_class=CostClass.CHEAP,
                supported_task_types=frozenset({"general"}),
                sandbox_required=False,
            ),
        )
    )
    bundle = build_bundle(
        document,
        settings,
        environment={
            "DB_DSN": "postgresql+psycopg://user:pass@db/vuzol",  # pragma: allowlist secret
            "BOT_TOKEN": "telegram-token",
            "MODEL_KEY": "model-key",
        },
    )
    return RuntimeConfiguration(settings=settings, registries=bundle)


def test_interpreter_runtime_composes_and_stops_cleanly(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    configured = runtime(tmp_path)
    engine = FakeEngine()
    handlers: dict[int, Any] = {}

    class FakePipeline:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def process_one(self) -> bool:
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return True

    migration_head = AsyncMock()
    monkeypatch.setenv("MODEL_KEY", "model-key")
    monkeypatch.setattr(interpreter_cli, "get_runtime_configuration", lambda **_kwargs: configured)
    monkeypatch.setattr(interpreter_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(interpreter_cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(interpreter_cli, "require_migration_head", migration_head)
    monkeypatch.setattr(interpreter_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(interpreter_cli, "resolve_database_dsn", lambda _settings: SecretStr("dsn"))
    monkeypatch.setattr(
        interpreter_cli,
        "resolve_bot_token",
        lambda _settings: SecretStr("telegram-token"),
    )
    monkeypatch.setattr(
        interpreter_cli,
        "OpenAICompatibleInterpreter",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(interpreter_cli, "Bot", FakeBot)
    monkeypatch.setattr(interpreter_cli, "InterpretationPipeline", FakePipeline)
    monkeypatch.setattr(
        signal,
        "signal",
        lambda signum, handler: handlers.update({signum: handler}),
    )

    asyncio.run(interpreter_cli.run())

    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    migration_head.assert_awaited_once_with(engine)
    assert engine.disposed


def test_interpreter_fails_closed_disposes_once_without_pipeline(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    """S-2.2b: gate refusal starts no Bot/pipeline and disposes engine exactly once."""

    configured = runtime(tmp_path)

    class CountingEngine(FakeEngine):
        def __init__(self) -> None:
            super().__init__()
            self.dispose_calls = 0

        async def dispose(self) -> None:
            self.dispose_calls += 1
            await super().dispose()

    engine = CountingEngine()
    order: list[str] = []

    async def refuse(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        raise MigrationHeadError(
            "migration_head_behind",
            "database schema is behind this release",
        )

    def boom_pipeline(*_args: object, **_kwargs: object) -> object:
        order.append("InterpretationPipeline")
        raise AssertionError("pipeline must not start after migration head failure")

    def boom_bot(*_args: object, **_kwargs: object) -> object:
        order.append("Bot")
        raise AssertionError("Bot must not start after migration head failure")

    def boom_factory(*_args: object, **_kwargs: object) -> object:
        order.append("create_session_factory")
        raise AssertionError("session factory must not run after migration head failure")

    monkeypatch.setenv("MODEL_KEY", "model-key")
    monkeypatch.setattr(interpreter_cli, "get_runtime_configuration", lambda **_kwargs: configured)
    monkeypatch.setattr(interpreter_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(interpreter_cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(interpreter_cli, "require_migration_head", refuse)
    monkeypatch.setattr(interpreter_cli, "create_session_factory", boom_factory)
    monkeypatch.setattr(interpreter_cli, "resolve_database_dsn", lambda _settings: SecretStr("dsn"))
    monkeypatch.setattr(
        interpreter_cli,
        "resolve_bot_token",
        lambda _settings: SecretStr("telegram-token"),
    )
    monkeypatch.setattr(
        interpreter_cli,
        "OpenAICompatibleInterpreter",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(interpreter_cli, "Bot", boom_bot)
    monkeypatch.setattr(interpreter_cli, "InterpretationPipeline", boom_pipeline)
    monkeypatch.setattr(signal, "signal", lambda *_args: None)

    with pytest.raises(MigrationHeadError) as excinfo:
        asyncio.run(interpreter_cli.run())

    assert excinfo.value.code == "migration_head_behind"
    assert order == ["require_migration_head"]
    assert engine.dispose_calls == 1
    assert engine.disposed
