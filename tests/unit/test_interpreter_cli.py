import asyncio
import signal
from pathlib import Path
from typing import Any

from pydantic import HttpUrl, SecretStr
from pytest import MonkeyPatch

from vuzol.cli import interpreter as interpreter_cli
from vuzol.config import (
    InterpretationSettings,
    LaunchMode,
    ProviderProfileConfig,
    RegistryDocument,
    RuntimeConfiguration,
    Settings,
    build_bundle,
)


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
                cost_class="cheap",
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

    monkeypatch.setenv("MODEL_KEY", "model-key")
    monkeypatch.setattr(interpreter_cli, "get_runtime_configuration", lambda: configured)
    monkeypatch.setattr(interpreter_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(interpreter_cli, "create_engine", lambda *_args: engine)
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
    assert engine.disposed
