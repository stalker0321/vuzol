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
    OpenRouterProviderRouting,
    ProviderProfileConfig,
    RegistryDocument,
    RuntimeConfiguration,
    ScopedSecretResolver,
    Settings,
    build_bundle,
)
from vuzol.config.models import Capability
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


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["assistant", "inbox"])
@pytest.mark.parametrize("enabled", [False, True])
async def test_workers_wire_rich_adapter_only_when_enabled(tmp_path, monkeypatch, kind, enabled):
    from vuzol.config.settings import AssistantSettings, InboxSettings

    configured = runtime(tmp_path)
    configured = RuntimeConfiguration(
        settings=configured.settings.model_copy(
            update={
                "assistant": AssistantSettings(profile_id="interpreter"),
                "inbox": InboxSettings(profile_id="interpreter"),
                "chat": configured.settings.chat.model_copy(update={"rich_messages": enabled}),
            }
        ),
        registries=configured.registries,
    )
    monkeypatch.setattr(interpreter_cli, "build_model_chain", lambda *args, **kwargs: object())
    bot = AsyncMock()
    bot.do_api_request.return_value = {"message_id": 17}
    builder = getattr(interpreter_cli, f"build_{kind}_worker")
    worker = builder(configured, object(), object(), bot, None, "test")
    if not enabled:
        assert worker._send_rich is None
        return
    kwargs = {"chat_id": -100, "html": "<h1>Ответ</h1>", "reply_to": 5}
    if kind == "assistant":
        kwargs["thread_id"] = 10
    assert await worker._send_rich(**kwargs) == 17
    assert bot.do_api_request.await_args.args == ("sendRichMessage",)
    payload = bot.do_api_request.await_args.kwargs["api_kwargs"]
    assert payload["rich_message"] == {"html": "<h1>Ответ</h1>"}
    assert payload["message_thread_id"] == (10 if kind == "assistant" else None)
    assert payload["reply_parameters"]["message_id"] == 5


@pytest.mark.parametrize("monitor_enabled", [False, True])
@pytest.mark.parametrize(
    "assistant_enabled,inbox_enabled", [(False, False), (True, False), (False, True), (True, True)]
)
def test_interpreter_runtime_composes_and_stops_cleanly(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    monitor_enabled: bool,
    assistant_enabled: bool,
    inbox_enabled: bool,
) -> None:
    from unittest.mock import Mock

    from vuzol.config.settings import AssistantSettings, InboxSettings, MonitorSettings

    configured = runtime(tmp_path)
    configured = RuntimeConfiguration(
        settings=configured.settings.model_copy(
            update={
                "assistant": AssistantSettings(
                    enabled=assistant_enabled,
                    profile_id="interpreter",
                    chat_id=-100,
                    message_thread_id=8,
                ),
                "inbox": InboxSettings(enabled=inbox_enabled, profile_id="interpreter"),
                "monitor": MonitorSettings(
                    enabled=monitor_enabled,
                    chat_id=-100,
                    message_thread_id=7,
                    profile_ids=("interpreter",),
                ),
            }
        ),
        registries=configured.registries,
    )
    monitor_builder = Mock(return_value=Mock(process_one=AsyncMock(return_value=False)))
    monkeypatch.setattr(interpreter_cli, "build_monitor_worker", monitor_builder)
    assistant_builder = Mock(return_value=Mock(process_one=AsyncMock(return_value=False)))
    inbox_builder = Mock(return_value=Mock(process_one=AsyncMock(return_value=False)))
    monkeypatch.setattr(interpreter_cli, "build_assistant_worker", assistant_builder)
    monkeypatch.setattr(interpreter_cli, "build_inbox_worker", inbox_builder)
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
    assert monitor_builder.call_count == int(monitor_enabled)
    assert assistant_builder.call_count == int(assistant_enabled)
    assert inbox_builder.call_count == int(inbox_enabled)


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


def _bundle_with_profile(profile: ProviderProfileConfig, tmp_path: Path) -> Any:
    settings = Settings(
        environment="test",
        database_dsn_reference="env:DB_DSN",
        telegram_bot_token_reference="env:BOT_TOKEN",  # noqa: S106  # pragma: allowlist secret
        repository_root=tmp_path / "repositories",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
        interpretation=InterpretationSettings(profile_id=profile.id),
    )
    settings.repository_root.mkdir(exist_ok=True)
    return build_bundle(
        RegistryDocument(profiles=(profile,)),
        settings,
        environment={
            "DB_DSN": "postgresql+psycopg://user:pass@db/vuzol",  # pragma: allowlist secret
            "BOT_TOKEN": "telegram-token",
            "MODEL_KEY": "model-key",
        },
    )


def test_interpreter_builder_wires_profile_limits_reasoning_and_routing(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    routing = OpenRouterProviderRouting.model_validate(
        {
            "sort": {"by": "price", "partition": "none"},
            "preferred_min_throughput": {"p90": 70},
            "quantizations": ["int8"],
        }
    )
    profile = ProviderProfileConfig(
        id="interpreter",
        provider="openai-compatible",
        model="deepseek/deepseek-v4-flash-0731",
        api_base_url=HttpUrl("https://openrouter.ai/api/v1"),
        launch_mode=LaunchMode.API,
        credential_reference="env:MODEL_KEY",
        output_limit=1_000,
        reasoning_enabled=False,
        provider_routing=routing,
        capabilities=frozenset(),
        concurrency_limit=1,
        cost_class=CostClass.CHEAP,
        supported_task_types=frozenset({"general"}),
        sandbox_required=False,
    )
    bundle = _bundle_with_profile(profile, tmp_path)
    monkeypatch.setenv("MODEL_KEY", "model-key")  # pragma: allowlist secret
    resolver = ScopedSecretResolver(
        access_policy={"env:MODEL_KEY": frozenset({"profile:interpreter"})},
        secret_file_root=tmp_path,
    )

    adapter = interpreter_cli.build_profile_interpreter(
        bundle,
        resolver,
        profile_id="interpreter",
        timeout_seconds=7.5,
    )

    assert adapter._base_url == "https://openrouter.ai/api/v1"
    assert adapter._credential.get_secret_value() == "model-key"
    assert adapter._output_token_limit == 1_000
    assert adapter._reasoning_enabled is False
    assert adapter._provider_routing is routing
    assert adapter._timeout == 7.5


def test_transcriber_builder_wires_transcription_timeout(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    profile = ProviderProfileConfig(
        id="transcriber",
        provider="openai-compatible",
        model="gpt-4o-transcribe",
        api_base_url=HttpUrl("https://api.openai.com/v1"),
        launch_mode=LaunchMode.API,
        credential_reference="env:MODEL_KEY",
        capabilities=frozenset({Capability.TRANSCRIPTION}),
        concurrency_limit=1,
        cost_class=CostClass.CHEAP,
        supported_task_types=frozenset({"file_processing"}),
        sandbox_required=False,
    )
    bundle = _bundle_with_profile(profile, tmp_path)
    monkeypatch.setenv("MODEL_KEY", "model-key")  # pragma: allowlist secret
    resolver = ScopedSecretResolver(
        access_policy={"env:MODEL_KEY": frozenset({"profile:transcriber"})},
        secret_file_root=tmp_path,
    )

    transcriber = interpreter_cli.build_profile_transcriber(
        bundle,
        resolver,
        profile_id="transcriber",
        timeout_seconds=30.0,
    )

    assert transcriber._base_url == "https://api.openai.com/v1"
    assert transcriber._credential.get_secret_value() == "model-key"
    assert transcriber._timeout == 30.0


def test_builder_rejects_non_api_profiles_and_missing_credentials(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    cli_profile = ProviderProfileConfig(
        id="codex-executor",
        provider="codex",
        model="gpt-5.6-sol",
        launch_mode=LaunchMode.CLI,
        runtime_identity="vuzol-executor",
        state_directory=tmp_path / "state",
        concurrency_limit=1,
        credential_required=False,
        enabled=False,
        capabilities=frozenset(),
        cost_class=CostClass.STRONG,
        supported_task_types=frozenset({"coding"}),
        sandbox_required=True,
    )
    no_credential_profile = ProviderProfileConfig(
        id="keyless",
        provider="openai-compatible",
        model="model",
        api_base_url=HttpUrl("https://provider.example/v1"),
        launch_mode=LaunchMode.API,
        credential_required=False,
        capabilities=frozenset(),
        concurrency_limit=1,
        cost_class=CostClass.CHEAP,
        supported_task_types=frozenset({"general"}),
        sandbox_required=False,
    )
    resolver = ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path)

    with pytest.raises(ValueError, match="unsupported interpreter profile"):
        interpreter_cli.build_profile_interpreter(
            _bundle_with_profile(cli_profile, tmp_path),
            resolver,
            profile_id="codex-executor",
            timeout_seconds=1.0,
        )
    with pytest.raises(ValueError, match="interpreter profile has no credential"):
        interpreter_cli.build_profile_interpreter(
            _bundle_with_profile(no_credential_profile, tmp_path),
            resolver,
            profile_id="keyless",
            timeout_seconds=1.0,
        )
    with pytest.raises(ValueError, match="unsupported transcription profile"):
        interpreter_cli.build_profile_transcriber(
            _bundle_with_profile(cli_profile, tmp_path),
            resolver,
            profile_id="codex-executor",
            timeout_seconds=1.0,
        )


@pytest.mark.anyio
async def test_monitor_builder_uses_model_chain_and_monitor_topic(tmp_path, monkeypatch):
    from vuzol.config.settings import MonitorSettings

    configured = runtime(tmp_path)
    configured = RuntimeConfiguration(
        settings=configured.settings.model_copy(
            update={
                "monitor": MonitorSettings(
                    enabled=True,
                    chat_id=-100,
                    message_thread_id=7,
                    profile_ids=("interpreter", "spare"),
                ),
            }
        ),
        registries=configured.registries,
    )
    calls = []
    model = object()

    def chain(*args, **kwargs):
        calls.append(kwargs)
        return model

    monkeypatch.setattr(interpreter_cli, "build_model_chain", chain)
    bot = AsyncMock()
    bot.send_message.return_value.message_id = 12
    bot.do_api_request.return_value = {"message_id": 17}
    worker = interpreter_cli.build_monitor_worker(configured, object(), object(), bot)
    assert worker._model is model
    assert worker._timezone == configured.settings.assistant.timezone
    assert calls[0]["profile_id"] == "interpreter"
    assert calls[0]["fallback_profile_ids"] == ("spare",)
    assert calls[0]["purpose"] == "monitor_classification"
    assert await worker._send(chat_id=-100, thread_id=7, html="Текст") == 12
    assert bot.send_message.await_args.kwargs["message_thread_id"] == 7
    assert await worker._send_rich(chat_id=-100, thread_id=7, html="<p>Текст</p>") == 17
    assert bot.do_api_request.await_args.kwargs["api_kwargs"]["message_thread_id"] == 7


@pytest.mark.parametrize("builder", ["build_inbox_model", "build_profile_transcriber"])
@pytest.mark.parametrize("problem", ["provider", "url", "credential"])
def test_model_builders_refuse_unusable_profiles(tmp_path, builder, problem):
    from types import SimpleNamespace
    from unittest.mock import Mock

    profile = SimpleNamespace(
        id="broken",
        provider="openai-compatible",
        api_base_url="https://example.test/v1",
        credential_reference="env:KEY",
    )
    setattr(
        profile,
        {"provider": "provider", "url": "api_base_url", "credential": "credential_reference"}[
            problem
        ],
        "codex" if problem == "provider" else None,
    )
    registries = Mock()
    registries.profiles.get.return_value = profile
    resolver = Mock()
    with pytest.raises(
        ValueError, match="no credential" if problem == "credential" else "unsupported"
    ):
        getattr(interpreter_cli, builder)(
            registries, resolver, profile_id="broken", timeout_seconds=1
        )
    resolver.get.assert_not_called()


@pytest.mark.parametrize("output_limit,org_id", [(None, None), (1234, "org-17")])
def test_inbox_model_keeps_account_header_and_output_budget(tmp_path, output_limit, org_id):
    from unittest.mock import Mock

    configured = runtime(tmp_path)
    profile = configured.registries.profiles.get("interpreter").model_copy(
        update={"output_limit": output_limit}
    )
    registries = Mock()
    registries.profiles.get.return_value = profile
    resolver = Mock()
    resolver.get.return_value = SecretStr("test-key")
    model = interpreter_cli.build_inbox_model(
        registries, resolver, profile_id=profile.id, timeout_seconds=9, org_id=org_id
    )
    assert model._output_tokens == (output_limit or 8000)
    assert model._headers == ({"x-opencode-org-id": org_id} if org_id else {})
    resolver.get.assert_called_once_with("env:MODEL_KEY", "profile:interpreter")


@pytest.mark.parametrize("provider", ["codex", "pi"])
@pytest.mark.parametrize("factory_present", [False, True])
def test_model_chain_relays_cli_only_with_executor_factory(
    tmp_path, monkeypatch, provider, factory_present
):
    from unittest.mock import Mock

    configured = runtime(tmp_path)
    profile = configured.registries.profiles.get("interpreter").model_copy(
        update={"provider": provider, "launch_mode": LaunchMode.CLI}
    )
    registries = Mock()
    registries.profiles.get.return_value = profile
    configured = RuntimeConfiguration.model_construct(
        settings=configured.settings, registries=registries
    )
    factory = object() if factory_present else None
    api = Mock(return_value=Mock(model="api"))
    relay = Mock(return_value=Mock(model="relay"))
    monkeypatch.setattr(interpreter_cli, "build_inbox_model", api)
    monkeypatch.setattr(interpreter_cli, "RelayedModel", relay)
    chain = interpreter_cli.build_model_chain(
        configured,
        object(),
        profile_id="interpreter",
        fallback_profile_ids=(),
        org_ids={"interpreter": "org"},
        timeout_seconds=7,
        factory=factory,
        purpose="assistant_turn",
        web_search=True,
        mcp_servers={"docs": "https://docs.test"},
    )
    assert chain.model == ("relay" if factory_present else "api")
    if factory_present:
        api.assert_not_called()
        assert relay.call_args.args == (factory,)
        assert relay.call_args.kwargs["web_search"] is (provider == "codex")
        assert relay.call_args.kwargs["mcp_servers"] == (
            {"docs": "https://docs.test"} if provider == "codex" else None
        )
        assert relay.call_args.kwargs["purpose"] == "assistant_turn"
    else:
        relay.assert_not_called()
        assert api.call_args.kwargs["org_id"] == "org"


def test_model_chain_skips_unavailable_spares_but_refuses_missing_primary(
    tmp_path, monkeypatch, caplog
):
    from unittest.mock import Mock

    configured = runtime(tmp_path)
    registries = Mock()
    registries.profiles.get.return_value = configured.registries.profiles.get("interpreter")
    configured = configured.model_copy(update={"registries": registries})
    models = [
        Mock(model="primary"),
        ValueError("key missing"),
        LookupError("unknown"),
        Mock(model="spare"),
    ]
    build = Mock(side_effect=models)
    monkeypatch.setattr(interpreter_cli, "build_inbox_model", build)
    chain = interpreter_cli.build_model_chain(
        configured,
        object(),
        profile_id="interpreter",
        fallback_profile_ids=("nokey", "unknown", "spare"),
        org_ids={},
        timeout_seconds=3,
    )
    assert [model.model for model in chain._models] == ["primary", "spare"]
    assert [
        r.profile_id for r in caplog.records if r.message == "fallback profile unavailable"
    ] == ["nokey", "unknown"]
    build.side_effect = ValueError("primary credential missing")
    with pytest.raises(ValueError, match="primary credential missing"):
        interpreter_cli.build_model_chain(
            configured,
            object(),
            profile_id="interpreter",
            fallback_profile_ids=("spare",),
            org_ids={},
            timeout_seconds=3,
        )


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["assistant", "inbox"])
@pytest.mark.parametrize("reply_to", [None, 8])
async def test_worker_callbacks_preserve_reply_markup_and_transcription(
    tmp_path, monkeypatch, kind, reply_to
):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from telegram.error import TelegramError

    from vuzol.config.settings import AssistantSettings, InboxSettings

    configured = runtime(tmp_path)
    configured = RuntimeConfiguration.model_construct(
        settings=configured.settings.model_copy(
            update={
                "assistant": AssistantSettings(profile_id="interpreter"),
                "inbox": InboxSettings(profile_id="interpreter"),
                "project_setup": configured.settings.project_setup.model_copy(
                    update={"enabled": True}
                ),
            }
        ),
        registries=configured.registries,
    )
    monkeypatch.setattr(interpreter_cli, "build_model_chain", Mock())
    monkeypatch.setattr(interpreter_cli, "transcription_keywords", lambda _path: ("Vuzol",))
    bot = AsyncMock()
    bot.send_message.return_value.message_id = 23
    bot.create_forum_topic.return_value.message_thread_id = 19
    transcriber = AsyncMock()
    transcriber.transcribe.return_value = SimpleNamespace(transcript="spoken request")
    worker = getattr(interpreter_cli, f"build_{kind}_worker")(
        configured, object(), object(), bot, transcriber, "owner"
    )
    kwargs = dict(
        chat_id=-100,
        html="<b>answer</b>",
        reply_to=reply_to,
        buttons=[[["Run", "run:1"]]] if reply_to else [],
    )
    if kind == "assistant":
        kwargs["thread_id"] = 7
    assert await worker._send(**kwargs) == 23
    sent = bot.send_message.await_args.kwargs
    assert sent["text"] == "<b>answer</b>"
    assert sent["parse_mode"] == "HTML"
    assert sent["link_preview_options"].is_disabled
    if reply_to:
        assert sent["reply_parameters"].message_id == reply_to
        assert sent["reply_parameters"].allow_sending_without_reply
        assert sent["reply_markup"].inline_keyboard[0][0].callback_data == "run:1"
    else:
        assert sent["reply_parameters"] is None
        assert sent["reply_markup"] is None
    assert await worker._transcribe(b"voice", "audio/ogg") == "spoken request"
    request = transcriber.transcribe.await_args.args[0]
    assert (request.content, request.media_type, request.keywords) == (
        b"voice",
        "audio/ogg",
        ("Vuzol",),
    )
    if kind == "assistant":
        assert await worker._create_topic(chat_id=-100, name="New project") == 19
        bot.create_forum_topic.assert_awaited_once_with(chat_id=-100, name="New project")
        bot.edit_message_reply_markup.side_effect = TelegramError("already cleared")
        await worker._clear_buttons(chat_id=-100, message_id=23)
        bot.edit_message_reply_markup.assert_awaited_once_with(
            chat_id=-100, message_id=23, reply_markup=None
        )


@pytest.mark.anyio
async def test_poll_logs_bad_item_then_continues_processing(monkeypatch, caplog):
    stop = asyncio.Event()
    calls = 0

    async def process():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("bad item")
        if calls == 3:
            stop.set()
        return calls == 2

    await asyncio.wait_for(interpreter_cli._poll(process, stop, 0.001), timeout=1)
    assert calls == 3
    assert any(getattr(r, "event", None) == "inbox.loop_error" for r in caplog.records)


@pytest.mark.anyio
async def test_assistant_lists_only_accepting_projects_and_submits_scoped_request(
    tmp_path, monkeypatch
):
    import uuid
    from types import SimpleNamespace
    from unittest.mock import Mock

    from vuzol.config.models import TopicKind
    from vuzol.config.settings import AssistantSettings, InboxSettings

    configured = runtime(tmp_path)
    configured = RuntimeConfiguration.model_construct(
        settings=configured.settings.model_copy(
            update={
                "assistant": AssistantSettings(
                    profile_id="interpreter", research_profile_ids=("research", "spare")
                ),
                "inbox": InboxSettings(searxng_url="https://search.test"),
            }
        ),
        registries=Mock(),
    )

    def topic(**overrides):
        return SimpleNamespace(
            **(
                dict(
                    kind=TopicKind.PROJECT,
                    project_id="active",
                    enabled=True,
                    accepts_new_tasks=True,
                    display_name=None,
                )
                | overrides
            )
        )

    configured.registries.topics.items.return_value = [
        topic(kind=TopicKind.INBOX),
        topic(project_id=None),
        topic(enabled=False),
        topic(accepts_new_tasks=False),
        topic(project_id="disabled"),
        topic(display_name="Topic name"),
        topic(display_name="Duplicate"),
        topic(project_id="other"),
    ]
    configured.registries.projects.get.side_effect = lambda key: SimpleNamespace(
        id=key, enabled=key != "disabled", display_name="Project name"
    )
    application = Mock(submit=AsyncMock(return_value={"accepted": True}))
    monkeypatch.setattr(interpreter_cli, "ProjectApplication", Mock(return_value=application))
    chain = Mock()
    monkeypatch.setattr(interpreter_cli, "build_model_chain", chain)
    worker = interpreter_cli.build_assistant_worker(
        configured, object(), object(), AsyncMock(), None, "owner"
    )
    assert [(p.project_id, p.name) for p in worker._projects()] == [
        ("active", "Topic name"),
        ("other", "Project name"),
    ]
    assert worker._create_topic is None
    assert worker._transcribe is None
    assert worker._researcher is not None
    assert chain.call_args_list[0].kwargs["purpose"] == "assistant_research"
    assert chain.call_args_list[0].kwargs["fallback_profile_ids"] == ("spare",)
    session_id, request_id = uuid.uuid4(), uuid.uuid4()
    assert await worker._submit(
        user_id=17,
        project_id="active",
        session_id=session_id,
        request_id=request_id,
        text="Build it",
    ) == {"accepted": True}
    access, request = application.submit.await_args.args
    assert access.user_id == 17 and access.projects == {"active": session_id}
    assert access.allow_submit
    assert request.project_id == "active" and request.request_id == request_id
    assert request.request == "Build it"
    assert application.submit.await_args.kwargs["preapproved"] is True
    assert application.submit.await_args.kwargs["origin"].channel == "telegram_assistant"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "case,reason",
    [
        ("no_profile", "interpretation.profile_id is required"),
        ("no_credential", "Jev profile requires a credential reference"),
        ("fallback", "Jev intake must not fall back"),
        ("no_canary", "Jev intake is not enabled"),
    ],
)
async def test_runtime_refuses_incomplete_intake_before_opening_database(
    tmp_path, monkeypatch, case, reason
):
    from unittest.mock import Mock

    configured = runtime(tmp_path)
    config = configured.settings.interpretation.model_copy(
        update={
            "profile_id": None if case == "no_profile" else "interpreter",
            "fallback_profile_ids": ("spare",) if case == "fallback" else (),
        }
    )
    registries = Mock()
    registries.profiles.items.return_value = ()
    registries.profiles.get.return_value = configured.registries.profiles.get(
        "interpreter"
    ).model_copy(
        update={
            "provider": "jev",
            "credential_reference": None if case == "no_credential" else "env:KEY",
        }
    )
    configured = RuntimeConfiguration.model_construct(
        settings=configured.settings.model_copy(update={"interpretation": config}),
        registries=registries,
    )
    monkeypatch.setattr(interpreter_cli, "get_runtime_configuration", lambda **kw: configured)
    monkeypatch.setattr(interpreter_cli, "configure_logging", Mock())
    monkeypatch.setattr(interpreter_cli, "canary_policy_from_settings", lambda _settings: None)
    engine = Mock()
    monkeypatch.setattr(interpreter_cli, "create_engine", engine)
    with pytest.raises(ValueError, match=reason):
        await interpreter_cli.run()
    engine.assert_not_called()


@pytest.mark.anyio
async def test_jev_runtime_keeps_canary_admission_and_structurer_binding(tmp_path, monkeypatch):
    from unittest.mock import Mock

    configured = runtime(tmp_path)
    primary = configured.registries.profiles.get("interpreter").model_copy(
        update={"provider": "jev"}
    )
    registries = Mock()
    registries.profiles.get.return_value = primary
    registries.profiles.items.return_value = (primary,)
    config = configured.settings.interpretation.model_copy(
        update={
            "structure_profile_id": "structurer",
            "transcription_profile_id": "voice",
            "automatic_execution_enabled": True,
            "evaluation_report_file": tmp_path / "evaluation.json",
        }
    )
    configured = configured.model_copy(
        update={
            "registries": registries,
            "settings": configured.settings.model_copy(update={"interpretation": config}),
        }
    )
    handlers = {}
    pipeline = Mock()

    async def process_one():
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return False

    pipeline.process_one = process_one
    canary = Mock()
    canary.admit.return_value.allowed = False
    jev, structurer, transcriber, gate = Mock(), Mock(), Mock(), Mock()
    engine = FakeEngine()
    monkeypatch.setenv("MODEL_KEY", "test-key")
    monkeypatch.setattr(interpreter_cli, "get_runtime_configuration", lambda **kw: configured)
    monkeypatch.setattr(interpreter_cli, "configure_logging", Mock())
    monkeypatch.setattr(interpreter_cli, "require_eligible_report", gate)
    monkeypatch.setattr(interpreter_cli, "canary_policy_from_settings", lambda _settings: canary)
    monkeypatch.setattr(interpreter_cli, "JevDecisionsInterpreter", jev)
    monkeypatch.setattr(interpreter_cli, "build_profile_interpreter", structurer)
    monkeypatch.setattr(interpreter_cli, "build_profile_transcriber", transcriber)
    monkeypatch.setattr(interpreter_cli, "create_engine", lambda *args: engine)
    monkeypatch.setattr(interpreter_cli, "resolve_database_dsn", Mock())
    monkeypatch.setattr(
        interpreter_cli, "resolve_bot_token", lambda _settings: SecretStr("telegram-token")
    )
    monkeypatch.setattr(interpreter_cli, "require_migration_head", AsyncMock())
    monkeypatch.setattr(interpreter_cli, "create_session_factory", Mock())
    monkeypatch.setattr(interpreter_cli, "Bot", FakeBot)
    builder = Mock(return_value=pipeline)
    monkeypatch.setattr(interpreter_cli, "InterpretationPipeline", builder)
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.update({signum: handler})
    )
    await interpreter_cli.run()
    gate.assert_called_once_with(config.evaluation_report_file)
    assert jev.call_args.kwargs["structurer"] is structurer.return_value
    assert structurer.call_args.kwargs["profile_id"] == "structurer"
    assert not jev.call_args.kwargs["admitted"]("opportunity-17")
    canary.admit.assert_called_once_with(decision_kind="intake", opportunity_id="opportunity-17")
    assert builder.call_args.kwargs["interpreter"] is jev.return_value
    assert builder.call_args.kwargs["transcriber"] is transcriber.return_value
    assert builder.call_args.kwargs["fallback_interpreters"] == ()
    assert engine.disposed
