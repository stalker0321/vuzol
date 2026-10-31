"""Dedicated Step 05 attachment and semantic interpretation runtime."""

import asyncio
import os
import signal
import socket
from collections.abc import Awaitable, Callable
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import Bot, LinkPreviewOptions, ReplyParameters
from telegram.constants import ParseMode

from vuzol.config import (
    ConfigurationBundle,
    RuntimeConfiguration,
    ScopedSecretResolver,
    get_runtime_configuration,
)
from vuzol.experiments.canary import canary_policy_from_settings
from vuzol.inbox.model import InboxModel
from vuzol.inbox.service import InboxWorker
from vuzol.interpretation.adapters import (
    OpenAICompatibleInterpreter,
    OpenAICompatibleTranscriber,
)
from vuzol.interpretation.domain import TranscriptionInput
from vuzol.interpretation.evaluation import require_eligible_report
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline, transcription_keywords
from vuzol.observability import configure_logging, get_logger
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.telegram.adapter import PythonTelegramClient, resolve_bot_token


def build_profile_interpreter(
    registries: ConfigurationBundle,
    resolver: ScopedSecretResolver,
    *,
    profile_id: str,
    timeout_seconds: float,
) -> OpenAICompatibleInterpreter:
    profile = registries.profiles.get(profile_id)
    if profile.provider != "openai-compatible" or profile.api_base_url is None:
        raise ValueError(f"unsupported interpreter profile: {profile.id}")
    if profile.credential_reference is None:
        raise ValueError(f"interpreter profile has no credential: {profile.id}")
    return OpenAICompatibleInterpreter(
        base_url=str(profile.api_base_url),
        credential=resolver.get(profile.credential_reference, f"profile:{profile.id}"),
        profile_id=profile.id,
        model=profile.model,
        provider_routing=profile.provider_routing,
        timeout_seconds=timeout_seconds,
        output_token_limit=profile.output_limit,
        reasoning_enabled=profile.reasoning_enabled,
    )


def build_profile_transcriber(
    registries: ConfigurationBundle,
    resolver: ScopedSecretResolver,
    *,
    profile_id: str,
    timeout_seconds: float,
) -> OpenAICompatibleTranscriber:
    profile = registries.profiles.get(profile_id)
    if profile.provider != "openai-compatible" or profile.api_base_url is None:
        raise ValueError(f"unsupported transcription profile: {profile.id}")
    if profile.credential_reference is None:
        raise ValueError(f"transcription profile has no credential: {profile.id}")
    return OpenAICompatibleTranscriber(
        base_url=str(profile.api_base_url),
        credential=resolver.get(profile.credential_reference, f"profile:{profile.id}"),
        profile_id=profile.id,
        model=profile.model,
        timeout_seconds=timeout_seconds,
    )


def build_inbox_model(
    registries: ConfigurationBundle,
    resolver: ScopedSecretResolver,
    *,
    profile_id: str,
    timeout_seconds: float,
) -> InboxModel:
    profile = registries.profiles.get(profile_id)
    if profile.provider != "openai-compatible" or profile.api_base_url is None:
        raise ValueError(f"unsupported inbox profile: {profile.id}")
    if profile.credential_reference is None:
        raise ValueError(f"inbox profile has no credential: {profile.id}")
    return InboxModel(
        base_url=str(profile.api_base_url),
        credential=resolver.get(profile.credential_reference, f"profile:{profile.id}"),
        model=profile.model,
        timeout_seconds=timeout_seconds,
        output_tokens=profile.output_limit or 8_000,
    )


def build_inbox_worker(
    runtime: RuntimeConfiguration,
    resolver: ScopedSecretResolver,
    factory: async_sessionmaker[AsyncSession],
    bot: Bot,
    transcriber: OpenAICompatibleTranscriber | None,
    owner: str,
) -> InboxWorker:
    config = runtime.settings.inbox
    assert config.profile_id is not None
    fast = build_inbox_model(
        runtime.registries,
        resolver,
        profile_id=config.profile_id,
        timeout_seconds=config.timeout_seconds,
    )
    deep = (
        build_inbox_model(
            runtime.registries,
            resolver,
            profile_id=config.deep_profile_id,
            timeout_seconds=config.timeout_seconds,
        )
        if config.deep_profile_id
        else None
    )
    client = PythonTelegramClient(bot)
    interpretation = runtime.settings.interpretation

    async def send(*, chat_id: int, html: str, reply_to: int | None) -> int:
        message = await bot.send_message(
            chat_id=chat_id,
            text=html,
            parse_mode=ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            reply_parameters=(
                ReplyParameters(message_id=reply_to, allow_sending_without_reply=True)
                if reply_to
                else None
            ),
        )
        return message.message_id

    async def transcribe(content: bytes, media_type: str) -> str:
        assert transcriber is not None
        result = await transcriber.transcribe(
            TranscriptionInput(
                content=content,
                media_type=media_type,
                language_hint=interpretation.language_hint,
                keywords=transcription_keywords(interpretation.transcription_keywords_file),
            )
        )
        return result.transcript

    return InboxWorker(
        factory,
        config,
        fast=fast,
        deep=deep,
        send=send,
        download=client.download,
        transcribe=transcribe if transcriber is not None else None,
        owner=owner,
    )


async def _poll(
    process_one: Callable[[], Awaitable[bool]], stop_event: asyncio.Event, interval: float
) -> None:
    while not stop_event.is_set():
        try:
            processed = await process_one()
        except Exception:  # a loop must survive one bad item
            get_logger(__name__).exception("inbox loop error", extra={"event": "inbox.loop_error"})
            processed = False
        if not processed:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=interval)


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-interpreter", level=settings.log_level)
    config = settings.interpretation
    if config.automatic_execution_enabled:
        assert config.evaluation_report_file is not None
        require_eligible_report(config.evaluation_report_file)
    if config.profile_id is None:
        raise ValueError("interpretation.profile_id is required for the interpreter runtime")
    primary_profile = runtime.registries.profiles.get(config.profile_id)
    fallback_ids = config.fallback_profile_ids or primary_profile.fallback_profile_ids
    resolver = ScopedSecretResolver(
        access_policy={
            profile.credential_reference: frozenset({f"profile:{profile.id}"})
            for profile in runtime.registries.profiles.items()
            if profile.credential_reference is not None
        },
        secret_file_root=settings.secret_file_root,
    )

    interpreter: JevDecisionsInterpreter | OpenAICompatibleInterpreter
    if primary_profile.provider == "jev":
        if primary_profile.credential_reference is None:
            raise ValueError("Jev profile requires a credential reference")
        if fallback_ids:
            raise ValueError("Jev intake must not fall back to another classifier")
        canary = canary_policy_from_settings(settings)
        if canary is None:
            raise ValueError("Jev intake is not enabled")
        structurer = None
        if config.structure_profile_id is not None:
            structurer = build_profile_interpreter(
                runtime.registries,
                resolver,
                profile_id=config.structure_profile_id,
                timeout_seconds=config.provider_timeout_seconds,
            )
        interpreter = JevDecisionsInterpreter(
            credential=resolver.get(
                primary_profile.credential_reference, f"profile:{primary_profile.id}"
            ),
            profile_id=primary_profile.id,
            model=primary_profile.model,
            timeout_seconds=config.provider_timeout_seconds,
            admitted=lambda opportunity: (
                canary.admit(
                    decision_kind="intake",
                    opportunity_id=opportunity,
                ).allowed
            ),
            structurer=structurer,
            manual_project_creation=config.manual_project_creation,
        )
    else:
        interpreter = build_profile_interpreter(
            runtime.registries,
            resolver,
            profile_id=primary_profile.id,
            timeout_seconds=config.provider_timeout_seconds,
        )
    fallbacks = tuple(
        build_profile_interpreter(
            runtime.registries,
            resolver,
            profile_id=profile_id,
            timeout_seconds=config.provider_timeout_seconds,
        )
        for profile_id in fallback_ids
    )
    transcriber = None
    if config.transcription_profile_id is not None:
        transcriber = build_profile_transcriber(
            runtime.registries,
            resolver,
            profile_id=config.transcription_profile_id,
            timeout_seconds=config.transcription_timeout_seconds,
        )
    engine = create_engine(settings, resolve_database_dsn(settings))
    stop_event = asyncio.Event()

    def request_stop(signum: int, _frame: object) -> None:
        get_logger(__name__).info(
            "Interpreter stop requested",
            extra={"event": "interpreter.stop_requested", "signal": signum},
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    owner = f"{socket.gethostname()}:{os.getpid()}"
    try:
        # S-2.2b: fail closed before session factory, Bot, pipeline, or poll loop.
        await require_migration_head(engine)
        async with Bot(resolve_bot_token(settings).get_secret_value()) as bot:
            pipeline = InterpretationPipeline(
                runtime,
                create_session_factory(engine),
                interpreter=interpreter,
                fallback_interpreters=fallbacks,
                downloader=PythonTelegramClient(bot),
                transcriber=transcriber,
                owner=owner,
            )
            loops = []
            if settings.inbox.enabled:
                inbox = build_inbox_worker(
                    runtime,
                    resolver,
                    create_session_factory(engine),
                    bot,
                    transcriber,
                    owner,
                )
                loops.append(
                    asyncio.create_task(
                        _poll(inbox.process_one, stop_event, config.poll_interval_seconds)
                    )
                )
            while not stop_event.is_set():
                processed = await pipeline.process_one()
                if not processed:
                    with suppress(TimeoutError):
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=config.poll_interval_seconds
                        )
            await asyncio.gather(*loops, return_exceptions=True)
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
