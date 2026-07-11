"""Dedicated Step 05 attachment and semantic interpretation runtime."""

import asyncio
import os
import signal
import socket
from contextlib import suppress

from telegram import Bot

from vuzol.config import ScopedSecretResolver, get_runtime_configuration
from vuzol.interpretation.adapters import (
    OpenAICompatibleInterpreter,
    OpenAICompatibleTranscriber,
)
from vuzol.interpretation.evaluation import require_eligible_report
from vuzol.interpretation.ports import SemanticInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.observability import configure_logging, get_logger
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.telegram.adapter import PythonTelegramClient, resolve_bot_token


async def run() -> None:
    runtime = get_runtime_configuration()
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

    def build_interpreter(profile_id: str) -> OpenAICompatibleInterpreter:
        profile = runtime.registries.profiles.get(profile_id)
        if profile.provider != "openai-compatible" or profile.api_base_url is None:
            raise ValueError(f"unsupported interpreter profile: {profile.id}")
        if profile.credential_reference is None:
            raise ValueError(f"interpreter profile has no credential: {profile.id}")
        return OpenAICompatibleInterpreter(
            base_url=str(profile.api_base_url),
            credential=resolver.get(profile.credential_reference, f"profile:{profile.id}"),
            profile_id=profile.id,
            model=profile.model,
            timeout_seconds=config.provider_timeout_seconds,
        )

    interpreter: SemanticInterpreter = build_interpreter(primary_profile.id)
    fallbacks: tuple[SemanticInterpreter, ...] = tuple(
        build_interpreter(profile_id) for profile_id in fallback_ids
    )
    transcriber = None
    if config.transcription_profile_id is not None:
        profile = runtime.registries.profiles.get(config.transcription_profile_id)
        if profile.provider != "openai-compatible" or profile.api_base_url is None:
            raise ValueError(f"unsupported transcription profile: {profile.id}")
        if profile.credential_reference is None:
            raise ValueError(f"transcription profile has no credential: {profile.id}")
        transcriber = OpenAICompatibleTranscriber(
            base_url=str(profile.api_base_url),
            credential=resolver.get(profile.credential_reference, f"profile:{profile.id}"),
            profile_id=profile.id,
            model=profile.model,
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
            while not stop_event.is_set():
                processed = await pipeline.process_one()
                if not processed:
                    with suppress(TimeoutError):
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=config.poll_interval_seconds
                        )
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
