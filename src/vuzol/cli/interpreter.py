"""Dedicated Step 05 attachment and semantic interpretation runtime."""

import asyncio
import os
import signal
import socket
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    ReplyParameters,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError

from vuzol.assistant.research import Researcher
from vuzol.assistant.service import AssistantWorker, ProjectRef
from vuzol.config import (
    ConfigurationBundle,
    RuntimeConfiguration,
    ScopedSecretResolver,
    get_runtime_configuration,
)
from vuzol.config.models import LaunchMode, TopicKind
from vuzol.experiments.canary import canary_policy_from_settings
from vuzol.inbox.fetch import describe_link
from vuzol.inbox.model import ChatModel, InboxModel, ModelChain
from vuzol.inbox.search import WebSearch
from vuzol.inbox.service import InboxWorker
from vuzol.interpretation.adapters import (
    OpenAICompatibleInterpreter,
    OpenAICompatibleTranscriber,
)
from vuzol.interpretation.domain import TranscriptionInput
from vuzol.interpretation.evaluation import require_eligible_report
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline, transcription_keywords
from vuzol.leadership.access import ProjectAccess, ProjectApplication, WorkRequest
from vuzol.leadership.provenance import RequestOrigin
from vuzol.model_calls.client import RelayedModel
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
    org_id: str | None = None,
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
        headers={"x-opencode-org-id": org_id} if org_id else None,
    )


def build_model_chain(
    runtime: RuntimeConfiguration,
    resolver: ScopedSecretResolver,
    *,
    profile_id: str,
    fallback_profile_ids: tuple[str, ...],
    org_ids: dict[str, str],
    timeout_seconds: float,
    factory: async_sessionmaker[AsyncSession] | None = None,
    purpose: str = "inbox",
    web_search: bool = False,
) -> ModelChain:
    def build(profile_id: str) -> ChatModel:
        profile = runtime.registries.profiles.get(profile_id)
        if profile.launch_mode is LaunchMode.CLI and factory is not None:
            # Subscription CLIs (Codex, Pi) run only in the executor's sandbox.
            return RelayedModel(
                factory,
                profile_id=profile.id,
                model=profile.model,
                purpose=purpose,
                timeout_seconds=timeout_seconds,
                web_search=web_search and profile.provider == "codex",
            )
        return build_inbox_model(
            runtime.registries,
            resolver,
            profile_id=profile_id,
            timeout_seconds=timeout_seconds,
            org_id=org_ids.get(profile_id),
        )

    models = [build(profile_id)]
    for fallback in fallback_profile_ids:
        try:
            models.append(build(fallback))
        except (LookupError, ValueError) as error:  # incl. a missing key file
            # A spare account without its key yet must not stop the worker.
            get_logger(__name__).warning(
                "fallback profile unavailable",
                extra={
                    "event": "inbox.fallback_unavailable",
                    "profile_id": fallback,
                    "error": type(error).__name__,
                },
            )
    return ModelChain(models)


def _markup(buttons: list[list[list[str]]]) -> InlineKeyboardMarkup | None:
    if not buttons:
        return None
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(label, callback_data=data) for label, data in row]
            for row in buttons
        ]
    )


def build_assistant_worker(
    runtime: RuntimeConfiguration,
    resolver: ScopedSecretResolver,
    factory: async_sessionmaker[AsyncSession],
    bot: Bot,
    transcriber: OpenAICompatibleTranscriber | None,
    owner: str,
) -> AssistantWorker:
    config = runtime.settings.assistant
    assert config.profile_id is not None
    application = ProjectApplication(runtime, factory)
    client = PythonTelegramClient(bot)
    interpretation = runtime.settings.interpretation

    def projects() -> list[ProjectRef]:
        found: dict[str, ProjectRef] = {}
        for topic in runtime.registries.topics.items():
            if (
                topic.kind is not TopicKind.PROJECT
                or topic.project_id is None
                or not topic.enabled
                or not topic.accepts_new_tasks
            ):
                continue
            project = runtime.registries.projects.get(topic.project_id)
            if project.enabled:
                name = topic.display_name or project.display_name
                found.setdefault(project.id, ProjectRef(project.id, name))
        return list(found.values())

    async def submit(
        *,
        user_id: int,
        project_id: str,
        session_id: uuid.UUID,
        request_id: uuid.UUID,
        text: str,
    ) -> dict[str, Any]:
        return await application.submit(
            ProjectAccess(user_id=user_id, projects={project_id: session_id}, allow_submit=True),
            WorkRequest(project_id=project_id, request_id=request_id, request=text),
            origin=RequestOrigin(
                channel="telegram_assistant",
                display_name="Ассистент",
                request_id=str(request_id),
            ),
            # Only the owner's own button press reaches here (assistant/ingress.py).
            preapproved=True,
        )

    async def send(
        *,
        chat_id: int,
        thread_id: int,
        html: str,
        reply_to: int | None,
        buttons: list[list[list[str]]],
    ) -> int:
        message = await bot.send_message(
            chat_id=chat_id,
            message_thread_id=thread_id,
            text=html,
            parse_mode=ParseMode.HTML,
            reply_markup=_markup(buttons),
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            reply_parameters=(
                ReplyParameters(message_id=reply_to, allow_sending_without_reply=True)
                if reply_to
                else None
            ),
        )
        return message.message_id

    async def create_topic(*, chat_id: int, name: str) -> int:
        topic = await bot.create_forum_topic(chat_id=chat_id, name=name)
        return topic.message_thread_id

    async def clear_buttons(*, chat_id: int, message_id: int) -> None:
        with suppress(TelegramError):  # already cleared or too old to edit
            await bot.edit_message_reply_markup(
                chat_id=chat_id, message_id=message_id, reply_markup=None
            )

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

    inbox = runtime.settings.inbox
    web = (
        WebSearch(brave_key_file=inbox.brave_key_file, searxng_url=inbox.searxng_url)
        if inbox.brave_key_file or inbox.searxng_url
        else None
    )
    researcher = None
    if web is not None and config.research_profile_ids:
        first, *rest = config.research_profile_ids
        researcher = Researcher(
            build_model_chain(
                runtime,
                resolver,
                profile_id=first,
                fallback_profile_ids=tuple(rest),
                org_ids=config.opencode_org_ids,
                timeout_seconds=config.timeout_seconds,
                factory=factory,
                purpose="assistant_research",
            ),
            web,
            describe_link,
            rounds=config.search_rounds,
            per_round=config.searches_per_round,
            timezone=config.timezone,
        )
    return AssistantWorker(
        factory,
        config,
        model=build_model_chain(
            runtime,
            resolver,
            profile_id=config.profile_id,
            fallback_profile_ids=config.fallback_profile_ids,
            org_ids=config.opencode_org_ids,
            timeout_seconds=config.timeout_seconds,
            factory=factory,
            purpose="assistant_turn",
            web_search=config.native_search,
        ),
        projects=projects,
        submit=submit,
        send=send,
        clear_buttons=clear_buttons,
        download=client.download,
        transcribe=transcribe if transcriber is not None else None,
        search=web,
        researcher=researcher,
        # The root host unit does the rest; without it the button would hang.
        create_topic=create_topic if runtime.settings.project_setup.enabled else None,
        owner=owner,
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
    models = build_model_chain(
        runtime,
        resolver,
        profile_id=config.profile_id,
        fallback_profile_ids=config.fallback_profile_ids,
        org_ids=config.opencode_org_ids,
        timeout_seconds=config.timeout_seconds,
    )
    client = PythonTelegramClient(bot)
    interpretation = runtime.settings.interpretation

    async def send(
        *, chat_id: int, html: str, reply_to: int | None, buttons: list[list[list[str]]]
    ) -> int:
        message = await bot.send_message(
            chat_id=chat_id,
            text=html,
            parse_mode=ParseMode.HTML,
            reply_markup=_markup(buttons),
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
        model=models,
        search=WebSearch(brave_key_file=config.brave_key_file, searxng_url=config.searxng_url),
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
            if settings.assistant.enabled:
                assistant = build_assistant_worker(
                    runtime,
                    resolver,
                    create_session_factory(engine),
                    bot,
                    transcriber,
                    owner,
                )
                loops.append(
                    asyncio.create_task(
                        _poll(assistant.process_one, stop_event, config.poll_interval_seconds)
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
