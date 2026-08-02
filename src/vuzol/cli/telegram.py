"""Telegram long-polling process composition."""

import asyncio

from telegram import Bot

from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.telegram.adapter import (
    PythonTelegramClient,
    build_long_polling_application,
    resolve_bot_token,
)
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.dogfood import TelegramDogfoodIngressService
from vuzol.telegram.domain import ControlUpdate, MessageUpdate, WorkPackageControlUpdate
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.work_packages import ContinueDiscussionOverrides
from vuzol.telegram.workspace import TelegramWorkspaceService


def main() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-telegram", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    # One process-owned loop for gate, PTB polling (AsyncEngine use), and dispose.
    # Avoid nested asyncio.run / multi-loop reuse of the same pooled AsyncEngine (S-2.2a C1).
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        # S-2.2a: fail closed before session factory, Bot wiring, polling, or schema work.
        loop.run_until_complete(require_migration_head(engine))
        factory = create_session_factory(engine)
        continue_discussion_overrides = ContinueDiscussionOverrides()
        ingress = TelegramIngressService(runtime, factory, continue_discussion_overrides)
        dogfood = TelegramDogfoodIngressService(runtime, factory)
        controls = TelegramControlService(runtime, factory, continue_discussion_overrides)
        workspace = TelegramWorkspaceService(factory, runtime.registries.topics)

        async def on_message(update: MessageUpdate) -> None:
            if await dogfood.accept_message(update) is None:
                await ingress.accept_message(update)

        async def on_control(update: ControlUpdate | WorkPackageControlUpdate) -> None:
            await controls.accept(update)

        async def on_startup(bot: Bot) -> None:
            await workspace.synchronize(PythonTelegramClient(bot))

        token = resolve_bot_token(settings).get_secret_value()
        application = build_long_polling_application(
            token,
            bot_id="main",
            on_message=on_message,
            on_control=on_control,
            on_startup=on_startup,
        )
        # Keep this loop open so handler DB work and later dispose share it.
        application.run_polling(
            allowed_updates=["message", "callback_query"],
            close_loop=False,
        )
    finally:
        try:
            if not loop.is_closed():
                loop.run_until_complete(_dispose_engine(engine))
        finally:
            if not loop.is_closed():
                loop.close()
            asyncio.set_event_loop(None)


async def _dispose_engine(engine: object) -> None:
    dispose = getattr(engine, "dispose", None)
    if dispose is not None:
        await dispose()


if __name__ == "__main__":
    main()
