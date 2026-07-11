"""Telegram long-polling process composition."""

from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.telegram.adapter import build_long_polling_application, resolve_bot_token
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, MessageUpdate
from vuzol.telegram.ingress import TelegramIngressService


def main() -> None:
    runtime = get_runtime_configuration()
    settings = runtime.settings
    configure_logging(service=f"{settings.service_name}-telegram", level=settings.log_level)
    engine = create_engine(settings, resolve_database_dsn(settings))
    factory = create_session_factory(engine)
    ingress = TelegramIngressService(runtime, factory)
    controls = TelegramControlService(runtime, factory)

    async def on_message(update: MessageUpdate) -> None:
        await ingress.accept_message(update)

    async def on_control(update: ControlUpdate) -> None:
        await controls.accept(update)

    token = resolve_bot_token(settings).get_secret_value()
    application = build_long_polling_application(
        token, bot_id="main", on_message=on_message, on_control=on_control
    )
    application.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()
