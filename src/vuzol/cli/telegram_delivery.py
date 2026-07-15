"""Dedicated Telegram outbound delivery process."""

import asyncio
import os
import signal
import socket

from telegram import Bot

from vuzol.config import get_runtime_configuration
from vuzol.observability import configure_logging, get_logger
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn
from vuzol.telegram.adapter import PythonTelegramClient, resolve_bot_token
from vuzol.telegram.delivery import TelegramDeliveryService, run_delivery_loop


async def run() -> None:
    runtime = get_runtime_configuration(validate_profile_credentials=False)
    settings = runtime.settings
    configure_logging(
        service=f"{settings.service_name}-telegram-delivery", level=settings.log_level
    )
    engine = create_engine(settings, resolve_database_dsn(settings))
    stop_event = asyncio.Event()

    def request_stop(signum: int, _frame: object) -> None:
        get_logger(__name__).info(
            "Telegram delivery stop requested",
            extra={"event": "telegram.delivery.stop_requested", "signal": signum},
        )
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    token = resolve_bot_token(settings).get_secret_value()
    owner = f"{socket.gethostname()}:{os.getpid()}"
    delivery = settings.telegram
    try:
        async with Bot(token) as bot:
            service = TelegramDeliveryService(
                create_session_factory(engine),
                PythonTelegramClient(bot),
                owner=owner,
                lease_seconds=delivery.delivery_lease_seconds,
                max_attempts=delivery.delivery_max_attempts,
                retry_min_seconds=delivery.delivery_retry_min_seconds,
                retry_max_seconds=delivery.delivery_retry_max_seconds,
                topics=runtime.registries.topics,
            )
            await run_delivery_loop(
                service,
                poll_interval_seconds=delivery.delivery_poll_interval_seconds,
                stop_event=stop_event,
            )
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
