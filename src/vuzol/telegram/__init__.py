"""Telegram forum workspace boundary."""

from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import (
    EditRateLimiter,
    FakeTelegramClient,
    LostTelegramResponse,
    StatusCard,
    TelegramClient,
    apply_status_projection,
    build_status_card,
    split_message,
    telegram_html,
)

__all__ = [
    "EditRateLimiter",
    "FakeTelegramClient",
    "LostTelegramResponse",
    "StatusCard",
    "TelegramClient",
    "TelegramControlService",
    "TelegramIngressService",
    "apply_status_projection",
    "build_status_card",
    "split_message",
    "telegram_html",
]
