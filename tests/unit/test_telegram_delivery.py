import asyncio
from collections.abc import Coroutine
from typing import Any

import pytest
from pydantic import ValidationError

from vuzol.cli import telegram_delivery as delivery_cli
from vuzol.config import TelegramSettings
from vuzol.telegram.delivery import PermanentDeliveryError, run_delivery_loop


class StoppingService:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event
        self.calls = 0

    async def deliver_one(self) -> bool:
        self.calls += 1
        self.stop_event.set()
        return False


def test_delivery_loop_stops_cleanly() -> None:
    async def scenario() -> None:
        stop_event = asyncio.Event()
        service = StoppingService(stop_event)
        await run_delivery_loop(service, poll_interval_seconds=0.1, stop_event=stop_event)
        assert service.calls == 1

    asyncio.run(scenario())


def test_delivery_retry_settings_require_ordered_bounds() -> None:
    with pytest.raises(ValidationError, match="minimum must not exceed maximum"):
        TelegramSettings(
            delivery_retry_min_seconds=10,
            delivery_retry_max_seconds=1,
        )


def test_permanent_error_exposes_only_category() -> None:
    error = PermanentDeliveryError("safe_category")
    assert error.category == "safe_category"
    assert str(error) == "safe_category"


def test_delivery_cli_main_runs_coroutine(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[Coroutine[Any, Any, None]] = []

    def capture(coroutine: Coroutine[Any, Any, None]) -> None:
        captured.append(coroutine)

    monkeypatch.setattr(asyncio, "run", capture)
    delivery_cli.main()
    assert len(captured) == 1
    captured[0].close()
