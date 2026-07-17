"""Unit tests for /update in the project status dashboard topic."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vuzol.providers.subscription_limits import LimitWindow, SubscriptionLimitSnapshot
from vuzol.telegram.domain import IngressStatus
from vuzol.telegram.layout import is_status_dashboard_topic, is_update_command
from vuzol.telegram.projections import build_project_status_dashboard


def test_is_update_command_parsing() -> None:
    assert is_update_command("/update")
    assert is_update_command("  /update@vuzol_bot  ")
    assert is_update_command("/update now")
    assert not is_update_command("/updates")
    assert not is_update_command("update")
    assert not is_update_command(None)
    assert not is_update_command("")


def test_status_dashboard_kind_helper() -> None:
    assert is_status_dashboard_topic("task_dashboard")
    assert not is_status_dashboard_topic("changelog")


@pytest.mark.anyio
async def test_dashboard_includes_updated_timestamp() -> None:
    snap = SubscriptionLimitSnapshot(
        profile_id="codex-subscription-prod",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(None, None, available=False),
        weekly=LimitWindow(remaining_percent=40, reset_at=None, available=True),
        observed_at=datetime(2026, 7, 16, 22, 30, tzinfo=UTC),
        ok=True,
    )
    session = MagicMock()
    session.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: []))
    card = await build_project_status_dashboard(
        session, chat_id=-100, subscription_snapshots=(snap,)
    )
    assert "Subscription limits" in card.html
    assert "Updated 2026-07-16 22:30 UTC" in card.html

    # Naive timestamps are treated as UTC.
    naive = SubscriptionLimitSnapshot(
        profile_id="codex-subscription-prod",
        company="OpenAI",
        plan_label="Plus",
        five_hour=LimitWindow(None, None, available=False),
        weekly=LimitWindow(remaining_percent=10, reset_at=None, available=True),
        observed_at=datetime(2026, 7, 16, 12, 0),
        ok=True,
    )
    card2 = await build_project_status_dashboard(
        session, chat_id=-100, subscription_snapshots=(naive,)
    )
    assert "Updated 2026-07-16 12:00 UTC" in card2.html


@pytest.mark.anyio
async def test_accept_message_routes_update_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.config.models import TopicConfig, TopicKind
    from vuzol.telegram.domain import MessageUpdate
    from vuzol.telegram.ingress import TelegramIngressService

    update = MessageUpdate(
        bot_id="main",
        update_id=7,
        chat_id=-100,
        message_thread_id=5,
        message_id=9,
        user_id=42,
        text="/update@bot",
    )
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=5,
        kind=TopicKind.TASK_DASHBOARD,
        accepts_new_tasks=False,
        default_workflow="simple_model_task",
        enabled=True,
    )
    runtime = MagicMock()
    runtime.settings = MagicMock()
    runtime.registries.topics.resolve.return_value = topic
    service = TelegramIngressService(runtime, MagicMock())
    handled = AsyncMock(return_value=MagicMock(status=IngressStatus.HANDLED))
    monkeypatch.setattr(service, "_handle_dashboard_update", handled)
    monkeypatch.setattr("vuzol.telegram.ingress.authorize", lambda *a, **k: None)
    monkeypatch.setattr("vuzol.telegram.ingress.validate_message", lambda *a, **k: None)
    result = await service.accept_message(update)
    assert result.status is IngressStatus.HANDLED
    handled.assert_awaited_once()


@pytest.mark.anyio
async def test_handle_dashboard_update_enqueues_refresh_and_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.config.models import TopicConfig, TopicKind
    from vuzol.providers.subscription_limits import SUBSCRIPTION_LIMITS_DESTINATION
    from vuzol.telegram.domain import MessageUpdate
    from vuzol.telegram.ingress import TelegramIngressService

    update = MessageUpdate(
        bot_id="main",
        update_id=99,
        chat_id=-100,
        message_thread_id=5,
        message_id=501,
        user_id=42,
        text="/update",
    )
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=5,
        kind=TopicKind.TASK_DASHBOARD,
        accepts_new_tasks=False,
        default_workflow="simple_model_task",
        enabled=True,
    )

    inbox_id = uuid4()
    enqueued: list[dict[str, object]] = []

    class FakeOutbox:
        async def enqueue(self, **kwargs: object) -> object:
            enqueued.append(kwargs)
            return uuid4()

    class FakeInbox:
        async def receive_once(self, **kwargs: object) -> tuple[object, bool]:
            del kwargs
            return inbox_id, True

        async def mark_processed(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

    class FakeUow:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.inbox = FakeInbox()
            self.outbox = FakeOutbox()
            self.session = MagicMock()

        async def __aenter__(self) -> FakeUow:
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

    monkeypatch.setattr("vuzol.telegram.ingress.UnitOfWork", FakeUow)
    service = TelegramIngressService(MagicMock(), MagicMock())
    result = await service._handle_dashboard_update(update, topic)
    assert result.status is IngressStatus.HANDLED
    assert len(enqueued) == 2
    destinations = {item["destination"] for item in enqueued}
    assert destinations == {SUBSCRIPTION_LIMITS_DESTINATION, "telegram"}
    delete = next(item for item in enqueued if item["destination"] == "telegram")
    assert delete["operation_type"] == "delete_message"
    assert delete["payload"]["message_id"] == 501  # type: ignore[index]
    refresh = next(
        item for item in enqueued if item["destination"] == SUBSCRIPTION_LIMITS_DESTINATION
    )
    assert refresh["operation_type"] == "refresh"
    assert refresh["payload"]["chat_id"] == -100  # type: ignore[index]


@pytest.mark.anyio
async def test_refresh_subscription_limits_tick_force(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.cli.executor import _refresh_subscription_limits_tick
    from vuzol.storage.records import OutboxLeaseToken
    from vuzol.storage.types import DeliveryStatus

    token = OutboxLeaseToken(
        item_id=uuid4(),
        status=DeliveryStatus.LEASED,
        owner="vuzol-executor-limits",
        generation=1,
        lease_expires_at=datetime(2026, 7, 17, tzinfo=UTC),
    )
    item = SimpleNamespace(payload={"chat_id": -100})
    session = MagicMock()
    session.get = AsyncMock(return_value=item)
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    factory = MagicMock()
    factory.begin.return_value = session_cm
    registries = MagicMock()
    registries.profiles.items.return_value = ()

    monkeypatch.setattr("vuzol.storage.leasing.claim_outbox_item", AsyncMock(return_value=token))
    monkeypatch.setattr("vuzol.storage.leasing.complete_outbox_item", AsyncMock(return_value=None))
    refresh = AsyncMock(return_value=())
    enqueue = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "vuzol.providers.subscription_limits.refresh_and_store_subscription_limits",
        refresh,
    )
    monkeypatch.setattr(
        "vuzol.telegram.projections.enqueue_project_status_dashboard",
        enqueue,
    )

    forced = await _refresh_subscription_limits_tick(factory, registries, due_periodic=False)
    assert forced is True
    refresh.assert_awaited_once()
    enqueue.assert_awaited_once()
    assert enqueue.await_args is not None
    assert enqueue.await_args.args[1] == -100


@pytest.mark.anyio
async def test_prepare_user_command_delete() -> None:
    from vuzol.telegram.delivery import DeliveryAction, prepare_delivery

    item = SimpleNamespace(
        operation_type="delete_message",
        payload={
            "role": "user_command_delete",
            "chat_id": -100,
            "message_id": 77,
            "message_thread_id": 5,
        },
        linked_entity_type="telegram_inbox",
        linked_entity_id=uuid4(),
    )
    prepared = await prepare_delivery(MagicMock(), item)  # type: ignore[arg-type]
    assert prepared.action == DeliveryAction.DELETE_MESSAGE
    assert prepared.chat_id == -100
    assert prepared.message_id == 77
