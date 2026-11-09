"""Quota refresh identity and the dashboard's rich send/edit fallback."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from xml.etree import ElementTree

import pytest
from sqlalchemy import func, select

from tests.fakes.telegram import FakeTelegramClient
from tests.integration.storage.helpers import storage
from tests.integration.telegram.test_delivery import service
from tests.integration.telegram.test_rich_discussion import RejectedRichClient
from vuzol.providers.subscription_limits import (
    LimitWindow,
    SubscriptionLimitSnapshot,
    persist_subscription_limits,
)
from vuzol.storage.models import TelegramMessageLink, TopicMapping, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.projections import (
    build_project_status_dashboard,
    enqueue_project_status_dashboard,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
CHAT_ID = -100123450001


def snapshot():
    now = datetime.now(UTC)
    return SubscriptionLimitSnapshot(
        "codex-a", "OpenAI", "Plus", LimitWindow(72, now), LimitWindow(None, None), now, True
    )


async def seed(factory, snap):
    async with factory.begin() as session:
        session.add(
            TopicMapping(
                chat_id=CHAT_ID,
                message_thread_id=5,
                topic_kind="task_dashboard",
                accepts_new_tasks=False,
                default_workflow="simple_model_task",
                enabled=True,
            )
        )
        await persist_subscription_limits(session, (snap,))
        await enqueue_project_status_dashboard(session, CHAT_ID)


async def test_two_refreshes_same_usage_enqueue_once_then_visible_changes_enqueue(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    snap = snapshot()
    try:
        await seed(factory, snap)
        async with factory.begin() as session:
            refreshed = replace(snap, observed_at=snap.observed_at + timedelta(minutes=2))
            await persist_subscription_limits(session, (refreshed,))
            await enqueue_project_status_dashboard(session, CHAT_ID)
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 1
            initial = await build_project_status_dashboard(session, CHAT_ID, now=snap.observed_at)
            at_30 = await build_project_status_dashboard(
                session, CHAT_ID, now=refreshed.observed_at + timedelta(minutes=30)
            )
            stale = await build_project_status_dashboard(
                session, CHAT_ID, now=refreshed.observed_at + timedelta(minutes=31)
            )
            later = await build_project_status_dashboard(
                session, CHAT_ID, now=refreshed.observed_at + timedelta(hours=3)
            )
            assert initial.revision == at_30.revision
            assert stale.revision != initial.revision
            assert stale.revision == later.revision
            assert "данные устарели" in stale.html
            assert "Обновлено:" not in stale.html
        # A -> B -> A must still deliver the second A (e.g. a quota reset).
        for remaining in (71, 72):
            async with factory.begin() as session:
                await persist_subscription_limits(
                    session,
                    (replace(snap, five_hour=LimitWindow(remaining, snap.five_hour.reset_at)),),
                )
                await enqueue_project_status_dashboard(session, CHAT_ID)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 3
    finally:
        await engine.dispose()


@pytest.mark.parametrize("rich", [False, True])
@pytest.mark.parametrize("rejected", [False, True])
async def test_dashboard_send_edit_and_rejection_fallback(postgres_dsn, rich, rejected):
    engine, factory = storage(postgres_dsn)
    snap = snapshot()
    client = RejectedRichClient() if rejected else FakeTelegramClient()
    try:
        await seed(factory, snap)
        delivery = service(factory, client, rich_messages=rich)
        assert await delivery.deliver_one()
        async with factory.begin() as session:
            await persist_subscription_limits(
                session, (replace(snap, five_hour=LimitWindow(71, snap.five_hour.reset_at)),)
            )
            await enqueue_project_status_dashboard(session, CHAT_ID)
        assert await delivery.deliver_one()
        use_rich = rich and not rejected
        sent = client.rich_sent if use_rich else client.sent
        edited = client.rich_edited if use_rich else client.edited
        assert len(sent) == len(edited) == 1
        assert edited[0][1] == 1
        assert "28%" in sent[0][2] and "29%" in edited[0][2]
        assert ("<table>" in sent[0][2]) == use_rich
        ElementTree.fromstring(f"<root>{sent[0][2]}</root>")  # noqa: S314 - only generated HTML from fixed fixtures
        ElementTree.fromstring(f"<root>{edited[0][2]}</root>")  # noqa: S314 - only generated HTML from fixed fixtures
        assert not await delivery.deliver_one()
        async with factory.begin() as session:
            await enqueue_project_status_dashboard(session, CHAT_ID)
            link = await session.scalar(select(TelegramMessageLink))
            card = await build_project_status_dashboard(session, CHAT_ID)
            assert link.message_id == 1 and link.projection_revision == card.revision
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(TransactionalOutbox)
                    .where(TransactionalOutbox.status == DeliveryStatus.PENDING)
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("missing_id", [False, True])
async def test_unknown_first_rich_dashboard_send_has_no_ordinary_duplicate(
    postgres_dsn, missing_id
):
    from telegram.error import NetworkError

    class UnknownClient(FakeTelegramClient):
        async def send_rich_message(self, **kwargs):
            if missing_id:
                return 0
            raise NetworkError("send outcome unknown")

    engine, factory = storage(postgres_dsn)
    client = UnknownClient()
    try:
        await seed(factory, snapshot())
        assert await service(factory, client, rich_messages=True).deliver_one()
        assert not client.sent
        async with factory() as session:
            item = await session.scalar(select(TransactionalOutbox))
            assert item.status is DeliveryStatus.AMBIGUOUS
            assert await session.scalar(select(TelegramMessageLink)) is None
    finally:
        await engine.dispose()
