"""Owner 2026-11-25: new mail is read every hour and the assistant offers what to do with it."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select, update

from tests.integration.assistant.test_assistant import (
    ASSISTANT,
    THREAD,
    FakeModel,
    admit,
    drain,
    press,
    reminders,
    worker_for,
)
from tests.integration.leadership.test_managed_goal_flow import _factory
from vuzol.assistant import service
from vuzol.assistant.google import GoogleError
from vuzol.assistant.mail import Briefing, MailOffers, Offer, quiet
from vuzol.assistant.schemas import NewEvent, Reminder
from vuzol.storage.models import AssistantEntry, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
CONCERT = datetime(2027, 1, 12, 19, tzinfo=UTC)


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Mailbox:
    def __init__(self, *, fail=False):
        self.queries = []
        self.added = []
        self.fail = fail

    async def mail(self, query, *, limit=6):
        if self.fail:
            raise GoogleError("gmail_503")
        self.queries.append((query, limit))
        return [{"id": "m1", "from": "Ticketmaster", "subject": "Билеты", "text": "12.01 20:00"}]

    async def events(self, start, end):
        return [{"title": "Стоматолог", "start": "2027-01-05T10:00:00+01:00", "end": None}]

    async def add_event(self, event_id, **event):
        self.added.append((event_id, event))
        return True


def offers(*items):
    return MailOffers(offers=list(items))


CONCERT_OFFER = Offer(
    kind="event",
    text="Билеты на концерт 12.01 в 20:00 — добавить в календарь?",
    label="Концерт 12.01",
    event=NewEvent(title="Концерт", start=CONCERT),
)
SPOTIFY_OFFER = Offer(
    kind="remind",
    text="Spotify списал 11 €, следующее списание 25.12 — напомнить за два дня?",
    label="Spotify",
    remind=Reminder(at=datetime.now(UTC) + timedelta(days=28), text="Spotify спишет 11 € 25.12"),
)
PROMO_OFFER = Offer(
    kind="notice", text="Промокод на 12 € в Thuisbezorgd, сгорает сегодня", label="Промокод"
)


def watcher(factory, model, mailbox, **settings):
    worker, telegram, _ = worker_for(factory, model)
    worker._google = mailbox
    worker._mail_model = model
    worker._settings = worker._settings.model_copy(update={"briefing": False, **settings})
    return worker, telegram


async def checks(factory):
    async with factory() as session:
        return list(
            await session.scalars(
                select(TransactionalOutbox)
                .where(TransactionalOutbox.idempotency_key.like("assistant:mailcheck:%"))
                .order_by(TransactionalOutbox.created_at)
            )
        )


async def run_check(worker):
    async with worker._factory.begin() as session:
        await session.execute(
            update(TransactionalOutbox)
            .where(
                TransactionalOutbox.idempotency_key.like("assistant:mailcheck:%"),
                TransactionalOutbox.status == DeliveryStatus.PENDING,
            )
            .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    assert await worker.process_one()
    await drain(worker)


async def offered(factory):
    async with factory() as session:
        return await session.scalar(
            select(AssistantEntry).where(AssistantEntry.offers.is_not(None))
        )


def awake():
    """Quiet hours that never include the current hour."""
    hour = datetime.now(ZoneInfo(ASSISTANT.timezone)).hour
    return ((hour + 1) % 24, (hour + 2) % 24)


async def test_new_mail_becomes_offers_acted_on_with_buttons(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        mailbox = Mailbox()
        model = FakeModel(offers(CONCERT_OFFER, SPOTIFY_OFFER, PROMO_OFFER))
        worker, telegram = watcher(factory, model, mailbox, mail_quiet_hours=awake())
        await worker.start_mail_watch()
        await worker.start_mail_watch()
        (first,) = await checks(factory)
        started = datetime.fromisoformat(first.payload["since"])

        await run_check(worker)

        ((query, limit),) = mailbox.queries
        assert f"after:{int(started.timestamp())} before:" in query and "-in:spam" in query
        assert limit == 30
        assert model.calls[0]["mail"] == [
            {
                "from": "Ticketmaster",
                "subject": "Билеты",
                "date": None,
                "snippet": None,
                "text": "12.01 20:00",
            }
        ]
        assert model.calls[0]["calendar"][0]["title"] == "Стоматолог"
        (message,) = telegram.sent
        assert message["thread"] == THREAD
        assert "Промокод на 12 €" in message["html"] and "Spotify" in message["html"]
        entry = await offered(factory)
        assert [row[0][0] for row in message["buttons"]] == [
            "📅 Концерт 12.01",
            "⏰ Spotify",
            "🙅 Не надо",
        ]
        # The next check reads from where this one stopped, an hour later.
        _, following = await checks(factory)
        assert following.status == DeliveryStatus.PENDING
        assert datetime.fromisoformat(following.payload["since"]) > started
        assert following.available_at > datetime.now(UTC) + timedelta(minutes=55)

        await admit(factory, press(1, "m0", entry.id), press(2, "m0", entry.id))
        await drain(worker)
        ((event_id, event),) = mailbox.added
        assert event_id == f"{entry.id.hex}m0" and event["start"] == CONCERT
        assert "📅 Добавил в календарь" in telegram.sent[1]["html"]
        assert "Это уже сделано" in telegram.sent[2]["html"]
        assert telegram.cleared == []

        await admit(factory, press(3, "m1", entry.id))
        await drain(worker)
        (reminder,) = await reminders(factory)
        assert reminder.payload["text"] == "Spotify спишет 11 € 25.12"
        assert "⏰ Напомню" in telegram.sent[3]["html"]
        # Every offer with a button is done: the buttons go.
        assert telegram.cleared == [9001]
        entry = await offered(factory)
        assert [o["state"] for o in entry.offers] == ["done", "done", "open"]


async def test_dismissing_clears_the_buttons_and_nothing_is_done(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        mailbox = Mailbox()
        worker, telegram = watcher(
            factory, FakeModel(offers(CONCERT_OFFER)), mailbox, mail_quiet_hours=awake()
        )
        await worker.start_mail_watch()
        await run_check(worker)
        entry = await offered(factory)
        await admit(factory, press(1, "x", entry.id))
        await drain(worker)
        assert mailbox.added == [] and telegram.cleared == [9001]
        assert (await offered(factory)).offers[0]["state"] == "dismissed"


async def test_quiet_mail_sends_nothing(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        worker, telegram = watcher(
            factory, FakeModel(offers()), Mailbox(), mail_quiet_hours=awake()
        )
        await worker.start_mail_watch()
        await run_check(worker)
        assert telegram.sent == [] and await offered(factory) is None


@pytest.mark.parametrize("why", ["failure", "night"])
async def test_a_failed_or_night_check_leaves_its_mail_for_the_next(postgres_dsn, why):
    hour = datetime.now(ZoneInfo(ASSISTANT.timezone)).hour
    async with _factory(postgres_dsn) as factory:
        model = FakeModel()
        worker, telegram = watcher(
            factory,
            model,
            Mailbox(fail=why == "failure"),
            mail_quiet_hours=(hour, (hour + 1) % 24) if why == "night" else awake(),
        )
        await worker.start_mail_watch()
        await run_check(worker)
        first, following = await checks(factory)
        assert first.status == DeliveryStatus.DELIVERED
        assert following.payload["since"] == first.payload["since"]
        assert telegram.sent == [] and model.calls == []


def test_quiet_hours_may_wrap_midnight():
    assert [h for h in range(24) if quiet(h, (23, 8))] == [0, 1, 2, 3, 4, 5, 6, 7, 23]
    assert [h for h in range(24) if quiet(h, (1, 3))] == [1, 2]


async def test_the_first_check_of_the_morning_is_a_briefing_once_a_day(postgres_dsn, monkeypatch):
    monkeypatch.setattr(service, "_BRIEFING_LATEST_HOUR", 24)
    async with _factory(postgres_dsn) as factory:
        mailbox = Mailbox()
        model = FakeModel(
            Briefing(text="**Сегодня**\n• Стоматолог в 10:00", offers=[CONCERT_OFFER]),
            offers(),
        )
        worker, telegram = watcher(factory, model, mailbox, mail_quiet_hours=awake(), briefing=True)
        await worker.start_mail_watch()
        await run_check(worker)

        seen = model.calls[0]
        assert {"today", "calendar", "reminders", "projects", "mail"} <= set(seen)
        assert [p["project_id"] for p in seen["projects"]] == ["diplom", "site"]
        assert seen["mail"][0]["subject"] == "Билеты"
        (message,) = telegram.sent
        assert message["html"].startswith("<b>☀️ Доброе утро</b>")
        assert "<b>Сегодня</b>" in message["html"] and "Стоматолог в 10:00" in message["html"]
        assert [row[0][0] for row in message["buttons"]] == ["📅 Концерт 12.01", "🙅 Не надо"]

        # Later checks the same day read mail as usual.
        await run_check(worker)
        assert len(model.calls) == 2 and "projects" not in model.calls[1]
        assert len(telegram.sent) == 1


async def test_a_quiet_morning_still_gets_its_briefing(postgres_dsn, monkeypatch):
    monkeypatch.setattr(service, "_BRIEFING_LATEST_HOUR", 24)
    async with _factory(postgres_dsn) as factory:
        model = FakeModel(Briefing(text="День свободен."))
        worker, telegram = watcher(
            factory, model, Mailbox(), mail_quiet_hours=awake(), briefing=True
        )
        await worker.start_mail_watch()
        await run_check(worker)
        (message,) = telegram.sent
        assert "День свободен." in message["html"] and message["buttons"] == []
