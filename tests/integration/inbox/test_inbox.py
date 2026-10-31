"""Owner 2026-10-31: forward anything to the bot in private, it builds a knowledge base."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update
from telegram import Bot, Update

from tests.integration.leadership.test_managed_goal_flow import CHAT, OWNER, _factory
from vuzol.config import Settings
from vuzol.config.settings import InboxSettings
from vuzol.inbox.model import InboxModelError
from vuzol.inbox.schemas import Answer, Card, Cards, Patch, Route, Search
from vuzol.inbox.service import InboxWorker
from vuzol.storage.models import InboxItem, InboxLink, InboxMessage, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.durable_ingress import DurableTelegramIngress

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
INBOX = InboxSettings(enabled=True, profile_id="fast", settle_seconds=1)
SETTINGS = Settings(
    environment="test", allowed_user_ids=(OWNER,), allowed_chat_ids=(CHAT,), inbox=INBOX
)
BOT = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
REPO = "https://github.com/acme/agent-kit"
POST = f"Новая утилита для агентов {REPO}"


@pytest.fixture
def anyio_backend():
    return "asyncio"


def private(number, *, user=OWNER, **message):
    return Update.de_json(
        {
            "update_id": number,
            "message": {
                "message_id": number,
                "date": 1792000000,
                "from": {"id": user, "is_bot": False, "first_name": "Owner"},
                "chat": {"id": user, "type": "private", "first_name": "Owner"},
                **message,
            },
        },
        BOT,
    )


class FakeModel:
    """Returns queued answers by output type and records what it was asked."""

    model = "fake"

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []

    async def ask(self, output, *, system, user, images=()):
        self.calls.append({"output": output, "user": user, "images": images})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        assert isinstance(answer, output)
        return answer


class Telegram:
    def __init__(self):
        self.sent = []

    async def send(self, *, chat_id, html, reply_to, buttons):
        self.sent.append(
            {"chat_id": chat_id, "html": html, "reply_to": reply_to, "buttons": buttons}
        )
        return 9000 + len(self.sent)


async def admit(factory, *updates):
    ingress = DurableTelegramIngress(
        factory,
        settings=SETTINGS,
        bot_id="fixture",
        on_message=AsyncMock(side_effect=AssertionError("topic path")),
        on_control=AsyncMock(),
    )
    for item in updates:
        await ingress.admit(item)
    while await ingress.process_one(BOT):
        pass


async def drain(worker):
    """Run every due inbox job; settle waits are skipped by backdating the burst."""
    async with worker._factory.begin() as session:
        await session.execute(
            update(InboxMessage).values(received_at=datetime.now(UTC) - timedelta(seconds=30))
        )
        await session.execute(
            update(TransactionalOutbox)
            .where(TransactionalOutbox.destination == "inbox")
            .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    while await worker.process_one():
        async with worker._factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.destination == "inbox")
                .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
            )


def worker_for(factory, model, telegram=None, links=None, search=None, download=None):
    telegram = telegram or Telegram()

    async def read_link(url):
        return (links or {}).get(url, f"[{url}] page")

    return (
        InboxWorker(
            factory,
            INBOX,
            model=model,
            send=telegram.send,
            download=AsyncMock(return_value=download or b"\xff\xd8jpeg"),
            read_link=read_link,
            search=search,
        ),
        telegram,
    )


async def test_comment_then_forward_is_one_record_with_an_answer(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(
            factory,
            private(1, text="Что думаешь, можно в Vuzol?"),
            private(
                2,
                text=POST,
                entities=[{"type": "url", "offset": POST.index("https"), "length": len(REPO)}],
                forward_origin={
                    "type": "channel",
                    "date": 1792000000,
                    "chat": {
                        "id": -100500,
                        "type": "channel",
                        "title": "AI news",
                        "username": "ainews",
                    },
                    "message_id": 77,
                },
            ),
            # Someone else writing to the bot is ignored.
            private(3, user=OWNER + 1, text="hi"),
        )
        card = Card(
            kind="repo",
            title="agent-kit",
            summary="Набор инструментов для агентов.",
            tags=["ai", "agents"],
            fields={"license": "MIT", "stars": "1200"},
            verdict="Живой, MIT, ставится через pip.",
        )
        fast = FakeModel(
            Cards(cards=[card], question="Что думаешь, можно в Vuzol?"),
            Answer(text="Да, подходит для инструментов лида."),
        )
        worker, telegram = worker_for(
            factory,
            fast,
            links={"https://github.com/acme/agent-kit": "[GitHub acme/agent-kit] README"},
        )
        await drain(worker)
        async with factory() as session:
            messages = list(
                await session.scalars(select(InboxMessage).order_by(InboxMessage.message_id))
            )
            assert len(messages) == 2 and len({m.batch_id for m in messages}) == 1
            assert messages[1].forward_url == "https://t.me/ainews/77"
            item = await session.scalar(select(InboxItem))
            assert item.kind == "repo" and item.fields["verdict"].startswith("Живой")
            assert "GitHub acme/agent-kit" in item.evidence
            assert item.answers[0]["answer"] == "Да, подходит для инструментов лида."
            links = list(await session.scalars(select(InboxLink)))
            assert {link.item_id for link in links} == {item.id}
        assert "Что думаешь" in fast.calls[0]["user"]
        assert [m["reply_to"] for m in telegram.sent] == [2, 2]
        assert "📥 Сохранил" in telegram.sent[0]["html"] and "agent-kit" in telegram.sent[0]["html"]
        assert "подходит" in telegram.sent[1]["html"]


async def test_reply_to_card_corrects_it_and_search_finds_events_by_date(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(
            factory,
            private(10, photo=[{"file_id": "p1", "file_unique_id": "u1", "width": 9, "height": 9}]),
        )
        event = Card(
            kind="event",
            title="Rave X",
            summary="Ночной рейв.",
            event_start=datetime(2026, 11, 15, 23, 0),
            fields={"location": "Арена"},
        )
        fast = FakeModel(
            Cards(cards=[event]),
            Route(
                action="update",
                target="replied",
                patch=Patch(event_start=datetime(2026, 11, 16, 23, 0)),
            ),
            Route(
                action="search",
                search=Search(kinds=["event"], date_from="2026-11-01", date_to="2026-11-30"),
            ),
            Answer(text="16 ноября, 23:00 — Rave X, Арена."),
        )
        worker, telegram = worker_for(factory, fast)
        await drain(worker)
        assert fast.calls[0]["images"]  # the poster went to the model
        card_message = 9001
        await admit(
            factory,
            private(
                11,
                text="Это 16-го",
                reply_to_message={
                    "message_id": card_message,
                    "date": 1792000000,
                    "chat": {"id": OWNER, "type": "private", "first_name": "Owner"},
                    "text": "card",
                },
            ),
        )
        await drain(worker)
        async with factory() as session:
            item = await session.scalar(select(InboxItem))
            assert item.event_start.astimezone(UTC).day == 16
        assert "✏️ Поправил" in telegram.sent[1]["html"]
        await admit(factory, private(12, text="Что у нас на ноябрь?"))
        await drain(worker)
        search_input = fast.calls[-1]["user"]
        assert "Rave X" in search_input
        assert "Rave X" in telegram.sent[-1]["html"]


async def test_model_failure_keeps_the_raw_message(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(factory, private(20, text="Дюна, Фрэнк Герберт"))
        fast = FakeModel(*(InboxModelError("http_500") for _ in range(3)))
        worker, telegram = worker_for(factory, fast)
        await drain(worker)
        async with factory() as session:
            item = await session.scalar(select(InboxItem))
            assert item.kind == "note" and item.title == "Дюна, Фрэнк Герберт"
            dead = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.status == DeliveryStatus.DEAD_LETTER
                )
            )
            assert dead is None
        assert "Не смог разобрать" in telegram.sent[0]["html"]


async def test_question_answer_searches_the_web_first(postgres_dsn):
    from vuzol.inbox.schemas import Lookup
    from vuzol.inbox.search import Hit

    class Search:
        async def search(self, query):
            return [Hit("Superwhisper", "https://superwhisper.example", "voice app")]

        async def github(self, query):
            return [{"repo": "x/whisper-kb", "stars": 900}]

    async with _factory(postgres_dsn) as factory:
        await admit(
            factory,
            private(
                30,
                text="Есть альтернативы? https://github.com/a/b",
                entities=[{"type": "url", "offset": 19, "length": 22}],
            ),
        )
        model = FakeModel(
            Cards(cards=[Card(kind="repo", title="b")], question="Есть альтернативы?"),
            Lookup(queries=["android whisper dictation"], github=["whisper keyboard android"]),
            Answer(text="Да: Superwhisper и x/whisper-kb (900★)."),
        )
        worker, telegram = worker_for(factory, model, search=Search())
        await drain(worker)
        answer_input = model.calls[-1]["user"]
        assert "superwhisper.example" in answer_input and "x/whisper-kb" in answer_input
        assert "[https://superwhisper.example] page" in answer_input  # the hit was opened
        assert "Superwhisper" in telegram.sent[-1]["html"]


async def test_reply_to_own_forward_and_plain_web_question_search_online(postgres_dsn):
    from vuzol.inbox.schemas import Lookup
    from vuzol.inbox.search import Hit

    class Search:
        def __init__(self):
            self.queries = []

        async def search(self, query):
            self.queries.append(query)
            return [Hit("DictateKeyboard", "https://dk.example", "keyboard")]

        async def github(self, query):
            return []

    def reply_to(number):
        return {
            "message_id": number,
            "date": 1792000000,
            "chat": {"id": OWNER, "type": "private", "first_name": "Owner"},
            "text": "post",
        }

    async with _factory(postgres_dsn) as factory:
        search = Search()
        await admit(
            factory,
            private(
                40,
                text="Голосовой ввод для Android",
                forward_origin={
                    "type": "hidden_user",
                    "date": 1792000000,
                    "sender_user_name": "Anon",
                },
            ),
        )
        model = FakeModel(
            Cards(cards=[Card(kind="tool", title="phone-whisper")]),
            # Reply to the owner's own forward, not to the bot's card.
            Route(action="ask", target="replied"),
            Lookup(queries=["mature android dictation apps"]),
            Answer(text="Зрелее: DictateKeyboard."),
            Route(action="web"),
            Lookup(queries=["android dictation privacy"]),
            Answer(text="Нашёл в сети: DictateKeyboard."),
        )
        worker, telegram = worker_for(factory, model, search=search)
        await drain(worker)
        await admit(factory, private(41, text="поищи альтернативы", reply_to_message=reply_to(40)))
        await drain(worker)
        assert '"title": "phone-whisper"' in model.calls[2]["user"]  # the record was found
        await admit(factory, private(42, text="не, поищи в инете"))
        await drain(worker)
        assert search.queries == ["mature android dictation apps", "android dictation privacy"]
        assert "Нашёл в сети" in telegram.sent[-1]["html"]
        async with factory() as session:
            item = await session.scalar(select(InboxItem))
            assert [a["answer"] for a in item.answers] == ["Зрелее: DictateKeyboard."]


async def test_week_ahead_reminder_and_sunday_digest(postgres_dsn):
    sunday_noon = datetime(2026, 11, 1, 11, 0, tzinfo=UTC)  # 12:00 in Amsterdam
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            for title, start, saved in (
                (
                    "Forgotten rave",
                    sunday_noon + timedelta(days=5),
                    sunday_noon - timedelta(days=60),
                ),
                ("Just saved", sunday_noon + timedelta(days=3), sunday_noon - timedelta(days=1)),
                ("Far away", sunday_noon + timedelta(days=40), sunday_noon - timedelta(days=2)),
            ):
                session.add(
                    InboxItem(
                        owner_user_id=OWNER,
                        chat_id=OWNER,
                        ordinal=0,
                        kind="event",
                        title=title,
                        summary="",
                        event_start=start,
                        evidence="",
                        answers=[],
                        created_at=saved,
                    )
                )
        worker, telegram = worker_for(factory, FakeModel())
        await worker._sweep(sunday_noon)
        await worker._sweep(sunday_noon + timedelta(minutes=10))  # no duplicates
        await drain(worker)
        reminders = [m for m in telegram.sent if "Напоминаю" in m["html"]]
        assert len(reminders) == 1 and "Forgotten rave" in reminders[0]["html"]
        assert "через 5 дн." in reminders[0]["html"]
        assert reminders[0]["buttons"][0][0][1].startswith("ib:a:")
        digests = [m for m in telegram.sent if "Неделя в инбоксе" in m["html"]]
        assert len(digests) == 1
        assert "Just saved" in digests[0]["html"] and "Far away" in digests[0]["html"]
        # Night: nothing is sent.
        before = len(telegram.sent)
        await worker._sweep(datetime(2026, 11, 2, 23, 0, tzinfo=UTC))
        await drain(worker)
        assert len(telegram.sent) == before


async def test_card_buttons_analyze_and_delete(postgres_dsn, monkeypatch):
    from telegram import CallbackQuery

    monkeypatch.setattr(CallbackQuery, "answer", AsyncMock())

    def press(number, action, item_id):
        return Update.de_json(
            {
                "update_id": number,
                "callback_query": {
                    "id": f"cb{number}",
                    "from": {"id": OWNER, "is_bot": False, "first_name": "Owner"},
                    "chat_instance": "ci",
                    "data": f"ib:{action}:{item_id.hex}",
                    "message": {
                        "message_id": 9001,
                        "date": 1792000000,
                        "chat": {"id": OWNER, "type": "private", "first_name": "Owner"},
                        "text": "card",
                    },
                },
            },
            BOT,
        )

    async with _factory(postgres_dsn) as factory:
        await admit(factory, private(50, text="Дюна, Фрэнк Герберт"))
        model = FakeModel(
            Route(action="save"),
            Cards(cards=[Card(kind="book", title="Дюна")]),
            Answer(text="Классика, читать."),
        )
        worker, telegram = worker_for(factory, model)
        await drain(worker)
        assert telegram.sent[0]["buttons"][0][1][0] == "🗑 Удалить"
        async with factory() as session:
            item = await session.scalar(select(InboxItem))
        await admit(factory, press(51, "a", item.id))
        await drain(worker)
        assert "Разбери подробно" in model.calls[-1]["user"]
        assert (
            telegram.sent[-1]["html"] == "Классика, читать."
            and telegram.sent[-1]["reply_to"] == 9001
        )
        await admit(factory, press(52, "d", item.id))
        await drain(worker)
        assert "🗑 Удалил: Дюна" in telegram.sent[-1]["html"]
        async with factory() as session:
            assert (await session.get(InboxItem, item.id)).removed_at is not None


async def test_documents_are_read_into_the_evidence(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(
            factory,
            private(
                60,
                caption="Программа фестиваля",
                document={
                    "file_id": "doc1",
                    "file_unique_id": "d1",
                    "file_name": "lineup.txt",
                    "mime_type": "text/plain",
                    "file_size": 40,
                },
            ),
        )
        model = FakeModel(Cards(cards=[Card(kind="event", title="Fest")]))
        worker, _ = worker_for(factory, model, download="Сцена A: Amelie Lens 23:00".encode())
        await drain(worker)
        assert "Amelie Lens 23:00" in model.calls[0]["user"]
