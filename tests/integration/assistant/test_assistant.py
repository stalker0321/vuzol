"""Owner 2026-11-02: a master topic that answers errands and hands tasks to project leads."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update
from telegram import Bot, CallbackQuery, Update

from tests.integration.leadership.test_managed_goal_flow import CHAT, OWNER, _factory
from vuzol.assistant.ingress import schedule
from vuzol.assistant.research import Finding, Researcher
from vuzol.assistant.schemas import Dispatch, NewProject, Turn
from vuzol.assistant.service import EDIT_LABEL, SEND_LABEL, AssistantWorker, ProjectRef
from vuzol.config import Settings
from vuzol.config.settings import AssistantSettings
from vuzol.inbox.search import Hit
from vuzol.storage.models import AssistantEntry, ProjectDiscussionSession, TransactionalOutbox
from vuzol.storage.repositories.discussion import DiscussionRepository
from vuzol.storage.types import DiscussionSessionStatus
from vuzol.telegram.durable_ingress import DurableTelegramIngress

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
THREAD = 77
ASSISTANT = AssistantSettings(
    enabled=True, profile_id="smart", chat_id=CHAT, message_thread_id=THREAD, search_rounds=2
)
SETTINGS = Settings(
    environment="test", allowed_user_ids=(OWNER,), allowed_chat_ids=(CHAT,), assistant=ASSISTANT
)
BOT = Bot("123:FAKE_TOKEN_NOT_A_CREDENTIAL")
PROJECTS = [ProjectRef("diplom", "Диплом"), ProjectRef("site", "Сайт")]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def topic(number, text, *, thread=THREAD):
    return Update.de_json(
        {
            "update_id": number,
            "message": {
                "message_id": number,
                "date": 1792000000,
                "from": {"id": OWNER, "is_bot": False, "first_name": "Owner"},
                "chat": {"id": CHAT, "type": "supergroup", "is_forum": True},
                "message_thread_id": thread,
                "is_topic_message": True,
                "text": text,
            },
        },
        BOT,
    )


def press(number, action, entry_id):
    return Update.de_json(
        {
            "update_id": number,
            "callback_query": {
                "id": f"cb{number}",
                "from": {"id": OWNER, "is_bot": False, "first_name": "Owner"},
                "chat_instance": "ci",
                "data": f"as:{action}:{entry_id.hex}",
                "message": {
                    "message_id": 9001,
                    "date": 1792000000,
                    "chat": {"id": CHAT, "type": "supergroup", "is_forum": True},
                    "message_thread_id": THREAD,
                    "is_topic_message": True,
                    "text": "plan",
                },
            },
        },
        BOT,
    )


class FakeModel:
    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []

    async def ask(self, output, *, system, user, images=()):
        self.calls.append(json.loads(user))
        return self.answers.pop(0)


class Telegram:
    def __init__(self):
        self.sent = []
        self.cleared = []

    async def send(self, *, chat_id, thread_id, html, reply_to, buttons):
        self.sent.append(
            {"thread": thread_id, "html": html, "reply_to": reply_to, "buttons": buttons}
        )
        return 9000 + len(self.sent)

    async def clear(self, *, chat_id, message_id):
        self.cleared.append(message_id)


class Leads:
    def __init__(self):
        self.calls = []

    async def submit(self, *, user_id, project_id, session_id, request_id, text):
        self.calls.append((project_id, request_id, text))
        return {"status": "accepted"}


class Search:
    async def search(self, query):
        return [Hit("Рейсы", "https://example.com/flights", f"дёшево: {query}")]


async def admit(factory, *updates):
    ingress = DurableTelegramIngress(
        factory,
        settings=SETTINGS,
        bot_id="fixture",
        on_message=AsyncMock(side_effect=AssertionError("project path")),
        on_control=AsyncMock(side_effect=AssertionError("project controls")),
    )
    for item in updates:
        await ingress.admit(item)
    while await ingress.process_one(BOT):
        pass


async def drain(worker):
    async def due():
        async with worker._factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.destination == "assistant")
                .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
            )

    await due()
    while await worker.process_one():
        await due()


def worker_for(factory, model, *, search=None, researcher=None, create_topic=None):
    telegram, leads = Telegram(), Leads()

    async def read_link(url):
        return f"[{url}] страница"

    worker = AssistantWorker(
        factory,
        ASSISTANT,
        model=model,
        projects=lambda: PROJECTS,
        submit=leads.submit,
        send=telegram.send,
        clear_buttons=telegram.clear,
        search=search,
        read_link=read_link,
        researcher=researcher,
        create_topic=create_topic,
    )
    return worker, telegram, leads


async def sessions(factory):
    async with factory.begin() as session:
        for project_id, thread in (("diplom", 10), ("site", 11)):
            session.add(
                ProjectDiscussionSession(
                    project_id=project_id,
                    chat_id=CHAT,
                    message_thread_id=thread,
                    status=DiscussionSessionStatus.ACTIVE,
                )
            )


async def test_plan_is_sent_to_project_leads_only_on_the_button(postgres_dsn, monkeypatch):
    monkeypatch.setattr(CallbackQuery, "answer", AsyncMock())
    async with _factory(postgres_dsn) as factory:
        await sessions(factory)
        await admit(factory, topic(1, "в дипломе поправь введение, а на сайте обнови футер"))
        model = FakeModel(
            Turn(
                reply="Две задачи, по одной на проект.",
                dispatches=[
                    Dispatch(project_id="diplom", request="Поправь введение."),
                    Dispatch(project_id="site", request="Обнови футер."),
                    Dispatch(project_id="ghost", request="?"),
                ],
            )
        )
        worker, telegram, leads = worker_for(factory, model)
        await drain(worker)

        context = model.calls[0]
        assert context["latest_owner_message"].startswith("в дипломе")
        assert [p["project_id"] for p in context["projects"]] == ["diplom", "site"]
        plan = telegram.sent[0]
        assert plan["thread"] == THREAD and plan["reply_to"] == 1
        assert "<b>→ Диплом</b>" in plan["html"] and "Не нашёл проекты: ghost" in plan["html"]
        assert [b[0] for b in plan["buttons"][0]] == [SEND_LABEL, EDIT_LABEL]
        assert leads.calls == []

        async with factory() as session:
            entry = await session.scalar(
                select(AssistantEntry).where(AssistantEntry.role == "assistant")
            )
        assert entry.telegram_message_id == 9001 and entry.plan_status == "pending"
        await admit(factory, press(2, "s", entry.id), press(3, "s", entry.id))
        await drain(worker)

        assert [(p, t) for p, _, t in leads.calls] == [
            ("diplom", "Поправь введение."),
            ("site", "Обнови футер."),
        ]
        assert "🚀 Отправил: Диплом, Сайт" in telegram.sent[1]["html"]
        assert "уже не актуален" in telegram.sent[2]["html"]
        assert telegram.cleared == [9001]


async def test_a_new_project_is_created_on_the_button_and_gets_its_first_task(
    postgres_dsn, monkeypatch
):
    monkeypatch.setattr(CallbackQuery, "answer", AsyncMock())
    topics = []

    async def create_topic(*, chat_id, name):
        topics.append((chat_id, name))
        return 2001

    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "заведи проект под марафон и составь план тренировок"))
        model = FakeModel(
            Turn(
                reply="Заведу проект.",
                new_project=NewProject(
                    project_id="marathon",
                    name="Марафон",
                    description="Подготовка к весеннему марафону.",
                    first_request="Составь план тренировок на 12 недель.",
                ),
            )
        )
        worker, telegram, leads = worker_for(factory, model, create_topic=create_topic)
        await drain(worker)
        plan = telegram.sent[0]
        assert "🆕 <b>Новый проект: Марафон</b>" in plan["html"] and plan["buttons"]

        async with factory() as session:
            entry = await session.scalar(
                select(AssistantEntry).where(AssistantEntry.role == "assistant")
            )
        await admit(factory, press(2, "s", entry.id))
        await drain(worker)
        assert topics == [(CHAT, "Марафон")]
        assert "🛠 Создаю проект «Марафон»" in telegram.sent[1]["html"]
        async with factory.begin() as session:
            setup = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "project_setup"
                )
            )
            assert setup.payload["thread_id"] == 2001 and setup.payload["project_id"] == "marathon"
            # What the host unit leaves behind once the project exists.
            await DiscussionRepository(session).create_session(
                project_id="marathon", chat_id=CHAT, message_thread_id=2001
            )
            await schedule(session, operation="project_ready", key="ready", payload=setup.payload)
        await drain(worker)
        welcome = telegram.sent[2]
        assert welcome["thread"] == 2001 and "Марафон" in welcome["html"]
        assert [(p, t) for p, _, t in leads.calls] == [
            ("marathon", "Составь план тренировок на 12 недель.")
        ]
        assert "✅ Создал проект «Марафон»" in telegram.sent[3]["html"]
        assert "https://t.me/c/" in telegram.sent[3]["html"]


async def test_edit_button_then_correction_replaces_the_plan(postgres_dsn, monkeypatch):
    monkeypatch.setattr(CallbackQuery, "answer", AsyncMock())
    async with _factory(postgres_dsn) as factory:
        await sessions(factory)
        await admit(factory, topic(1, "в диплом: перепиши выводы"))
        model = FakeModel(
            Turn(reply="Ок.", dispatches=[Dispatch(project_id="diplom", request="Выводы.")]),
            Turn(
                reply="Поправил.",
                dispatches=[Dispatch(project_id="diplom", request="Выводы, коротко.")],
            ),
        )
        worker, telegram, leads = worker_for(factory, model)
        await drain(worker)
        async with factory() as session:
            first = await session.scalar(
                select(AssistantEntry).where(AssistantEntry.role == "assistant")
            )
        await admit(factory, press(2, "e", first.id))
        await drain(worker)
        assert "Напиши, что поменять" in telegram.sent[1]["html"]

        await admit(factory, topic(3, "пусть будет коротко"))
        await drain(worker)
        history = model.calls[1]["conversation"]
        assert history[-1]["plan_status"] == "editing"
        async with factory() as session:
            statuses = sorted(
                (e.body, e.plan_status)
                for e in await session.scalars(
                    select(AssistantEntry).where(AssistantEntry.role == "assistant")
                )
            )
        assert statuses == [("Ок.", "superseded"), ("Поправил.", "pending")]
        assert telegram.cleared == [9001]
        assert leads.calls == []


async def test_errand_searches_the_web_and_other_topics_are_untouched(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "найди рейсы в Берлин на выходные"))
        model = FakeModel(
            Turn(reply="Ищу.", searches=["Amsterdam Berlin flights weekend"]),
            Turn(reply="Нашёл дёшево на example.com."),
        )
        worker, telegram, _ = worker_for(factory, model, search=Search())
        await drain(worker)
        assert model.calls[1]["web"]["pages"] == ["[https://example.com/flights] страница"]
        assert telegram.sent[0]["html"] == "Нашёл дёшево на example.com."
        assert telegram.sent[0]["buttons"] == []

        await admit(factory, topic(2, "это в проектный топик", thread=10))
        async with factory() as session:
            threads = set(await session.scalars(select(AssistantEntry.message_thread_id)))
        assert threads == {THREAD}


async def test_a_second_search_round_is_run_and_the_last_round_must_answer(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "сколько стоит ChatGPT Plus в евро и бизнес на одного"))
        model = FakeModel(
            Turn(reply="Ищу.", searches=["ChatGPT Plus price EUR"]),
            Turn(reply="Секунду, уточню.", searches=["ChatGPT Business price per seat"]),
            Turn(reply="Plus 23 €, Business 29 € за место.", searches=["ещё"]),
        )
        worker, telegram, _ = worker_for(factory, model, search=Search())
        await drain(worker)
        assert [call["searches_left"] for call in model.calls] == [2, 1, 0]  # search_rounds=2
        assert list(model.calls[2]["web"]["searches"]) == [
            "ChatGPT Plus price EUR",
            "ChatGPT Business price per seat",
        ]
        assert [s["html"] for s in telegram.sent] == ["Plus 23 €, Business 29 € за место."]


async def test_the_researcher_reads_the_web_and_the_assistant_gets_only_findings(postgres_dsn):
    async def read_link(url):
        return "x" * 50_000  # a long page reaches the researcher, never the assistant

    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "сколько стоит перелёт в Барселону на выходных"))
        cheap = FakeModel(
            Finding(answer="ищу", searches=["Amsterdam Barcelona 14 Nov"]),
            Finding(answer="Transavia от €84, 14 ноября.", sources=["https://example.com/flights"]),
        )
        model = FakeModel(
            Turn(reply="Ищу.", research=["Цена AMS→BCN 14–15 ноября 2026"]),
            Turn(reply="Transavia от €84."),
        )
        researcher = Researcher(cheap, Search(), read_link, rounds=2, per_round=5, timezone="UTC")
        worker, telegram, _ = worker_for(factory, model, search=Search(), researcher=researcher)
        await drain(worker)
        assert cheap.calls[0] == {"question": "Цена AMS→BCN 14–15 ноября 2026", "searches_left": 2}
        assert len(cheap.calls[1]["web"]["pages"][0]) == 15_000
        assert [call["research_left"] for call in model.calls] == [2, 1]
        (finding,) = model.calls[1]["research"]
        assert finding["answer"] == "Transavia от €84, 14 ноября."
        assert "web" not in model.calls[1]
        assert [s["html"] for s in telegram.sent] == ["Transavia от €84."]


def voice(number):
    data = topic(number, "x").to_dict()
    del data["message"]["text"]
    data["message"]["voice"] = {
        "file_id": f"v{number}",
        "file_unique_id": f"u{number}",
        "duration": 3,
        "mime_type": "audio/ogg",
    }
    return Update.de_json(data, BOT)


async def test_each_voice_is_heard_and_echoed_before_the_answer(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(factory, voice(1), voice(2))
        model = FakeModel(Turn(reply="Понял."))
        worker, telegram, _ = worker_for(factory, model)

        async def download(file_id):
            return file_id.encode()

        async def transcribe(content, mime):
            return {"v1": "первая часть", "v2": "вторая часть"}[content.decode()]

        worker._download, worker._transcribe = download, transcribe
        await drain(worker)
        assert model.calls[0]["latest_owner_message"] == "первая часть\nвторая часть"
        assert [(s["reply_to"], s["html"]) for s in telegram.sent] == [
            (1, "🎙 Расслышал: «первая часть»\nЕсли не так — поправь текстом."),
            (2, "🎙 Расслышал: «вторая часть»\nЕсли не так — поправь текстом."),
            (2, "Понял."),
        ]
