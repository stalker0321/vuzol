"""Owner 2026-11-02: a master topic that answers errands and hands tasks to project leads."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update
from telegram import Bot, CallbackQuery, Update

from tests.integration.leadership.test_managed_goal_flow import CHAT, OWNER, _factory
from vuzol.assistant.ingress import schedule
from vuzol.assistant.research import Finding, Researcher
from vuzol.assistant.schemas import Dispatch, NewEvent, NewProject, Note, Period, Reminder, Turn
from vuzol.assistant.service import EDIT_LABEL, SEND_LABEL, AssistantWorker, ProjectRef
from vuzol.config import Settings
from vuzol.config.settings import AssistantSettings
from vuzol.inbox.search import Hit
from vuzol.leadership.persistence import GoalDelegation, ManagedGoal
from vuzol.storage.models import (
    AssistantEntry,
    InboxItem,
    ProjectDiscussionSession,
    ProjectTaskQueueItem,
    TransactionalOutbox,
)
from vuzol.storage.repositories.discussion import DiscussionRepository
from vuzol.storage.types import DiscussionSessionStatus
from vuzol.telegram.durable_ingress import DurableTelegramIngress
from vuzol.telegram.formatting import telegram_rich_html
from vuzol.telegram.projections import LostTelegramResponse, RichMessageRejected

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
                .where(
                    TransactionalOutbox.destination == "assistant",
                    # Reminders wait for their time; the next mail check for its hour.
                    TransactionalOutbox.idempotency_key.not_like("assistant:remind:%"),
                    TransactionalOutbox.idempotency_key.not_like("assistant:mailcheck:%"),
                )
                .values(available_at=datetime.now(UTC) - timedelta(seconds=1))
            )

    await due()
    while await worker.process_one():
        await due()


def worker_for(factory, model, *, search=None, researcher=None, create_topic=None, send_rich=None):
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
        send_rich=send_rich,
        clear_buttons=telegram.clear,
        search=search,
        read_link=read_link,
        researcher=researcher,
        create_topic=create_topic,
    )
    return worker, telegram, leads


@pytest.mark.parametrize("planned", [False, True])
async def test_formatted_reply_uses_rich_only_without_plan_buttons(postgres_dsn, planned):
    text = "# Итог\n\n" + "Подробный ответ. " * 30
    send_rich = AsyncMock(return_value=9017)
    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "Расскажи подробнее"))
        turn = Turn(
            reply=text,
            dispatches=[Dispatch(project_id="diplom", request="Сделай.")] if planned else [],
        )
        worker, telegram, _ = worker_for(factory, FakeModel(turn), send_rich=send_rich)
        await drain(worker)
        if planned:
            send_rich.assert_not_awaited()
            assert len(telegram.sent) == 1 and telegram.sent[0]["buttons"]
        else:
            send_rich.assert_awaited_once_with(
                chat_id=CHAT, thread_id=THREAD, html=telegram_rich_html(text), reply_to=1
            )
            assert not telegram.sent
            async with factory() as session:
                answer = await session.scalar(
                    select(AssistantEntry).where(AssistantEntry.role == "assistant")
                )
                assert answer.telegram_message_id == 9017


@pytest.mark.parametrize("outcome", ["rejected", "timeout", "missing_id", "bad_request"])
async def test_rich_reply_falls_back_only_after_definite_rejection(postgres_dsn, outcome):
    from telegram.error import BadRequest, TimedOut

    from vuzol.storage.types import DeliveryStatus

    text = "# Итог\n\nПодробности."
    failures = {
        "rejected": RichMessageRejected("unsupported"),
        "timeout": TimedOut(),
        "missing_id": LostTelegramResponse("no confirmation"),
        "bad_request": BadRequest("invalid request"),
    }
    send_rich = AsyncMock(side_effect=failures[outcome])
    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "Расскажи"))
        worker, telegram, _ = worker_for(factory, FakeModel(Turn(reply=text)), send_rich=send_rich)
        await drain(worker)
        assert send_rich.await_count == (3 if outcome == "bad_request" else 1)
        assert len(telegram.sent) == int(outcome == "rejected")
        async with factory() as session:
            reply = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like("assistant:reply:%")
                )
            )
            assert (
                reply.status
                is {
                    "rejected": DeliveryStatus.DELIVERED,
                    "timeout": DeliveryStatus.AMBIGUOUS,
                    "missing_id": DeliveryStatus.AMBIGUOUS,
                    "bad_request": DeliveryStatus.DEAD_LETTER,
                }[outcome]
            )


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


async def test_a_reply_that_timed_out_is_not_sent_twice(postgres_dsn):
    from telegram.error import TimedOut

    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "привет"))
        worker, _, _ = worker_for(factory, FakeModel(Turn(reply="Привет!")))
        sends = []

        async def timed_out(**kwargs):
            sends.append(kwargs)
            raise TimedOut()  # Telegram may still have delivered it

        worker._send = timed_out
        await drain(worker)
        assert len(sends) == 1


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


async def test_an_overview_question_sees_each_projects_queue_and_recent_goals(postgres_dsn):
    """Owner 2026-11-23: a project overview needs more than the live goal."""

    now = datetime.now(UTC)
    async with _factory(postgres_dsn) as factory:
        await sessions(factory)
        async with factory.begin() as session:
            discussion = {
                row.project_id: row.id
                for row in await session.scalars(select(ProjectDiscussionSession))
            }
            delegation = GoalDelegation(
                project_id="diplom",
                owner_user_id=OWNER,
                agent_principal_id="agent:lead-diplom",
                policy_revision="p1",
                integration_branch="vuzol/goal",
            )
            session.add(delegation)
            await session.flush()

            def goal(request, lifecycle, age, **extra):
                return ManagedGoal(
                    project_id="diplom",
                    owner_user_id=OWNER,
                    discussion_session_id=discussion["diplom"],
                    delegation_id=delegation.id,
                    original_request=request,
                    scope_text=request,
                    integration_branch="vuzol/goal",
                    lifecycle=lifecycle,
                    created_at=now - age,
                    updated_at=now - age,
                    **extra,
                )

            live = goal("Глава 2", "waiting", timedelta(days=2), waiting_reason="owner_approval")
            session.add_all(
                [
                    live,
                    goal("Введение", "completed", timedelta(days=3)),
                    goal(
                        "Черновик",
                        "cancelled",
                        timedelta(days=1),
                        waiting_reason="closed_by_owner",
                    ),
                    goal("Старое", "completed", timedelta(days=30)),
                ]
            )
            await session.flush()
            session.add(
                ProjectTaskQueueItem(
                    project_id="diplom",
                    owner_user_id=OWNER,
                    discussion_session_id=discussion["diplom"],
                    source_event_id=uuid.uuid4(),
                    body={},
                    summary="Список литературы",
                    queued_by_goal_id=live.id,
                )
            )
        await admit(factory, topic(1, "что у нас по проектам?"))
        model = FakeModel(Turn(reply="Диплом ждёт тебя."))
        worker, _, _ = worker_for(factory, model)
        await drain(worker)

        diplom, site = model.calls[0]["projects"]
        assert diplom["live_goal"]["request"] == "Глава 2"
        assert diplom["live_goal"]["updated"] == (now - timedelta(days=2)).date().isoformat()
        assert diplom["queued_tasks"] == ["Список литературы"]
        assert [(g["request"], g["lifecycle"]) for g in diplom["recent_goals"]] == [
            ("Черновик", "cancelled"),
            ("Введение", "completed"),
        ]
        assert diplom["recent_goals"][0]["ended"] == (now - timedelta(days=1)).date().isoformat()
        assert site == {
            "project_id": "site",
            "name": "Сайт",
            "live_goal": None,
            "queued_tasks": [],
            "recent_goals": [],
        }


async def reminders(factory):
    async with factory() as session:
        return list(
            await session.scalars(
                select(TransactionalOutbox)
                .where(TransactionalOutbox.idempotency_key.like("assistant:remind:%"))
                .order_by(TransactionalOutbox.available_at)
            )
        )


async def test_a_reminder_comes_back_into_the_topic_at_its_time_and_can_be_cancelled(
    postgres_dsn,
):
    now = datetime.now(UTC).replace(microsecond=0)
    at = now + timedelta(days=1)
    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "напомни завтра купить хлеб, и вчера позвонить"))
        model = FakeModel(
            Turn(
                reply="Хорошо.",
                remind=[
                    Reminder(at=at, text="Купить хлеб"),
                    Reminder(at=now - timedelta(hours=1), text="Позвонить"),
                ],
            ),
            Turn(reply="Отменил.", cancel_reminders=["placeholder"]),
        )
        worker, telegram, _ = worker_for(factory, model)
        await drain(worker)

        (pending,) = await reminders(factory)
        assert abs((pending.available_at - at).total_seconds()) < 5
        assert pending.payload["thread_id"] == THREAD
        assert pending.payload["html"] == "⏰ Напоминаю: Купить хлеб"
        assert "⏰ Напомню" in telegram.sent[0]["html"]
        assert "Позвонить" in telegram.sent[0]["html"] and "прошло" in telegram.sent[0]["html"]

        model.answers[0].cancel_reminders = [str(pending.id)]
        await admit(factory, topic(2, "отмени напоминание про хлеб"))
        await drain(worker)
        listed = model.calls[1]["reminders"]
        assert [(r["id"], r["text"]) for r in listed] == [(str(pending.id), "Купить хлеб")]
        assert await reminders(factory) == []
        assert "Отменил напоминание: Купить хлеб" in telegram.sent[1]["html"]


async def test_the_assistant_searches_and_saves_the_knowledge_base(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                InboxItem(
                    owner_user_id=OWNER,
                    chat_id=OWNER,
                    kind="book",
                    title="Дюна",
                    summary="Фрэнк Герберт, советовал Петя",
                )
            )
        await admit(factory, topic(1, "что мне советовали почитать? и запомни фильм Сталкер"))
        model = FakeModel(
            Turn(reply="ищу", knowledge=["книги советовали"]),
            Turn(
                reply="Дюну.",
                save=[
                    Note(kind="film", title="Сталкер", summary="Тарковский"),
                    Note(kind="nonsense", title="Мысль", summary="про лес"),
                ],
            ),
        )
        worker, telegram, _ = worker_for(factory, model)
        await drain(worker)

        assert "knowledge" not in model.calls[0]
        (hit,) = model.calls[1]["knowledge"]["книги советовали"]
        assert "Дюна" in hit["text"]
        async with factory() as session:
            saved = {
                item.title: item
                for item in await session.scalars(
                    select(InboxItem).where(InboxItem.title != "Дюна")
                )
            }
        assert {title: item.kind for title, item in saved.items()} == {
            "Сталкер": "film",
            "Мысль": "note",
        }
        assert saved["Сталкер"].owner_user_id == saved["Сталкер"].chat_id == OWNER
        assert len(telegram.sent) == 1
        assert "📝 Сохранил: Сталкер, Мысль" in telegram.sent[0]["html"]


class FakeGoogle:
    def __init__(self):
        self.added = []

    async def events(self, start, end):
        return [{"title": f"Стоматолог {start.isoformat()}", "start": "10:00"}]

    async def mail(self, query, *, limit=6):
        return [{"from": "KLM", "subject": f"Билет ({query})", "text": "рейс KL1234"}]

    async def add_event(self, event_id, **event):
        self.added.append((event_id, event))
        return True


async def test_calendar_and_mail_are_read_before_the_answer_and_events_are_added(postgres_dsn):
    start = datetime(2026, 11, 24, 15, tzinfo=UTC)
    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "что у меня завтра, где билет klm, и поставь созвон в 16"))
        model = FakeModel(
            Turn(
                reply="смотрю", calendar=Period(start="2026-11-24", end="2026-11-24"), mail=["klm"]
            ),
            Turn(reply="Завтра стоматолог.", add_events=[NewEvent(title="Созвон", start=start)]),
        )
        worker, telegram, _ = worker_for(factory, model)
        google = worker._google = FakeGoogle()
        await drain(worker)

        second = model.calls[1]
        assert second["calendar"] == [{"title": "Стоматолог 2026-11-24", "start": "10:00"}]
        assert second["mail"]["klm"][0]["subject"] == "Билет (klm)"
        ((event_id, event),) = google.added
        assert len(event_id) >= 5 and set(event_id) <= set("0123456789abcdefghijklmnopqrstuv")
        assert event["end"] - event["start"] == timedelta(hours=1)
        assert "📅 Добавил в календарь" in telegram.sent[0]["html"]


async def test_without_google_the_model_is_told_and_nothing_is_added(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await admit(factory, topic(1, "что у меня завтра?"))
        model = FakeModel(
            Turn(reply="смотрю", calendar=Period(start="2026-11-24", end="2026-11-24")),
            Turn(
                reply="Календарь не подключён.",
                add_events=[NewEvent(title="Созвон", start=datetime(2026, 11, 24, 15, tzinfo=UTC))],
            ),
        )
        worker, telegram, _ = worker_for(factory, model)
        await drain(worker)
        assert model.calls[1]["google"] == {"error": "календарь и почта не подключены"}
        assert "Календарь не подключён, событие не добавлено" in telegram.sent[0]["html"]


NEWS_THREAD = 55


async def test_a_reply_to_a_news_item_is_discussed_with_the_assistant_in_the_news_topic(
    postgres_dsn,
):
    from vuzol.assistant.ingress import is_assistant_update
    from vuzol.config.settings import MonitorSettings
    from vuzol.storage.models import MonitorEvent

    settings = SETTINGS.model_copy(
        update={
            "monitor": MonitorSettings(
                enabled=True, chat_id=CHAT, message_thread_id=NEWS_THREAD, profile_ids=("fast",)
            )
        }
    )
    reply = Update.de_json(
        {
            "update_id": 1,
            "message": {
                "message_id": 801,
                "date": 1792000000,
                "from": {"id": OWNER, "is_bot": False, "first_name": "Owner"},
                "chat": {"id": CHAT, "type": "supergroup", "is_forum": True},
                "message_thread_id": NEWS_THREAD,
                "is_topic_message": True,
                "text": "а чем это лучше того, что у нас?",
                "reply_to_message": {
                    "message_id": 700,
                    "date": 1792000000,
                    "chat": {"id": CHAT, "type": "supergroup", "is_forum": True},
                    "message_thread_id": NEWS_THREAD,
                    "text": "news",
                },
            },
        },
        BOT,
    )
    verdict = Update.de_json(
        {
            "update_id": 2,
            "callback_query": {
                "id": "cb2",
                "from": {"id": OWNER, "is_bot": False, "first_name": "Owner"},
                "chat_instance": "ci",
                "data": f"mn:l:{uuid.uuid4().hex}",
                "message": {
                    "message_id": 700,
                    "date": 1792000000,
                    "chat": {"id": CHAT, "type": "supergroup", "is_forum": True},
                    "message_thread_id": NEWS_THREAD,
                    "is_topic_message": True,
                    "text": "news",
                },
            },
        },
        BOT,
    )
    # The 👍 · 💾 · 👎 under the news stay the monitor's.
    assert is_assistant_update(reply, settings) and not is_assistant_update(verdict, settings)
    off = settings.model_copy(
        update={"monitor": settings.monitor.model_copy(update={"discussion": False})}
    )
    assert not is_assistant_update(reply, off)
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                MonitorEvent(
                    source_id="hn",
                    external_id="x",
                    url="https://example.com/agent",
                    title="Новый агент",
                    excerpt="",
                    fetched_at=datetime.now(UTC),
                    digested_at=datetime.now(UTC),
                    state="digested",
                    summary="Агент для кода",
                    details="Что даёт: много",
                    message_id=700,
                )
            )
        ingress = DurableTelegramIngress(
            factory,
            settings=settings,
            bot_id="fixture",
            on_message=AsyncMock(side_effect=AssertionError("project path")),
            on_control=AsyncMock(side_effect=AssertionError("project controls")),
        )
        await ingress.admit(reply)
        while await ingress.process_one(BOT):
            pass
        model = FakeModel(Turn(reply="Он умеет больше."))
        worker, telegram, _ = worker_for(factory, model)
        await drain(worker)
        (news,) = model.calls[0]["news"]
        assert news["title"] == "Новый агент" and news["details"] == "Что даёт: много"
        assert news["url"] == "https://example.com/agent"
        assert telegram.sent[0]["thread"] == NEWS_THREAD
        assert telegram.sent[0]["html"] == "Он умеет больше."
