"""Short follow-ups retain the surrounding owner/bot conversation."""

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from tests.integration.inbox.test_inbox import (
    FakeModel,
    admit,
    drain,
    private,
    worker_for,
)
from tests.integration.leadership.test_managed_goal_flow import OWNER, _factory
from vuzol.inbox.ingress import schedule
from vuzol.inbox.schemas import Answer, Card, Cards, Route
from vuzol.storage.models import InboxMessage, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize(
    ("words", "action"),
    [
        ("игра такая", "chat"),
        ("ну ебаный в рот, игра такая в стим, найди и сохрани", "save"),
        ("PATHOGENIC Я ТЕБЕ ТОЛЬКО ЧТО НАПИСАЛ", "ask"),
        ("блять ну поищи они есть в базе, просто наверное с дебильными тегами", "search"),
    ],
)
async def test_followup_payloads_keep_title_and_bot_question(postgres_dsn, words, action):
    question = 'Не понял, что делать со словом "pathogenic"…'
    answers = [Route(action="chat", reply=question), Route(action=action)]
    if action == "save":
        answers.append(Cards(cards=[Card(kind="other", title="Pathogenic")]))
    elif action in {"ask", "search"}:
        answers.append(Answer(text="Нашёл"))
    async with _factory(postgres_dsn) as factory:
        model = FakeModel(*answers)
        worker, _ = worker_for(factory, model)
        await admit(factory, private(1, text="pathogenic"))
        await drain(worker)
        await admit(
            factory,
            private(
                2,
                text=words,
                reply_to_message={
                    "message_id": 9001,
                    "date": 1792000000,
                    "chat": {"id": OWNER, "type": "private"},
                    "text": question,
                },
            ),
        )
        await drain(worker)
        for call in model.calls[1:]:
            payload = json.loads(call["user"])
            conversation = payload["conversation"]
            assert [(m["role"], m["text"]) for m in conversation] == [
                ("owner", "pathogenic"),
                ("bot", question),
            ]
            assert all(
                datetime.fromisoformat(m["time"]).utcoffset() is not None for m in conversation
            )
        async with factory() as session:
            reply = await session.scalar(
                select(TransactionalOutbox)
                .where(TransactionalOutbox.idempotency_key.like("inbox:reply:chat:%"))
                .order_by(TransactionalOutbox.created_at)
            )
            assert reply.payload["message_id"] == 9001


async def owner_message(session, number, at, **values):
    message = InboxMessage(
        chat_id=OWNER,
        user_id=OWNER,
        message_id=number,
        received_at=at,
        batch_id=uuid.uuid4(),
        **values,
    )
    session.add(message)
    await session.flush()
    return message


async def bot_message(session, number, at, text, *, status=DeliveryStatus.DELIVERED):
    await schedule(
        session,
        operation="reply",
        key=f"context:{number}",
        payload={"chat_id": OWNER, "message_id": number, "markdown": text},
    )
    row = await session.scalar(
        select(TransactionalOutbox).where(
            TransactionalOutbox.idempotency_key == f"context:{number}"
        )
    )
    row.status = status
    row.created_at = at
    row.delivered_at = at if status == DeliveryStatus.DELIVERED else None


@pytest.mark.parametrize("reply_role", ["owner", "bot"])
async def test_context_window_ends_at_old_reply_and_excludes_current_batch(
    postgres_dsn, reply_role
):
    now = datetime(2026, 11, 22, 12, tzinfo=UTC)
    yesterday = now - timedelta(days=1)
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            await owner_message(
                session, 1, yesterday - timedelta(hours=3, seconds=1), body="too old"
            )
            await owner_message(
                session,
                2,
                yesterday - timedelta(hours=3),
                transcript="voice",
                forward_label="Канал",
            )
            await owner_message(session, 3, yesterday, body="old owner")
            await bot_message(session, 4, yesterday, "old bot")
            await owner_message(session, 5, yesterday + timedelta(seconds=1), body="after anchor")
            await owner_message(session, 6, now - timedelta(minutes=1), body="recent")
            current = await owner_message(
                session,
                7,
                now,
                body="follow-up",
                reply_to_message_id=3 if reply_role == "owner" else 4,
            )
            sibling = await owner_message(session, 8, now, body="same batch")
            sibling.batch_id = current.batch_id
        worker, _ = worker_for(factory, FakeModel())
        conversation = await worker._conversation(OWNER, [current, sibling], now=now)
        assert [m["text"] for m in conversation] == ["Канал\nvoice", "old owner", "old bot"]
        assert conversation[-1]["time"] == "2026-11-21T13:00:00+01:00"


async def test_context_keeps_latest_fifteen_and_always_keeps_replied_message(postgres_dsn):
    now = datetime(2026, 11, 22, 12, tzinfo=UTC)
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            for number in range(20):
                await owner_message(
                    session, number + 1, now - timedelta(minutes=20 - number), body=str(number)
                )
            await bot_message(session, 30, now, "unseen", status=DeliveryStatus.PENDING)
            await bot_message(session, 31, now + timedelta(seconds=1), "future")
            current = await owner_message(session, 40, now, body="excluded", reply_to_message_id=1)
        worker, _ = worker_for(factory, FakeModel())
        conversation = await worker._conversation(OWNER, [], now=now)
        assert [m["text"] for m in conversation] == [str(n) for n in range(6, 20)] + ["excluded"]
        conversation = await worker._conversation(OWNER, [current], now=now)
        assert [m["text"] for m in conversation] == ["0"] + [str(n) for n in range(6, 20)]
        worker._settings = worker._settings.model_copy(
            update={"context_messages": 2, "context_hours": 1}
        )
        assert len(await worker._conversation(OWNER, [current], now=now)) == 2


async def test_context_caps_text_and_scopes_chat(postgres_dsn):
    now = datetime.now(UTC)
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            await owner_message(session, 1, now, body="x" * 800)
            other = await owner_message(session, 2, now, body="private")
            other.chat_id = OWNER + 1
        worker, _ = worker_for(factory, FakeModel())
        conversation = await worker._conversation(OWNER, [], now=now)
        assert len(conversation) == 1
        assert len(conversation[0]["text"]) == 500


async def test_context_hours_setting_changes_window(postgres_dsn):
    now = datetime.now(UTC)
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            await owner_message(session, 1, now - timedelta(hours=2), body="earlier")
            await owner_message(session, 2, now - timedelta(minutes=30), body="recent")
        worker, _ = worker_for(factory, FakeModel())
        assert len(await worker._conversation(OWNER, [], now=now)) == 2
        worker._settings = worker._settings.model_copy(update={"context_hours": 1})
        assert [m["text"] for m in await worker._conversation(OWNER, [], now=now)] == ["recent"]
