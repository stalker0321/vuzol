"""Real ingress, authorized reads, durable chunk delivery; no external Telegram I/O."""
# ruff: noqa: RUF001

import pytest
from sqlalchemy import func, select
from telegram.error import BadRequest, TimedOut
from tests.integration.leadership.test_lead_surface import _message
from tests.integration.leadership.test_managed_goal_flow import _factory, _runtime
from tests.integration.telegram.test_delivery import service as delivery_service
from tests.unit.test_telegram_reply_chunks import visible

from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import Task, TransactionalOutbox, WorkPackage
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.domain import IngressStatus
from vuzol.telegram.formatting import telegram_markdown_html
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient, enqueue_chat_notice

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def drain(factory, client):
    worker = delivery_service(factory, client)
    for _ in range(30):
        if not await worker.deliver_one():
            return
    raise AssertionError("delivery did not drain")


async def test_short_state_and_lossless_full_goal_are_distinct_authorized_reads(
    postgres_dsn, tmp_path, monkeypatch
):
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    repo = tmp_path / "repositories" / "vuzol"
    repo.mkdir(parents=True)
    runtime = _runtime(tmp_path, repo, lead=True, discussion=False)
    disabled = _runtime(tmp_path, repo, lead=False, discussion=False)
    request = "Исследуй доступные решения. " * 100 + "ФИНАЛ <b> & **literal**"
    async with _factory(postgres_dsn) as factory:
        ingress = TelegramIngressService(runtime, factory)
        client = FakeTelegramClient()
        result = await ingress.accept_message(_message(701, "/lead " + request))
        assert result.status is IngressStatus.HANDLED
        await drain(factory, client)
        assert len(client.sent) == 1, "command echo duplicated the durable state card"
        card = client.sent[0][2]
        assert "<b>В РАБОТЕ · Руководитель</b>" in card
        assert len(visible(card)) < 1000 and "ФИНАЛ" not in visible(card)
        assert "/lead_goal" in card and "/lead_details" in card
        assert client.rich_sent == [], "short system cards should use ordinary HTML"
        await ingress.accept_message(_message(701, "/lead " + request))
        await drain(factory, client)
        assert len(client.sent) == 1, "replayed mutation must not echo the successful receipt"

        await ingress.accept_message(_message(702, "/lead_pause"))
        before = len(client.sent)
        await drain(factory, client)
        assert len(client.sent) == before + 1
        assert "НА ПАУЗЕ" in client.sent[-1][2]
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            version, goal_id = goal.version, goal.id
        reader = TelegramIngressService(disabled, factory)
        before = len(client.sent)
        full = _message(703, "/lead_goal")
        assert (await reader.accept_message(full)).status is IngressStatus.HANDLED
        # Only the first chunk exists until its send is confirmed.
        async with factory() as session:
            notices = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "telegram",
                        TransactionalOutbox.status == DeliveryStatus.PENDING,
                    )
                )
            )
            assert len(notices) == 1
            assert len(notices[0].payload["reply_parts"]) > 1
        await drain(factory, client)
        full_text = "".join(visible(body) for _, _, body in client.sent[before:])
        assert request in full_text
        assert "Критерии готовности" in full_text
        assert all(
            len(visible(body).encode("utf-16-le")) // 2 <= 4096
            for _, _, body in client.sent[before:]
        )
        count = len(client.sent)
        await reader.accept_message(full)  # Same delivery identity, no duplicate parts.
        await drain(factory, client)
        assert len(client.sent) == count
        await reader.accept_message(_message(704, "/lead_details"))
        await drain(factory, client)
        assert "Технические подробности" in client.sent[-1][2]
        assert str(goal_id) in client.sent[-1][2]
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "paused" and goal.version == version
            assert goal.original_request == request
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert await session.scalar(select(func.count()).select_from(WorkPackage)) == 0


async def test_an_allowed_telegram_user_cannot_read_another_owners_goal(
    postgres_dsn, tmp_path, monkeypatch
):
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    repo = tmp_path / "repositories" / "vuzol"
    repo.mkdir(parents=True)
    runtime = _runtime(tmp_path, repo, lead=True, discussion=False)
    runtime = runtime.model_copy(
        update={"settings": runtime.settings.model_copy(update={"allowed_user_ids": (42, 99)})}
    )
    async with _factory(postgres_dsn) as factory:
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(_message(711, "/lead private-contract-canary"))
        await drain(factory, FakeTelegramClient())
        client = FakeTelegramClient()
        for index, command in enumerate(("/lead_status", "/lead_goal", "/lead_details"), 712):
            reply = await ingress.accept_message(
                _message(index, command).model_copy(update={"user_id": 99})
            )
            assert reply.reason == "denied"
        await drain(factory, client)
        assert len(client.sent) == 3
        assert all("private-contract-canary" not in body for _, _, body in client.sent)
        assert all("КОМАНДА НЕ ПРИМЕНЕНА" in body for _, _, body in client.sent)


async def test_completed_status_without_evidence_never_becomes_green(
    postgres_dsn, tmp_path, monkeypatch
):
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    repo = tmp_path / "repositories" / "vuzol"
    repo.mkdir(parents=True)
    runtime = _runtime(tmp_path, repo, lead=True, discussion=False)
    async with _factory(postgres_dsn) as factory:
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(_message(721, "/lead outcome needs proof"))
        await drain(factory, FakeTelegramClient())
        # Inject an inconsistent/historical row. Rendering must not repair or trust it.
        async with factory.begin() as session:
            goal = await session.scalar(select(ManagedGoal))
            goal.lifecycle = "completed"
        await ingress.accept_message(_message(722, "/lead_status"))
        client = FakeTelegramClient()
        await drain(factory, client)
        assert len(client.sent) == 1
        assert client.sent[0][2].startswith("🟡")
        assert "НУЖНА ПРОВЕРКА" in client.sent[0][2] and "✅" not in client.sent[0][2]


async def test_command_without_an_origin_notice_still_replies_to_its_caller(
    postgres_dsn, tmp_path, monkeypatch
):
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    repo = tmp_path / "repositories" / "vuzol"
    repo.mkdir(parents=True)
    runtime = _runtime(tmp_path, repo, lead=True, discussion=False)
    async with _factory(postgres_dsn) as factory:
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(_message(731, "/lead preserve replies"))
        await drain(factory, FakeTelegramClient())
        async with factory.begin() as session:
            goal = await session.scalar(select(ManagedGoal))
            # Represent a goal without a Telegram notification origin.
            goal.source_chat_id = 0
            goal.source_thread_id = None
        reply = await ingress.accept_message(_message(732, "/lead_pause"))
        assert reply.reason == "applied"
        client = FakeTelegramClient()
        await drain(factory, client)
        assert len(client.sent) == 1
        assert client.sent[0][:2] == (-100, 10)
        assert "НА ПАУЗЕ" in client.sent[0][2]


async def test_formatted_notice_uses_plain_fallback_and_keeps_chunk_order(postgres_dsn):
    class RejectMarkupOnce(FakeTelegramClient):
        rejected = False

        async def send_message(self, **kwargs):
            if not self.rejected:
                self.rejected = True
                raise BadRequest("can't parse entities")
            return await super().send_message(**kwargs)

    text = "**Первая часть**\n\n" + "😀 <literal> & конец " * 700
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            await enqueue_chat_notice(
                session, chat_id=-100, thread_id=10, key="long-notice", text=text, markdown=True
            )
        client = RejectMarkupOnce()
        first_worker = delivery_service(factory, client)
        assert await first_worker.deliver_one()
        assert len(client.sent) == 1
        # A replacement worker resumes the persisted chain, without repeating part one.
        await drain(factory, client)
        assert client.rejected and len(client.sent) > 1
        assert "<b>" not in client.sent[0][2]
        assert "".join(visible(body) for _, _, body in client.sent) == visible(
            telegram_markdown_html(text)
        )
        async with factory() as session:
            rows = list(await session.scalars(select(TransactionalOutbox)))
            assert len(rows) == len(client.sent)
            assert all(row.status == DeliveryStatus.DELIVERED for row in rows)


async def test_unknown_first_send_never_admits_the_next_notice_part(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            await enqueue_chat_notice(
                session,
                chat_id=-100,
                thread_id=10,
                key="ambiguous-notice",
                text="**Очень длинный ответ**\n\n" + "я" * 9000,
                markdown=True,
            )
        client = FakeTelegramClient(fail=TimedOut("response lost"))
        await drain(factory, client)
        async with factory() as session:
            rows = list(await session.scalars(select(TransactionalOutbox)))
            assert len(rows) == 1
            assert rows[0].status == DeliveryStatus.AMBIGUOUS
        assert client.sent == []


async def test_existing_plain_notices_remain_literal(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            await enqueue_chat_notice(
                session,
                chat_id=-100,
                thread_id=10,
                key="old-notice",
                text="<b>not formatting</b> & **plain**",
            )
        client = FakeTelegramClient()
        await drain(factory, client)
        assert [body for _, _, body in client.sent] == [
            "&lt;b&gt;not formatting&lt;/b&gt; &amp; **plain**"
        ]
