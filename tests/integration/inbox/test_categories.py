"""Catalog instructions, model proposals and corrections stay consistent with storage."""

import uuid

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
from vuzol.inbox.schemas import Card, Cards, Patch, Route
from vuzol.storage.models import InboxCategory, InboxItem, TransactionalOutbox

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def proposal(slug="podcast"):
    return {
        "slug": slug,
        "name": "Подкасты",
        "emoji": "🎙",
        "description": "Разговорные аудиопрограммы и их выпуски. Музыка сюда не относится.",
        "why_none_fits": "Это аудиопередача, а не музыка, статья или пост.",
    }


async def test_card_prompt_uses_current_database_catalog(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                InboxCategory(
                    **{k: v for k, v in proposal().items() if k != "why_none_fits"},
                    created_by="model",
                )
            )
        model = FakeModel(Cards(cards=[Card(category="podcast", title="Выпуск")]))
        worker, telegram = worker_for(factory, model)
        await admit(
            factory,
            private(
                1,
                text="Сохрани выпуск",
                forward_origin={
                    "type": "hidden_user",
                    "sender_user_name": "Канал",
                    "date": 1792000000,
                },
            ),
        )
        await drain(worker)
        system = model.calls[0]["system"]
        assert all(value in system for value in ("podcast", "Подкасты", proposal()["description"]))
        assert "Видеоигры любых платформ" in system
        async with factory() as session:
            item = await session.scalar(select(InboxItem))
            assert item.kind == "podcast"
            assert item.category.name == "Подкасты"
        assert "🆕" not in telegram.sent[0]["html"]


@pytest.mark.parametrize("slug", ["podcast", "audio-program"])
async def test_new_category_is_created_with_its_card_and_notice(postgres_dsn, slug):
    async with _factory(postgres_dsn) as factory:
        worker, _ = worker_for(factory, FakeModel())
        await worker._store(
            uuid.uuid4(),
            OWNER,
            OWNER,
            [Card(new_category=proposal(slug), title="Выпуск")],
            "",
            reply_to=1,
        )
        async with factory() as session:
            category = await session.get(InboxCategory, slug)
            assert category.created_by == "model" and category.created_at
            item = await session.scalar(select(InboxItem))
            assert item.kind == category.slug
            reply = await session.scalar(select(TransactionalOutbox))
            assert (
                f"🆕 новая категория: Подкасты — {category.description}"
                in reply.payload["html"].splitlines()
            )


@pytest.mark.parametrize("slug", ["Bad_slug", "подкаст", "-podcast", "pod--cast", "x" * 31, "book"])
async def test_invalid_or_duplicate_proposal_falls_back(postgres_dsn, slug, caplog):
    async with _factory(postgres_dsn) as factory:
        worker, _ = worker_for(factory, FakeModel())
        await worker._store(
            uuid.uuid4(),
            OWNER,
            OWNER,
            [Card(new_category=proposal(slug), title="Выпуск")],
            "",
            reply_to=1,
        )
        async with factory() as session:
            item = await session.scalar(select(InboxItem))
            assert item.kind == "other"
            reply = await session.scalar(select(TransactionalOutbox))
            assert "🆕" not in reply.payload["html"]
            assert len(list(await session.scalars(select(InboxCategory)))) == 17
        assert "category" in caplog.text


@pytest.mark.parametrize("slug", ["unknown", "", "bad\x00slug"])
async def test_unknown_category_falls_back_without_losing_card(postgres_dsn, caplog, slug):
    async with _factory(postgres_dsn) as factory:
        worker, _ = worker_for(factory, FakeModel())
        await worker._store(
            uuid.uuid4(), OWNER, OWNER, [Card(category=slug, title="Выпуск")], "", reply_to=1
        )
        async with factory() as session:
            assert (await session.scalar(select(InboxItem))).kind == "other"
        assert "category" in caplog.text


@pytest.mark.parametrize(
    "patch, expected",
    [
        (dict(category="game"), "game"),
        (dict(category="unknown"), "other"),
        (dict(new_category=proposal()), "podcast"),
        (dict(new_category=proposal("book")), "other"),
    ],
)
async def test_route_update_resolves_category(postgres_dsn, patch, expected):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            item = InboxItem(owner_user_id=OWNER, chat_id=OWNER, kind="note", title="Выпуск")
            session.add(item)
        model = FakeModel(Route(action="update", target="recent", patch=Patch(**patch)))
        worker, telegram = worker_for(factory, model)
        await admit(factory, private(1, text="Поправь категорию последней записи"))
        await drain(worker)
        async with factory() as session:
            stored = await session.get(InboxItem, item.id)
            assert stored.kind == expected
        assert "slug" in model.calls[0]["system"]
        assert ("🆕 новая категория:" in telegram.sent[0]["html"]) == (expected == "podcast")


async def test_render_digest_and_recall_use_catalog_labels(postgres_dsn):
    from vuzol.inbox.recall import inbox_hits
    from vuzol.inbox.render import card_html, digest_html, digest_markdown

    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                InboxCategory(
                    slug="reading",
                    name="Чтение",
                    emoji="🔖",
                    description="Тексты.",
                    created_by="model",
                )
            )
            await session.flush()
            session.add(
                InboxItem(
                    owner_user_id=OWNER, chat_id=OWNER, kind="reading", title="Каталог приключений"
                )
            )
        async with factory() as session:
            item = await session.scalar(select(InboxItem))
            assert "🔖 Чтение" in card_html(item, "UTC")
            assert "🔖 Чтение" in digest_html([item], [], "UTC")
            assert "🔖 Чтение" in digest_markdown([item], [], "UTC")
            hits = await inbox_hits(session, owner_user_id=OWNER, query="приключений")
            assert "🔖 Чтение" in hits[0]["text"]


async def test_search_uses_catalog_names_and_boosts_without_filtering(postgres_dsn):
    import json

    from vuzol.inbox.schemas import Answer, Search

    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            session.add(
                InboxCategory(
                    **{k: v for k, v in proposal().items() if k != "why_none_fits"},
                    created_by="model",
                )
            )
            await session.flush()
            session.add_all(
                [
                    InboxItem(owner_user_id=OWNER, chat_id=OWNER, kind="podcast", title="Выпуск"),
                    InboxItem(
                        owner_user_id=OWNER, chat_id=OWNER, kind="note", title="Подкасты для дороги"
                    ),
                    InboxItem(owner_user_id=OWNER, chat_id=OWNER, kind="book", title="Чужая книга"),
                ]
            )
        model = FakeModel(Answer(text="Нашёл"))
        worker, _ = worker_for(factory, model)
        await worker._search(
            Route(action="search", search=Search(kinds=["podcast"])),
            "Подкасты?",
            OWNER,
            OWNER,
            reply_to=1,
            key="catalog",
        )
        records = json.loads(model.calls[0]["user"])["records"]
        assert [record["title"] for record in records] == ["Выпуск", "Подкасты для дороги"]
