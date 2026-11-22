"""Topic searches cross record kinds and include tags in their evidence."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from tests.integration.inbox.test_inbox import FakeModel, worker_for
from tests.integration.leadership.test_managed_goal_flow import OWNER, _factory
from vuzol.inbox.schemas import Answer, Route, Search
from vuzol.storage.models import InboxItem

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize("words", ["фильмы", None])
async def test_film_search_keeps_notes_posts_and_tags_and_boosts_kind(postgres_dsn, words):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            rows = [
                ("note", "Киноляпы и фильмы — список из 30 фильмов", "", []),
                ("post", "Коллекция фильмов #фильмы, позиции 31–53", "", []),
                ("series", "Тегированная коллекция", "", ["фильмы"]),
                ("note", "Подборка", "Пять фильмов на вечер", []),
                ("film", "Arrival", "", []),
                ("series", "Dark", "", []),
                ("series", "Severance", "", []),
                ("series", "The Wire", "", []),
            ]
            for ordinal, (kind, title, summary, tags) in enumerate(rows):
                session.add(
                    InboxItem(
                        owner_user_id=OWNER,
                        chat_id=OWNER,
                        kind=kind,
                        title=title,
                        summary=summary,
                        tags=tags,
                        created_at=datetime.now(UTC) - timedelta(minutes=ordinal),
                    )
                )
            session.add_all(
                [
                    InboxItem(
                        owner_user_id=OWNER + 1,
                        chat_id=OWNER + 1,
                        kind="film",
                        title="Чужие фильмы",
                    ),
                    InboxItem(
                        owner_user_id=OWNER,
                        chat_id=OWNER,
                        kind="film",
                        title="Удалённые фильмы",
                        removed_at=datetime.now(UTC),
                    ),
                ]
            )
        model = FakeModel(Answer(text="Нашёл подборки"))
        worker, _ = worker_for(factory, model)
        await worker._search(
            Route(action="search", search=Search(words=words, kinds=["film"])),
            "поищи какие фильмы я сохранял",
            OWNER,
            OWNER,
            reply_to=100,
            key="films",
        )
        records = json.loads(model.calls[-1]["user"])["records"]
        titles = [record["title"] for record in records]
        assert "Киноляпы и фильмы — список из 30 фильмов" in titles
        assert "Коллекция фильмов #фильмы, позиции 31–53" in titles
        assert "Тегированная коллекция" in titles
        assert "Подборка" in titles
        assert titles[0] == "Arrival"
        assert len(titles) == 5
