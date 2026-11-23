"""Thin saves get web evidence before the card is built."""

import json
from unittest.mock import AsyncMock

import pytest

from tests.integration.inbox.test_inbox import FakeModel, admit, drain, private, worker_for
from tests.integration.leadership.test_managed_goal_flow import _factory
from vuzol.inbox.schemas import Card, Cards, Lookup, Route
from vuzol.inbox.search import Hit

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
STEAM = "https://store.steampowered.com/app/123/pathogenic/"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize(
    "words", ["pathogenic", "запиши игру pathogenic. найди ее в стим и сохрани"]
)
async def test_own_title_is_looked_up_before_card(postgres_dsn, words):
    web = AsyncMock()
    web.search.return_value = [Hit("PATHOGENIC on Steam", STEAM, "A survival game")]
    page = AsyncMock(return_value="PATHOGENIC, Developer: Studio, Release: 2026")
    async with _factory(postgres_dsn) as factory:
        model = FakeModel(
            Route(action="save"),
            Lookup(queries=["pathogenic Steam"]),
            Cards(cards=[Card(category="other", title="PATHOGENIC", fields={"url": STEAM})]),
        )
        worker, _ = worker_for(factory, model, search=web)
        worker._read_link = page
        await admit(factory, private(1, text=words))
        await drain(worker)
        web.search.assert_awaited_once_with("pathogenic Steam")
        page.assert_awaited_once_with(STEAM)
        assert [c["output"] for c in model.calls] == [Route, Lookup, Cards]
        payload = json.loads(model.calls[-1]["user"])
        assert payload["web"]["pages"] == [page.return_value]
        assert payload["web"]["searches"]["pathogenic Steam"][0]["url"] == STEAM


async def test_forward_with_link_does_not_search_again(postgres_dsn):
    web = AsyncMock()
    page = AsyncMock(return_value="Detailed forwarded post")
    async with _factory(postgres_dsn) as factory:
        model = FakeModel(Cards(cards=[Card(category="post", title="Игры")]))
        worker, _ = worker_for(factory, model, search=web)
        worker._read_link = page
        await admit(
            factory,
            private(
                1,
                text=STEAM,
                entities=[{"type": "url", "offset": 0, "length": len(STEAM)}],
                forward_origin={
                    "type": "hidden_user",
                    "date": 1792000000,
                    "sender_user_name": "Игры",
                },
            ),
        )
        await drain(worker)
        web.search.assert_not_awaited()
        assert [c["output"] for c in model.calls] == [Cards]
        page.assert_awaited_once_with(STEAM)


async def test_thin_material_gets_lookup_even_without_route(postgres_dsn):
    web = AsyncMock()
    web.search.return_value = []
    async with _factory(postgres_dsn) as factory:
        model = FakeModel(
            Lookup(queries=["pathogenic game"]),
            Cards(cards=[Card(category="other", title="Pathogenic")]),
        )
        worker, _ = worker_for(factory, model, search=web, download=b"pathogenic")
        await admit(
            factory,
            private(
                1,
                document={
                    "file_id": "doc",
                    "file_unique_id": "doc1",
                    "file_name": "game.txt",
                    "mime_type": "text/plain",
                    "file_size": 10,
                },
            ),
        )
        await drain(worker)
        web.search.assert_awaited_once_with("pathogenic game")
        assert json.loads(model.calls[-1]["user"])["web"] == {
            "searches": {"pathogenic game": []},
            "pages": [],
            "github": {},
        }
