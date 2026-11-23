"""Owner 2026-11-23: the assistant reads the owner's calendar and mail, and adds events."""

import base64
import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from vuzol.assistant.google import GoogleAccount, GoogleError

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def token(tmp_path: Path) -> Path:
    path = tmp_path / "token.json"
    path.write_text(json.dumps({"client_id": "c", "client_secret": "s", "refresh_token": "r"}))
    return path


def account(
    tmp_path: Path, handler: Callable[[httpx.Request], httpx.Response]
) -> tuple[GoogleAccount, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "oauth2.googleapis.com":
            return httpx.Response(200, json={"access_token": "A", "expires_in": 3600})
        assert request.headers["Authorization"] == "Bearer A"
        return handler(request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(record))
    return GoogleAccount(token(tmp_path), timezone="Europe/Amsterdam", client=client), seen


async def test_events_cover_whole_local_days_and_the_token_is_refreshed_once(tmp_path):
    def handler(request):
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "summary": "Стоматолог",
                        "start": {"dateTime": "2026-11-24T10:00:00+01:00"},
                        "end": {"dateTime": "2026-11-24T11:00:00+01:00"},
                    },
                    {
                        "summary": "Отпуск",
                        "start": {"date": "2026-11-25"},
                        "end": {"date": "2026-11-26"},
                    },
                ]
            },
        )

    google, seen = account(tmp_path, handler)
    events = await google.events(date(2026, 11, 24), date(2026, 11, 25))
    await google.events(date(2026, 11, 24), date(2026, 11, 24))
    assert [e["title"] for e in events] == ["Стоматолог", "Отпуск"]
    assert events[1]["start"] == "2026-11-25"
    assert [r.url.host for r in seen].count("oauth2.googleapis.com") == 1
    params = seen[1].url.params
    assert params["timeMin"] == "2026-11-24T00:00:00+01:00"
    assert params["timeMax"] == "2026-11-26T00:00:00+01:00"


async def test_mail_returns_headers_and_plain_text(tmp_path):
    body = base64.urlsafe_b64encode("Ваш билет: рейс KL1234".encode()).decode().rstrip("=")

    def handler(request):
        if request.url.path.endswith("/messages"):
            assert request.url.params["q"] == "from:klm"
            return httpx.Response(200, json={"messages": [{"id": "m1"}]})
        return httpx.Response(
            200,
            json={
                "snippet": "Ваш билет &amp; посадка",
                "payload": {
                    "mimeType": "multipart/alternative",
                    "headers": [
                        {"name": "From", "value": "KLM <no-reply@klm.com>"},
                        {"name": "Subject", "value": "Билет"},
                    ],
                    "parts": [
                        {"mimeType": "text/html", "body": {"data": "PGI-"}},
                        {"mimeType": "text/plain", "body": {"data": body}},
                    ],
                },
            },
        )

    google, _ = account(tmp_path, handler)
    (message,) = await google.mail("from:klm")
    assert message["from"].startswith("KLM") and message["subject"] == "Билет"
    assert message["snippet"] == "Ваш билет & посадка"
    assert message["text"] == "Ваш билет: рейс KL1234"


async def test_adding_an_event_twice_is_not_an_error(tmp_path):
    statuses = [200, 409]

    def handler(request):
        assert json.loads(request.content)["id"] == "abc0"
        return httpx.Response(statuses.pop(0), json={})

    google, _ = account(tmp_path, handler)
    start = datetime(2026, 11, 24, 9, tzinfo=UTC)
    kwargs = {"title": "Созвон", "start": start, "end": start}
    assert await google.add_event("abc0", **kwargs) is True
    assert await google.add_event("abc0", **kwargs) is False


async def test_a_revoked_login_is_a_google_error(tmp_path):
    def refuse(request):
        return httpx.Response(400, json={"error": "invalid_grant"})

    google = GoogleAccount(
        token(tmp_path),
        timezone="UTC",
        client=httpx.AsyncClient(transport=httpx.MockTransport(refuse)),
    )
    with pytest.raises(GoogleError, match="google_refresh_400"):
        await google.events(date(2026, 1, 1), date(2026, 1, 1))
