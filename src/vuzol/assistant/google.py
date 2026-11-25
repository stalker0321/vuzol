"""The owner's Google Calendar and Gmail for the assistant: read mail, read and add events.

One token file holds the OAuth client and the owner's refresh token
(``vuzol.cli.google_login`` writes it). Mail is read-only; nothing here sends mail.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import date, datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

SCOPES = (
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/gmail.readonly",
)
TOKEN_URL = "https://oauth2.googleapis.com/token"  # noqa: S105 - a URL, not a secret
_CALENDAR = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
_GMAIL = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
_MAIL_RESULTS = 6
_BODY_CHARS = 2_500
_EVENTS = 50


class GoogleError(Exception):
    pass


class GoogleAccount:
    def __init__(
        self, token_path: Path, *, timezone: str, client: httpx.AsyncClient | None = None
    ) -> None:
        self._path = token_path
        self._tz = timezone
        self._client = client or httpx.AsyncClient(timeout=30)
        self._access: tuple[str, float] | None = None

    async def _token(self) -> str:
        if self._access is not None and self._access[1] > time.monotonic() + 60:
            return self._access[0]
        try:
            stored = json.loads(self._path.read_text())
        except (OSError, ValueError) as error:
            raise GoogleError("google_token_unreadable") from error
        response = await self._client.post(
            TOKEN_URL,
            data={
                "client_id": stored["client_id"],
                "client_secret": stored["client_secret"],
                "refresh_token": stored["refresh_token"],
                "grant_type": "refresh_token",
            },
        )
        if response.status_code != 200:
            # invalid_grant: the owner revoked access or the app is still in testing mode.
            raise GoogleError(f"google_refresh_{response.status_code}")
        body = response.json()
        self._access = (body["access_token"], time.monotonic() + float(body["expires_in"]))
        return self._access[0]

    async def _call(self, method: str, url: str, **kwargs: Any) -> httpx.Response:  # noqa: ANN401
        headers = {"Authorization": f"Bearer {await self._token()}"}
        return await self._client.request(method, url, headers=headers, **kwargs)

    async def events(self, start: date, end: date) -> list[dict[str, Any]]:
        zone = ZoneInfo(self._tz)
        response = await self._call(
            "GET",
            _CALENDAR,
            params={
                "timeMin": datetime.combine(start, datetime.min.time(), zone).isoformat(),
                "timeMax": datetime.combine(
                    end + timedelta(days=1), datetime.min.time(), zone
                ).isoformat(),
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": _EVENTS,
                "timeZone": self._tz,
            },
        )
        if response.status_code != 200:
            raise GoogleError(f"calendar_{response.status_code}")
        return [
            {
                "title": item.get("summary", ""),
                "start": item.get("start", {}).get("dateTime") or item.get("start", {}).get("date"),
                "end": item.get("end", {}).get("dateTime") or item.get("end", {}).get("date"),
                "location": item.get("location"),
            }
            for item in response.json().get("items", [])
        ]

    async def add_event(
        self,
        event_id: str,
        *,
        title: str,
        start: datetime,
        end: datetime,
        location: str | None = None,
        description: str | None = None,
    ) -> bool:
        """False when an event with this id exists already (a retried turn)."""

        body: dict[str, Any] = {
            "id": event_id,
            "summary": title,
            "start": {"dateTime": start.isoformat()},
            "end": {"dateTime": end.isoformat()},
        }
        if location:
            body["location"] = location
        if description:
            body["description"] = description
        response = await self._call("POST", _CALENDAR, json=body)
        if response.status_code == 409:
            return False
        if response.status_code not in (200, 201):
            raise GoogleError(f"calendar_insert_{response.status_code}")
        return True

    async def mail(self, query: str, *, limit: int = _MAIL_RESULTS) -> list[dict[str, Any]]:
        listed = await self._call("GET", _GMAIL, params={"q": query, "maxResults": limit})
        if listed.status_code != 200:
            raise GoogleError(f"gmail_{listed.status_code}")
        found = []
        for ref in listed.json().get("messages", []):
            response = await self._call("GET", f"{_GMAIL}/{ref['id']}", params={"format": "full"})
            if response.status_code != 200:
                continue
            message = response.json()
            headers = {
                h["name"].lower(): h["value"] for h in message.get("payload", {}).get("headers", [])
            }
            found.append(
                {
                    "id": ref["id"],
                    "from": headers.get("from"),
                    "subject": headers.get("subject"),
                    "date": headers.get("date"),
                    "snippet": unescape(message.get("snippet", "")),
                    "text": _plain_text(message.get("payload", {}))[:_BODY_CHARS],
                }
            )
        return found


def _plain_text(part: dict[str, Any]) -> str:
    if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
        data = part["body"]["data"]
        return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4)).decode(errors="replace")
    for child in part.get("parts", []) or []:
        text = _plain_text(child)
        if text:
            return text
    return ""
