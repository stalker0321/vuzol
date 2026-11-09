"""Bounded, unauthenticated source readers. All fetched text is untrusted data."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import NoReturn
from urllib.parse import urlencode, urljoin, urlsplit
from xml.parsers import expat

import httpx

from vuzol.config.settings import MonitorSource

_MAX_BYTES = 2 * 1024 * 1024
_USER_AGENT = "Vuzol-Monitor/1.0 (public-source digest; low-frequency reader)"


@dataclass(frozen=True, slots=True)
class CollectedItem:
    external_id: str
    url: str
    title: str
    excerpt: str = ""
    published_at: datetime | None = None


def _date(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, UTC)
        try:
            moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            moment = parsedate_to_datetime(str(value))
        return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)
    except (ValueError, TypeError, OverflowError):
        return None


def _url(source: MonitorSource) -> str:
    if source.kind == "rss":
        return source.target
    if source.kind == "github_releases":
        return f"https://api.github.com/repos/{source.target}/releases"
    if source.kind == "hn":
        return "https://hn.algolia.com/api/v1/search_by_date?" + urlencode(
            {"tags": "story", "query": source.target}
        )
    return f"https://www.reddit.com/r/{source.target}/new.json?limit=50"


def _item(
    external_id: object, url: str, title: object, excerpt: object = "", published: object = None
) -> CollectedItem:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("source item has no HTTP link")
    return CollectedItem(
        str(external_id or url),
        url,
        str(title or "Без заголовка")[:300],
        str(excerpt or "")[:4000],
        _date(published),
    )


def _xml(body: bytes, base_url: str) -> list[CollectedItem]:
    # Expat sees declarations regardless of XML encoding (including UTF-16).
    # Reject before ElementTree can expand internal entities or build a tree.
    def reject(*_args: object) -> NoReturn:
        raise ValueError("XML declarations and entities are forbidden")

    parser = expat.ParserCreate()
    parser.StartDoctypeDeclHandler = reject
    parser.EntityDeclHandler = reject
    parser.ExternalEntityRefHandler = reject
    try:
        parser.Parse(body, True)
        root = ET.fromstring(body)  # noqa: S314 - declarations rejected above
    except (expat.ExpatError, ET.ParseError) as error:
        raise ValueError("invalid feed XML") from error
    items: list[CollectedItem] = []
    for entry in root.iter():
        if entry.tag.rsplit("}", 1)[-1] not in {"item", "entry"}:
            continue
        fields = {child.tag.rsplit("}", 1)[-1]: "".join(child.itertext()) for child in entry}
        link = fields.get("link", "")
        for child in entry:
            if (
                child.tag.rsplit("}", 1)[-1] == "link"
                and child.get("rel", "alternate") == "alternate"
            ):
                link = child.get("href") or link
                break
        if not link:
            continue
        items.append(
            _item(
                fields.get("guid") or fields.get("id"),
                urljoin(base_url, link),
                fields.get("title"),
                fields.get("description") or fields.get("summary") or fields.get("content") or "",
                fields.get("pubDate") or fields.get("published") or fields.get("updated"),
            )
        )
    return items


def _json(body: bytes, kind: str) -> list[CollectedItem]:
    data = json.loads(body)
    if kind == "github_releases":
        return [
            _item(
                row["id"],
                row["html_url"],
                row.get("name") or row.get("tag_name"),
                row.get("body"),
                row.get("published_at"),
            )
            for row in data
            if not row.get("draft")
        ]
    if kind == "hn":
        return [
            _item(
                row["objectID"],
                row.get("url") or f"https://news.ycombinator.com/item?id={row['objectID']}",
                row.get("title"),
                row.get("story_text"),
                row.get("created_at"),
            )
            for row in data["hits"]
        ]
    return [
        _item(
            row["id"],
            urljoin("https://www.reddit.com", row["permalink"]),
            row.get("title"),
            row.get("selftext"),
            row.get("created_utc"),
        )
        for child in data["data"]["children"]
        for row in [child["data"]]
    ]


async def collect(
    source: MonitorSource, *, client: httpx.AsyncClient | None = None
) -> list[CollectedItem]:
    if client is None:
        async with httpx.AsyncClient() as owned:
            return await collect(source, client=owned)
    url = _url(source)
    host = urlsplit(url).hostname
    for _ in range(4):
        async with client.stream(
            "GET", url, headers={"User-Agent": _USER_AGENT}, timeout=20, follow_redirects=False
        ) as response:
            if response.is_redirect:
                redirected = urljoin(url, response.headers["location"])
                parsed = urlsplit(redirected)
                if (
                    parsed.hostname != host
                    or parsed.scheme not in {"http", "https"}
                    or parsed.username
                ):
                    raise ValueError("cross-host redirect forbidden")
                url = redirected
                continue
            response.raise_for_status()
            if int(response.headers.get("content-length", "0")) > _MAX_BYTES:
                raise ValueError("source response exceeds 2 MB")
            body = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                body.extend(chunk)
                if len(body) > _MAX_BYTES:
                    raise ValueError("source response exceeds 2 MB")
            return (
                _xml(bytes(body), url) if source.kind == "rss" else _json(bytes(body), source.kind)
            )
    raise ValueError("too many source redirects")
