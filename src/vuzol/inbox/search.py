"""Web and GitHub search for inbox answers.

Brave first (its key is read on every search, so dropping the file in turns it
on without a restart), then a self-hosted SearXNG. GitHub repository search is
separate: it gives real stars and activity for "what are the alternatives".
Results are untrusted data for the model, like fetched pages.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from vuzol.observability import get_logger

_LOG = get_logger(__name__)
_TIMEOUT = 15.0
_RESULTS = 5


@dataclass(frozen=True, slots=True)
class Hit:
    title: str
    url: str
    snippet: str

    def as_dict(self) -> dict[str, str]:
        return {"title": self.title, "url": self.url, "snippet": self.snippet}


class WebSearch:
    def __init__(
        self,
        *,
        brave_key_file: Path | None = None,
        searxng_url: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._brave_key_file = brave_key_file
        self._searxng_url = searxng_url.rstrip("/") if searxng_url else None
        self._client = client

    async def search(self, query: str) -> list[Hit]:
        for name, engine in (("brave", self._brave), ("searxng", self._searxng)):
            try:
                hits = await engine(query)
            except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
                _LOG.warning(
                    "inbox search engine failed",
                    extra={
                        "event": "inbox.search_failed",
                        "engine": name,
                        "error": str(error)[:200],
                    },
                )
                continue
            if hits:
                return hits[:_RESULTS]
        return []

    async def github(self, query: str) -> list[dict[str, Any]]:
        try:
            response = await self._get(
                "https://api.github.com/search/repositories",
                params={"q": query, "sort": "stars", "per_page": str(_RESULTS)},
                headers={"Accept": "application/vnd.github+json"},
            )
            response.raise_for_status()
            items = response.json().get("items", [])
        except (httpx.HTTPError, ValueError) as error:
            _LOG.warning(
                "inbox github search failed",
                extra={"event": "inbox.github_search_failed", "error": str(error)[:200]},
            )
            return []
        return [
            {
                "repo": item.get("full_name"),
                "url": item.get("html_url"),
                "description": item.get("description"),
                "stars": item.get("stargazers_count"),
                "last_push": item.get("pushed_at"),
                "license": (item.get("license") or {}).get("spdx_id"),
                "archived": item.get("archived"),
            }
            for item in items[:_RESULTS]
        ]

    async def _brave(self, query: str) -> list[Hit]:
        key = _read_key(self._brave_key_file)
        if key is None:
            return []
        response = await self._get(
            "https://api.search.brave.com/res/v1/web/search",
            params={"q": query, "count": str(_RESULTS)},
            headers={"Accept": "application/json", "X-Subscription-Token": key},
        )
        response.raise_for_status()
        return [
            Hit(str(r.get("title", "")), str(r["url"]), str(r.get("description", "")))
            for r in response.json().get("web", {}).get("results", [])
        ]

    async def _searxng(self, query: str) -> list[Hit]:
        if self._searxng_url is None:
            return []
        response = await self._get(
            f"{self._searxng_url}/search", params={"q": query, "format": "json"}, headers={}
        )
        response.raise_for_status()
        return [
            Hit(str(r.get("title", "")), str(r["url"]), str(r.get("content", "")))
            for r in response.json().get("results", [])
        ]

    async def _get(
        self, url: str, *, params: dict[str, str], headers: dict[str, str]
    ) -> httpx.Response:
        if self._client is not None:
            return await self._client.get(url, params=params, headers=headers, timeout=_TIMEOUT)
        async with httpx.AsyncClient() as client:
            return await client.get(url, params=params, headers=headers, timeout=_TIMEOUT)


def _read_key(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        key = path.read_text().strip()
    except OSError:
        return None
    return key or None
