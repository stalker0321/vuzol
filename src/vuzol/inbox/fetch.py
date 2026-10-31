"""Read what an inbox link points to: a web page or a GitHub repository.

Links come from forwarded posts, so they are untrusted: public http(s) only, no
private or loopback addresses (checked on every redirect), bounded size and time.
Fetched text is evidence for the model, never instructions.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import re
from contextlib import suppress
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import httpx

_MAX_BYTES = 1_500_000
_MAX_REDIRECTS = 3
_TIMEOUT = 15.0
_PAGE_CHARS = 8_000
_README_CHARS = 10_000
_FILE_CHARS = 3_000
_USER_AGENT = "Mozilla/5.0 (compatible; VuzolInbox/1.0)"
_GITHUB = re.compile(r"^https?://(?:www\.)?github\.com/([\w.-]+)/([\w.-]+?)(?:\.git)?(?:[/?#].*)?$")
# Files that show how a tool installs and what it runs on a user's machine.
_INSTALL_FILES = (
    "install.sh",
    "setup.sh",
    "package.json",
    "pyproject.toml",
    "setup.py",
    "Dockerfile",
    "Makefile",
    "Cargo.toml",
    "go.mod",
)


class FetchError(RuntimeError):
    pass


async def describe_link(url: str, client: httpx.AsyncClient | None = None) -> str:
    """Short text about the link, or a note why it could not be read."""
    try:
        async with _client(client) as http:
            match = _GITHUB.match(url)
            if match and match.group(2) not in {"orgs", "topics", "features", "settings"}:
                return await _github(http, match.group(1), match.group(2))
            return await _page(http, url)
    except (FetchError, httpx.HTTPError, ValueError) as error:
        return f"[{url}] не удалось открыть: {type(error).__name__}: {str(error)[:120]}"


class _client:
    def __init__(self, client: httpx.AsyncClient | None) -> None:
        self._given = client
        self._own: httpx.AsyncClient | None = None

    async def __aenter__(self) -> httpx.AsyncClient:
        if self._given is not None:
            return self._given
        self._own = httpx.AsyncClient(
            timeout=_TIMEOUT, follow_redirects=False, headers={"User-Agent": _USER_AGENT}
        )
        return self._own

    async def __aexit__(self, *_exc: object) -> None:
        if self._own is not None:
            await self._own.aclose()


async def _public(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise FetchError("only public http(s) links")
    if parts.port not in {None, 80, 443}:
        raise FetchError("non-standard port")
    infos = await asyncio.get_running_loop().getaddrinfo(parts.hostname, None)
    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if not address.is_global:
            raise FetchError("private address")


async def _get(http: httpx.AsyncClient, url: str, **headers: str) -> httpx.Response:
    for _ in range(_MAX_REDIRECTS + 1):
        await _public(url)
        async with http.stream("GET", url, headers=headers) as response:
            if response.is_redirect and "location" in response.headers:
                url = urljoin(url, response.headers["location"])
                continue
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > _MAX_BYTES:
                    break
            # The body is already decompressed: drop the encoding so it is not decoded twice.
            headers = {
                key: value
                for key, value in response.headers.items()
                if key.lower() not in {"content-encoding", "content-length", "transfer-encoding"}
            }
            return httpx.Response(
                response.status_code,
                headers=headers,
                content=bytes(body[:_MAX_BYTES]),
                request=response.request,
            )
    raise FetchError("too many redirects")


async def _page(http: httpx.AsyncClient, url: str) -> str:
    response = await _get(http, url)
    if response.status_code >= 400:
        raise FetchError(f"HTTP {response.status_code}")
    kind = response.headers.get("content-type", "")
    if "html" in kind or not kind:
        text = page_text(response.text)
    elif kind.startswith(("text/", "application/json")):
        text = response.text
    else:
        return f"[{url}] файл {kind}, не текст"
    return f"[{url}]\n{text[:_PAGE_CHARS]}"


async def _github(http: httpx.AsyncClient, owner: str, repo: str) -> str:
    api = f"https://api.github.com/repos/{owner}/{repo}"
    headers = {"Accept": "application/vnd.github+json"}
    info = await _get(http, api, **headers)
    if info.status_code >= 400:
        raise FetchError(f"GitHub HTTP {info.status_code}")
    meta = info.json()
    facts = {
        "repo": meta.get("full_name"),
        "description": meta.get("description"),
        "homepage": meta.get("homepage"),
        "stars": meta.get("stargazers_count"),
        "forks": meta.get("forks_count"),
        "open_issues": meta.get("open_issues_count"),
        "language": meta.get("language"),
        "topics": meta.get("topics"),
        "license": (meta.get("license") or {}).get("spdx_id"),
        "created_at": meta.get("created_at"),
        "last_push": meta.get("pushed_at"),
        "archived": meta.get("archived"),
        "fork": meta.get("fork"),
    }
    parts = [f"[GitHub {owner}/{repo}]", json.dumps(facts, ensure_ascii=False)]
    listing = await _get(http, f"{api}/contents", **headers)
    names = (
        [entry.get("name") for entry in listing.json() if isinstance(entry, dict)]
        if listing.status_code == 200 and isinstance(listing.json(), list)
        else []
    )
    if names:
        parts.append("root files: " + ", ".join(str(name) for name in names[:80]))
    readme = await _get(http, f"{api}/readme", Accept="application/vnd.github.raw")
    if readme.status_code == 200:
        parts.append("README:\n" + readme.text[:_README_CHARS])
    branch = meta.get("default_branch") or "HEAD"
    for name in [name for name in _INSTALL_FILES if name in names][:4]:
        raw = await _get(http, f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{name}")
        if raw.status_code == 200:
            parts.append(f"{name}:\n{raw.text[:_FILE_CHARS]}")
    return "\n\n".join(parts)


class _Text(HTMLParser):
    _SKIP = frozenset({"script", "style", "noscript", "svg", "nav", "footer", "header", "form"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.meta: dict[str, str] = {}
        self.chunks: list[str] = []
        self._skip = 0
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._SKIP:
            self._skip += 1
        elif tag == "title":
            self._in_title = True
        elif tag == "meta":
            values = dict(attrs)
            key = values.get("property") or values.get("name")
            if (
                key
                and values.get("content")
                and key
                in {
                    "description",
                    "og:title",
                    "og:description",
                    "og:site_name",
                    "article:published_time",
                }
            ):
                self.meta[key] = values["content"] or ""

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP and self._skip:
            self._skip -= 1
        elif tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
        elif not self._skip and data.strip():
            self.chunks.append(data.strip())


def page_text(html: str) -> str:
    parser = _Text()
    with suppress(AssertionError, ValueError):
        parser.feed(html)
    head = [f"title: {parser.title.strip()}"] if parser.title.strip() else []
    head += [f"{key}: {value}" for key, value in parser.meta.items()]
    body = unescape(" ".join(parser.chunks))
    return "\n".join([*head, re.sub(r"\s+", " ", body)])
