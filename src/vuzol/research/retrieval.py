"""Approved HTTP retrieval adapter, variant B (WP06).

Deterministic in CI (frozen fixtures, live forbidden); live fetch is an
explicit manual opt-in with allowlist + bounded redirects/bytes/time.
Retrieved bytes are opaque data: never executed, injection stays inert.
"""

from __future__ import annotations

import hashlib
import urllib.parse
from dataclasses import dataclass, field
from typing import Protocol

from vuzol.projects.descriptors import CapabilityDescriptor, descriptor_for_capability


@dataclass(frozen=True, slots=True)
class RetrievalBounds:
    max_redirects: int = 3
    max_bytes: int = 1_000_000
    timeout_seconds: float = 10.0


@dataclass(frozen=True, slots=True)
class RetrievedSource:
    uri: str
    final_uri: str
    status_code: int
    content: bytes
    retrieved_at: str
    redirect_count: int = 0

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    def as_text(self) -> str:
        """Decode as opaque data; markup/scripts stay inert bytes-turned-text."""

        return self.content.decode("utf-8", errors="replace")


class RetrievalError(RuntimeError):
    """Stable, fail-closed retrieval rejection."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class TransportResponse:
    status_code: int
    content: bytes
    location: str | None = None


class RetrievalTransport(Protocol):
    def get(self, uri: str, bounds: RetrievalBounds) -> TransportResponse: ...


@dataclass(slots=True)
class ApprovedHttpRetrieval:
    """Bounded fetch behind an allowlist; live requires explicit opt-in."""

    allowlist: frozenset[str] = frozenset()
    bounds: RetrievalBounds = field(default_factory=RetrievalBounds)
    allow_live: bool = False
    transport: RetrievalTransport | None = None

    def fetch(self, uri: str, *, now: str) -> RetrievedSource:
        if self.transport is None and not self.allow_live:
            raise RetrievalError("live_forbidden")
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme not in ("http", "https"):
            raise RetrievalError("scheme_not_allowed")
        if parsed.hostname not in self.allowlist:
            raise RetrievalError("host_not_allowlisted")
        current = uri
        redirects = 0
        while True:
            response = self._get(current)
            if response.status_code in (301, 302, 303, 307, 308) and response.location:
                redirects += 1
                if redirects > self.bounds.max_redirects:
                    raise RetrievalError("too_many_redirects")
                current = urllib.parse.urljoin(current, response.location)
                if urllib.parse.urlparse(current).hostname not in self.allowlist:
                    raise RetrievalError("host_not_allowlisted")
                continue
            if len(response.content) > self.bounds.max_bytes:
                raise RetrievalError("byte_limit_exceeded")
            return RetrievedSource(
                uri=uri,
                final_uri=current,
                status_code=response.status_code,
                content=response.content,
                retrieved_at=now,
                redirect_count=redirects,
            )

    def _get(self, uri: str) -> TransportResponse:
        if self.transport is not None:
            try:
                result: TransportResponse = self.transport.get(uri, self.bounds)
            except RetrievalError:
                raise
            except Exception as error:
                raise RetrievalError("fetch_failed") from error
            return result
        return _urllib_get(uri, self.bounds)


def _urllib_get(uri: str, bounds: RetrievalBounds) -> TransportResponse:
    import urllib.request

    request = urllib.request.Request(uri, headers={"User-Agent": "vuzol-research/1"})  # noqa: S310
    try:
        with urllib.request.urlopen(request, timeout=bounds.timeout_seconds) as reply:  # noqa: S310
            status = reply.status
            if status in (301, 302, 303, 307, 308):
                return TransportResponse(
                    status_code=status, content=b"", location=reply.headers.get("Location")
                )
            chunks: list[bytes] = []
            remaining = bounds.max_bytes + 1
            while remaining > 0:
                chunk = reply.read(min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            return TransportResponse(status_code=status, content=b"".join(chunks))
    except TimeoutError as error:
        raise RetrievalError("timeout") from error
    except RetrievalError:
        raise
    except Exception as error:
        raise RetrievalError("fetch_failed") from error


@dataclass(frozen=True, slots=True)
class FixtureRetrieval:
    """Deterministic CI retrieval over frozen fixtures (no network)."""

    fixtures: dict[str, bytes]

    def fetch(self, uri: str, *, now: str) -> RetrievedSource:
        try:
            content = self.fixtures[uri]
        except KeyError as error:
            raise RetrievalError("fixture_missing") from error
        return RetrievedSource(
            uri=uri,
            final_uri=uri,
            status_code=200,
            content=content,
            retrieved_at=now,
            redirect_count=0,
        )


def select_retrieval_descriptor() -> CapabilityDescriptor:
    """Select the vetted retrieval capability from the WP03 registry."""

    descriptor: CapabilityDescriptor | None = descriptor_for_capability("web_research")
    if descriptor is None:
        raise RetrievalError("retrieval_capability_unregistered")
    return descriptor


def utc_now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
