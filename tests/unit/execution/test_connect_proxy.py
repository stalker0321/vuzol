import asyncio
import json
import socket
from pathlib import Path
from typing import Any

import pytest

from vuzol.execution.connect_proxy import (
    MAX_DNS_RESULTS,
    ConnectProxyError,
    ConnectProxyPolicy,
    _address_is_public,
    _handle_client,
    _open_pinned_connection,
    _parse_connect_request,
    _relay,
    _resolve_public,
)


def _policy(tmp_path: Path, **updates: Any) -> Path:
    body: dict[str, Any] = {
        "version": 1,
        "targets": [{"hostname": "api.openai.com", "port": 443}],
        "connect_timeout_seconds": 5,
        "idle_timeout_seconds": 30,
        "tunnel_timeout_seconds": 300,
        "maximum_bytes_per_direction": 67_108_864,
    }
    body.update(updates)
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(body))
    return path


def test_policy_loads_only_closed_exact_targets(tmp_path: Path) -> None:
    policy = ConnectProxyPolicy.load(_policy(tmp_path))
    assert policy.targets == frozenset({("api.openai.com", 443)})
    assert policy.maximum_bytes_per_direction == 67_108_864


@pytest.mark.parametrize(
    "targets",
    [
        [],
        [{"hostname": "*.openai.com", "port": 443}],
        [{"hostname": "API.openai.com", "port": 443}],
        [{"hostname": "api.openai.com", "port": 80}],
        [{"hostname": "127.0.0.1", "port": 443}],
        [{"hostname": "api.openai.com", "port": 443, "purpose": "leak"}],
    ],
)
def test_policy_rejects_broadened_or_malformed_targets(
    tmp_path: Path, targets: list[dict[str, object]]
) -> None:
    with pytest.raises(ConnectProxyError):
        ConnectProxyPolicy.load(_policy(tmp_path, targets=targets))


def test_policy_rejects_unknown_fields_and_bad_limits(tmp_path: Path) -> None:
    path = _policy(tmp_path, **{"unexpected": "must-not-be-accepted"})
    with pytest.raises(ConnectProxyError, match="shape"):
        ConnectProxyPolicy.load(path)
    with pytest.raises(ConnectProxyError, match="out of bounds"):
        ConnectProxyPolicy.load(_policy(tmp_path, connect_timeout_seconds=31))


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"version": 2}, "version"),
        ({"targets": "api.openai.com"}, "version"),
        ({"connect_timeout_seconds": True}, "invalid"),
        ({"maximum_bytes_per_direction": 1023}, "invalid"),
        ({"maximum_bytes_per_direction": 1.5}, "invalid"),
    ],
)
def test_policy_rejects_bad_versions_types_and_bounds(
    tmp_path: Path, updates: dict[str, object], message: str
) -> None:
    with pytest.raises(ConnectProxyError, match=message):
        ConnectProxyPolicy.load(_policy(tmp_path, **updates))


def test_policy_rejects_missing_and_invalid_json(tmp_path: Path) -> None:
    with pytest.raises(ConnectProxyError, match="unavailable"):
        ConnectProxyPolicy.load(tmp_path / "missing.json")
    invalid = tmp_path / "invalid.json"
    invalid.write_text("{")
    with pytest.raises(ConnectProxyError, match="malformed"):
        ConnectProxyPolicy.load(invalid)


@pytest.mark.parametrize(
    "hostname",
    [
        "localhost",
        "metadata.google.internal",
        "service.local",
        ".example.com",
        "example.com.",
        "bad..example",
        "-bad.example",
        "bad-.example",
        "éxample.com",
    ],
)
def test_policy_rejects_additional_noncanonical_hostnames(tmp_path: Path, hostname: str) -> None:
    with pytest.raises(ConnectProxyError, match="canonical"):
        ConnectProxyPolicy.load(_policy(tmp_path, targets=[{"hostname": hostname, "port": 443}]))


def test_connect_parser_requires_exact_hostname_port_and_host_header() -> None:
    request = b"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com:443\r\n\r\n"
    assert _parse_connect_request(request) == ("api.openai.com", 443)


@pytest.mark.parametrize(
    "raw_request",
    [
        b"CONNECT api.openai.com:80 HTTP/1.1\r\nHost: api.openai.com:80\r\n\r\n",
        b"CONNECT 8.8.8.8:443 HTTP/1.1\r\nHost: 8.8.8.8:443\r\n\r\n",
        b"CONNECT [::1]:443 HTTP/1.1\r\nHost: [::1]:443\r\n\r\n",
        b"CONNECT api.openai.com:443 HTTP/1.0\r\n\r\n",
        b"GET https://api.openai.com/ HTTP/1.1\r\nHost: api.openai.com\r\n\r\n",
        b"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: other.example:443\r\n\r\n",
        b"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com:443\r\n folded\r\n\r\n",
    ],
)
def test_connect_parser_rejects_bypass_forms(raw_request: bytes) -> None:
    with pytest.raises(ConnectProxyError):
        _parse_connect_request(raw_request)


def test_health_endpoint_is_local_process_readiness_only() -> None:
    assert _parse_connect_request(b"GET /healthz HTTP/1.1\r\nHost: proxy\r\n\r\n") is None
    with pytest.raises(ConnectProxyError):
        _parse_connect_request(b"GET / HTTP/1.1\r\nHost: proxy\r\n\r\n")
    with pytest.raises(ConnectProxyError, match="ASCII"):
        _parse_connect_request(b"CONNECT \xff:443 HTTP/1.1\r\nHost: x:443\r\n\r\n")
    with pytest.raises(ConnectProxyError, match="malformed"):
        _parse_connect_request(b"CONNECT api.openai.com:443 HTTP/1.1\n\n")


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "0.0.0.0",  # noqa: S104 - classification input, not a bind
        "10.0.0.1",
        "172.16.0.1",
        "192.168.0.1",
        "169.254.169.254",
        "224.0.0.1",
        "240.0.0.1",
        "::",
        "::1",
        "fe80::1",
        "fc00::1",
        "ff02::1",
        "::ffff:8.8.8.8",
        "not-an-address",
    ],
)
def test_address_policy_rejects_non_public_and_mapped_addresses(address: str) -> None:
    assert _address_is_public(address) is False


def test_address_policy_accepts_public_ipv4_and_ipv6() -> None:
    assert _address_is_public("8.8.8.8") is True
    assert _address_is_public("2606:4700:4700::1111") is True


@pytest.mark.anyio
async def test_resolution_rejects_entire_answer_set_if_one_address_is_private(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = __import__("asyncio").get_running_loop()

    async def answers(*_args: object, **_kwargs: object) -> list[tuple[Any, ...]]:
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ]

    monkeypatch.setattr(loop, "getaddrinfo", answers)
    with pytest.raises(ConnectProxyError, match="prohibited address"):
        await _resolve_public("api.openai.com", 443)


@pytest.mark.anyio
async def test_resolution_returns_numeric_sockaddr_without_second_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = __import__("asyncio").get_running_loop()

    async def answers(*_args: object, **_kwargs: object) -> list[tuple[Any, ...]]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "ignored", ("8.8.8.8", 443))]

    monkeypatch.setattr(loop, "getaddrinfo", answers)
    assert await _resolve_public("api.openai.com", 443) == (
        socket.AF_INET,
        ("8.8.8.8", 443),
    )


@pytest.mark.anyio
async def test_resolution_rejects_failure_empty_and_excessive_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = asyncio.get_running_loop()

    async def failure(*_args: object, **_kwargs: object) -> list[tuple[Any, ...]]:
        raise OSError("resolver unavailable")

    monkeypatch.setattr(loop, "getaddrinfo", failure)
    with pytest.raises(ConnectProxyError, match="resolution failed"):
        await _resolve_public("api.openai.com", 443)

    async def empty(*_args: object, **_kwargs: object) -> list[tuple[Any, ...]]:
        return [(socket.AF_UNIX, socket.SOCK_STREAM, 0, "", ("ignored",))]

    monkeypatch.setattr(loop, "getaddrinfo", empty)
    with pytest.raises(ConnectProxyError, match="empty"):
        await _resolve_public("api.openai.com", 443)

    async def excessive(*_args: object, **_kwargs: object) -> list[tuple[Any, ...]]:
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", (f"8.8.8.{index}", 443))
            for index in range(1, MAX_DNS_RESULTS + 2)
        ]

    monkeypatch.setattr(loop, "getaddrinfo", excessive)
    with pytest.raises(ConnectProxyError, match="excessive"):
        await _resolve_public("api.openai.com", 443)


@pytest.mark.anyio
async def test_connect_handler_pins_validated_sockaddr_and_relays_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(await reader.read(1024))
        await writer.drain()
        writer.close()

    upstream = await asyncio.start_server(echo, "127.0.0.1", 0)
    upstream_port = upstream.sockets[0].getsockname()[1]

    async def pinned(_hostname: str, _port: int) -> tuple[int, tuple[str, int]]:
        return socket.AF_INET, ("127.0.0.1", upstream_port)

    monkeypatch.setattr("vuzol.execution.connect_proxy._resolve_public", pinned)
    policy = ConnectProxyPolicy(
        targets=frozenset({("api.openai.com", 443)}),
        connect_timeout_seconds=1,
        idle_timeout_seconds=1,
        tunnel_timeout_seconds=2,
        maximum_bytes_per_direction=1024,
    )
    proxy = await asyncio.start_server(
        lambda reader, writer: _handle_client(reader, writer, policy), "127.0.0.1", 0
    )
    proxy_port = proxy.sockets[0].getsockname()[1]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
        writer.write(b"CONNECT api.openai.com:443 HTTP/1.1\r\nHost: api.openai.com:443\r\n\r\n")
        await writer.drain()
        assert await reader.readuntil(b"\r\n\r\n") == (
            b"HTTP/1.1 200 Connection Established\r\n\r\n"
        )
        writer.write(b"bounded tunnel")
        await writer.drain()
        assert await reader.readexactly(len(b"bounded tunnel")) == b"bounded tunnel"
        writer.close()
        await writer.wait_closed()
    finally:
        proxy.close()
        upstream.close()
        await proxy.wait_closed()
        await upstream.wait_closed()


@pytest.mark.anyio
async def test_connect_handler_returns_closed_responses_for_health_and_denial() -> None:
    policy = ConnectProxyPolicy(
        targets=frozenset({("api.openai.com", 443)}),
        connect_timeout_seconds=1,
        idle_timeout_seconds=1,
        tunnel_timeout_seconds=1,
        maximum_bytes_per_direction=1024,
    )
    proxy = await asyncio.start_server(
        lambda reader, writer: _handle_client(reader, writer, policy), "127.0.0.1", 0
    )
    port = proxy.sockets[0].getsockname()[1]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /healthz HTTP/1.1\r\nHost: proxy\r\n\r\n")
        await writer.drain()
        assert b"204 No Content" in await reader.read()

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"CONNECT bad.example:443 HTTP/1.1\r\nHost: bad.example:443\r\n\r\n")
        await writer.drain()
        assert b"403 Forbidden" in await reader.read()
    finally:
        proxy.close()
        await proxy.wait_closed()


@pytest.mark.anyio
async def test_open_pinned_connection_uses_numeric_socket() -> None:
    async def accept(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        _reader, writer = await _open_pinned_connection(socket.AF_INET, ("127.0.0.1", port), 1)
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.anyio
async def test_open_pinned_connection_closes_socket_on_failure() -> None:
    with pytest.raises(OSError):
        await _open_pinned_connection(socket.AF_INET, ("127.0.0.1", 1), 0.1)


@pytest.mark.anyio
async def test_relay_enforces_directional_byte_limit() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(b"too many bytes")
    reader.feed_eof()

    class Writer:
        def write(self, _chunk: bytes) -> None:
            raise AssertionError("over-limit bytes must not be written")

        async def drain(self) -> None:
            raise AssertionError("over-limit bytes must not be drained")

    with pytest.raises(ConnectProxyError, match="byte limit"):
        await _relay(reader, Writer(), maximum=4, idle_timeout=1)  # type: ignore[arg-type]
