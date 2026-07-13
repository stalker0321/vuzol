"""Minimal CONNECT-only proxy with pinned, post-resolution destination checks.

The process that validates DNS is also the process that opens the outbound
socket.  It connects to the validated numeric sockaddr, so the operating
system never performs a second hostname lookup between policy and connect.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import json
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MAX_HEADER_BYTES = 8192
MAX_DNS_RESULTS = 16


class ConnectProxyError(RuntimeError):
    """A request or runtime condition violated the closed proxy contract."""


@dataclass(frozen=True)
class ConnectProxyPolicy:
    targets: frozenset[tuple[str, int]]
    connect_timeout_seconds: float
    idle_timeout_seconds: float
    tunnel_timeout_seconds: float
    maximum_bytes_per_direction: int

    @classmethod
    def load(cls, path: Path) -> ConnectProxyPolicy:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ConnectProxyError("proxy policy is unavailable or malformed") from error
        if not isinstance(raw, dict) or set(raw) != {
            "version",
            "targets",
            "connect_timeout_seconds",
            "idle_timeout_seconds",
            "tunnel_timeout_seconds",
            "maximum_bytes_per_direction",
        }:
            raise ConnectProxyError("proxy policy has an unexpected shape")
        if raw["version"] != 1 or not isinstance(raw["targets"], list) or not raw["targets"]:
            raise ConnectProxyError("proxy policy version or targets are invalid")
        targets: set[tuple[str, int]] = set()
        for item in raw["targets"]:
            if not isinstance(item, dict) or set(item) != {"hostname", "port"}:
                raise ConnectProxyError("proxy target has an unexpected shape")
            hostname, port = item["hostname"], item["port"]
            if not isinstance(hostname, str) or not _is_canonical_hostname(hostname):
                raise ConnectProxyError("proxy target hostname is not canonical")
            if port != 443:
                raise ConnectProxyError("proxy target port is prohibited")
            targets.add((hostname, port))
        return cls(
            targets=frozenset(targets),
            connect_timeout_seconds=_bounded_number(raw, "connect_timeout_seconds", 0.1, 30),
            idle_timeout_seconds=_bounded_number(raw, "idle_timeout_seconds", 1, 300),
            tunnel_timeout_seconds=_bounded_number(raw, "tunnel_timeout_seconds", 1, 3600),
            maximum_bytes_per_direction=_bounded_integer(
                raw, "maximum_bytes_per_direction", 1024, 1_073_741_824
            ),
        )


def _bounded_number(raw: dict[str, Any], key: str, minimum: float, maximum: float) -> float:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConnectProxyError(f"proxy policy {key} is invalid")
    result = float(value)
    if not minimum <= result <= maximum:
        raise ConnectProxyError(f"proxy policy {key} is out of bounds")
    return result


def _bounded_integer(raw: dict[str, Any], key: str, minimum: int, maximum: int) -> int:
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ConnectProxyError(f"proxy policy {key} is invalid")
    return int(value)


def _is_canonical_hostname(hostname: str) -> bool:
    if not hostname or hostname != hostname.lower() or len(hostname) > 253:
        return False
    if hostname.startswith(".") or hostname.endswith(".") or ".." in hostname:
        return False
    if hostname == "localhost" or hostname == "metadata.google.internal":
        return False
    if hostname.endswith((".local", ".localhost")):
        return False
    with contextlib.suppress(ValueError):
        ipaddress.ip_address(hostname)
        return False
    labels = hostname.split(".")
    return all(
        1 <= len(label) <= 63
        and label[0].isalnum()
        and label[-1].isalnum()
        and all(
            character.isascii() and (character.isalnum() or character == "-") for character in label
        )
        for label in labels
    )


def _parse_connect_request(header: bytes) -> tuple[str, int] | None:
    try:
        text = header.decode("ascii")
    except UnicodeDecodeError as error:
        raise ConnectProxyError("proxy request headers must be ASCII") from error
    lines = text.split("\r\n")
    if not lines or lines[-2:] != ["", ""]:
        raise ConnectProxyError("proxy request headers are malformed")
    request = lines[0].split(" ")
    if request == ["GET", "/healthz", "HTTP/1.1"]:
        return None
    if len(request) != 3 or request[0] != "CONNECT" or request[2] != "HTTP/1.1":
        raise ConnectProxyError("only HTTP/1.1 CONNECT is supported")
    authority = request[1]
    if authority.count(":") != 1:
        raise ConnectProxyError("CONNECT authority must be hostname:port")
    hostname, port_text = authority.rsplit(":", 1)
    if not _is_canonical_hostname(hostname) or port_text != "443":
        raise ConnectProxyError("CONNECT authority is prohibited")
    host_headers = [line[5:].strip() for line in lines[1:-2] if line.lower().startswith("host:")]
    if len(host_headers) != 1 or host_headers[0].lower() != authority:
        raise ConnectProxyError("Host header must exactly match CONNECT authority")
    if any(line[:1] in {" ", "\t"} or ":" not in line for line in lines[1:-2]):
        raise ConnectProxyError("proxy request contains a malformed header")
    return hostname, 443


def _address_is_public(address: str) -> bool:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return False
    return bool(
        parsed.is_global
        and not parsed.is_private
        and not parsed.is_loopback
        and not parsed.is_link_local
        and not parsed.is_multicast
        and not parsed.is_reserved
        and not parsed.is_unspecified
        and not (isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None)
    )


async def _resolve_public(hostname: str, port: int) -> tuple[int, tuple[Any, ...]]:
    loop = asyncio.get_running_loop()
    try:
        answers = await loop.getaddrinfo(
            hostname, port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
        )
    except OSError as error:
        raise ConnectProxyError("approved hostname resolution failed") from error
    unique: list[tuple[int, tuple[Any, ...]]] = []
    seen: set[tuple[int, str]] = set()
    for family, socktype, _protocol, _canonical, sockaddr in answers:
        if family not in {socket.AF_INET, socket.AF_INET6} or socktype != socket.SOCK_STREAM:
            continue
        address = str(sockaddr[0])
        key = family, address
        if key not in seen:
            seen.add(key)
            unique.append((family, sockaddr))
    if not unique or len(unique) > MAX_DNS_RESULTS:
        raise ConnectProxyError("approved hostname resolution is empty or excessive")
    if any(not _address_is_public(str(sockaddr[0])) for _family, sockaddr in unique):
        raise ConnectProxyError("approved hostname resolved to a prohibited address")
    return unique[0]


async def _open_pinned_connection(
    family: int, sockaddr: tuple[Any, ...], timeout_seconds: float
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setblocking(False)
    try:
        async with asyncio.timeout(timeout_seconds):
            await asyncio.get_running_loop().sock_connect(sock, sockaddr)
        return await asyncio.open_connection(sock=sock)
    except BaseException:
        sock.close()
        raise


async def _relay(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    maximum: int,
    idle_timeout: float,
) -> None:
    total = 0
    while True:
        chunk = await asyncio.wait_for(reader.read(65_536), timeout=idle_timeout)
        if not chunk:
            return
        total += len(chunk)
        if total > maximum:
            raise ConnectProxyError("proxy tunnel byte limit exceeded")
        writer.write(chunk)
        await writer.drain()


async def _handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, policy: ConnectProxyPolicy
) -> None:
    upstream: asyncio.StreamWriter | None = None
    try:
        header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), policy.idle_timeout_seconds)
        if len(header) > MAX_HEADER_BYTES:
            raise ConnectProxyError("proxy request headers are too large")
        target = _parse_connect_request(header)
        if target is None:
            writer.write(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            return
        if target not in policy.targets:
            raise ConnectProxyError("CONNECT destination is not approved")
        family, sockaddr = await asyncio.wait_for(
            _resolve_public(*target), policy.connect_timeout_seconds
        )
        upstream_reader, upstream = await _open_pinned_connection(
            family, sockaddr, policy.connect_timeout_seconds
        )
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        async with asyncio.timeout(policy.tunnel_timeout_seconds):
            await asyncio.gather(
                _relay(
                    reader,
                    upstream,
                    maximum=policy.maximum_bytes_per_direction,
                    idle_timeout=policy.idle_timeout_seconds,
                ),
                _relay(
                    upstream_reader,
                    writer,
                    maximum=policy.maximum_bytes_per_direction,
                    idle_timeout=policy.idle_timeout_seconds,
                ),
            )
    except (ConnectProxyError, asyncio.IncompleteReadError, OSError, TimeoutError):
        if upstream is None:
            writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            with contextlib.suppress(OSError):
                await writer.drain()
    finally:
        for stream in (upstream, writer):
            if stream is not None:
                stream.close()
                with contextlib.suppress(OSError):
                    await stream.wait_closed()


async def serve(
    policy_path: Path,
    host: str = "0.0.0.0",  # noqa: S104 - container network listener
    port: int = 8888,
) -> None:
    policy = ConnectProxyPolicy.load(policy_path)
    server = await asyncio.start_server(
        lambda reader, writer: _handle_client(reader, writer, policy), host, port
    )
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(serve(args.policy))


if __name__ == "__main__":
    main()
