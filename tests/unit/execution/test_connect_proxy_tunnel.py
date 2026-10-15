"""Duplex activity, bounded silence/lifetime and complete relay cleanup."""

import asyncio
import socket
from contextlib import suppress

import pytest

from vuzol.execution.connect_proxy import (
    ConnectProxyError,
    ConnectProxyPolicy,
    _handle_client,
    _relay_tunnel,
)

pytestmark = pytest.mark.anyio


class Sink:
    def __init__(self, *, stall=False):
        self.data = bytearray()
        self.stall = stall

    def write(self, data):
        self.data.extend(data)

    async def drain(self):
        if self.stall:
            await asyncio.Future()


def policy(*, idle=0.1, lifetime=2, maximum=100000):
    return ConnectProxyPolicy(frozenset({("example.com", 443)}), 1, idle, lifetime, maximum)


def assert_no_relays():
    assert not [t for t in asyncio.all_tasks() if t.get_name().startswith("vuzol-connect-relay-")]


@pytest.mark.parametrize("direction", ["request", "response"])
async def test_active_one_way_tunnel_survives_silent_other_direction(direction):
    request, response = asyncio.StreamReader(), asyncio.StreamReader()
    client, upstream = Sink(), Sink()
    tunnel = asyncio.create_task(_relay_tunnel(request, client, response, upstream, policy()))
    active = request if direction == "request" else response
    try:
        # Keep one direction completely silent for three idle intervals.
        for _ in range(15):
            active.feed_data(b"chunk")
            await asyncio.sleep(0.02)
            assert not tunnel.done()
        request.feed_eof()
        response.feed_eof()
        await asyncio.wait_for(tunnel, 1)
        assert bytes((upstream if direction == "request" else client).data) == b"chunk" * 15
    finally:
        tunnel.cancel()
        with suppress(asyncio.CancelledError):
            await tunnel
    assert_no_relays()


async def test_silence_of_both_directions_still_expires():
    with pytest.raises(TimeoutError):
        await _relay_tunnel(
            asyncio.StreamReader(), Sink(), asyncio.StreamReader(), Sink(), policy()
        )
    assert_no_relays()


async def test_absolute_lifetime_expires_despite_continuous_activity():
    request, response = asyncio.StreamReader(), asyncio.StreamReader()

    async def feed():
        while True:
            response.feed_data(b"heartbeat")
            await asyncio.sleep(0.01)

    producer = asyncio.create_task(feed())
    try:
        with pytest.raises(TimeoutError):
            await _relay_tunnel(request, Sink(), response, Sink(), policy(idle=1, lifetime=0.1))
    finally:
        producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)
    assert_no_relays()


@pytest.mark.parametrize("direction", ["request", "response"])
async def test_byte_limit_still_cancels_sibling(direction):
    request, response = asyncio.StreamReader(), asyncio.StreamReader()
    (request if direction == "request" else response).feed_data(b"too large")
    with pytest.raises(ConnectProxyError, match="byte limit"):
        await _relay_tunnel(request, Sink(), response, Sink(), policy(maximum=4))
    assert_no_relays()


async def test_disconnect_drains_both_relays():
    request, response = asyncio.StreamReader(), asyncio.StreamReader()
    request.feed_eof()
    response.feed_eof()
    await _relay_tunnel(request, Sink(), response, Sink(), policy())
    assert_no_relays()


async def test_half_closed_request_can_still_receive_response():
    request, response = asyncio.StreamReader(), asyncio.StreamReader()
    client = Sink()
    request.feed_eof()
    tunnel = asyncio.create_task(_relay_tunnel(request, client, response, Sink(), policy()))
    for _ in range(10):
        response.feed_data(b"reply")
        await asyncio.sleep(0.02)
    response.feed_eof()
    await asyncio.wait_for(tunnel, 1)
    assert client.data == b"reply" * 10
    assert_no_relays()


async def test_outer_cancellation_cancels_both_relays():
    tunnel = asyncio.create_task(
        _relay_tunnel(
            asyncio.StreamReader(), Sink(), asyncio.StreamReader(), Sink(), policy(idle=10)
        )
    )
    await asyncio.sleep(0)
    tunnel.cancel()
    with pytest.raises(asyncio.CancelledError):
        await tunnel
    assert_no_relays()


async def test_stalled_writer_drain_is_also_bounded():
    request, response = asyncio.StreamReader(), asyncio.StreamReader()
    response.feed_data(b"reply")
    with pytest.raises(TimeoutError):
        await _relay_tunnel(request, Sink(stall=True), response, Sink(), policy())
    assert_no_relays()


async def test_connect_streams_response_past_idle_with_silent_http_client(monkeypatch):
    finished = asyncio.Event()
    frames = [b"SSE response heartbeat\n"] * 15

    async def serve_response(reader, writer):
        try:
            assert await reader.readuntil(b"\r\n\r\n") == b"GET /stream HTTP/1.1\r\n\r\n"
            for frame in frames:
                writer.write(frame)
                await writer.drain()
                await asyncio.sleep(0.02)
        finally:
            writer.close()
            await writer.wait_closed()

    upstream = await asyncio.start_server(serve_response, "127.0.0.1", 0)

    async def pinned(_host, _port):
        return socket.AF_INET, ("127.0.0.1", upstream.sockets[0].getsockname()[1])

    monkeypatch.setattr("vuzol.execution.connect_proxy._resolve_public", pinned)

    async def handle(reader, writer):
        try:
            await _handle_client(reader, writer, policy())
        finally:
            finished.set()

    proxy = await asyncio.start_server(handle, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.sockets[0].getsockname()[1])
    try:
        writer.write(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
        await writer.drain()
        assert b"200 Connection Established" in await reader.readuntil(b"\r\n\r\n")
        writer.write(b"GET /stream HTTP/1.1\r\n\r\n")
        await writer.drain()
        assert await asyncio.wait_for(reader.readexactly(sum(map(len, frames))), 2) == b"".join(
            frames
        )
    finally:
        writer.close()
        await writer.wait_closed()
        await asyncio.wait_for(finished.wait(), 1)
        proxy.close()
        upstream.close()
        await proxy.wait_closed()
        await upstream.wait_closed()
    assert_no_relays()
