"""Regression probes from the independent 2026-10-20 audit."""

import asyncio
import gzip
import os
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tests.unit.workflows import test_runtime_preview as runtime_fixture
from tests.unit.workflows.test_static_publish import _handler as static_handler
from vuzol.app import preview_gateway
from vuzol.execution.sandbox import RootlessDockerRuntime, SandboxInterrupted
from vuzol.ops.static_publish import StaticSite, publish, rollback
from vuzol.workflows import runtime_preview
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_blocked_hash_mismatch_must_not_leave_unreviewed_site_live(tmp_path):
    handler, request = static_handler(tmp_path)
    # The fixture captured the build hash before this ordinary filesystem drift.
    (tmp_path / "repositories/demo/index.html").write_text("<h1>Unreviewed bytes</h1>")
    outcome = await handler.execute(request, CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "static_build_hash_mismatch"
    live = tmp_path / "sites/demo/current/index.html"
    assert not live.exists(), (
        f"blocked publication remains LIVE: {live.read_text()!r}; "
        "there was no earlier release to roll back to"
    )


async def test_failed_publish_must_restore_actual_previous_release(tmp_path):
    source, sites = tmp_path / "source", tmp_path / "sites"
    source.mkdir()
    sites.mkdir()
    site = StaticSite("demo", source, sites / "demo", (Path("index.html"),))
    releases = {}
    for n, label in enumerate(("A", "B", "C"), 1):
        (source / "index.html").write_text(label)
        result = publish(site, source_root=tmp_path, site_root=sites)
        path = sites / "demo/releases" / result.release
        os.utime(path, ns=(n * 1_000_000_000, n * 1_000_000_000))
        releases[label] = result.release
    # The previous active state can legitimately be older after owner rollback.
    rollback(site, site_root=sites)
    assert (sites / "demo/current/index.html").read_text() == "B"
    (source / "index.html").write_text("D-invalid")
    publish(site, source_root=tmp_path, site_root=sites)
    rollback(site, site_root=sites)
    assert (sites / "demo/current/index.html").read_text() == "B", (
        "rollback after failed D switched to C, not the actual previous B"
    )


async def test_stop_detected_before_publish_must_prevent_new_effect(tmp_path):
    handler, request = static_handler(tmp_path)
    cancellation = CancellationContext()
    session = handler._factory.return_value.__aenter__.return_value
    values = iter(session.scalar.side_effect)

    async def read_after_stop(*args, **kwargs):
        cancellation.request()
        return next(values)

    session.scalar.side_effect = read_after_stop
    outcome = await handler.execute(request, cancellation)
    live = tmp_path / "sites/demo/current/index.html"
    assert not live.exists(), (
        f"cancellation was already observed before publish, "
        f"but new release is LIVE and handler returned {outcome.kind}"
    )


async def test_sandbox_cancel_during_stdin_must_cleanup_spawned_container(tmp_path, monkeypatch):
    from tests.unit.execution._execution_helpers import envelope

    runtime = RootlessDockerRuntime(tmp_path / "unused-docker.sock")
    runtime._stop_owned_container = AsyncMock()
    runtime._remove_owned_container = AsyncMock()
    process = MagicMock(returncode=None)
    process.wait = AsyncMock()
    process.stdin.drain = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    with pytest.raises(SandboxInterrupted) as stopped:
        await runtime.run(envelope(tmp_path), CancellationContext())
    assert stopped.value.result.exit_code == 130
    assert runtime._stop_owned_container.await_count == 1, (
        "docker run was spawned but cancellation during stdin.drain escaped before cleanup try"
    )
    assert runtime._remove_owned_container.await_count == 1


def prepare_runtime(tmp_path, monkeypatch):
    handler, read_session, _write_session = runtime_fixture._handler(tmp_path)
    read_session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    read_session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_preview,
        "current_environment",
        AsyncMock(return_value=runtime_fixture._environment()),
    )
    runtime_fixture._allow_confinement(monkeypatch)
    runtime_fixture._allow_node_runtime(monkeypatch, tmp_path)
    runtime_fixture._prepare_git_pointer(tmp_path)
    runtime_fixture._mock_materialization(monkeypatch)
    spawn = runtime_fixture._mock_healthy_spawn(monkeypatch)
    return handler, spawn


async def test_cancelling_preview_start_must_stop_spawned_process(tmp_path, monkeypatch):
    handler, spawn = prepare_runtime(tmp_path, monkeypatch)
    monkeypatch.setattr(
        runtime_preview, "_wait_until_ready", AsyncMock(side_effect=asyncio.CancelledError())
    )
    with pytest.raises(asyncio.CancelledError):
        await handler.execute(runtime_fixture._request(), CancellationContext())
    process = spawn.return_value
    assert not handler._registry.targets
    assert process.terminate.called or process.kill.called, (
        "spawn succeeded, cancellation escaped, child was neither stopped nor registered"
    )


async def test_owner_stop_while_preview_is_starting_prevents_exposure(tmp_path, monkeypatch):
    handler, spawn = prepare_runtime(tmp_path, monkeypatch)
    cancellation = CancellationContext()

    async def health(*args, **kwargs):
        cancellation.request()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime_preview, "_wait_until_ready", health)
    result = await handler.execute(runtime_fixture._request(), cancellation)
    assert result.kind is OutcomeKind.CANCELLED
    assert not handler._registry.targets
    assert spawn.return_value.terminate.called
    assert not list((tmp_path / "previews/.runtime/runs/demo").glob("*/app"))


async def test_failed_first_public_probe_leaves_no_live_site(tmp_path):
    handler, request = static_handler(tmp_path)
    handler._probe = AsyncMock(side_effect=ValueError("unhealthy"))
    result = await handler.execute(request, CancellationContext())
    assert result.kind is OutcomeKind.BLOCKED
    assert result.category == "static_publish_probe_failed"
    assert not (tmp_path / "sites/demo/current").exists()


async def test_gateway_must_not_strip_encoding_while_forwarding_gzip_bytes(tmp_path, monkeypatch):
    body = b"<h1>Working preview</h1>"
    compressed = gzip.compress(body)

    class Upstream:
        status_code = 200
        headers: ClassVar = {"content-type": "text/html", "content-encoding": "gzip"}
        aclose = AsyncMock()

        async def aiter_raw(self):
            yield compressed

    proxy = MagicMock()
    proxy.build_request.return_value = object()
    proxy.send = AsyncMock(return_value=Upstream())
    proxy.aclose = AsyncMock()
    original_client = httpx.AsyncClient
    monkeypatch.setattr(preview_gateway.httpx, "AsyncClient", lambda **kwargs: proxy)
    registry = runtime_preview.PreviewRuntimeRegistry(
        {
            "demo": runtime_preview.RuntimeTarget(
                "demo", 43123, SimpleNamespace(returncode=None), "a" * 40
            )
        }
    )
    app = preview_gateway.create_preview_gateway(registry, static_root=tmp_path)
    async with (
        app.router.lifespan_context(app),
        original_client(
            transport=httpx.ASGITransport(app=app), base_url="http://preview.test"
        ) as client,
    ):
        response = await client.get("/demo/")
    assert response.content == body, (
        f"received gzip magic {response.content[:2]!r} "
        f"with content-encoding={response.headers.get('content-encoding')!r}"
    )


async def test_secret_ingress_must_bound_body_before_reading_entire_stream(tmp_path):
    from vuzol.app.factory import create_app
    from vuzol.config.settings import SecretIngressSettings, Settings

    settings = Settings(_env_file=None).model_copy(
        update={
            "secret_ingress": SecretIngressSettings(
                enabled=True,
                public_base_url="https://fixture.invalid",
                storage_root=tmp_path,
                allowed_names=("FIXTURE_KEY",),
                maximum_secret_bytes=64,
            ),
        }
    )
    factory = MagicMock()
    app = create_app(settings, session_factory=factory)
    chunks_read = 0

    async def oversized_body():
        nonlocal chunks_read
        for _ in range(64):
            chunks_read += 1
            yield b"x" * 4096  # Only 256 KB in this fixture, never a live HTTP request.

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://fixture"
    ) as client:
        response = await client.post("/secret/" + "a" * 43, content=oversized_body())
    assert response.status_code == 413
    factory.assert_not_called()
    assert chunks_read <= 1, (
        f"64-byte secret limit read all {chunks_read} chunks before rejecting; "
        "a syntactically valid but nonexistent token reaches unbounded body buffering"
    )
