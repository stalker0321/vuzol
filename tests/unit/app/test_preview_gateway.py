from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from vuzol.app.preview_gateway import (
    _resolve_preview_request,
    _static_preview,
    create_preview_gateway,
)
from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry, RuntimeTarget


@pytest.mark.anyio
async def test_gateway_serves_static_preview_and_spa_fallback(tmp_path: Path) -> None:
    release = tmp_path / "notes" / "current"
    release.mkdir(parents=True)
    (release / "index.html").write_text("<main>Notes</main>\n")
    app = create_preview_gateway(PreviewRuntimeRegistry(), static_root=tmp_path)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://preview.test"
    ) as client:
        response = await client.get("/notes/settings")

    assert response.status_code == 200
    assert "Notes" in response.text
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.anyio
async def test_gateway_resolves_absolute_asset_from_preview_referer(tmp_path: Path) -> None:
    release = tmp_path / "notes" / "current"
    release.mkdir(parents=True)
    (release / "index.html").write_text("<main>Notes</main>\n")
    (release / "styles.css").write_text("body { color: red; }\n")
    app = create_preview_gateway(PreviewRuntimeRegistry(), static_root=tmp_path)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://preview.test"
    ) as client:
        response = await client.get(
            "/styles.css", headers={"referer": "http://preview.test/notes/"}
        )

    assert response.status_code == 200
    assert "color: red" in response.text


@pytest.mark.anyio
async def test_gateway_rejects_unknown_or_traversal_preview(tmp_path: Path) -> None:
    app = create_preview_gateway(PreviewRuntimeRegistry(), static_root=tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://preview.test"
    ) as client:
        unknown = await client.get("/missing/index.html")
        traversal = await client.get("/../etc/passwd")

    assert unknown.status_code == 404
    assert traversal.status_code == 404


@pytest.mark.anyio
async def test_gateway_proxies_runtime_stream_and_filters_transport_headers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Upstream:
        status_code = 201
        headers: ClassVar[dict[str, str]] = {
            "content-type": "text/event-stream",
            "content-length": "999",
            "connection": "keep-alive",
            "x-preview": "yes",
        }

        async def aiter_raw(self) -> Any:
            yield b"data: ready\n\n"

        aclose = AsyncMock()

    proxy = MagicMock()
    proxy.build_request.return_value = object()
    proxy.send = AsyncMock(return_value=Upstream())
    proxy.aclose = AsyncMock()
    client_type = httpx.AsyncClient
    monkeypatch.setattr(
        "vuzol.app.preview_gateway.httpx.AsyncClient", MagicMock(return_value=proxy)
    )
    process = SimpleNamespace(returncode=None)
    registry = PreviewRuntimeRegistry(
        {"demo": RuntimeTarget("demo", 43123, cast(Any, process), "a" * 40)}
    )
    app = create_preview_gateway(registry, static_root=tmp_path)

    async with client_type(
        transport=httpx.ASGITransport(app=app), base_url="http://preview.test"
    ) as client:
        response = await client.post(
            "/demo/api/stream?room=1",
            content=b"hello",
            headers={"authorization": "Bearer test", "connection": "close"},
        )

    assert response.status_code == 201
    assert response.content == b"data: ready\n\n"
    assert response.headers["x-preview"] == "yes"
    assert "content-length" not in response.headers
    assert proxy.send.await_args is not None
    assert proxy.send.await_args.kwargs["stream"] is True
    proxy.build_request.assert_called_once()


@pytest.mark.anyio
async def test_gateway_redirects_absolute_document_to_project_prefix(tmp_path: Path) -> None:
    release = tmp_path / "demo" / "current"
    release.mkdir(parents=True)
    (release / "index.html").write_text("<main>Demo</main>")
    app = create_preview_gateway(PreviewRuntimeRegistry(), static_root=tmp_path)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://preview.test",
        follow_redirects=False,
    ) as client:
        response = await client.get(
            "/settings",
            headers={
                "referer": "http://preview.test/demo/",
                "sec-fetch-dest": "document",
            },
        )

    assert response.status_code == 307
    assert response.headers["location"] == "/demo/settings"


@pytest.mark.anyio
async def test_gateway_health_and_root(tmp_path: Path) -> None:
    app = create_preview_gateway(PreviewRuntimeRegistry(), static_root=tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://preview.test"
    ) as client:
        health = await client.get("/health/ready")
        root = await client.get("/")

    assert health.json() == {"status": "ok"}
    assert root.text == "Vuzol preview environment"


@pytest.mark.anyio
async def test_gateway_lifespan_closes_proxy_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy = MagicMock()
    proxy.aclose = AsyncMock()
    monkeypatch.setattr(
        "vuzol.app.preview_gateway.httpx.AsyncClient", MagicMock(return_value=proxy)
    )
    app = create_preview_gateway(PreviewRuntimeRegistry(), static_root=tmp_path)

    async with app.router.lifespan_context(app):
        pass

    proxy.aclose.assert_awaited_once()


def test_gateway_ignores_invalid_referer_and_stopped_runtime(tmp_path: Path) -> None:
    process = SimpleNamespace(returncode=1)
    registry = PreviewRuntimeRegistry(
        {"demo": RuntimeTarget("demo", 43123, cast(Any, process), "a" * 40)}
    )

    assert _resolve_preview_request(
        registry,
        static_root=tmp_path,
        request_path="asset.js",
        referer="http://[::1",
    ) == (None, "asset.js")
    assert _resolve_preview_request(
        registry,
        static_root=tmp_path,
        request_path="demo/file.js",
        referer=None,
    ) == (None, "demo/file.js")


def test_static_preview_handles_directories_missing_index_and_traversal(tmp_path: Path) -> None:
    release = tmp_path / "demo" / "current"
    nested = release / "docs"
    nested.mkdir(parents=True)
    (release / "index.html").write_text("root")
    (nested / "index.html").write_text("nested")

    directory = _static_preview(tmp_path, project_id="demo", path="docs")
    traversal = _static_preview(tmp_path, project_id="demo", path="../../secret")
    unavailable = _static_preview(tmp_path, project_id="missing", path="")
    (release / "index.html").unlink()
    missing = _static_preview(tmp_path, project_id="demo", path="not-found")

    assert directory.status_code == 200
    assert traversal.status_code == 400
    assert unavailable.status_code == 404
    assert missing.status_code == 404
