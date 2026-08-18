"""HTTP gateway for static and managed runtime previews."""

from __future__ import annotations

import mimetypes
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import (
    FileResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.background import BackgroundTask

from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry

_PROJECT_ID = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def create_preview_gateway(registry: PreviewRuntimeRegistry, *, static_root: Path) -> FastAPI:
    client = httpx.AsyncClient(timeout=httpx.Timeout(connect=5.0, read=None, write=30.0, pool=5.0))

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(
        title="Vuzol preview gateway",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

    @app.get("/health/ready")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.api_route(
        "/{request_path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def preview(request_path: str, request: Request) -> Response:
        project_id, preview_path = _resolve_preview_request(
            registry,
            static_root=static_root,
            request_path=request_path,
            referer=request.headers.get("referer"),
        )
        if project_id is None:
            if not request_path:
                return PlainTextResponse("Vuzol preview environment")
            return PlainTextResponse("Unknown preview", status_code=404)
        if request.headers.get("sec-fetch-dest") == "document" and not request_path.startswith(
            f"{project_id}/"
        ):
            suffix = f"/{preview_path}" if preview_path else "/"
            return RedirectResponse(f"/{project_id}{suffix}", status_code=307)
        target = registry.targets.get(project_id)
        if target is not None and target.process.returncode is None:
            return await _proxy(client, request, port=target.port, path=preview_path)
        return _static_preview(static_root, project_id=project_id, path=preview_path)

    return app


def _resolve_preview_request(
    registry: PreviewRuntimeRegistry,
    *,
    static_root: Path,
    request_path: str,
    referer: str | None,
) -> tuple[str | None, str]:
    first, separator, remainder = request_path.partition("/")
    if _project_exists(registry, static_root, first):
        return first, remainder if separator else ""
    if referer is not None:
        try:
            referer_path = httpx.URL(referer).path.lstrip("/")
        except httpx.InvalidURL:
            referer_path = ""
        referring_project = referer_path.partition("/")[0]
        if _project_exists(registry, static_root, referring_project):
            return referring_project, request_path
    return None, request_path


def _project_exists(registry: PreviewRuntimeRegistry, static_root: Path, project_id: str) -> bool:
    if _PROJECT_ID.fullmatch(project_id) is None:
        return False
    target = registry.targets.get(project_id)
    if target is not None and target.process.returncode is None:
        return True
    return (static_root / project_id / "current").is_dir()


async def _proxy(
    client: httpx.AsyncClient, request: Request, *, port: int, path: str
) -> StreamingResponse:
    query = request.url.query
    url = f"http://127.0.0.1:{port}/{path}" + (f"?{query}" if query else "")
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in _HOP_BY_HOP | {"host", "content-length"}
    }
    upstream = await client.send(
        client.build_request(
            request.method,
            url,
            headers=headers,
            content=await request.body(),
        ),
        stream=True,
    )
    response_headers = {
        key: value
        for key, value in upstream.headers.items()
        if key.lower() not in _HOP_BY_HOP | {"content-length", "content-encoding"}
    }
    return StreamingResponse(
        upstream.aiter_raw(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(upstream.aclose),
    )


def _static_preview(static_root: Path, *, project_id: str, path: str) -> Response:
    release = (static_root / project_id / "current").resolve()
    if not release.is_dir():
        return PlainTextResponse("Preview is not available", status_code=404)
    candidate = (release / path).resolve()
    if release != candidate and release not in candidate.parents:
        return PlainTextResponse("Invalid preview path", status_code=400)
    if candidate.is_dir():
        candidate = candidate / "index.html"
    if not candidate.is_file():
        candidate = release / "index.html"
    if not candidate.is_file():
        return PlainTextResponse("Preview is not available", status_code=404)
    media_type, _encoding = mimetypes.guess_type(candidate.name)
    return FileResponse(
        candidate,
        media_type=media_type,
        headers={"Cache-Control": "no-store"},
    )
