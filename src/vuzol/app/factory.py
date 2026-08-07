"""Application composition root."""

import html
import re
from urllib.parse import parse_qs

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol import __version__
from vuzol.app.health import HealthStatus, health_status
from vuzol.config import Settings, get_settings
from vuzol.security.secret_ingress import consume_request, inspect_request
from vuzol.storage import create_engine, create_session_factory, resolve_database_dsn

_TOKEN = re.compile(r"^[A-Za-z0-9_-]{40,100}$")


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> FastAPI:
    """Build an application with explicit settings injection."""

    resolved = settings or get_settings()
    app = FastAPI(title="Vuzol", version=__version__)
    engine = None
    if resolved.secret_ingress.enabled and session_factory is None:
        engine = create_engine(resolved, resolve_database_dsn(resolved))
        session_factory = create_session_factory(engine)

    @app.get("/health/live", response_model=HealthStatus)
    @app.get("/health/ready", response_model=HealthStatus)
    def health() -> HealthStatus:
        return health_status(service=resolved.service_name, environment=resolved.environment)

    if engine is not None:
        app.router.add_event_handler("shutdown", engine.dispose)

    if resolved.secret_ingress.enabled:
        assert session_factory is not None

        @app.get("/secret/{token}", response_class=HTMLResponse)
        async def secret_form(token: str) -> HTMLResponse:
            if _TOKEN.fullmatch(token) is None:
                return _page("Ссылка недействительна", status=404)
            async with session_factory() as session:
                pending = await inspect_request(session, token)
            if pending is None:
                return _page("Ссылка истекла или уже использована", status=410)
            name = html.escape(pending.secret_name)
            body = f"""
            <h1>Добавить секрет</h1><p><code>{name}</code></p>
            <form method="post" autocomplete="off">
              <input type="password" name="value" required autofocus maxlength="65536">
              <button type="submit">Сохранить</button>
            </form>"""
            return _page(body)

        @app.post("/secret/{token}", response_class=HTMLResponse)
        async def secret_submit(token: str, incoming: Request) -> HTMLResponse:
            if _TOKEN.fullmatch(token) is None:
                return _page("Ссылка недействительна", status=404)
            raw = await incoming.body()
            if len(raw) > resolved.secret_ingress.maximum_secret_bytes * 2:
                return _page("Значение слишком большое", status=413)
            values = parse_qs(raw.decode("utf-8", "strict"), keep_blank_values=True)
            submitted = values.get("value", [])
            if len(submitted) != 1:
                return _page("Значение не передано", status=400)
            try:
                async with session_factory.begin() as session:
                    name = await consume_request(
                        session,
                        resolved.secret_ingress,
                        token=token,
                        value=submitted[0].encode(),
                    )
            except (LookupError, ValueError, UnicodeError):
                return _page("Ссылка истекла или значение некорректно", status=410)
            return _page(f"Секрет <code>{html.escape(name)}</code> сохранён")

    return app


def _page(body: str, *, status: int = 200) -> HTMLResponse:
    document = f"""<!doctype html><html lang="ru"><head>
    <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <meta name="referrer" content="no-referrer"><title>Vuzol Secret</title>
    <style>body{{font:16px system-ui;max-width:34rem;margin:4rem auto;padding:1rem}}
    input,button{{box-sizing:border-box;width:100%;padding:.8rem;margin:.5rem 0}}</style>
    </head><body>{body}</body></html>"""
    return HTMLResponse(
        document,
        status_code=status,
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
    )
