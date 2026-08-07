from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient
from pydantic import HttpUrl
from pytest import MonkeyPatch
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.app import factory
from vuzol.config.settings import SecretIngressSettings, Settings


class _SessionContext(AbstractAsyncContextManager[object]):
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_args: object) -> None:
        return None


class _SessionFactory:
    def __call__(self) -> _SessionContext:
        return _SessionContext()

    def begin(self) -> _SessionContext:
        return _SessionContext()


def _session_factory() -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], _SessionFactory())


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        environment="test",
        secret_ingress=SecretIngressSettings(
            enabled=True,
            public_base_url=HttpUrl("https://vuzol.example"),
            storage_root=tmp_path,
            allowed_names=("TOKENROUTER_API_KEY",),
            maximum_secret_bytes=8,
        ),
    )


def test_secret_form_rejects_invalid_and_unavailable_tokens(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    inspect = AsyncMock(return_value=None)
    monkeypatch.setattr(factory, "inspect_request", inspect)
    client = TestClient(factory.create_app(_settings(tmp_path), _session_factory()))

    invalid = client.get("/secret/nope")
    expired = client.get("/secret/" + "a" * 43)

    assert invalid.status_code == 404
    assert expired.status_code == 410
    assert expired.headers["cache-control"] == "no-store"
    inspect.assert_awaited_once()


def test_secret_form_and_submit_success(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    pending = type("Pending", (), {"secret_name": "TOKENROUTER_API_KEY"})()
    inspect = AsyncMock(return_value=pending)
    consume = AsyncMock(return_value="TOKENROUTER_API_KEY")
    monkeypatch.setattr(factory, "inspect_request", inspect)
    monkeypatch.setattr(factory, "consume_request", consume)
    client = TestClient(factory.create_app(_settings(tmp_path), _session_factory()))
    token = "a" * 43

    form = client.get(f"/secret/{token}")
    saved = client.post(f"/secret/{token}", data={"value": "secret"})

    assert form.status_code == 200
    assert "TOKENROUTER_API_KEY" in form.text
    assert saved.status_code == 200
    assert "сохранён" in saved.text
    assert consume.await_args is not None
    assert consume.await_args.kwargs["value"] == b"secret"


def test_secret_submit_rejects_bad_payloads(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    consume = AsyncMock(side_effect=LookupError)
    monkeypatch.setattr(factory, "consume_request", consume)
    client = TestClient(factory.create_app(_settings(tmp_path), _session_factory()))
    token = "a" * 43

    assert client.post("/secret/nope", data={"value": "x"}).status_code == 404
    assert client.post(f"/secret/{token}", content=b"value=" + b"x" * 20).status_code == 413
    assert client.post(f"/secret/{token}", content=b"other=x").status_code == 400
    assert client.post(f"/secret/{token}", data={"value": "valid"}).status_code == 410


def test_enabled_app_builds_and_disposes_its_database_engine(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    engine = MagicMock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(factory, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(factory, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(factory, "create_session_factory", lambda _engine: _session_factory())

    with TestClient(factory.create_app(_settings(tmp_path))) as client:
        assert client.get("/health/live").status_code == 200

    engine.dispose.assert_awaited_once()
