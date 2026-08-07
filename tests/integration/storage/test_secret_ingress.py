from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import HttpUrl

from vuzol.app import create_app
from vuzol.config import Settings
from vuzol.config.settings import SecretIngressSettings
from vuzol.security.secret_ingress import cancel_request, create_request

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def test_one_time_form_installs_secret_without_persisting_value(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    configured = SecretIngressSettings(
        enabled=True,
        public_base_url=HttpUrl("https://vuzol.example"),
        storage_root=tmp_path,
        allowed_names=("TOKENROUTER_API_KEY",),
    )
    async with factory.begin() as session:
        request, token = await create_request(
            session,
            configured,
            secret_name="TOKENROUTER_API_KEY",  # noqa: S106  # pragma: allowlist secret
            user_id=42,
            chat_id=-1001,
            thread_id=7,
        )
    app = create_app(
        Settings(environment="test", secret_ingress=configured), session_factory=factory
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        shown = await client.get(f"/secret/{token}")
        assert shown.status_code == 200
        assert shown.headers["cache-control"] == "no-store"
        saved = await client.post(f"/secret/{token}", data={"value": "top-secret-value"})
        assert saved.status_code == 200
        repeated = await client.post(f"/secret/{token}", data={"value": "replacement"})
        assert repeated.status_code == 410

    assert (tmp_path / "TOKENROUTER_API_KEY").read_text() == "top-secret-value"
    async with factory() as session:
        stored = await session.get(type(request), request.id)
        assert stored is not None
        assert stored.status == "consumed"
        assert "top-secret-value" not in repr(stored.__dict__)
    async with factory.begin() as session:
        cancelled, cancelled_token = await create_request(
            session,
            configured,
            secret_name="TOKENROUTER_API_KEY",  # noqa: S106  # pragma: allowlist secret
            user_id=42,
            chat_id=-1001,
            thread_id=7,
        )
        assert await cancel_request(session, cancelled.id, user_id=42) is True
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.get(f"/secret/{cancelled_token}")).status_code == 410
    await engine.dispose()
