"""Real MCP HTTP/OAuth and application commands against a disposable database."""

import time
import uuid
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import func, select

pytest.importorskip("mcp")
jwt = pytest.importorskip("jwt")

from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from tests.integration.leadership.test_managed_goal_flow import (  # noqa: E402
    OWNER,
    PROJECT,
    _factory,
    _runtime,
    _session,
)

from vuzol.leadership.access import ProjectAccess, ProjectApplication  # noqa: E402
from vuzol.leadership.persistence import ManagedGoal  # noqa: E402
from vuzol.mcp.auth import MCPConfig  # noqa: E402
from vuzol.mcp.server import create_http_app, create_server  # noqa: E402
from vuzol.storage.models import Event, TransactionalOutbox  # noqa: E402

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def signer(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    monkeypatch.setattr(
        jwt.PyJWKClient,
        "get_signing_key_from_jwt",
        lambda self, token: SimpleNamespace(key=key.public_key()),
    )

    def sign(**overrides):
        claims = {
            "sub": "owner",
            "iss": "https://identity.example/",
            "aud": "https://vuzol.example/mcp",
            "iat": int(time.time()),
            "exp": int(time.time()) + 300,
            "scope": "vuzol:read vuzol:submit",
        }
        claims.update(overrides)
        return jwt.encode(claims, key, algorithm="RS256")

    return sign


def _config(session_id, *, allow_submit=True):
    return MCPConfig(
        resource_url="https://vuzol.example/mcp",
        issuer_url="https://identity.example/",
        jwks_url="https://identity.example/keys",
        subjects={
            "owner": ProjectAccess(
                user_id=OWNER, projects={PROJECT: session_id}, allow_submit=allow_submit
            )
        },
        clients={"chatgpt-test-client": "ChatGPT"},
    )


async def _call(client, name, arguments=None, token=None):
    response = await client.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments or {}},
        },
        headers={"Authorization": f"Bearer {token}"} if token else {},
    )
    assert response.status_code == 200, response.text
    return response.json()["result"]


def _client(app, token):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://vuzol.example",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
        },
    )


async def test_http_submit_restart_retry_and_read(postgres_dsn, tmp_path, signer):
    async with _factory(postgres_dsn) as factory:
        session_id = await _session(factory)
        (tmp_path / "repositories" / PROJECT).mkdir(parents=True)
        runtime = _runtime(tmp_path, tmp_path / "repositories" / PROJECT, lead=True)
        config = _config(session_id)
        work = {
            "project_id": PROJECT,
            "request_id": str(uuid.uuid4()),
            "request": "Write a marker",
            "scope": "MARKER.txt only; no push or deployment",
            "criteria": ["MARKER.txt contains hello"],
        }
        goal_id = None
        for restart in range(2):
            if restart:
                config = config.model_copy(
                    update={"clients": {"chatgpt-test-client": "Renamed client"}}
                )
            app = create_http_app(
                create_server(ProjectApplication(runtime, factory), config), config
            )
            async with (
                app.router.lifespan_context(app),
                _client(app, signer(azp="chatgpt-test-client")) as client,
            ):
                initialized = await client.post(
                    "/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 0,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-11-25",
                            "capabilities": {},
                            "clientInfo": {"name": "test", "version": "1"},
                        },
                    },
                )
                assert initialized.status_code == 200
                metadata = await client.get("/.well-known/oauth-protected-resource/mcp")
                assert metadata.json()["scopes_supported"] == ["vuzol:read", "vuzol:submit"]
                listed = await client.post(
                    "/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/list",
                        "params": {},
                    },
                )
                assert {t["name"] for t in listed.json()["result"]["tools"]} == {
                    "list_projects",
                    "get_project_status",
                    "get_result",
                    "submit_request",
                }
                result = await _call(client, "submit_request", {"work": work})
                assert not result.get("isError"), result
                receipt = result["structuredContent"]
                assert receipt["reason"] == ("duplicate" if restart else None), receipt
                assert receipt["status"] == ("duplicate" if restart else "applied"), receipt
                if goal_id is None:
                    goal_id = receipt["goal_id"]
                assert receipt["goal_id"] == goal_id
                conflict = await _call(
                    client, "submit_request", {"work": {**work, "request": "different work"}}
                )
                assert conflict["structuredContent"]["reason"] == "idempotency_conflict"
                status = await _call(
                    client, "get_result", {"project_id": PROJECT, "goal_id": goal_id}
                )
                assert not status.get("isError"), status
                denied = await _call(client, "get_project_status", {"project_id": "other"})
                assert denied["isError"]
                missing = await _call(
                    client, "get_result", {"project_id": PROJECT, "goal_id": str(uuid.uuid4())}
                )
                assert missing["isError"]
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1
            goal = await session.scalar(select(ManagedGoal))
            assert goal.constraints["request_origin"]["display_name"] == "ChatGPT"
            assert goal.constraints["request_origin"]["client_id"] == "chatgpt-test-client"
            events = list(
                await session.scalars(
                    select(Event).where(Event.event_type == "managed_goal.request_received")
                )
            )
            assert len(events) == 1
            assert events[0].payload["request"] == work["request"]
            notices = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key
                        == f"managed-goal:{goal_id}:incoming-request"
                    )
                )
            )
            assert len(notices) == 1
            assert "ChatGPT" in notices[0].payload["html"]
            assert work["request"] in notices[0].payload["html"]
            assert work["scope"] in notices[0].payload["html"]


async def test_authentication_and_write_boundaries(postgres_dsn, tmp_path, signer):
    async with _factory(postgres_dsn) as factory:
        config = _config(await _session(factory))
        (tmp_path / "repositories" / PROJECT).mkdir(parents=True)
        runtime = _runtime(tmp_path, tmp_path / "repositories" / PROJECT, lead=True)
        app = create_http_app(create_server(ProjectApplication(runtime, factory), config), config)
        async with app.router.lifespan_context(app), _client(app, signer()) as client:
            for overrides in (
                {"sub": "stranger"},
                {"iss": "https://evil.example"},
                {"aud": "other-resource"},
                {"exp": 1},
                {"scope": ""},
            ):
                response = await client.post(
                    "/mcp", json={}, headers={"Authorization": f"Bearer {signer(**overrides)}"}
                )
                assert response.status_code == 401
                assert "resource_metadata" in response.headers["www-authenticate"]
            for token in ("invalid", "", jwt.encode({"sub": "owner"}, "", algorithm="none")):
                response = await client.post(
                    "/mcp", json={}, headers={"Authorization": f"Bearer {token}"}
                )
                assert response.status_code == 401
            work = {
                "project_id": PROJECT,
                "request_id": str(uuid.uuid4()),
                "request": "work",
                "scope": "isolated",
                "criteria": ["verified"],
            }
            result = await _call(
                client, "submit_request", {"work": work}, signer(scope="vuzol:read")
            )
            assert result["isError"]
            assert "vuzol:submit" in result["_meta"]["mcp/www_authenticate"][0]
            read = await _call(
                client, "get_project_status", {"project_id": PROJECT}, signer(scope="vuzol:read")
            )
            assert read["structuredContent"]["goal"] is None
            injected = await _call(
                client, "submit_request", {"work": {**work, "user_id": OWNER, "approved": True}}
            )
            assert injected["isError"]
            hostile = await client.post("/mcp", json={}, headers={"Host": "evil.example"})
            assert hostile.status_code == 421
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
