"""Second-node unit tests: eligibility, transfer, verified reads, chain (no DB)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from vuzol.execution.artifacts import ArtifactError, ArtifactHashMismatch, ArtifactStore
from vuzol.execution.remote import (
    TRANSFER_PROTOCOL_VERSION,
    TransferError,
    TransferHashMismatch,
    pull_artifact,
)
from vuzol.projects.nodes import NODE_PROTOCOL_VERSION, node_is_eligible


def _node(**updates: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "node_id": "remote-1",
        "trust_class": "remote",
        "status": "online",
        "protocol_version": NODE_PROTOCOL_VERSION,
        "last_heartbeat_at": datetime.now(UTC),
    }
    values.update(updates)
    return SimpleNamespace(**values)


def test_node_eligibility_is_fail_closed() -> None:
    assert node_is_eligible(_node()) is True  # type: ignore[arg-type]
    assert node_is_eligible(None) is False
    assert node_is_eligible(_node(status="offline")) is False  # type: ignore[arg-type]
    assert node_is_eligible(_node(status="revoked")) is False  # type: ignore[arg-type]
    assert node_is_eligible(_node(protocol_version="other")) is False  # type: ignore[arg-type]
    assert node_is_eligible(_node(last_heartbeat_at=None)) is False  # type: ignore[arg-type]
    stale = _node(last_heartbeat_at=datetime.now(UTC) - timedelta(seconds=10_000))
    assert node_is_eligible(stale, health_ttl_seconds=900) is False  # type: ignore[arg-type]
    future = _node(last_heartbeat_at=datetime.now(UTC) + timedelta(seconds=60))
    assert node_is_eligible(future) is False  # type: ignore[arg-type]


def _mock_client(content: bytes, status: int = 200, length: str | None = None) -> httpx.AsyncClient:
    headers = {}
    if length is not None:
        headers["content-length"] = length

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.startswith("/artifacts/")
        assert "secret" not in str(request.url).lower()
        return httpx.Response(status, headers=headers, content=content)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.anyio
async def test_pull_verifies_hash_and_caps_size() -> None:
    content = b"artifact bytes for node transfer"
    digest = hashlib.sha256(content).hexdigest()
    pulled = await pull_artifact(
        "https://node-2.example.invalid", digest, client=_mock_client(content)
    )
    assert pulled.content == content
    assert pulled.content_hash == digest
    assert TRANSFER_PROTOCOL_VERSION == "node-transfer.v1"
    with pytest.raises(TransferHashMismatch):
        await pull_artifact(
            "https://node-2.example.invalid", "ab" * 32, client=_mock_client(content)
        )
    with pytest.raises(TransferError, match="status 404"):
        await pull_artifact(
            "https://node-2.example.invalid", digest, client=_mock_client(content, status=404)
        )
    big = b"x" * 100
    with pytest.raises(TransferError, match="byte limit"):
        await pull_artifact(
            "https://node-2.example.invalid",
            hashlib.sha256(big).hexdigest(),
            client=_mock_client(big, length=str(10_000)),
            max_bytes=10,
        )
    with pytest.raises(TransferError, match="byte limit"):
        await pull_artifact(
            "https://node-2.example.invalid",
            hashlib.sha256(big).hexdigest(),
            client=_mock_client(big),
            max_bytes=10,
        )
    with pytest.raises(TransferError, match="sha256"):
        await pull_artifact("https://node-2.example.invalid", "not-a-hash")


def test_read_verified_rejects_corruption(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000, retention_days=1)
    content = b"verified content"
    digest = hashlib.sha256(content).hexdigest()
    relative = Path(digest[:2]) / digest
    destination = tmp_path / "artifacts" / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    uri = f"artifact:{relative.as_posix()}"
    assert store.read_verified(uri, digest) == content
    with pytest.raises(ArtifactHashMismatch):
        store.read_verified(uri, "cc" * 32)
    destination.write_bytes(b"tampered")
    with pytest.raises(ArtifactHashMismatch):
        store.read_verified(uri, digest)
    with pytest.raises(ArtifactError):
        store.read_verified("artifact:../escape", digest)


def test_migration_chain_has_single_head() -> None:
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    config = Config("alembic.ini")
    config.set_main_option("script_location", "alembic")
    script = ScriptDirectory.from_config(config)
    heads = script.get_heads()
    assert heads == ["d3c1a8f2e4b7"]
    seen: set[str] = set()
    revision: str | None = heads[0]
    while revision is not None:
        assert revision not in seen
        seen.add(revision)
        node = script.get_revision(revision)
        assert node is not None
        parents = node.down_revision
        revision = parents if isinstance(parents, str) else (parents[0] if parents else None)
    assert len(seen) >= 30
