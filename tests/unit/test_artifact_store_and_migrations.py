"""Artifact verified reads and the migration chain (no DB)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from vuzol.execution.artifacts import ArtifactError, ArtifactHashMismatch, ArtifactStore


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
    # Role assignment, supervised turn calls and account admission share one chain.
    assert heads == ["e1b7c9d4a6f2"]  # pragma: allowlist secret - migration identifier
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
