"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from ._execution_helpers import *


def test_artifact_redaction_and_secret_rejection(tmp_path: Path) -> None:
    store = ArtifactStore(
        tmp_path / "artifacts",
        max_bytes=1000,
        retention_days=1,
        redaction_patterns=(r"token-[a-z]+",),
    )
    redacted, revision = store.redact(b"value=token-secret")
    assert redacted == b"value=[REDACTED]"
    assert revision is not None
    with pytest.raises(ArtifactSecretError):
        store.reject_secrets(b"diff contains token-secret")
    store.reject_secrets(b"safe content")


@pytest.mark.anyio
async def test_artifact_persist_with_mock_session(tmp_path: Path) -> None:
    """Test ArtifactStore.persist path with mock session (real persist logic + redaction)."""
    store = ArtifactStore(tmp_path / "art", max_bytes=10_000, retention_days=1)
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.flush = AsyncMock()

    art = await store.persist(
        mock_session,
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        artifact_type="test",
        content=b"hello world",
        media_type="text/plain",
    )
    assert art is not None
    assert art.content_hash is not None
    mock_session.add.assert_called()


def test_artifact_store_size_limit(tmp_path: Path) -> None:
    """Test ArtifactStore max_bytes is respected (construction and limit behavior)."""
    store = ArtifactStore(tmp_path / "art", max_bytes=100, retention_days=1)
    assert store._max_bytes == 100
