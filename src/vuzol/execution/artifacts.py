"""Bounded atomic content-addressed artifact persistence."""

import hashlib
import os
import uuid
from datetime import timedelta
from pathlib import Path

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.execution.paths import contained, trusted_root
from vuzol.storage.models import Artifact
from vuzol.storage.types import ArtifactStorageState


class ArtifactError(RuntimeError):
    """Artifact persistence failed a size or containment invariant."""


class ArtifactStore:
    def __init__(self, root: Path, *, max_bytes: int, retention_days: int) -> None:
        self._root = trusted_root(root, create=True)
        self._max_bytes = max_bytes
        self._retention_days = retention_days

    async def persist(
        self,
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        run_id: uuid.UUID,
        step_id: uuid.UUID,
        artifact_type: str,
        content: bytes,
        media_type: str,
        sensitivity: str = "internal",
        visibility: str = "private",
        redaction_revision: str | None = None,
        producer_process_id: uuid.UUID | None = None,
    ) -> Artifact:
        if len(content) > self._max_bytes:
            raise ArtifactError("artifact exceeds configured byte limit")
        digest = hashlib.sha256(content).hexdigest()
        relative = Path(digest[:2]) / digest
        storage_key = f"{task_id}/{run_id}/{step_id}/{artifact_type}/{digest}"
        destination = self._root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        contained(self._root, destination.parent)
        if not destination.exists():
            temporary = destination.with_name(f".{digest}.{uuid.uuid4()}.tmp")
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
            )
            try:
                os.write(descriptor, content)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(temporary, destination)
        row = Artifact(
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            artifact_type=artifact_type,
            content_uri=f"artifact:{relative.as_posix()}",
            storage_key=storage_key,
            size_bytes=len(content),
            content_hash=digest,
            media_type=media_type,
            sensitivity=sensitivity,
            visibility=visibility,
            retention_until=func.now() + timedelta(days=self._retention_days),
            storage_state=ArtifactStorageState.AVAILABLE,
            redaction_revision=redaction_revision,
            verified_at=func.now(),
            producer_process_id=producer_process_id,
        )
        session.add(row)
        await session.flush()
        return row
