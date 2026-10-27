"""J1 decision binding + chain PostgreSQL integration (transaction/competition)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.integration.providers._test_routing_helpers import (
    seed_provider_step,
    storage,
)
from vuzol.context.decision_binding import (
    Coverage,
    DecisionBinding,
)
from vuzol.ops.retention import pin_artifacts_for_audit
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.models import (
    Artifact,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@dataclass(frozen=True, slots=True)
class _Result:
    usage: NormalizedUsage | None
    provider_request_id: str | None


def _binding() -> DecisionBinding:
    return DecisionBinding(
        decision_kind="intake",
        request_hash="a" * 64,
        schema_hash="b" * 64,
        prompt_hash="c" * 64,
        packet_hash="d" * 64,
        coverage=Coverage.COMPLETE,
    )


async def test_pin_artifacts_for_audit_only_extends(postgres_dsn: str, tmp_path: Path) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        task_id, _run_id, _step = await seed_provider_step(factory)
        now = datetime.now(UTC)
        old = now - timedelta(days=1)
        async with factory.begin() as session:
            artifact = Artifact(
                task_id=task_id,
                artifact_type="decision-input",
                content_uri="artifact://decision/input",
                size_bytes=1,
                content_hash="a" * 64,
                media_type="application/json",
                sensitivity="private",
                visibility="task",
                retention_until=old,
                metadata_json={},
            )
            session.add(artifact)
            await session.flush()
            artifact_id = artifact.id
        async with factory.begin() as session:
            assert (
                await pin_artifacts_for_audit(
                    session, artifact_ids=(artifact_id,), retention_days=30, now=now
                )
                == 1
            )
        async with factory() as session:
            row = await session.get(Artifact, artifact_id)
            assert row is not None and row.retention_until > old
        async with factory.begin() as session:
            assert (
                await pin_artifacts_for_audit(
                    session, artifact_ids=(artifact_id,), retention_days=30, now=now
                )
                == 0
            )
    finally:
        await engine.dispose()
