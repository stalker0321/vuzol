"""WP10: procedure receipts persist as hash-linked Artifacts (PG)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.execution.artifacts import ArtifactStore
from vuzol.projects.installations import (
    InstallationState,
    installation_states,
    record_installation,
)
from vuzol.projects.procedures import repo_quality_procedure
from vuzol.projects.receipts import GateResult, ProcedureReceipt, require_receipt_link
from vuzol.storage.models import Task
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _receipt(environment_hash: str) -> ProcedureReceipt:
    procedure = repo_quality_procedure()
    return ProcedureReceipt(
        procedure_ref=procedure.ref,
        descriptor_hash=procedure.descriptor_hash,
        environment_hash=environment_hash,
        gates=(GateResult(gate="secret-scan", passed=True),),
        created_at="2026-09-27T10:00:00Z",
    )


async def test_receipt_persisted_as_artifact_with_verified_link(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000_000, retention_days=14)
    receipt = _receipt("e" * 64)
    content = receipt.canonical_bytes()
    _task, run_id, step = await seed_task_run_step(factory)
    async with factory.begin() as session:
        task = await session.get(Task, _task.id, with_for_update=True)
        assert task is not None
        task.project_id = "vuzol"
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        artifact = await store.persist(
            uow.session,
            task_id=_task.id,
            run_id=run_id,
            step_id=step.id,
            artifact_type="procedure_receipt",
            content=content,
            media_type="application/json",
        )
    assert artifact.content_hash == receipt.receipt_hash
    async with factory() as session:
        stored = store.read(artifact.content_uri)
    require_receipt_link(receipt, stored)
    await engine.dispose()


async def test_failed_probe_installation_excluded_from_selection(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        await record_installation(
            uow.session,
            probe=(InstallationState.FAILED, None, "probe refused binary"),
            capability_key="python-runtime",
            installation_root=tmp_path / "tools",
            node_id="local",
            environment_hash=None,
            health_ttl_seconds=3600,
        )
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        states = await installation_states(uow.session, node_id="local")
    # Failed probe quarantines by exclusion: never selected as healthy.
    assert states["python-runtime"] == InstallationState.FAILED.value
    assert states["python-runtime"] != InstallationState.INSTALLED.value
    await engine.dispose()
