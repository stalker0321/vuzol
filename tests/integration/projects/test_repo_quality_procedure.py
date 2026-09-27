"""WP10: two horizons run one procedure without new setup; smoke; failure evidence (PG)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.execution.artifacts import ArtifactStore
from vuzol.projects.installations import (
    InstallationState,
    installation_states,
    record_installation,
)
from vuzol.projects.procedures import (
    ProcedureRegistry,
    approve_procedure,
    repo_quality_procedure,
)
from vuzol.projects.receipts import require_receipt_link
from vuzol.projects.runner import (
    GateOutcome,
    GateRunner,
    ProcedureRun,
    ProcedureRunFailed,
    publish_receipt_artifact,
    run_procedure,
)
from vuzol.storage.models import Artifact, CapabilityInstallation, Task
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


class _FakeGates(GateRunner):
    async def run(self, command_id: str) -> GateOutcome:
        return GateOutcome(command_id=command_id, passed=True, detail="fake pass")


async def _environment(
    factory: async_sessionmaker[AsyncSession], node_id: str = "local"
) -> dict[str, tuple[str, str | None]]:
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        states = await installation_states(uow.session, node_id=node_id)
    async with factory() as session:
        rows = tuple(
            (
                await session.scalars(
                    select(CapabilityInstallation).where(CapabilityInstallation.node_id == node_id)
                )
            ).all()
        )
    hashes = {row.capability_key: row.environment_hash for row in rows}
    return {key: (state, hashes.get(key)) for key, state in states.items()}


async def _seed_healthy(factory: async_sessionmaker[AsyncSession], root: Path) -> None:
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        for key in ("git", "python-runtime"):
            await record_installation(
                uow.session,
                probe=(InstallationState.INSTALLED, None, "healthy fixture install"),
                capability_key=key,
                installation_root=root / "tools",
                node_id="local",
                environment_hash="a" * 64,
                health_ttl_seconds=3600,
            )


def _registry() -> ProcedureRegistry:
    procedure = repo_quality_procedure()
    registry = ProcedureRegistry()
    registry.promote(procedure, approval=approve_procedure(procedure, approver="lead"))
    return registry


async def _publish(
    factory: async_sessionmaker[AsyncSession], store: ArtifactStore, run: ProcedureRun
) -> None:
    _task, run_id, step = await seed_task_run_step(factory)
    async with factory.begin() as session:
        task = await session.get(Task, _task.id, with_for_update=True)
        assert task is not None
        task.project_id = "vuzol"
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        artifact = await publish_receipt_artifact(
            store,
            uow.session,
            task_id=_task.id,
            run_id=run_id,
            step_id=step.id,
            receipt=run.receipt,
        )
    assert artifact.content_hash == run.receipt.receipt_hash
    async with factory() as session:
        stored = store.read(artifact.content_uri)
    require_receipt_link(run.receipt, stored)


async def test_two_horizons_run_one_procedure_without_new_setup(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000_000, retention_days=14)
    await _seed_healthy(factory, tmp_path)
    registry = _registry()
    receipts = []
    for _horizon in ("first horizon", "second horizon"):
        environment = await _environment(factory)
        run = await run_procedure(
            registry,
            "repo.quality@1",
            gates=("make lint",),
            environment=environment,
            gate_runner=_FakeGates(),
            created_at="2026-09-27T10:00:00Z",
        )
        assert run.environment == {"git": "installed", "python-runtime": "installed"}
        await _publish(factory, store, run)
        receipts.append(run.receipt.receipt_hash)
    assert receipts[0] == receipts[1]
    async with factory() as session:
        installs = await session.scalar(select(func.count()).select_from(CapabilityInstallation))
        artifacts = await session.scalar(select(func.count()).select_from(Artifact))
    # One setup, two runs: installations untouched, two receipts published.
    assert installs == 2
    assert artifacts == 2
    await engine.dispose()


async def test_procedure_failure_retains_receipt_evidence(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000_000, retention_days=14)
    await _seed_healthy(factory, tmp_path)

    class _FailingGates(GateRunner):
        async def run(self, command_id: str) -> GateOutcome:
            return GateOutcome(command_id=command_id, passed=False, detail="boom")

    with pytest.raises(ProcedureRunFailed) as failure:
        await run_procedure(
            _registry(),
            "repo.quality@1",
            gates=("make lint",),
            environment=await _environment(factory),
            gate_runner=_FailingGates(),
            created_at="2026-09-27T10:00:00Z",
        )
    assert failure.value.code == "gate_failed"
    receipt = failure.value.receipt
    assert receipt is not None and receipt.gates[0].passed is False
    _task, run_id, step = await seed_task_run_step(factory)
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        artifact = await publish_receipt_artifact(
            store,
            uow.session,
            task_id=_task.id,
            run_id=run_id,
            step_id=step.id,
            receipt=receipt,
        )
    assert artifact.content_hash == receipt.receipt_hash
    await engine.dispose()


async def test_local_conformance_smoke(tmp_path: Path) -> None:
    from vuzol.projects.runner import LocalGateRunner

    probe = tmp_path / "smoke.test.js"
    probe.write_text("const t = require('node:test');\nt('smoke', () => {});\n")
    runner = LocalGateRunner(workdir=tmp_path, timeout_seconds=60.0)
    outcome = await runner.run("node --test")
    assert outcome.command_id == "node --test"
    assert outcome.passed is True
