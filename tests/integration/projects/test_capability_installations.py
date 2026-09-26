"""WP03 integration: installation records, health TTL and run version pins."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from vuzol.projects.installations import (
    InstallationState,
    installation_states,
    pin_capability,
    pin_matches,
    probe_toolchain,
    receipt_digest,
    record_installation,
)
from vuzol.projects.toolchains import ToolchainSpec
from vuzol.storage.models import CapabilityInstallation, Run, Task
from vuzol.storage.types import RunStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork

from ..storage.helpers import storage

pytestmark = pytest.mark.postgresql


def _spec(version: str = "1.0.0") -> ToolchainSpec:
    return ToolchainSpec(
        capability_key="node-runtime",
        version=version,
        archive_sha256="a" * 64,
        executables=(("node", "bin/node"),),
    )


def _install_toolchain(root: Path, spec: ToolchainSpec) -> Path:
    toolchain = root / spec.capability_key
    receipt = toolchain / ".vuzol-toolchain.json"
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps(spec.receipt()), encoding="utf-8")
    receipt.chmod(0o644)
    executable = toolchain / "bin" / "node"
    executable.parent.mkdir(parents=True, exist_ok=True)
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    return root


def test_probe_requires_confined_readability(tmp_path: Path) -> None:
    spec = _spec()
    root = _install_toolchain(tmp_path / "toolchains", spec)

    state, probed, _detail = probe_toolchain(root, "node-runtime", confined_roots=(root,))
    assert state is InstallationState.INSTALLED
    assert probed is not None and probed.version == "1.0.0"

    failed, _spec_out, detail = probe_toolchain(root, "node-runtime", confined_roots=())
    assert failed is InstallationState.FAILED
    assert "confined" in detail


def test_installation_records_and_health_ttl(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        spec = _spec()
        root = _install_toolchain(tmp_path / "toolchains", spec)
        probe = probe_toolchain(root, "node-runtime", confined_roots=(root,))
        moment = datetime.now(UTC)
        async with factory.begin() as session:
            await record_installation(
                session,
                probe=probe,
                capability_key="node-runtime",
                installation_root=root,
                node_id="local",
                environment_hash="b" * 64,
                health_ttl_seconds=600,
                now=moment,
            )
            # Idempotent: a second record updates the same row.
            await record_installation(
                session,
                probe=probe,
                capability_key="node-runtime",
                installation_root=root,
                node_id="local",
                environment_hash="b" * 64,
                health_ttl_seconds=600,
                now=moment,
            )
        async with factory() as session:
            rows = list((await session.scalars(select(CapabilityInstallation))).all())
            assert len(rows) == 1 and rows[0].status == "installed"
            fresh = await installation_states(session, now=moment)
            assert fresh["node-runtime"] == "installed"
            expired = await installation_states(session, now=moment + timedelta(hours=1))
            assert expired["node-runtime"] == "stale"

        failed_probe = probe_toolchain(root, "node-runtime", confined_roots=())
        async with factory.begin() as session:
            await record_installation(
                session,
                probe=failed_probe,
                capability_key="node-runtime",
                installation_root=root,
                node_id="local",
                environment_hash="b" * 64,
                health_ttl_seconds=600,
                now=moment,
            )
        async with factory() as session:
            states = await installation_states(session, now=moment)
            assert states["node-runtime"] == "failed"
        await engine.dispose()

    asyncio.run(scenario())


def test_run_pin_blocks_silent_version_change(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=1, chat_id=-100, original_text="pin", task_type="coding"
            )
            assert uow.session is not None
            stored = await uow.session.get(Task, task.id)
            assert stored is not None
            stored.status = TaskStatus.EXECUTING
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.RUNNING,
            )
        spec_v1 = _spec("1.0.0")
        spec_v2 = _spec("2.0.0")
        async with factory.begin() as session:
            pin = await pin_capability(session, run_id=run_id, spec=spec_v1)
            assert pin.version == "1.0.0"
        async with factory.begin() as session:
            assert await pin_matches(session, run_id=run_id, spec=spec_v1) is True
            # A downgrade/upgrade must not silently change an in-flight run.
            assert await pin_matches(session, run_id=run_id, spec=spec_v2) is False
        async with factory() as session:
            run = await session.get(Run, run_id)
            assert run is not None
            assert receipt_digest(spec_v1) != receipt_digest(spec_v2)
        await engine.dispose()

    asyncio.run(scenario())
