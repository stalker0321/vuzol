"""PrepareWorktreeHandler re-checks disk pressure before materialization."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from vuzol.config.settings import DiskPressureSettings, Settings
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.ops.disk_pressure import DISK_PRESSURE_CATEGORY
from vuzol.storage.records import LeaseToken
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class _Probe:
    def __init__(self, free: int) -> None:
        self.free = free

    def free_bytes(self, path: Path) -> int:
        del path
        return self.free


def _request() -> StepExecutionRequest:
    lease = MagicMock(spec=LeaseToken)
    return StepExecutionRequest(
        task_id=uuid4(),
        run_id=uuid4(),
        step_id=uuid4(),
        step_type="prepare_worktree",
        payload={},
        timeout_seconds=60,
        lease=lease,
    )


def test_prepare_worktree_defers_when_disk_low(tmp_path: Path) -> None:
    async def scenario() -> None:
        settings = Settings(
            environment="test",
            repository_root=tmp_path / "repositories",
            worktree_root=tmp_path / "worktrees",
            artifact_root=tmp_path / "artifacts",
            secret_file_root=tmp_path / "secrets",
            disk_pressure=DiskPressureSettings(min_free_bytes=1_000),
        )
        worktrees = MagicMock()
        worktrees.prepare = AsyncMock()
        handler = PrepareWorktreeHandler(
            MagicMock(),
            MagicMock(),
            worktrees,
            owner="test",
            settings=settings,
            free_space_probe=_Probe(10),
        )
        outcome = await handler.execute(_request(), CancellationContext())
        assert outcome.kind is OutcomeKind.TRANSIENT_FAILURE
        assert outcome.category == DISK_PRESSURE_CATEGORY
        worktrees.prepare.assert_not_called()

    asyncio.run(scenario())


def test_prepare_worktree_missing_settings_fails_closed() -> None:
    async def scenario() -> None:
        worktrees = MagicMock()
        worktrees.prepare = AsyncMock()
        handler = PrepareWorktreeHandler(
            MagicMock(),
            MagicMock(),
            worktrees,
            owner="test",
            settings=None,
        )

        outcome = await handler.execute(_request(), CancellationContext())

        assert outcome.kind is OutcomeKind.TRANSIENT_FAILURE
        assert outcome.category == DISK_PRESSURE_CATEGORY
        worktrees.prepare.assert_not_called()

    asyncio.run(scenario())


def test_prepare_worktree_proceeds_when_disk_ok(tmp_path: Path) -> None:
    async def scenario() -> None:
        settings = Settings(
            environment="test",
            repository_root=tmp_path / "repositories",
            worktree_root=tmp_path / "worktrees",
            artifact_root=tmp_path / "artifacts",
            secret_file_root=tmp_path / "secrets",
            disk_pressure=DiskPressureSettings(min_free_bytes=1_000),
        )
        worktrees = MagicMock()
        worktrees.prepare = AsyncMock(
            return_value=SimpleNamespace(id=uuid4(), base_commit="abc", branch="main")
        )
        task = SimpleNamespace(id=uuid4(), project_id="proj")
        session = AsyncMock()
        session.get = AsyncMock(return_value=task)
        begin_cm = MagicMock()
        begin_cm.__aenter__ = AsyncMock(return_value=session)
        begin_cm.__aexit__ = AsyncMock(return_value=None)
        factory = MagicMock()
        factory.begin = MagicMock(return_value=begin_cm)
        registries = MagicMock()
        registries.projects.get = MagicMock(return_value=MagicMock())
        handler = PrepareWorktreeHandler(
            factory,
            registries,
            worktrees,
            owner="test",
            settings=settings,
            free_space_probe=_Probe(50_000),
        )
        outcome = await handler.execute(_request(), CancellationContext())
        assert outcome.kind is OutcomeKind.SUCCEEDED
        worktrees.prepare.assert_awaited_once()

    asyncio.run(scenario())
