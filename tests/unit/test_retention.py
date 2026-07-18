"""Unit coverage for retention policy, dry-run purity, and path safety."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr, ValidationError

from vuzol.config.settings import RetentionDefaults
from vuzol.ops.retention import (
    QUARANTINE_DIR_NAME,
    RetentionOutcome,
    RetentionSweeper,
    RetentionSweepMode,
    effective_worktree_retention_until,
)
from vuzol.storage.types import TaskStatus


def test_failed_retention_cannot_be_shorter_than_completed() -> None:
    with pytest.raises(ValidationError, match="failed worktree retention"):
        RetentionDefaults(completed_worktree_days=14, failed_worktree_days=3)


def test_effective_retention_never_cleans_blocked_or_open_tasks() -> None:
    now = datetime(2026, 7, 1, tzinfo=UTC)
    assert (
        effective_worktree_retention_until(
            task_status=TaskStatus.BLOCKED,
            retention_until=now - timedelta(days=30),
            task_updated_at=now - timedelta(days=30),
            completed_days=3,
            failed_days=14,
        )
        is None
    )


def test_completed_uses_shorter_window() -> None:
    updated = datetime(2026, 7, 1, tzinfo=UTC)
    stamped = updated + timedelta(days=14)
    effective = effective_worktree_retention_until(
        task_status=TaskStatus.COMPLETED,
        retention_until=stamped,
        task_updated_at=updated,
        completed_days=3,
        failed_days=14,
    )
    assert effective == updated + timedelta(days=3)


def test_dry_run_does_not_create_missing_roots(tmp_path: Path) -> None:
    missing_wt = tmp_path / "missing-worktrees"
    missing_art = tmp_path / "missing-artifacts"
    repos = tmp_path / "repos"
    repos.mkdir()

    class Factory:
        def __call__(self) -> object:
            raise AssertionError("db unused when roots missing")

    sweeper = RetentionSweeper(
        Factory(),  # type: ignore[arg-type]
        worktree_root=missing_wt,
        artifact_root=missing_art,
        repository_root=repos,
        retention=RetentionDefaults(),
        owner="test",
        lock_timeout_seconds=0.1,
    )

    async def run() -> None:
        report = await sweeper.run(mode=RetentionSweepMode.DRY_RUN)
        assert report.lock_acquired is False
        assert report.actions[0].reason == "roots_unavailable"
        assert not missing_wt.exists()
        assert not missing_art.exists()
        assert not (missing_art / QUARANTINE_DIR_NAME).exists()

    import asyncio

    asyncio.run(run())


def test_dry_run_bind_roots_does_not_create_quarantine(tmp_path: Path) -> None:
    worktree_root = tmp_path / "worktrees"
    artifact_root = tmp_path / "artifacts"
    repos = tmp_path / "repos"
    worktree_root.mkdir()
    artifact_root.mkdir()
    repos.mkdir()

    class Factory:
        def __call__(self) -> object:
            raise AssertionError("unused")

    sweeper = RetentionSweeper(
        Factory(),  # type: ignore[arg-type]
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        repository_root=repos,
        retention=RetentionDefaults(),
        owner="test",
    )
    sweeper._bind_roots(mode=RetentionSweepMode.DRY_RUN)
    assert not (artifact_root / QUARANTINE_DIR_NAME).exists()


@pytest.mark.anyio
async def test_orphan_symlink_dry_run_does_not_move(tmp_path: Path) -> None:
    worktree_root = tmp_path / "worktrees"
    artifact_root = tmp_path / "artifacts"
    repos = tmp_path / "repos"
    worktree_root.mkdir()
    artifact_root.mkdir()
    repos.mkdir()
    outside = tmp_path / "secret.txt"
    outside.write_text("secret\n")
    link = artifact_root / "escape"
    link.symlink_to(outside)

    class Factory:
        def __call__(self) -> object:
            raise AssertionError("db must not be used")

    sweeper = RetentionSweeper(
        Factory(),  # type: ignore[arg-type]
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        repository_root=repos,
        retention=RetentionDefaults(),
        owner="test",
    )
    sweeper._bind_roots(mode=RetentionSweepMode.DRY_RUN)
    sweeper._known_content_hashes = AsyncMock(return_value=set())  # type: ignore[method-assign]
    actions = await sweeper._reconcile_orphan_artifacts(mode=RetentionSweepMode.DRY_RUN)
    assert any(
        action.outcome is RetentionOutcome.WOULD_QUARANTINE and action.reason == "symlink_refused"
        for action in actions
    )
    assert link.exists() and link.is_symlink()
    assert outside.read_text() == "secret\n"


def test_retention_cli_defaults_to_dry_run() -> None:
    from vuzol.cli.retention import _parse_args

    args = _parse_args([])
    assert args.apply is False
    apply_args = _parse_args(["--apply"])
    assert apply_args.apply is True


@pytest.mark.anyio
async def test_retention_cli_run_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from datetime import UTC

    from vuzol.cli import retention as retention_cli
    from vuzol.config import RegistryDocument, RuntimeConfiguration, Settings, build_bundle
    from vuzol.ops.retention import RetentionAction, RetentionSweepReport

    settings = Settings(
        environment="test",
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        repository_root=tmp_path / "repos",
    )
    (tmp_path / "worktrees").mkdir()
    (tmp_path / "artifacts").mkdir()
    (tmp_path / "repos").mkdir()
    runtime = RuntimeConfiguration(
        settings=settings, registries=build_bundle(RegistryDocument(), settings)
    )
    report = RetentionSweepReport(
        mode=RetentionSweepMode.DRY_RUN,
        lock_acquired=True,
        started_at=datetime(2026, 7, 1, tzinfo=UTC),
        finished_at=datetime(2026, 7, 1, 0, 1, tzinfo=UTC),
        actions=(
            RetentionAction("worktree", "w1", RetentionOutcome.WOULD_CLEAN, "retention_expired"),
        ),
    )

    class Engine:
        async def dispose(self) -> None:
            return None

    class FakeSweeper:
        def __init__(self, *_a: object, **_k: object) -> None:
            return None

        async def run(self, *, mode: RetentionSweepMode) -> RetentionSweepReport:
            assert mode is RetentionSweepMode.DRY_RUN
            return report

    monkeypatch.setattr(retention_cli, "get_runtime_configuration", lambda **_k: runtime)
    monkeypatch.setattr(retention_cli, "configure_logging", lambda **_k: None)
    monkeypatch.setattr(
        retention_cli, "resolve_database_dsn", lambda _s: SecretStr("postgresql+psycopg://x")
    )
    monkeypatch.setattr(retention_cli, "create_engine", lambda *_a, **_k: Engine())
    monkeypatch.setattr(retention_cli, "create_session_factory", lambda _e: object())
    monkeypatch.setattr(retention_cli, "RetentionSweeper", FakeSweeper)

    assert await retention_cli._run(retention_cli._parse_args([])) == 0
    assert "would_clean" in capsys.readouterr().out


def test_retention_unit_templates_disabled_comments() -> None:
    root = Path(__file__).resolve().parents[2]
    service = (root / "deploy/systemd/vuzol-retention.service").read_text()
    timer = (root / "deploy/systemd/vuzol-retention.timer").read_text()
    assert "Do not install" in service
    assert "Do not install" in timer
    assert "--apply" in service
