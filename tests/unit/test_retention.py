"""Unit coverage for retention policy helpers and filesystem safety edges."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.config.settings import RetentionDefaults, Settings
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
    assert (
        effective_worktree_retention_until(
            task_status=TaskStatus.EXECUTING,
            retention_until=now - timedelta(days=30),
            task_updated_at=now - timedelta(days=30),
            completed_days=3,
            failed_days=14,
        )
        is None
    )


def test_completed_uses_shorter_window_without_lengthening_failed_stamp() -> None:
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


def test_failed_keeps_at_least_failed_window() -> None:
    updated = datetime(2026, 7, 1, tzinfo=UTC)
    short_stamp = updated + timedelta(days=1)
    effective = effective_worktree_retention_until(
        task_status=TaskStatus.FAILED,
        retention_until=short_stamp,
        task_updated_at=updated,
        completed_days=3,
        failed_days=14,
    )
    assert effective == updated + timedelta(days=14)


def test_report_payload_is_projection_ready() -> None:
    from vuzol.ops.retention import RetentionAction, RetentionSweepReport

    report = RetentionSweepReport(
        mode=RetentionSweepMode.DRY_RUN,
        lock_acquired=True,
        started_at=datetime(2026, 7, 1, tzinfo=UTC),
        finished_at=datetime(2026, 7, 1, 0, 1, tzinfo=UTC),
        actions=(
            RetentionAction("worktree", "w1", RetentionOutcome.WOULD_CLEAN, "retention_expired"),
            RetentionAction("artifact", "a1", RetentionOutcome.SKIPPED, "blocked_task"),
        ),
    )
    payload = report.to_operational_payload()
    assert payload["schema_version"] == "retention-sweep-report.v1"
    assert payload["cleaned_count"] == 0
    assert payload["skipped_count"] == 1
    counts = payload["outcome_counts"]
    assert isinstance(counts, dict)
    assert counts["would_clean"] == 1


@pytest.mark.anyio
async def test_orphan_symlink_is_reported_for_quarantine_without_following(tmp_path: Path) -> None:
    from unittest.mock import AsyncMock

    worktree_root = tmp_path / "worktrees"
    artifact_root = tmp_path / "artifacts"
    worktree_root.mkdir()
    artifact_root.mkdir()
    outside = tmp_path / "secret.txt"
    outside.write_text("secret\n")
    link = artifact_root / "escape"
    link.symlink_to(outside)

    class Factory:
        def __call__(self) -> object:
            raise AssertionError("database must not be used for this dry-run orphan check")

    sweeper = RetentionSweeper(
        Factory(),  # type: ignore[arg-type]
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        retention=RetentionDefaults(),
        owner="test",
        lock_timeout_seconds=0.1,
    )
    sweeper._known_content_hashes = AsyncMock(return_value=set())  # type: ignore[method-assign]
    actions = await sweeper._reconcile_orphan_artifacts(mode=RetentionSweepMode.DRY_RUN)
    assert any(
        action.outcome is RetentionOutcome.WOULD_QUARANTINE and action.reason == "symlink_refused"
        for action in actions
    )
    assert link.exists() and link.is_symlink()
    assert outside.read_text() == "secret\n"
    assert not (artifact_root / QUARANTINE_DIR_NAME).exists() or not any(
        (artifact_root / QUARANTINE_DIR_NAME).rglob("*")
    )


def test_settings_accept_sweep_batch_defaults() -> None:
    settings = Settings(environment="test")
    assert settings.retention.sweep_batch_size == 50
    assert settings.retention.completed_worktree_days == 3
    assert settings.retention.failed_worktree_days == 14


def test_retention_cli_defaults_to_dry_run() -> None:
    from vuzol.cli.retention import _parse_args

    args = _parse_args([])
    assert args.apply is False
    assert args.dry_run is True
    apply_args = _parse_args(["--apply", "--json"])
    assert apply_args.apply is True
    assert apply_args.json is True


@pytest.mark.anyio
async def test_retention_cli_run_emits_text_and_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from datetime import UTC, datetime

    from pydantic import SecretStr

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
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            return None

        async def run(self, *, mode: RetentionSweepMode) -> RetentionSweepReport:
            assert mode is RetentionSweepMode.DRY_RUN
            return report

    monkeypatch.setattr(retention_cli, "get_runtime_configuration", lambda **_k: runtime)
    monkeypatch.setattr(retention_cli, "configure_logging", lambda **_k: None)
    monkeypatch.setattr(
        retention_cli, "resolve_database_dsn", lambda _settings: SecretStr("postgresql+psycopg://x")
    )
    monkeypatch.setattr(retention_cli, "create_engine", lambda *_a, **_k: Engine())
    monkeypatch.setattr(retention_cli, "create_session_factory", lambda _engine: object())
    monkeypatch.setattr(retention_cli, "RetentionSweeper", FakeSweeper)

    code = await retention_cli._run(retention_cli._parse_args([]))
    assert code == 0
    out = capsys.readouterr().out
    assert "would_clean" in out
    assert "worktree" in out

    code = await retention_cli._run(retention_cli._parse_args(["--json"]))
    assert code == 0
    payload = capsys.readouterr().out
    assert '"schema_version": "retention-sweep-report.v1"' in payload

    locked = RetentionSweepReport(
        mode=RetentionSweepMode.APPLY,
        lock_acquired=False,
        started_at=datetime(2026, 7, 1, tzinfo=UTC),
        finished_at=datetime(2026, 7, 1, 0, 1, tzinfo=UTC),
        actions=(
            RetentionAction("sweep", "lock", RetentionOutcome.SKIPPED, "advisory_lock_unavailable"),
        ),
    )

    class LockedSweeper(FakeSweeper):
        async def run(self, *, mode: RetentionSweepMode) -> RetentionSweepReport:
            return locked

    monkeypatch.setattr(retention_cli, "RetentionSweeper", LockedSweeper)
    assert await retention_cli._run(retention_cli._parse_args(["--apply"])) == 2

    failed = RetentionSweepReport(
        mode=RetentionSweepMode.APPLY,
        lock_acquired=True,
        started_at=datetime(2026, 7, 1, tzinfo=UTC),
        finished_at=datetime(2026, 7, 1, 0, 1, tzinfo=UTC),
        actions=(RetentionAction("worktree", "w1", RetentionOutcome.FAILED, "path_violation"),),
    )

    class FailedSweeper(FakeSweeper):
        async def run(self, *, mode: RetentionSweepMode) -> RetentionSweepReport:
            return failed

    monkeypatch.setattr(retention_cli, "RetentionSweeper", FailedSweeper)
    assert await retention_cli._run(retention_cli._parse_args(["--apply"])) == 1


def test_retention_cli_main_exits_with_code(monkeypatch: pytest.MonkeyPatch) -> None:
    from vuzol.cli import retention as retention_cli

    async def fake_run(_args: object) -> int:
        return 0

    monkeypatch.setattr(retention_cli, "_run", fake_run)

    with pytest.raises(SystemExit) as raised:
        retention_cli.main([])
    assert raised.value.code == 0


def test_retention_unit_templates_are_oneshot_and_not_auto_installed() -> None:
    root = Path(__file__).resolve().parents[2]
    service = (root / "deploy/systemd/vuzol-retention.service").read_text()
    timer = (root / "deploy/systemd/vuzol-retention.timer").read_text()
    assert "Type=oneshot" in service
    assert "vuzol-retention --apply" in service
    assert "Do not install" in service
    assert "Unit=vuzol-retention.service" in timer
    assert "Do not install" in timer


def test_remove_worktree_tree_and_entry_root_helpers(tmp_path: Path) -> None:
    worktree_root = tmp_path / "worktrees"
    artifact_root = tmp_path / "artifacts"
    worktree_root.mkdir()
    artifact_root.mkdir()

    class Factory:
        def __call__(self) -> object:
            raise AssertionError("unused")

    sweeper = RetentionSweeper(
        Factory(),  # type: ignore[arg-type]
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        retention=RetentionDefaults(),
        owner="test",
    )
    missing = worktree_root / "gone"
    assert RetentionSweeper._remove_worktree_tree(missing) is True

    tree = worktree_root / "project" / "run"
    tree.mkdir(parents=True)
    (tree / "file").write_text("x\n")
    assert RetentionSweeper._remove_worktree_tree(tree) is True
    assert not tree.exists()

    link = worktree_root / "link"
    link.symlink_to(tmp_path / "outside-target")
    assert RetentionSweeper._remove_worktree_tree(link) is False

    inside = artifact_root / "aa" / ("a" * 64)
    inside.parent.mkdir()
    inside.write_bytes(b"1")
    assert sweeper._entry_is_under_root(inside) is True
    foreign = tmp_path / "foreign"
    foreign.write_text("nope\n")
    assert sweeper._entry_is_under_root(foreign) is False


@pytest.mark.anyio
async def test_foreign_and_malformed_artifact_files_are_classified(tmp_path: Path) -> None:
    from unittest.mock import AsyncMock

    worktree_root = tmp_path / "worktrees"
    artifact_root = tmp_path / "artifacts"
    worktree_root.mkdir()
    artifact_root.mkdir()
    foreign = tmp_path / "outside.bin"
    foreign.write_bytes(b"x")
    malformed = artifact_root / "zz" / "not-a-hash"
    malformed.parent.mkdir()
    malformed.write_bytes(b"y")

    class Factory:
        def __call__(self) -> object:
            raise AssertionError("unused")

    sweeper = RetentionSweeper(
        Factory(),  # type: ignore[arg-type]
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        retention=RetentionDefaults(),
        owner="test",
    )
    sweeper._known_content_hashes = AsyncMock(return_value=set())  # type: ignore[method-assign]
    sweeper._iter_managed_artifact_files = (  # type: ignore[method-assign]
        lambda: [foreign, malformed]
    )
    actions = await sweeper._reconcile_orphan_artifacts(mode=RetentionSweepMode.DRY_RUN)
    reasons = {action.reason for action in actions}
    assert "path_violation" in reasons
    assert "malformed_identity" in reasons


def test_artifact_path_rejects_bad_uris(tmp_path: Path) -> None:
    worktree_root = tmp_path / "worktrees"
    artifact_root = tmp_path / "artifacts"
    worktree_root.mkdir()
    artifact_root.mkdir()

    class Factory:
        def __call__(self) -> object:
            raise AssertionError("unused")

    sweeper = RetentionSweeper(
        Factory(),  # type: ignore[arg-type]
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        retention=RetentionDefaults(),
        owner="test",
    )
    from vuzol.execution.paths import PathViolation
    from vuzol.storage.models import Artifact
    from vuzol.storage.types import ArtifactStorageState

    bad = Artifact(
        task_id=None,
        run_id=None,
        step_id=None,
        artifact_type="x",
        content_uri="file:/tmp/escape",
        size_bytes=1,
        content_hash="a" * 64,
        media_type="text/plain",
        sensitivity="internal",
        visibility="private",
        retention_until=datetime.now(UTC),
        storage_state=ArtifactStorageState.AVAILABLE,
    )
    with pytest.raises(ValueError, match="unsupported"):
        sweeper._artifact_path(bad)
    bad.content_uri = "artifact:../escape"
    with pytest.raises(PathViolation):
        sweeper._artifact_path(bad)
