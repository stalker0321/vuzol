"""Fail-closed restore path and DSN isolation guards."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.config.settings import BackupSettings, Settings
from vuzol.ops.backup.paths import (
    BackupPathError,
    ProductionRoots,
    assert_isolated_restore_dsn,
    assert_safe_restore_paths,
    normalize_dsn_identity,
)


def _production(tmp_path: Path) -> ProductionRoots:
    roots = {
        "repository_root": tmp_path / "srv" / "repositories",
        "worktree_root": tmp_path / "srv" / "worktrees",
        "artifact_root": tmp_path / "srv" / "artifacts",
        "secret_file_root": tmp_path / "run" / "secrets",
        "config_root": tmp_path / "etc" / "vuzol",
        "deploy_root": tmp_path / "opt" / "vuzol",
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)
    return ProductionRoots(**roots)


def test_restore_artifact_root_must_not_equal_production(tmp_path: Path) -> None:
    prod = _production(tmp_path)
    with pytest.raises(BackupPathError, match="conflicts"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=prod.artifact_root,
        )


def test_restore_nested_under_production_is_rejected(tmp_path: Path) -> None:
    prod = _production(tmp_path)
    nested = prod.worktree_root / "drill"
    nested.mkdir()
    with pytest.raises(BackupPathError, match="conflicts"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=tmp_path / "safe-artifacts",
            restore_worktree_root=nested,
        )


def test_restore_parent_of_production_is_rejected(tmp_path: Path) -> None:
    prod = _production(tmp_path)
    parent = prod.artifact_root.parent
    with pytest.raises(BackupPathError, match="conflicts"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=parent,
        )


def test_safe_isolated_restore_paths_accepted(tmp_path: Path) -> None:
    prod = _production(tmp_path)
    drill = tmp_path / "var" / "tmp" / "vuzol-restore-drill" / "run1"
    artifacts = drill / "artifacts"
    config = drill / "config"
    artifacts.mkdir(parents=True)
    config.mkdir(parents=True)
    assert_safe_restore_paths(
        production=prod,
        restore_artifact_root=artifacts,
        restore_config_root=config,
        restore_staging_root=drill,
    )


def test_relative_restore_path_rejected(tmp_path: Path) -> None:
    prod = _production(tmp_path)
    with pytest.raises(BackupPathError, match="absolute"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=Path("relative/artifacts"),
        )


def test_symlink_into_production_is_rejected(tmp_path: Path) -> None:
    prod = _production(tmp_path)
    link = tmp_path / "alias-artifacts"
    link.symlink_to(prod.artifact_root, target_is_directory=True)
    with pytest.raises(BackupPathError, match="conflicts"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=link,
        )


def test_dsn_normalization_strips_credentials_and_aliases() -> None:
    host, port, database = normalize_dsn_identity(
        "postgresql+psycopg://vuzol:s3cret-value@localhost:5432/vuzol"  # pragma: allowlist secret
    )
    assert host == "127.0.0.1"
    assert port == 5432
    assert database == "vuzol"
    host2, _, db2 = normalize_dsn_identity("postgresql://vuzol@127.0.0.1/vuzol_restore")
    assert host2 == "127.0.0.1"
    assert db2 == "vuzol_restore"


def test_restore_dsn_must_not_equal_production() -> None:
    prod = "postgresql://vuzol@127.0.0.1:5432/vuzol"
    # Same host/db identity via localhost alias; password ignored for equality.
    restore = "postgresql+psycopg://vuzol:other@localhost:5432/vuzol"  # pragma: allowlist secret
    with pytest.raises(BackupPathError, match="must not equal"):
        assert_isolated_restore_dsn(production_dsn=prod, restore_dsn=restore)


def test_restore_dsn_requires_isolated_database_name() -> None:
    with pytest.raises(BackupPathError, match="must end with"):
        assert_isolated_restore_dsn(
            production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
            restore_dsn="postgresql://vuzol@127.0.0.1/vuzol_staging",
        )


def test_restore_dsn_accepts_suffix_and_drill_identity() -> None:
    assert_isolated_restore_dsn(
        production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
        restore_dsn="postgresql://vuzol@127.0.0.1/vuzol_restore",
    )
    assert_isolated_restore_dsn(
        production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
        restore_dsn="postgresql://vuzol@127.0.0.1/vuzol_drill_20260718",
    )


def test_restore_dsn_rejects_non_loopback_when_required() -> None:
    with pytest.raises(BackupPathError, match="loopback"):
        assert_isolated_restore_dsn(
            production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
            restore_dsn="postgresql://vuzol@10.0.0.5/vuzol_restore",
        )


def test_backup_settings_default_disabled(tmp_path: Path) -> None:
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "repositories",
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    assert settings.backup.enabled is False
    assert settings.backup.rpo_seconds_target == 86_400
    with pytest.raises(ValidationError, match="cannot be true"):
        BackupSettings(enabled=True)


def test_backup_settings_require_absolute_roots() -> None:
    with pytest.raises(ValidationError, match="absolute"):
        BackupSettings(staging_root=Path("relative/staging"))


def test_normalize_dsn_rejects_empty_and_non_postgres() -> None:
    with pytest.raises(BackupPathError, match="empty"):
        normalize_dsn_identity("   ")
    with pytest.raises(BackupPathError, match="postgresql scheme"):
        normalize_dsn_identity("mysql://localhost/db")
    with pytest.raises(BackupPathError, match="host"):
        normalize_dsn_identity("postgresql:///onlydb")
    with pytest.raises(BackupPathError, match="database"):
        normalize_dsn_identity("postgresql://127.0.0.1/")


def test_assert_safe_restore_optional_roots(tmp_path: Path) -> None:
    prod = _production(tmp_path)
    safe = tmp_path / "drill"
    safe.mkdir()
    with pytest.raises(BackupPathError, match="config_root"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=safe / "artifacts",
            restore_config_root=prod.config_root,
        )
    (safe / "artifacts").mkdir()
    with pytest.raises(BackupPathError, match="deploy_root"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=safe / "artifacts",
            restore_deploy_root=prod.deploy_root,
        )
    with pytest.raises(BackupPathError, match="repository_root"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=safe / "artifacts",
            restore_repository_root=prod.repository_root,
        )


def test_backup_settings_suffix_whitespace_rejected() -> None:
    with pytest.raises(ValidationError):
        BackupSettings(drill_database_name_suffix="  ")
    with pytest.raises(ValidationError):
        BackupSettings(drill_database_name_suffix="bad suffix")
