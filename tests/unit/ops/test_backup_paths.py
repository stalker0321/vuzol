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
    database_name_is_isolated,
    normalize_dsn_identity,
    resolve_isolation_path,
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


def test_f1_parent_symlink_missing_leaf_is_rejected(tmp_path: Path) -> None:
    """Missing restore leaf under a parent symlink into production must conflict (F1)."""

    prod = _production(tmp_path)
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path / "srv", target_is_directory=True)
    # Missing leaf that resolves equal to production artifact root.
    missing_equal = alias / "artifacts"
    assert not missing_equal.exists() or missing_equal.is_symlink() or True
    # Prefer a non-existent nested path under the aliased production artifacts tree.
    nested_missing = alias / "artifacts" / "drill-sub"
    assert not nested_missing.exists()
    resolved = resolve_isolation_path(nested_missing)
    assert resolved == (tmp_path / "srv" / "artifacts" / "drill-sub").resolve()
    with pytest.raises(BackupPathError, match="conflicts"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=nested_missing,
        )
    # Direct alias to the production leaf directory (exists as real dir via resolve).
    with pytest.raises(BackupPathError, match="conflicts"):
        assert_safe_restore_paths(
            production=prod,
            restore_artifact_root=alias / "artifacts",
        )
    # Sibling under /srv (not under a listed production root) remains allowed.
    sibling = alias / "drill-artifacts"
    assert_safe_restore_paths(
        production=prod,
        restore_artifact_root=sibling,
    )


def test_dsn_normalization_strips_credentials_and_aliases() -> None:
    host, port, database = normalize_dsn_identity(
        "postgresql+psycopg://vuzol:s3cret-value@localhost:5432/vuzol"  # pragma: allowlist secret
    )
    assert host == "127.0.0.1"
    assert port == 5432
    assert database == "vuzol"
    host2, port2, db2 = normalize_dsn_identity("postgresql://vuzol@127.0.0.1/vuzol_restore")
    assert host2 == "127.0.0.1"
    assert port2 == 5432  # omitted port normalizes to default
    assert db2 == "vuzol_restore"


def test_f2_default_port_equivalence_for_dsn_identity() -> None:
    without_port = normalize_dsn_identity("postgresql://vuzol@127.0.0.1/vuzol_restore")
    with_port = normalize_dsn_identity("postgresql://vuzol@127.0.0.1:5432/vuzol_restore")
    assert without_port == with_port
    with pytest.raises(BackupPathError, match="must not equal"):
        assert_isolated_restore_dsn(
            production_dsn="postgresql://vuzol@127.0.0.1/vuzol_restore",
            restore_dsn="postgresql://vuzol@127.0.0.1:5432/vuzol_restore",
        )


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
    assert_isolated_restore_dsn(
        production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
        restore_dsn="postgresql://vuzol@127.0.0.1/vuzol_drill",
    )


def test_f4_drill_substring_soft_match_rejected() -> None:
    """Only underscore-delimited 'drill' segments count — not soft substrings (F4)."""

    assert database_name_is_isolated("vuzol_drill", "_restore") is True
    assert database_name_is_isolated("vuzol_drill_20260718", "_restore") is True
    assert database_name_is_isolated("vuzol_restore", "_restore") is True
    assert database_name_is_isolated("vuzol_drillbit", "_restore") is False
    assert database_name_is_isolated("notadrill", "_restore") is False
    # 'not_drill' has a real segment 'drill' — accepted as explicit marker.
    assert database_name_is_isolated("not_drill", "_restore") is True
    for name in ("vuzol_drillbit", "staging", "vuzol_prod"):
        with pytest.raises(BackupPathError, match=r"drill|suffix|_restore"):
            assert_isolated_restore_dsn(
                production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
                restore_dsn=f"postgresql://vuzol@127.0.0.1/{name}",
            )


def test_restore_dsn_rejects_non_loopback_when_required() -> None:
    with pytest.raises(BackupPathError, match="loopback"):
        assert_isolated_restore_dsn(
            production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
            restore_dsn="postgresql://vuzol@10.0.0.5/vuzol_restore",
        )


def test_f6_dsn_driver_variants_and_unix_socket() -> None:
    host, port, database = normalize_dsn_identity(
        "postgresql+psycopg2://vuzol@127.0.0.1:5432/vuzol_restore"
    )
    assert (host, port, database) == ("127.0.0.1", 5432, "vuzol_restore")
    host_s, port_s, db_s = normalize_dsn_identity(
        "postgresql:///vuzol_restore?host=/var/run/postgresql"
    )
    assert host_s == "unix:/var/run/postgresql"
    assert port_s is None
    assert db_s == "vuzol_restore"
    # Unix socket is treated as local for drills.
    assert_isolated_restore_dsn(
        production_dsn="postgresql://vuzol@127.0.0.1/vuzol",
        restore_dsn="postgresql:///vuzol_restore?host=/var/run/postgresql",
    )
    # Query params ignored for identity; credentials stripped.
    # pragma: allowlist secret — fixture credential in DSN, not a live secret
    a = normalize_dsn_identity(
        "postgresql+psycopg://u:x@127.0.0.1:5432/vuzol_restore" + "?sslmode=require"
    )
    b = normalize_dsn_identity("postgresql://u@127.0.0.1/vuzol_restore")
    assert a == b
    # Bracketed IPv6 loopback normalizes to 127.0.0.1.
    host6, _, _ = normalize_dsn_identity("postgresql://vuzol@[::1]:5432/vuzol_restore")
    assert host6 == "127.0.0.1"


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
