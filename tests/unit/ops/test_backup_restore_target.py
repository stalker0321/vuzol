"""Unit tests for B3.1 pure restore target-isolation preflight."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from vuzol.ops.backup.paths import ProductionRoots
from vuzol.ops.backup.restore_target import (
    CODE_DATABASE,
    CODE_DSN,
    CODE_HOST,
    CODE_IDENTITY,
    CODE_OK,
    CODE_PATH_CONFLICT,
    CODE_PATH_IO,
    preflight_restore_target,
)

_PROD_DSN = "postgresql://prod_user:s3cret@127.0.0.1:5432/vuzol"
_RESTORE_DSN = "postgresql://restore_user:other@127.0.0.1:5432/vuzol_restore"


def _production(tmp_path: Path) -> ProductionRoots:
    roots = ProductionRoots(
        repository_root=tmp_path / "repos",
        worktree_root=tmp_path / "wt",
        artifact_root=tmp_path / "art",
        secret_file_root=tmp_path / "secrets",
        config_root=tmp_path / "etc" / "vuzol",
        deploy_root=tmp_path / "opt" / "vuzol",
    )
    for root in roots.all_roots():
        root.mkdir(parents=True, exist_ok=True)
    return roots


def _safe_drill(tmp_path: Path) -> Path:
    drill = tmp_path / "drill"
    drill.mkdir()
    return drill


def test_target_ok_loopback_suffix(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=_RESTORE_DSN,
        production=production,
        drill_root=drill,
    )
    assert report.ok is True
    assert report.code == CODE_OK
    assert report.host == "127.0.0.1"
    assert report.port == 5432
    assert report.database == "vuzol_restore"
    payload = report.to_operational_payload()
    assert payload["ok"] is True
    assert payload["schedule"] == "disabled"
    # Never leak credentials or raw DSN.
    assert "s3cret" not in str(payload)
    assert "other" not in str(payload)
    assert "prod_user" not in str(payload)
    assert "restore_user" not in str(payload)
    assert "postgresql://" not in str(payload)
    assert str(drill) not in str(payload)


def test_target_ok_drill_segment_and_localhost_alias(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://u:p@localhost/vuzol_drill_20260719",
        production=production,
        drill_root=drill,
    )
    assert report.ok is True
    assert report.host == "127.0.0.1"
    assert report.port == 5432
    assert report.database == "vuzol_drill_20260719"


def test_target_ok_unix_socket_redacts_socket_path(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    socket = "/var/run/postgresql"
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=f"postgresql:///vuzol_restore?host={socket}",
        production=production,
        drill_root=drill,
    )
    assert report.ok is True
    assert report.host == "unix"
    assert report.port is None
    assert report.database == "vuzol_restore"
    assert socket not in str(report.to_operational_payload())
    assert socket not in report.message


def test_refuse_identity_equals_production(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    # Same host/port/db; credentials differ and must not affect identity compare.
    # Identity is classified before database-name isolation (B1 order).
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://other:x@127.0.0.1:5432/vuzol",
        production=production,
        drill_root=drill,
        required_database_suffix="_restore",
    )
    assert report.ok is False
    assert report.code == CODE_IDENTITY
    assert report.host is None
    assert "postgresql://" not in report.message
    assert "s3cret" not in report.message


def test_refuse_equal_identity_with_isolated_name_collision(tmp_path: Path) -> None:
    """Production already on an isolated-looking name still blocks equal restore identity."""
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    prod = "postgresql://u:p@127.0.0.1:5432/vuzol_restore"
    report = preflight_restore_target(
        production_dsn=prod,
        restore_dsn="postgresql://u2:p2@127.0.0.1:5432/vuzol_restore",
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_IDENTITY
    assert report.database is None


def test_refuse_non_isolated_database_name(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://u:p@127.0.0.1:5432/vuzol_app",
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_DATABASE
    assert report.host is None
    assert str(tmp_path) not in report.message


def test_refuse_remote_host(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://u:p@db.example.com:5432/vuzol_restore",
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_HOST
    assert "example.com" not in report.message
    assert report.host is None


def test_refuse_invalid_restore_dsn(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="not-a-dsn",
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_DSN


def test_refuse_malformed_restore_port_redacted(tmp_path: Path) -> None:
    """ValueError from urlparse invalid port → CODE_DSN without token leak."""

    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    bad = "postgresql://u:p@127.0.0.1:notaport/vuzol_restore"
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=bad,
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_DSN
    assert report.host is None
    assert "notaport" not in report.message
    assert "postgresql://" not in report.message
    assert "s3cret" not in report.message
    payload = report.to_operational_payload()
    assert "notaport" not in str(payload)


def test_refuse_malformed_production_port_redacted(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    bad_prod = "postgresql://u:s3cret@127.0.0.1:99999999/vuzol"
    report = preflight_restore_target(
        production_dsn=bad_prod,
        restore_dsn=_RESTORE_DSN,
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_DSN
    assert "99999999" not in report.message
    assert "s3cret" not in report.message
    assert "postgresql://" not in report.message


def test_refuse_unmatched_bracket_dsn_redacted(tmp_path: Path) -> None:
    """Unmatched bracket / IPv6-ish DSN must not leak raw fragment."""

    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    bad = "postgresql://u:p@[::1/vuzol_restore"
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=bad,
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_DSN
    assert "[::1" not in report.message
    assert "::1" not in report.message
    assert "postgresql://" not in report.message
    payload = report.to_operational_payload()
    assert "[::1" not in str(payload)


def test_refuse_drill_nested_under_production_artifact(tmp_path: Path) -> None:
    production = _production(tmp_path)
    evil = production.artifact_root / "nested-drill"
    evil.mkdir()
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=_RESTORE_DSN,
        production=production,
        drill_root=evil,
    )
    assert report.ok is False
    assert report.code == CODE_PATH_CONFLICT
    assert str(evil) not in report.message
    assert str(production.artifact_root) not in report.message
    assert report.host is None


def test_refuse_drill_symlink_into_production(tmp_path: Path) -> None:
    production = _production(tmp_path)
    link = tmp_path / "drill-link"
    link.symlink_to(production.artifact_root, target_is_directory=True)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=_RESTORE_DSN,
        production=production,
        drill_root=link,
    )
    assert report.ok is False
    assert report.code == CODE_PATH_CONFLICT
    assert str(link) not in report.message
    assert str(production.artifact_root) not in report.message


def test_refuse_relative_drill_root(tmp_path: Path) -> None:
    production = _production(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=_RESTORE_DSN,
        production=production,
        drill_root=Path("relative-drill"),
    )
    assert report.ok is False
    assert report.code == CODE_PATH_CONFLICT
    assert "relative-drill" not in report.message


def test_refuse_soft_drill_substring(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://u:p@127.0.0.1:5432/vuzol_drillbit",
        production=production,
        drill_root=drill,
    )
    assert report.ok is False
    assert report.code == CODE_DATABASE


def test_refuse_empty_required_suffix(tmp_path: Path) -> None:
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=_RESTORE_DSN,
        production=production,
        drill_root=drill,
        required_database_suffix="   ",
    )
    assert report.ok is False
    assert report.code == CODE_DATABASE
    assert report.host is None
    assert "s3cret" not in report.message
    assert str(drill) not in report.message


def test_equal_identity_plus_empty_suffix_is_identity(tmp_path: Path) -> None:
    """Stacked faults: identity wins (B1 authority order), not empty-suffix first."""

    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://other:x@127.0.0.1:5432/vuzol",
        production=production,
        drill_root=drill,
        required_database_suffix="",
    )
    assert report.ok is False
    assert report.code == CODE_IDENTITY
    assert report.host is None
    assert "s3cret" not in report.message


def test_allow_non_local_host_when_toggle_disabled(tmp_path: Path) -> None:
    """allow_local_hosts_only=False permits remote host if other isolation holds."""
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://u:p@db.example.com:5432/vuzol_restore",
        production=production,
        drill_root=drill,
        allow_local_hosts_only=False,
    )
    assert report.ok is True
    assert report.code == CODE_OK
    assert report.host == "db.example.com"
    assert report.port == 5432
    assert report.database == "vuzol_restore"
    payload = report.to_operational_payload()
    assert "postgresql://" not in str(payload)
    assert "u:p" not in str(payload)


def test_refuse_remote_host_when_local_only_default(tmp_path: Path) -> None:
    """Explicit toggle True matches default remote refusal."""
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)
    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn="postgresql://u:p@db.example.com:5432/vuzol_restore",
        production=production,
        drill_root=drill,
        allow_local_hosts_only=True,
    )
    assert report.ok is False
    assert report.code == CODE_HOST
    assert "example.com" not in report.message
    assert report.host is None


def test_path_resolution_oserror_maps_to_path_io(tmp_path: Path) -> None:
    """OSError during path resolve → preflight_path_io, no path leak."""
    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)

    def _raise_oserror(**_kwargs: object) -> None:
        raise OSError(5, "input/output error")

    with patch(
        "vuzol.ops.backup.restore_target.assert_safe_restore_paths",
        side_effect=_raise_oserror,
    ):
        report = preflight_restore_target(
            production_dsn=_PROD_DSN,
            restore_dsn=_RESTORE_DSN,
            production=production,
            drill_root=drill,
        )
    assert report.ok is False
    assert report.code == CODE_PATH_IO
    assert report.host is None
    assert "input/output error" not in report.message
    assert str(drill) not in report.message
    assert str(tmp_path) not in report.message
    payload = report.to_operational_payload()
    assert payload["code"] == CODE_PATH_IO
    assert "input/output error" not in str(payload)


def test_path_resolution_runtimeerror_symlink_loop_maps_to_path_io(tmp_path: Path) -> None:
    """Symlink loop / RuntimeError during resolve → CODE_PATH_IO, no absolute path leak."""

    production = _production(tmp_path)
    a = tmp_path / "loop-a"
    b = tmp_path / "loop-b"
    a.symlink_to(b)
    b.symlink_to(a)

    report = preflight_restore_target(
        production_dsn=_PROD_DSN,
        restore_dsn=_RESTORE_DSN,
        production=production,
        drill_root=a,
    )
    assert report.ok is False
    # Either PATH_IO (RuntimeError/OSError) or PATH_CONFLICT if resolve maps differently;
    # report contract: no absolute paths, fixed code family.
    assert report.code in {CODE_PATH_IO, CODE_PATH_CONFLICT}
    assert report.host is None
    assert str(a) not in report.message
    assert str(b) not in report.message
    assert str(tmp_path) not in report.message
    payload = report.to_operational_payload()
    assert str(a) not in str(payload)
    assert str(tmp_path) not in str(payload)

    # Explicit RuntimeError path (platform-independent).
    def _raise_runtime(**_kwargs: object) -> None:
        raise RuntimeError(f"Symlink loop from {tmp_path / 'evil'}")

    with patch(
        "vuzol.ops.backup.restore_target.assert_safe_restore_paths",
        side_effect=_raise_runtime,
    ):
        report2 = preflight_restore_target(
            production_dsn=_PROD_DSN,
            restore_dsn=_RESTORE_DSN,
            production=production,
            drill_root=_safe_drill(tmp_path),
        )
    assert report2.ok is False
    assert report2.code == CODE_PATH_IO
    assert "Symlink" not in report2.message
    assert str(tmp_path) not in report2.message
    assert "evil" not in report2.message


def test_non_string_dsn_inputs_return_redacted_dsn_code(tmp_path: Path) -> None:
    """Runtime type escapes must not raise out of the report contract."""

    production = _production(tmp_path)
    drill = _safe_drill(tmp_path)

    for bad in (None, 123, b"postgresql://x", ["postgresql://x"]):
        report = preflight_restore_target(
            production_dsn=bad,  # type: ignore[arg-type]
            restore_dsn=_RESTORE_DSN,
            production=production,
            drill_root=drill,
        )
        assert report.ok is False
        assert report.code == CODE_DSN
        assert report.host is None
        assert "postgresql://" not in report.message

        report2 = preflight_restore_target(
            production_dsn=_PROD_DSN,
            restore_dsn=bad,  # type: ignore[arg-type]
            production=production,
            drill_root=drill,
        )
        assert report2.ok is False
        assert report2.code == CODE_DSN
        assert "s3cret" not in report2.message
