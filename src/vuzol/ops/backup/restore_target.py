"""B3.1 pure restore target-isolation preflight (no SQL, secrets, or process).

Composes B1 DSN and path guards: restore identity must be distinct from
production, local-only, database-name isolated, and drill_root must not
equal or nest any production root after isolation resolution.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from vuzol.ops.backup.paths import (
    BackupPathError,
    ProductionRoots,
    assert_isolated_restore_dsn,
    assert_safe_restore_paths,
    database_name_is_isolated,
    normalize_dsn_identity,
)

# Stable operational codes — never embed absolute paths, users, passwords, or raw DSNs.
CODE_OK = "target_ok"
CODE_DSN = "preflight_dsn"
CODE_IDENTITY = "preflight_target_identity"
CODE_DATABASE = "preflight_target_database"
CODE_HOST = "preflight_target_host"
CODE_PATH_CONFLICT = "preflight_path_conflict"
CODE_PATH_IO = "preflight_path_io"


@dataclass(frozen=True, slots=True)
class TargetPreflightReport:
    """Safe operational report for restore target isolation.

    On success may include normalized host/port/database only. Never carries
    user, password, query, raw DSN, or absolute filesystem paths.
    """

    ok: bool
    code: str
    message: str
    host: str | None = None
    port: int | None = None
    database: str | None = None

    def to_operational_payload(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schedule": "disabled",
        }


def preflight_restore_target(
    *,
    production_dsn: str,
    restore_dsn: str,
    production: ProductionRoots,
    drill_root: Path,
    required_database_suffix: str = "_restore",
    allow_local_hosts_only: bool = True,
) -> TargetPreflightReport:
    """Fail-closed target isolation preflight (B3.1).

    Pure composition of B1 guards: no secret resolution, engine/SQL, emptiness
    probe, CREATE DATABASE, locks, settings mutation, CLI, decrypt, or restore
    process. Path resolution uses existing isolation helpers only.
    """

    # bool is a subclass of int — require an actual bool so 0/1 cannot bypass local-only.
    if not isinstance(allow_local_hosts_only, bool):
        return _fail(CODE_HOST, "restore host is not local-only")

    if not isinstance(required_database_suffix, str):
        return _fail(CODE_DATABASE, "required database suffix is empty")

    try:
        prod_host, prod_port, prod_db = normalize_dsn_identity(production_dsn)
    except BackupPathError:
        return _fail(CODE_DSN, "production DSN is invalid")
    except (ValueError, TypeError, AttributeError):
        # urlparse / non-string inputs: redacted CODE_DSN (no token/path leak).
        return _fail(CODE_DSN, "production DSN is invalid")

    try:
        rest_host, rest_port, rest_db = normalize_dsn_identity(restore_dsn)
    except BackupPathError:
        return _fail(CODE_DSN, "restore DSN is invalid")
    except (ValueError, TypeError, AttributeError):
        return _fail(CODE_DSN, "restore DSN is invalid")

    # Structural classify mirrors B1 assert order (identity before empty suffix).
    predicted = _classify_dsn_isolation(
        prod_host=prod_host,
        prod_port=prod_port,
        prod_db=prod_db,
        rest_host=rest_host,
        rest_port=rest_port,
        rest_db=rest_db,
        required_database_suffix=required_database_suffix,
        allow_local_hosts_only=allow_local_hosts_only,
    )

    # B1 assert remains final authority; on failure use structural class, else fallback.
    # Only invoked after bool/str wrapper validation so falsey non-bool cannot bypass.
    try:
        assert_isolated_restore_dsn(
            production_dsn=production_dsn,
            restore_dsn=restore_dsn,
            required_database_suffix=required_database_suffix,
            allow_local_hosts_only=allow_local_hosts_only,
        )
    except BackupPathError:
        if predicted is not None:
            return predicted
        return _fail(CODE_DSN, "restore DSN failed isolation checks")
    except (ValueError, TypeError, AttributeError):
        return _fail(CODE_DSN, "restore DSN failed isolation checks")

    try:
        # Same containment composition as safe staging: candidate as artifact + staging.
        assert_safe_restore_paths(
            production=production,
            restore_artifact_root=drill_root,
            restore_staging_root=drill_root,
        )
    except BackupPathError:
        return _fail(CODE_PATH_CONFLICT, "drill root conflicts with production roots")
    except (OSError, RuntimeError):
        # Resolution I/O / symlink-loop RuntimeError distinct from containment conflict.
        return _fail(CODE_PATH_IO, "drill root is not resolvable")
    except (TypeError, AttributeError):
        # Invalid production/drill_root runtime types (None, bad roots) — fixed PATH_IO.
        return _fail(CODE_PATH_IO, "drill root is not resolvable")

    return TargetPreflightReport(
        ok=True,
        code=CODE_OK,
        message="restore target isolation ok",
        host=_safe_host_for_report(rest_host),
        port=rest_port,
        database=rest_db,
    )


def _classify_dsn_isolation(
    *,
    prod_host: str,
    prod_port: int | None,
    prod_db: str,
    rest_host: str,
    rest_port: int | None,
    rest_db: str,
    required_database_suffix: str,
    allow_local_hosts_only: bool,
) -> TargetPreflightReport | None:
    """Return a failure report if B1 isolation rules fail, else None.

    Matches ``assert_isolated_restore_dsn`` authority order: identity first,
    then database name (including empty-suffix refuse), then host. Does not use
    exception message text. Caller must pass an actual ``bool`` and ``str``.
    """

    if (rest_host, rest_port, rest_db) == (prod_host, prod_port, prod_db):
        return _fail(CODE_IDENTITY, "restore identity matches production")

    suffix = required_database_suffix.strip()
    if not suffix:
        return _fail(CODE_DATABASE, "required database suffix is empty")

    # database_name_is_isolated raises BackupPathError on empty suffix (already handled).
    if not database_name_is_isolated(rest_db, required_database_suffix):
        return _fail(CODE_DATABASE, "restore database name is not isolated")

    if allow_local_hosts_only:
        is_loopback = rest_host == "127.0.0.1"
        is_unix = rest_host.startswith("unix:")
        if not (is_loopback or is_unix):
            return _fail(CODE_HOST, "restore host is not local-only")

    return None


def _safe_host_for_report(host: str) -> str:
    """Report host without embedding unix socket filesystem paths."""

    if host.startswith("unix:"):
        return "unix"
    return host


def _fail(code: str, message: str) -> TargetPreflightReport:
    return TargetPreflightReport(ok=False, code=code, message=message)
