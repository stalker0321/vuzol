"""Fail-closed path and DSN guards for backup restore drills.

Restore targets must never equal or nest production repository/worktree/artifact/
config/deploy roots. Drill DSNs must name an isolated database identity and must
not equal the configured production DSN.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

# Default production-ish roots used when callers pass explicit roots (settings).
DEFAULT_PRODUCTION_CONFIG_ROOT = Path("/etc/vuzol")
DEFAULT_PRODUCTION_DEPLOY_ROOT = Path("/opt/vuzol")


class BackupPathError(ValueError):
    """Restore path or DSN violates isolation rules."""


@dataclass(frozen=True, slots=True)
class ProductionRoots:
    """Absolute production roots that restore targets must not equal or nest under."""

    repository_root: Path
    worktree_root: Path
    artifact_root: Path
    secret_file_root: Path
    config_root: Path = DEFAULT_PRODUCTION_CONFIG_ROOT
    deploy_root: Path = DEFAULT_PRODUCTION_DEPLOY_ROOT

    def all_roots(self) -> tuple[Path, ...]:
        return (
            self.repository_root,
            self.worktree_root,
            self.artifact_root,
            self.secret_file_root,
            self.config_root,
            self.deploy_root,
        )


def _resolve_existing_or_absolute(path: Path) -> Path:
    """Resolve path without requiring existence; never follow a missing leaf."""

    if not path.is_absolute():
        raise BackupPathError(f"restore path must be absolute: {path}")
    # Reject relative components that escape after normalization.
    normalized = Path(path).expanduser()
    if ".." in normalized.parts:
        # resolve() will collapse ..; we still want absolute.
        try:
            return normalized.resolve(strict=False)
        except OSError as error:
            raise BackupPathError(f"restore path is not resolvable: {path}") from error
    try:
        # Prefer strict resolve when the path exists so symlinks are expanded.
        if normalized.exists() or normalized.is_symlink():
            return normalized.resolve(strict=False)
    except OSError as error:
        raise BackupPathError(f"restore path is not resolvable: {path}") from error
    # Non-existent absolute path: resolve parents only.
    return Path(normalized.as_posix())


def _paths_conflict(candidate: Path, root: Path) -> bool:
    """True when candidate equals root or either nests under the other."""

    try:
        cand = candidate.resolve(strict=False) if candidate.exists() else candidate
        base = root.resolve(strict=False) if root.exists() else root
    except OSError:
        cand, base = candidate, root
    if cand == base:
        return True
    try:
        cand.relative_to(base)
        return True
    except ValueError:
        pass
    try:
        base.relative_to(cand)
        return True
    except ValueError:
        return False


def assert_safe_restore_paths(
    *,
    production: ProductionRoots,
    restore_artifact_root: Path,
    restore_config_root: Path | None = None,
    restore_worktree_root: Path | None = None,
    restore_repository_root: Path | None = None,
    restore_deploy_root: Path | None = None,
    restore_staging_root: Path | None = None,
) -> None:
    """Refuse restore targets that equal or nest production data/config/deploy roots.

    Also rejects candidates that are symbolic links into production when the path
    exists. Relative and non-absolute candidates always fail.
    """

    candidates: list[tuple[str, Path]] = [
        ("artifact_root", restore_artifact_root),
    ]
    if restore_config_root is not None:
        candidates.append(("config_root", restore_config_root))
    if restore_worktree_root is not None:
        candidates.append(("worktree_root", restore_worktree_root))
    if restore_repository_root is not None:
        candidates.append(("repository_root", restore_repository_root))
    if restore_deploy_root is not None:
        candidates.append(("deploy_root", restore_deploy_root))
    if restore_staging_root is not None:
        candidates.append(("staging_root", restore_staging_root))

    production_resolved = tuple(
        _resolve_existing_or_absolute(Path(root)) for root in production.all_roots()
    )

    for label, raw in candidates:
        candidate = _resolve_existing_or_absolute(Path(raw))
        # Symlink leaf into production is still a conflict after resolve.
        for prod in production_resolved:
            if _paths_conflict(candidate, prod):
                raise BackupPathError(f"restore {label} conflicts with production root {prod}")


_DSN_PASSWORD_RE = re.compile(r":([^:@/?#]*)@")


def normalize_dsn_identity(dsn: str) -> tuple[str, int | None, str]:
    """Return (host, port, database) for comparison without credentials.

    Accepts ``postgresql://``, ``postgresql+psycopg://``, and ``postgres://``.
    Rejects empty, non-URL, or missing database names.
    """

    cleaned = dsn.strip()
    if not cleaned:
        raise BackupPathError("DSN must not be empty")
    # Strip SQLAlchemy dialect driver suffix for parsing.
    for prefix in (
        "postgresql+psycopg://",
        "postgresql+asyncpg://",
        "postgresql://",
        "postgres://",
    ):
        if cleaned.lower().startswith(prefix):
            # Re-parse with standard postgresql scheme.
            cleaned = "postgresql://" + cleaned[len(prefix) :]
            break
    else:
        raise BackupPathError("DSN must use a postgresql scheme")

    parsed = urlparse(cleaned)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise BackupPathError("DSN must use a postgresql scheme")
    host = (parsed.hostname or "").lower()
    if not host:
        raise BackupPathError("DSN must include a host")
    # Normalize localhost aliases.
    if host in {"localhost", "127.0.0.1", "::1"}:
        host = "127.0.0.1"
    database = unquote((parsed.path or "").lstrip("/"))
    if not database or "/" in database:
        raise BackupPathError("DSN must include a single database name")
    port = parsed.port
    return host, port, database


def assert_isolated_restore_dsn(
    *,
    production_dsn: str,
    restore_dsn: str,
    required_database_suffix: str = "_restore",
    allow_local_hosts_only: bool = True,
) -> None:
    """Refuse restore DSNs that target production or lack an isolated DB identity.

    Rules:
    - restore DSN must not normalize equal to production (host+port+database);
    - restore database name must end with ``required_database_suffix`` (default ``_restore``)
      or contain ``_drill`` as an alternate isolated identity;
    - when ``allow_local_hosts_only`` is true, host must be loopback.
    """

    prod_host, prod_port, prod_db = normalize_dsn_identity(production_dsn)
    rest_host, rest_port, rest_db = normalize_dsn_identity(restore_dsn)

    if (rest_host, rest_port, rest_db) == (prod_host, prod_port, prod_db):
        raise BackupPathError("restore DSN must not equal the production DSN identity")

    suffix = required_database_suffix.strip()
    isolated = rest_db.endswith(suffix) or "_drill" in rest_db
    if not isolated:
        raise BackupPathError(f"restore database name must end with {suffix!r} or include '_drill'")

    if allow_local_hosts_only and rest_host != "127.0.0.1":
        raise BackupPathError("restore DSN host must be loopback for drills")
