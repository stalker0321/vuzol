"""Fail-closed path and DSN guards for backup restore drills.

Restore targets must never equal or nest production repository/worktree/artifact/
config/deploy roots. Drill DSNs must name an isolated database identity and must
not equal the configured production DSN.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

# Default production-ish roots used when callers pass explicit roots (settings).
DEFAULT_PRODUCTION_CONFIG_ROOT = Path("/etc/vuzol")
DEFAULT_PRODUCTION_DEPLOY_ROOT = Path("/opt/vuzol")

# PostgreSQL default TCP port; omitted and explicit 5432 must compare equal.
_DEFAULT_POSTGRES_PORT = 5432

# postgresql / postgres, optional SQLAlchemy +driver suffix (psycopg, asyncpg, psycopg2, …).
_POSTGRES_SCHEME_RE = re.compile(r"^postgres(?:ql)?(?:\+[a-z0-9_]+)?://", re.IGNORECASE)


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


def resolve_isolation_path(path: Path) -> Path:
    """Resolve a path for isolation checks, including missing leaves.

    Always expands user and resolves through real existing parents so a missing
    leaf under a parent symlink cannot escape production containment (F1).
    """

    if not path.is_absolute():
        raise BackupPathError(f"restore path must be absolute: {path}")
    try:
        # strict=False resolves all existing symlink parents, then appends the
        # remaining non-existent suffix — correct for drill paths created later.
        return Path(path).expanduser().resolve(strict=False)
    except OSError as error:
        raise BackupPathError(f"restore path is not resolvable: {path}") from error


def _paths_conflict(candidate: Path, root: Path) -> bool:
    """True when candidate equals root or either nests under the other.

    Both inputs must already be fully resolved isolation paths.
    """

    if candidate == root:
        return True
    try:
        candidate.relative_to(root)
        return True
    except ValueError:
        pass
    try:
        root.relative_to(candidate)
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

    Candidates and production roots are resolved through real parents so missing
    leaves under symlink aliases cannot bypass containment.
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
        resolve_isolation_path(Path(root)) for root in production.all_roots()
    )

    for label, raw in candidates:
        candidate = resolve_isolation_path(Path(raw))
        for prod in production_resolved:
            if _paths_conflict(candidate, prod):
                raise BackupPathError(f"restore {label} conflicts with production root {prod}")


def normalize_dsn_identity(dsn: str) -> tuple[str, int | None, str]:
    """Return (host, port, database) for comparison without credentials.

    Accepts ``postgresql://``, ``postgres://``, and ``postgresql+driver://``
    (including ``psycopg``, ``asyncpg``, ``psycopg2``). Omitted TCP port normalizes
    to ``5432``. Unix-socket DSNs use a ``unix:`` host sentinel and port ``None``.
    """

    cleaned = dsn.strip()
    if not cleaned:
        raise BackupPathError("DSN must not be empty")
    if not _POSTGRES_SCHEME_RE.match(cleaned):
        raise BackupPathError("DSN must use a postgresql scheme")
    # Normalize scheme so urlparse always sees postgresql://…
    cleaned = _POSTGRES_SCHEME_RE.sub("postgresql://", cleaned, count=1)

    parsed = urlparse(cleaned)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise BackupPathError("DSN must use a postgresql scheme")

    query = parse_qs(parsed.query, keep_blank_values=False)
    host = (parsed.hostname or "").lower()
    port = parsed.port
    socket_path: str | None = None

    # libpq-style unix socket: postgresql:///dbname?host=/var/run/postgresql
    if not host:
        host_params = query.get("host") or query.get("hostaddr")
        if host_params:
            candidate = unquote(host_params[0])
            if candidate.startswith("/"):
                socket_path = candidate
            else:
                host = candidate.lower()
        if not host and not socket_path:
            raise BackupPathError("DSN must include a host")

    if socket_path is not None:
        host = f"unix:{socket_path}"
        port = None
    else:
        # Normalize localhost aliases (including IPv6 loopback).
        if host in {"localhost", "127.0.0.1", "::1"}:
            host = "127.0.0.1"
        # Default TCP port equivalence (F2).
        if port is None:
            port = _DEFAULT_POSTGRES_PORT

    database = unquote((parsed.path or "").lstrip("/"))
    if not database or "/" in database:
        raise BackupPathError("DSN must include a single database name")
    return host, port, database


def database_name_is_isolated(database: str, required_suffix: str) -> bool:
    """Return True when the database name uses an explicit isolated-identity rule.

    Accepted forms (F4):
    - ends with the configured ``required_suffix`` (default ``_restore``);
    - underscore-delimited segment ``drill`` (e.g. ``vuzol_drill``, ``vuzol_drill_20260718``).

    Rejects soft substring matches such as ``vuzol_drillbit`` or ``notadrill``.
    """

    suffix = required_suffix.strip()
    if not suffix:
        raise BackupPathError("required database suffix must not be empty")
    if database.endswith(suffix):
        return True
    # Full segment only — not a substring of another token.
    return "drill" in database.split("_")


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
    - restore database name must satisfy :func:`database_name_is_isolated`;
    - when ``allow_local_hosts_only`` is true, host must be loopback or a unix socket.
    """

    prod_host, prod_port, prod_db = normalize_dsn_identity(production_dsn)
    rest_host, rest_port, rest_db = normalize_dsn_identity(restore_dsn)

    if (rest_host, rest_port, rest_db) == (prod_host, prod_port, prod_db):
        raise BackupPathError("restore DSN must not equal the production DSN identity")

    if not database_name_is_isolated(rest_db, required_database_suffix):
        raise BackupPathError(
            f"restore database name must end with {required_database_suffix.strip()!r} "
            "or include an underscore-delimited 'drill' segment"
        )

    if allow_local_hosts_only:
        is_loopback = rest_host == "127.0.0.1"
        is_unix = rest_host.startswith("unix:")
        if not (is_loopback or is_unix):
            raise BackupPathError("restore DSN host must be loopback for drills")
