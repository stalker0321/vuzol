"""Staging layout, safe root checks, atomic publish, and gc for B2."""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

from vuzol.ops.backup.paths import (
    ProductionRoots,
    assert_safe_restore_paths,
    resolve_isolation_path,
)

STATE_STARTING = "starting"
STATE_DUMPING = "dumping"
STATE_MANIFESTING = "manifesting"
STATE_PUBLISHED = "published"
STATE_FAILED = "failed"
STATE_CANCELLED = "cancelled"


class BackupStagingError(ValueError):
    """Staging path or publish state error."""


def assert_safe_staging_root(staging_root: Path, production: ProductionRoots) -> Path:
    """Containment check: staging must not equal/nest production roots.

    Implemented as thin wrapper around B1 restore-path guards, passing staging
    as the sole candidate and full live production roots.
    """

    resolved = resolve_isolation_path(staging_root)
    assert_safe_restore_paths(
        production=production,
        restore_artifact_root=resolved,
        restore_staging_root=resolved,
    )
    return resolved


def free_space_bytes(path: Path) -> int:
    stats = os.statvfs(path)
    return int(stats.f_bavail * stats.f_frsize)


def ensure_staging_tree(staging_root: Path, run_id: uuid.UUID) -> tuple[Path, Path, Path]:
    """Create runs/{id}/{tmp,publish} with 0700; return (run_dir, tmp, publish)."""

    run_dir = staging_root / "runs" / str(run_id)
    tmp = run_dir / "tmp"
    publish = run_dir / "publish"
    run_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    tmp.mkdir(mode=0o700)
    publish.mkdir(mode=0o700)
    write_state(run_dir, STATE_STARTING)
    return run_dir, tmp, publish


def write_state(run_dir: Path, state: str) -> None:
    path = run_dir / "STATE"
    path.write_text(state + "\n", encoding="utf-8")
    _fsync_path(path)


def read_state(run_dir: Path) -> str | None:
    path = run_dir / "STATE"
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8").strip()


def publish_run(
    *,
    run_dir: Path,
    tmp: Path,
    publish: Path,
    files: dict[str, Path],
) -> None:
    """Move named files from tmp into publish, fsync, then STATE=published."""

    for name, source in files.items():
        if not source.is_file():
            raise BackupStagingError(f"missing publish source: {name}")
        dest = publish / name
        os.replace(source, dest)
        _fsync_path(dest)
    _fsync_dir(publish)
    _fsync_dir(run_dir)
    write_state(run_dir, STATE_PUBLISHED)


def cleanup_run_dir(
    run_dir: Path,
    staging_root: Path,
    *,
    production: ProductionRoots | None = None,
) -> None:
    """Delete run_dir only if contained under staging_root.

    When ``production`` is provided, reassert staging-root isolation against
    production roots (symlink/ancestor conflicts) immediately before delete.
    """

    if production is not None:
        staging_root = assert_safe_staging_root(staging_root, production)
    resolved_run = resolve_isolation_path(run_dir)
    resolved_root = resolve_isolation_path(staging_root)
    try:
        resolved_run.relative_to(resolved_root)
    except ValueError as error:
        raise BackupStagingError("refuse cleanup outside staging_root") from error
    if resolved_run.exists():
        shutil.rmtree(resolved_run)


def prune_published_runs(staging_root: Path, *, keep: int) -> int:
    runs_root = staging_root / "runs"
    if not runs_root.is_dir():
        return 0
    published: list[Path] = []
    for child in runs_root.iterdir():
        if not child.is_dir():
            continue
        if read_state(child) == STATE_PUBLISHED:
            published.append(child)
    published.sort(key=lambda path: path.stat().st_mtime)
    removed = 0
    while len(published) > keep:
        victim = published.pop(0)
        cleanup_run_dir(victim, staging_root)
        removed += 1
    return removed


def _require_under_staging(path: Path, staging: Path) -> Path:
    """Resolve ``path`` and require it stays under resolved ``staging`` (no escape)."""

    resolved = resolve_isolation_path(path)
    try:
        resolved.relative_to(staging)
    except ValueError as error:
        raise BackupStagingError("refuse path outside staging_root") from error
    return resolved


def gc_incomplete_runs(
    staging_root: Path,
    production: ProductionRoots,
    *,
    max_age_seconds: float = 3600.0,
) -> int:
    """Remove incomplete staging runs under an isolation-checked staging root.

    Reasserts configured staging-root isolation against ``production`` immediately
    before directory traversal and again immediately before each deletion.
    Binds ``runs/`` under the asserted staging root before any ``iterdir``; refuses
    symlink/escaping ``runs`` or child entries before ``read_state``/``stat``.
    Incomplete-only/age semantics are unchanged: published runs are never removed;
    candidates match max age or failed/cancelled/missing STATE.
    """

    import time

    # Fail closed before any traversal (resolves symlinks / missing-leaf parents).
    staging = assert_safe_staging_root(staging_root, production)
    runs_path = staging / "runs"
    if not runs_path.exists():
        return 0
    # Bind runs/ under staging before scan (refuse escaping symlink targets).
    runs_root = _require_under_staging(runs_path, staging)
    if not runs_root.is_dir():
        return 0
    now = time.time()
    removed = 0
    for child in runs_root.iterdir():
        # Refuse escaping symlink children before read_state/stat side effects.
        bound_child = _require_under_staging(child, staging)
        if not bound_child.is_dir():
            continue
        # Prefer the bound path for metadata so we do not re-follow a hostile link.
        state = read_state(bound_child)
        if state == STATE_PUBLISHED:
            continue
        age = now - bound_child.stat().st_mtime
        if age >= max_age_seconds or state in {STATE_FAILED, STATE_CANCELLED, None}:
            # Reassert isolation immediately before deletion (TOCTOU / symlink race).
            cleanup_run_dir(bound_child, staging, production=production)
            removed += 1
    return removed


def _fsync_path(path: Path) -> None:
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
