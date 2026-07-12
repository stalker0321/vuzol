"""Canonical, no-symlink path derivation for execution resources."""

import os
import uuid
from pathlib import Path


class PathViolation(ValueError):
    """A path is outside its configured execution boundary."""


def trusted_root(path: Path, *, create: bool = False) -> Path:
    if not path.is_absolute():
        raise PathViolation("trusted root must be absolute")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    path.lstat()
    if not path.is_dir() or os.path.islink(path):
        raise PathViolation("trusted root must be a real directory")
    return path.resolve(strict=True)


def contained(root: Path, candidate: Path, *, must_exist: bool = True) -> Path:
    normalized_root = trusted_root(root)
    resolved = candidate.resolve(strict=must_exist)
    try:
        resolved.relative_to(normalized_root)
    except ValueError as error:
        raise PathViolation("path escapes configured root") from error
    current = resolved
    while current != normalized_root:
        if current.exists() and current.is_symlink():
            raise PathViolation("symlink is prohibited in execution path")
        current = current.parent
    return resolved


def worktree_path(root: Path, project_id: str, run_id: uuid.UUID) -> Path:
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_-"
    if not project_id or any(character not in allowed for character in project_id):
        raise PathViolation("invalid project ID for worktree path")
    return root / project_id / str(run_id)


def worktree_branch(task_id: uuid.UUID, run_id: uuid.UUID) -> str:
    return f"vuzol/task-{task_id}-run-{str(run_id)[:12]}"
