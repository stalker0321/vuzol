"""Bounded, symlink-free commit exports shared by CLI and API planners."""

from __future__ import annotations

import io
import shutil
import stat
import uuid
import zipfile
from pathlib import Path, PurePosixPath

from vuzol.execution.git import GitError, LocalGit

MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
MAX_SNAPSHOT_FILES = 20000


def snapshot_path(invocation_id: uuid.UUID) -> Path:
    # Stable across executor restarts; never derive a host path from model output.
    return Path("/tmp") / f"vuzol-planner-{invocation_id}"  # noqa: S108 - exclusive mkdir


def safe_repo_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or "\\" in value
        or "\x00" in value
        or any(part in {"..", ".git"} for part in path.parts)
        or path == PurePosixPath(".")
    ):
        raise ValueError("unsafe repository path")
    return path


async def commit_files(repository: Path, commit: str) -> dict[str, bytes]:
    archive = await LocalGit().source_archive(repository, commit, max_bytes=MAX_SNAPSHOT_BYTES)
    files: dict[str, bytes] = {}
    total = 0
    with zipfile.ZipFile(io.BytesIO(archive)) as source:
        if len(source.infolist()) > MAX_SNAPSHOT_FILES:
            raise GitError("planner snapshot file limit exceeded")
        for item in source.infolist():
            safe_repo_path(item.filename)
            mode = item.external_attr >> 16
            if stat.S_ISLNK(mode) or (stat.S_IFMT(mode) not in {0, stat.S_IFREG, stat.S_IFDIR}):
                raise GitError("planner snapshot requires regular files")
            if item.is_dir():
                continue
            total += item.file_size
            if total > MAX_SNAPSHOT_BYTES:
                raise GitError("planner snapshot byte limit exceeded")
            files[item.filename] = source.read(item)
    return files


async def build_snapshot(repository: Path, commit: str, invocation_id: uuid.UUID) -> Path:
    files = await commit_files(repository, commit)
    root = snapshot_path(invocation_id)
    root.mkdir(mode=0o700)  # Never reuse or follow an existing directory/symlink.
    try:
        for name, content in files.items():
            target = root / safe_repo_path(name)
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as stream:
                stream.write(content)
            target.chmod(0o444)
        for path in sorted(root.rglob("*"), reverse=True):
            if path.is_dir():
                path.chmod(0o555)
        root.chmod(0o555)
        return root
    except BaseException:
        cleanup_snapshot(invocation_id)
        raise


def cleanup_snapshot(invocation_id: uuid.UUID) -> None:
    root = snapshot_path(invocation_id)
    if root.is_symlink():
        raise ValueError("planner snapshot root is a symlink")
    if not root.exists():
        return
    root.chmod(0o700)
    for path in root.rglob("*"):
        if path.is_dir() and not path.is_symlink():
            path.chmod(0o700)
    shutil.rmtree(root)


async def read_repo(
    repository: Path, commit: str, *, paths: tuple[str, ...] = (), max_bytes: int = 60000
) -> dict[str, object]:
    """One bounded API context read, including a manifest and explicit truncation.

    Reads Git objects, never the live worktree. No shell, arbitrary ref, write,
    or symlink traversal is exposed to the planner.
    """
    if not 1 <= max_bytes <= 100000:
        raise ValueError("repository read budget out of range")
    for path in paths:
        safe_repo_path(path)
    files = await commit_files(repository, commit)
    names = sorted(files)
    selected = list(paths) if paths else sorted(names, key=lambda name: (name.count("/"), name))
    contents: dict[str, str] = {}
    remaining = max_bytes
    truncated = []
    for name in selected:
        value = files.get(name)
        if value is None:
            raise ValueError("repository file does not exist at commit")
        try:
            value.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if b"\x00" in value:
            continue
        size = min(len(value), remaining, 12000)
        if size:
            contents[name] = value[:size].decode("utf-8", errors="ignore")
            remaining -= size
        if size < len(value):
            truncated.append(name)
    return {
        "commit": commit,
        "files": names[:1000],
        "manifest_truncated": len(names) > 1000,
        "contents": contents,
        "truncated": truncated[:1000],
    }
