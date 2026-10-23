"""Commit isolation, containment, bounded reads and filesystem write denial."""

import subprocess
import uuid

import pytest

from vuzol.execution.git import GitError
from vuzol.execution.repo_snapshot import (
    build_snapshot,
    cleanup_snapshot,
    read_repo,
    safe_repo_path,
    snapshot_path,
)


def git(repository, *args):
    return subprocess.check_output(["git", "-C", str(repository), *args]).decode().strip()


@pytest.fixture
def repository(tmp_path):
    git(tmp_path, "init", "-b", "main")
    git(tmp_path, "config", "user.email", "test@example.invalid")
    git(tmp_path, "config", "user.name", "Test")
    (tmp_path / "known.txt").write_text("committed\n")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-m", "fixture")
    return tmp_path


@pytest.mark.anyio
async def test_snapshot_reads_exact_commit_denies_writes_and_cleans(repository):
    commit = git(repository, "rev-parse", "HEAD")
    (repository / "known.txt").write_text("uncommitted")
    invocation_id = uuid.uuid4()
    root = await build_snapshot(repository, commit, invocation_id)
    try:
        assert (root / "known.txt").read_text() == "committed\n"
        assert not (root / ".git").exists()
        with pytest.raises(PermissionError):
            (root / "known.txt").write_text("mutate")
        with pytest.raises(PermissionError):
            (root / "new.txt").touch()
        observed = await read_repo(repository, commit, paths=("known.txt",), max_bytes=4)
        assert observed["contents"] == {"known.txt": "comm"}
        assert observed["truncated"] == ["known.txt"]
        assert (repository / "known.txt").read_text() == "uncommitted"
    finally:
        cleanup_snapshot(invocation_id)
    assert not root.exists()
    cleanup_snapshot(invocation_id)


@pytest.mark.parametrize(
    "value", ["../outside", "/etc/passwd", ".git/config", "a/../../x", "a\\b", ""]
)
def test_repo_paths_cannot_escape(value):
    with pytest.raises(ValueError, match="unsafe repository path"):
        safe_repo_path(value)


@pytest.mark.anyio
async def test_committed_symlink_and_existing_root_are_rejected(repository, tmp_path):
    commit = git(repository, "rev-parse", "HEAD")
    invocation_id = uuid.uuid4()
    snapshot_path(invocation_id).symlink_to(tmp_path, target_is_directory=True)
    try:
        with pytest.raises(FileExistsError):
            await build_snapshot(repository, commit, invocation_id)
    finally:
        snapshot_path(invocation_id).unlink()
    (repository / "escape").symlink_to("/etc/passwd")
    git(repository, "add", ".")
    git(repository, "commit", "-m", "symlink")
    with pytest.raises(GitError, match="regular files"):
        await build_snapshot(repository, git(repository, "rev-parse", "HEAD"), invocation_id)
    assert not snapshot_path(invocation_id).exists()
