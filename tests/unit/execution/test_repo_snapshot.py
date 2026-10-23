"""Commit isolation, containment, bounded reads and filesystem write denial."""

import subprocess
import uuid

import pytest

from vuzol.execution.git import GitError
from vuzol.execution.repo_snapshot import (
    build_snapshot,
    cleanup_snapshot,
    is_snapshot_path,
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
    worktrees = repository.parent / f"worktrees-{invocation_id}"
    worktrees.mkdir()
    root = await build_snapshot(repository, commit, invocation_id, worktrees)
    assert root == snapshot_path(invocation_id, worktrees)
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
        cleanup_snapshot(root)
    assert not root.exists()
    cleanup_snapshot(root)


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
    worktrees = tmp_path / "worktrees"
    target = snapshot_path(invocation_id, worktrees)
    target.parent.mkdir(parents=True)  # also creates the worktree root
    target.symlink_to(tmp_path, target_is_directory=True)
    try:
        with pytest.raises(FileExistsError):
            await build_snapshot(repository, commit, invocation_id, worktrees)
    finally:
        target.unlink()
    (repository / "escape").symlink_to("/etc/passwd")
    git(repository, "add", ".")
    git(repository, "commit", "-m", "symlink")
    with pytest.raises(GitError, match="regular files"):
        await build_snapshot(
            repository, git(repository, "rev-parse", "HEAD"), invocation_id, worktrees
        )
    assert not target.exists()


def test_snapshot_lives_under_the_shared_worktree_root_not_private_tmp(tmp_path):
    # The executor has PrivateTmp; rootless Docker only sees the worktree root.
    invocation_id = uuid.uuid4()
    path = snapshot_path(invocation_id, tmp_path)
    assert path.parent == tmp_path / ".planner-snapshots"
    assert is_snapshot_path(path, invocation_id)
    assert not is_snapshot_path(path, uuid.uuid4())
    assert not is_snapshot_path(tmp_path / f"vuzol-planner-{invocation_id}", invocation_id)
