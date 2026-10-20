"""Capture is not acceptance, even if the files repeat every criterion word."""

import asyncio
import subprocess

import pytest

from vuzol.execution.git import GitError
from vuzol.leadership.evidence import GOAL_HEAD_CHECKER, observe_commit


def test_snapshot_uses_the_commit_not_dirty_or_untracked_files(tmp_path):
    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "test@example.org"], check=True
    )
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.name", "Fixture"], check=True)
    readme = tmp_path / "README.md"
    readme.write_text("Working OAuth login is a TODO\n")
    subprocess.run(["git", "-C", str(tmp_path), "add", "README.md"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-m", "fixture"], check=True, capture_output=True
    )
    commit = subprocess.check_output(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], text=True
    ).strip()
    readme.write_text("Pretend it works now\n")
    (tmp_path / "secrets.txt").write_text("untracked")
    assert asyncio.run(observe_commit(str(tmp_path), commit)) == {
        "README.md": "Working OAuth login is a TODO\n"
    }
    assert GOAL_HEAD_CHECKER != "goal_head_v1"
    with pytest.raises(GitError):
        asyncio.run(observe_commit(str(tmp_path), "HEAD"))
