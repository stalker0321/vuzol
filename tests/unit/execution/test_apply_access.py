"""Apply admission observes the real filesystem without mutating Git state."""

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from vuzol.execution import git as git_module
from vuzol.execution.git import GitError, LocalGit, require_writable_directory

from ._execution_helpers import _git


def _repository(tmp_path: Path) -> Path:
    path = tmp_path / "repository"
    path.mkdir()
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.email", "test@example.com")
    _git(path, "config", "user.name", "Test")
    (path / "value.txt").write_text("base\n")
    _git(path, "add", "value.txt")
    _git(path, "commit", "-m", "base")
    return path


def _metadata_snapshot(path: Path) -> dict[str, str]:
    return {
        str(item.relative_to(path)): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in path.rglob("*")
        if item.is_file()
    }


@pytest.mark.anyio
async def test_apply_access_checks_real_git_without_modifying_any_metadata(tmp_path: Path) -> None:
    repository = _repository(tmp_path)
    before = _metadata_snapshot(repository)
    await LocalGit().require_apply_access(repository)
    assert _metadata_snapshot(repository) == before


def test_readonly_mount_rejected_even_when_dac_permissions_allow_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(git_module.os, "statvfs", lambda _: SimpleNamespace(f_flag=os.ST_RDONLY))
    monkeypatch.setattr(git_module.os, "access", lambda *_args, **_kwargs: True)
    with pytest.raises(GitError, match="read-only filesystem"):
        require_writable_directory(tmp_path)


def test_writable_mount_does_not_bypass_uid_or_acl_denial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(git_module.os, "access", lambda *_args, **_kwargs: False)
    with pytest.raises(GitError, match="lacks write access"):
        require_writable_directory(tmp_path)


def test_missing_directory_rejected(tmp_path: Path) -> None:
    with pytest.raises(GitError, match="unavailable"):
        require_writable_directory(tmp_path / "missing")


def test_uninspectable_directory_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def denied(_path: object) -> object:
        raise PermissionError("denied")

    monkeypatch.setattr(git_module.os, "statvfs", denied)
    with pytest.raises(GitError, match="cannot inspect"):
        require_writable_directory(tmp_path)


@pytest.mark.anyio
@pytest.mark.parametrize("target", ["common", "FETCH_HEAD", "index", "refs/heads"])
async def test_linked_repository_checks_real_common_and_worktree_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    repository = _repository(tmp_path)
    linked = tmp_path / "linked"
    _git(repository, "worktree", "add", "-b", "linked", str(linked))
    await LocalGit().require_apply_access(linked)
    common = repository / ".git"
    (common / "FETCH_HEAD").write_text("retained receipt\n")
    before = _metadata_snapshot(repository)
    metadata = Path(_git(linked, "rev-parse", "--absolute-git-dir").strip())
    blocked = (
        common
        if target == "common"
        else (metadata / "index" if target == "index" else common / target)
    )
    original = os.statvfs

    def mounts(path: object) -> object:
        if Path(str(path)) == blocked:
            return SimpleNamespace(f_flag=os.ST_RDONLY)
        return original(path)

    monkeypatch.setattr(git_module.os, "statvfs", mounts)
    with pytest.raises(GitError, match="read-only filesystem"):
        await LocalGit().require_apply_access(linked)
    assert _metadata_snapshot(repository) == before
