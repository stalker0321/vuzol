"""Fail-closed guardrail tests for typed LocalGit operations."""

from __future__ import annotations

import pytest

from ._execution_helpers import GitError, LocalGit, Path, _git


class _ForgingGit(LocalGit):
    """Substitute one genuine git reply to exercise fail-closed verification guards."""

    def __init__(self, command: tuple[str, ...], payload: bytes, *, genuine_first: int = 0) -> None:
        super().__init__()
        self._command = command
        self._payload = payload
        self._genuine_left = genuine_first

    async def _run(
        self, cwd: Path, *args: str, extra_environment: dict[str, str] | None = None
    ) -> bytes:
        if tuple(args) == self._command:
            if self._genuine_left == 0:
                return self._payload
            self._genuine_left -= 1
        return await super()._run(cwd, *args, extra_environment=extra_environment)


class _UnreliableTreeMatch(LocalGit):
    """Report one managed-tree comparison as diverged even though the tree is clean."""

    def __init__(self, lying_commit: str, times: int) -> None:
        super().__init__()
        self._lying_commit = lying_commit
        self._times = times

    async def _tree_matches_commit(self, repository: Path, commit: str) -> bool:
        matches = await super()._tree_matches_commit(repository, commit)
        if commit == self._lying_commit and self._times > 0:
            self._times -= 1
            return False
        return matches


def _repository(tmp_path: Path, *, detach: bool = False) -> tuple[Path, str]:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "value.txt").write_text("base\n")
    _git(repository, "add", "value.txt")
    _git(repository, "commit", "-m", "base")
    base = _git(repository, "rev-parse", "HEAD").strip()
    if detach:
        _git(repository, "switch", "--detach")
    return repository, base


def _clonable_source(tmp_path: Path) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-b", "trunk")
    _git(source, "config", "user.email", "test@example.com")
    _git(source, "config", "user.name", "Test")
    (source / "README.md").write_text("imported project\n")
    _git(source, "add", "README.md")
    _git(source, "commit", "-m", "initial")
    return source


async def _produce_result(
    git: LocalGit, repository: Path, worktree: Path, base: str, *, marker: str = "approved"
) -> str:
    await git.add_worktree(repository, worktree, "result", base)
    (worktree / "value.txt").write_text(f"{marker}\n")
    await git.stage_paths(worktree, ("value.txt",))
    return await git.create_commit(worktree, f"{marker} result")


@pytest.mark.anyio
async def test_initialize_rejects_file_at_project_path(tmp_path: Path) -> None:
    obstacle = tmp_path / "notes"
    obstacle.write_text("not a directory\n")
    with pytest.raises(GitError, match="path is not a directory"):
        await LocalGit().initialize_repository(obstacle, readme="# Notes\n")


@pytest.mark.anyio
async def test_initialize_rejects_occupied_directory_without_git(tmp_path: Path) -> None:
    repository = tmp_path / "notes"
    repository.mkdir()
    (repository / "leftover.txt").write_text("stray\n")
    with pytest.raises(GitError, match="path is not empty"):
        await LocalGit().initialize_repository(repository, readme="# Notes\n")


@pytest.mark.anyio
async def test_initialize_rejects_unfinished_readme_drift(tmp_path: Path) -> None:
    repository = tmp_path / "notes"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    (repository / "README.md").write_text("# Someone Else\n")
    with pytest.raises(GitError, match="unfinished project README differs"):
        await LocalGit().initialize_repository(repository, readme="# Notes\n")


@pytest.mark.anyio
async def test_initialize_rejects_unfinished_makefile_drift(tmp_path: Path) -> None:
    repository = tmp_path / "notes"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    (repository / "README.md").write_text("# Notes\n")
    (repository / "Makefile").write_text("all:\n\techo hijacked\n")
    with pytest.raises(GitError, match="unfinished project Makefile differs"):
        await LocalGit().initialize_repository(repository, readme="# Notes\n")


@pytest.mark.anyio
async def test_clone_rejects_existing_path_outside_git(tmp_path: Path) -> None:
    destination = tmp_path / "imported"
    destination.mkdir()
    (destination / "keep.txt").write_text("occupied\n")
    with pytest.raises(GitError, match="is not a Git repository"):
        await LocalGit().clone_repository(destination, url=str(tmp_path / "source"))


@pytest.mark.anyio
async def test_clone_rejects_origin_rewrite(tmp_path: Path) -> None:
    source = _clonable_source(tmp_path)
    destination = tmp_path / "imported"
    git = LocalGit()
    await git.clone_repository(destination, url=str(source))
    with pytest.raises(GitError, match="different origin"):
        await git.clone_repository(destination, url="https://example.invalid/mirror.git")


@pytest.mark.anyio
async def test_clone_timeout_kills_process_and_removes_destination(tmp_path: Path) -> None:
    source = _clonable_source(tmp_path)
    destination = tmp_path / "imported"
    with pytest.raises(GitError, match="repository clone timed out"):
        await LocalGit(timeout_seconds=0).clone_repository(destination, url=str(source))
    assert not destination.exists()


@pytest.mark.anyio
async def test_clone_failure_removes_partial_destination(tmp_path: Path) -> None:
    destination = tmp_path / "imported"
    with pytest.raises(GitError, match="repository clone failed"):
        await LocalGit().clone_repository(destination, url=str(tmp_path / "missing-source"))
    assert not destination.exists()


@pytest.mark.anyio
async def test_clone_rejects_missing_default_branch(tmp_path: Path) -> None:
    source = _clonable_source(tmp_path)
    destination = tmp_path / "imported"
    git = _ForgingGit(("branch", "--show-current"), b"")
    with pytest.raises(GitError, match="default branch is missing or invalid"):
        await git.clone_repository(destination, url=str(source))
    assert not destination.exists()


@pytest.mark.anyio
async def test_resolve_commit_rejects_non_hex_identity(tmp_path: Path) -> None:
    repository, _ = _repository(tmp_path)
    git = _ForgingGit(("rev-parse", "--verify", "HEAD^{commit}"), b"not-a-commit\n")
    with pytest.raises(GitError, match="invalid commit identity"):
        await git.resolve_commit(repository, "HEAD")


@pytest.mark.anyio
async def test_require_clean_source_rejects_in_progress_operation(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    (repository / ".git" / "MERGE_HEAD").write_text(f"{base}\n")
    with pytest.raises(GitError, match="in-progress Git operation"):
        await LocalGit().require_clean_source(repository)


@pytest.mark.anyio
async def test_git_operations_enforce_timeout_budget(tmp_path: Path) -> None:
    repository, _ = _repository(tmp_path)
    with pytest.raises(GitError, match="Git operation timed out"):
        await LocalGit(timeout_seconds=0).require_clean_source(repository)


@pytest.mark.anyio
async def test_stage_paths_rejects_empty_and_escaping_paths(tmp_path: Path) -> None:
    repository, _ = _repository(tmp_path)
    git = LocalGit()
    with pytest.raises(GitError, match="empty path set"):
        await git.stage_paths(repository, ())
    for unsafe in ("../escape.txt", "/etc/passwd"):
        with pytest.raises(GitError, match="not repository-relative"):
            await git.stage_paths(repository, (unsafe,))


@pytest.mark.anyio
async def test_amend_commit_replaces_tip_message(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path, detach=True)
    git = LocalGit()
    worktree = tmp_path / "worktree"
    result = await _produce_result(git, repository, worktree, base)
    amended = await git.amend_commit(worktree, "corrected message")
    assert amended != result
    assert _git(worktree, "log", "-1", "--format=%B").strip() == "corrected message"
    assert await git.commit_parent(worktree, amended) == base


@pytest.mark.parametrize(
    "message",
    [
        "",
        "   ",
        "broken\rmessage",
        "broken\x00message",
        "x" * 1001,
        "lead\n\n\n\ntrail",
    ],
)
@pytest.mark.anyio
async def test_create_commit_rejects_invalid_messages(tmp_path: Path, message: str) -> None:
    repository, base = _repository(tmp_path, detach=True)
    git = LocalGit()
    worktree = tmp_path / "worktree"
    await _produce_result(git, repository, worktree, base)
    with pytest.raises(GitError, match="commit message is invalid"):
        await git.create_commit(worktree, message)


@pytest.mark.anyio
async def test_require_clean_worktree_rejects_generated_files(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    git = LocalGit()
    worktree = tmp_path / "worktree"
    await git.add_worktree(repository, worktree, "result", base)
    (worktree / "artifact.log").write_text("generated\n")
    with pytest.raises(GitError, match="finalized worktree is dirty"):
        await git.require_clean_worktree(worktree)


@pytest.mark.anyio
async def test_add_worktree_rejects_retained_remote(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path, detach=True)
    worktree = tmp_path / "worktree"
    suspicious = _ForgingGit(("remote",), b"origin\n")
    with pytest.raises(GitError, match="unexpectedly retained a remote"):
        await suspicious.add_worktree(repository, worktree, "result", base)
    assert not worktree.exists()


@pytest.mark.anyio
async def test_add_worktree_cleans_up_after_failed_fetch(tmp_path: Path) -> None:
    repository, _ = _repository(tmp_path, detach=True)
    worktree = tmp_path / "worktree"
    with pytest.raises(GitError, match="Git operation failed: fetch"):
        await LocalGit().add_worktree(repository, worktree, "result", "0" * 40)
    assert not worktree.exists()


@pytest.mark.anyio
async def test_remove_worktree_deregisters_linked_checkout(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    linked = tmp_path / "linked"
    _git(repository, "worktree", "add", "-b", "side-task", str(linked), base)
    assert (linked / ".git").is_file()
    await LocalGit().remove_worktree(repository, linked)
    assert not linked.exists()
    assert str(linked) not in _git(repository, "worktree", "list", "--porcelain")


@pytest.mark.anyio
async def test_apply_result_is_idempotent_on_clean_checked_out_branch(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    git = LocalGit()
    worktree = tmp_path / "worktree"
    result = await _produce_result(git, repository, worktree, base)
    assert await git.apply_result(
        repository, worktree, target_branch="main", expected_head=base, result_commit=result
    )
    assert not await git.apply_result(
        repository, worktree, target_branch="main", expected_head=base, result_commit=result
    )
    assert _git(repository, "rev-parse", "main").strip() == result


@pytest.mark.anyio
async def test_apply_result_rejects_diverged_managed_tree(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    git = LocalGit()
    worktree = tmp_path / "worktree"
    result = await _produce_result(git, repository, worktree, base)
    assert await git.apply_result(
        repository, worktree, target_branch="main", expected_head=base, result_commit=result
    )
    (repository / "value.txt").write_text("tampered\n")
    with pytest.raises(GitError, match="diverged during apply recovery"):
        await git.apply_result(
            repository, worktree, target_branch="main", expected_head=base, result_commit=result
        )


@pytest.mark.anyio
async def test_apply_result_fails_closed_when_managed_tree_wont_recover(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    honest = LocalGit()
    worktree = tmp_path / "worktree"
    result = await _produce_result(honest, repository, worktree, base)
    _git(repository, "fetch", "--no-tags", str(worktree), result)
    _git(repository, "update-ref", "refs/heads/main", result, base)
    suspicious = _UnreliableTreeMatch(result, times=2)
    with pytest.raises(GitError, match="did not recover to the approved result"):
        await suspicious.apply_result(
            repository, worktree, target_branch="main", expected_head=base, result_commit=result
        )


@pytest.mark.anyio
async def test_apply_result_rejects_moved_target_branch(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    git = LocalGit()
    worktree = tmp_path / "worktree"
    result = await _produce_result(git, repository, worktree, base)
    (repository / "value.txt").write_text("advanced alone\n")
    _git(repository, "add", "value.txt")
    _git(repository, "commit", "-m", "platform advanced alone")
    with pytest.raises(GitError, match="target branch changed after the result"):
        await git.apply_result(
            repository, worktree, target_branch="main", expected_head=base, result_commit=result
        )


@pytest.mark.anyio
async def test_apply_result_rejects_result_from_divergent_base(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path, detach=True)
    git = LocalGit()
    first = tmp_path / "first"
    first_head = await _produce_result(git, repository, first, base, marker="first")
    assert await git.apply_result(
        repository, first, target_branch="main", expected_head=base, result_commit=first_head
    )
    second = tmp_path / "second"
    competing = await _produce_result(git, repository, second, base, marker="competing")
    with pytest.raises(GitError, match="not a descendant of the approved base"):
        await git.apply_result(
            repository,
            second,
            target_branch="main",
            expected_head=first_head,
            result_commit=competing,
        )


@pytest.mark.anyio
async def test_apply_result_rejects_forged_fetch_identity(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path, detach=True)
    worktree = tmp_path / "worktree"
    result = await _produce_result(LocalGit(), repository, worktree, base)
    forged = _ForgingGit(("rev-parse", "--verify", "FETCH_HEAD^{commit}"), b"f" * 40)
    with pytest.raises(GitError, match="fetched result identity does not match"):
        await forged.apply_result(
            repository, worktree, target_branch="main", expected_head=base, result_commit=result
        )


@pytest.mark.anyio
async def test_apply_result_verifies_cas_advance(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path, detach=True)
    worktree = tmp_path / "worktree"
    result = await _produce_result(LocalGit(), repository, worktree, base)
    target_ref = "refs/heads/vuzol/pkg/task"
    forged = _ForgingGit(("rev-parse", "--verify", f"{target_ref}^{{commit}}"), b"e" * 40)
    with pytest.raises(GitError, match="target branch did not advance"):
        await forged.apply_result(
            repository,
            worktree,
            target_branch="vuzol/pkg/task",
            expected_head=base,
            result_commit=result,
        )


@pytest.mark.anyio
async def test_apply_result_verifies_managed_reset(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    worktree = tmp_path / "worktree"
    result = await _produce_result(LocalGit(), repository, worktree, base)
    forged = _ForgingGit(("rev-parse", "--verify", "HEAD^{commit}"), b"d" * 40)
    with pytest.raises(GitError, match="managed worktree did not reset"):
        await forged.apply_result(
            repository, worktree, target_branch="main", expected_head=base, result_commit=result
        )
