"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from ._execution_helpers import *


def test_local_git_initializes_project_repository_idempotently(tmp_path: Path) -> None:
    async def scenario() -> None:
        repository = tmp_path / "notes"
        git = LocalGit()
        first = await git.initialize_repository(repository, readme="# Notes\n\nA project.\n")
        second = await git.initialize_repository(repository, readme="# Notes\n\nA project.\n")
        assert first == second == await git.resolve_commit(repository, "HEAD")
        assert (repository / "README.md").read_text() == "# Notes\n\nA project.\n"
        makefile = (repository / "Makefile").read_text()
        assert "scaffold: no project tests yet" in makefile
        # Green scaffold: empty managed projects must not fail validation for lack of tests.
        completed = subprocess.run(
            ("/usr/bin/make", "test"),
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0
        await git.require_clean_source(repository)

    asyncio.run(scenario())


@pytest.mark.anyio
async def test_typed_git_creates_isolated_worktree_and_collects_diff(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "tracked.txt").write_text("base\n")
    _git(repository, "add", "tracked.txt")
    _git(repository, "commit", "-m", "base")
    _git(repository, "rev-parse", "HEAD").strip()  # initial

    git = LocalGit()
    await git.require_clean_source(repository)
    identity, remote = await git.repository_identity(repository)
    assert len(identity) == 64 and remote is None
    run_id = uuid.uuid4()
    branch = worktree_branch(uuid.uuid4(), run_id)
    worktree = tmp_path / "worktrees" / str(run_id)
    worktree.parent.mkdir()
    # Set up additional files in the *repository* (so worktree starts with them at primary_head)
    (repository / "to-delete.txt").write_text("will be deleted\n")
    (repository / "to-rename.txt").write_text("will be renamed\n")
    _git(repository, "add", "to-delete.txt", "to-rename.txt")
    _git(repository, "commit", "-m", "add del/rename targets")
    new_primary = _git(repository, "rev-parse", "HEAD").strip()

    await git.add_worktree(repository, worktree, branch, new_primary)
    assert (worktree / ".git").is_dir()
    assert _git(worktree, "rev-parse", "--is-shallow-repository").strip() == "true"
    assert _git(worktree, "remote").strip() == ""
    assert str(repository) not in (worktree / ".git" / "config").read_text()
    assert str(worktree) not in _git(repository, "worktree", "list", "--porcelain")

    # Now changes in worktree (tracked mod, new untracked, delete, rename)
    (worktree / "tracked.txt").write_text("changed\n")
    (worktree / "new-untracked.txt").write_text("new content\n")
    (worktree / "to-delete.txt").unlink()
    _git(worktree, "mv", "to-rename.txt", "renamed.txt")

    inspection = await git.inspect(worktree)
    assert inspection.head == new_primary
    names = set(inspection.changed_files)
    assert "tracked.txt" in names
    assert "new-untracked.txt" in names
    assert "to-delete.txt" in names
    assert "renamed.txt" in names
    assert b"changed" in inspection.diff
    assert b"new content" in inspection.diff or b"new-untracked.txt" in inspection.diff
    assert b"deleted file mode" in inspection.diff or b"to-delete.txt" in inspection.diff
    assert b"rename from" in inspection.diff or b"renamed.txt" in inspection.diff

    assert _git(repository, "rev-parse", "HEAD").strip() == new_primary
    assert (repository / "tracked.txt").read_text() == "base\n"
    _git(worktree, "add", ".")
    _git(
        worktree,
        "-c",
        "user.name=Worker",
        "-c",
        "user.email=worker@example.invalid",
        "commit",
        "-m",
        "worker commit",
    )
    assert _git(worktree, "rev-parse", "HEAD").strip() != new_primary
    committed = await git.inspect(worktree, new_primary)
    assert set(committed.changed_files) == names
    assert b"changed" in committed.diff
    await git.remove_worktree(repository, worktree)


@pytest.mark.anyio
async def test_typed_git_rejects_dirty_primary(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    (repository / "untracked").write_text("unsafe")
    with pytest.raises(GitError, match="dirty"):
        await LocalGit().require_clean_source(repository)


@pytest.mark.anyio
async def test_typed_git_applies_one_approved_result_with_target_cas(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "value.txt").write_text("base\n")
    _git(repository, "add", "value.txt")
    _git(repository, "commit", "-m", "base")
    base = _git(repository, "rev-parse", "HEAD").strip()
    _git(repository, "switch", "--detach")

    worktree = tmp_path / "worktree"
    git = LocalGit()
    await git.add_worktree(repository, worktree, "result", base)
    (worktree / "value.txt").write_text("approved\n")
    await git.stage_paths(worktree, ("value.txt",))
    result = await git.create_commit(worktree, "approved result")

    assert await git.apply_result(
        repository,
        worktree,
        target_branch="main",
        expected_head=base,
        result_commit=result,
    )
    assert _git(repository, "rev-parse", "main").strip() == result
    assert not await git.apply_result(
        repository,
        worktree,
        target_branch="main",
        expected_head=base,
        result_commit=result,
    )


@pytest.mark.anyio
async def test_typed_git_applies_when_target_branch_is_checked_out(tmp_path: Path) -> None:
    """Freshly provisioned repos keep main checked out; apply must still CAS-advance."""

    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "value.txt").write_text("base\n")
    _git(repository, "add", "value.txt")
    _git(repository, "commit", "-m", "base")
    base = _git(repository, "rev-parse", "HEAD").strip()
    assert _git(repository, "branch", "--show-current").strip() == "main"

    worktree = tmp_path / "worktree"
    git = LocalGit()
    await git.add_worktree(repository, worktree, "result", base)
    (worktree / "value.txt").write_text("approved-on-main\n")
    await git.stage_paths(worktree, ("value.txt",))
    result = await git.create_commit(worktree, "approved result")

    assert await git.apply_result(
        repository,
        worktree,
        target_branch="main",
        expected_head=base,
        result_commit=result,
    )
    assert _git(repository, "rev-parse", "main").strip() == result
    assert _git(repository, "rev-parse", "HEAD").strip() == result
    assert (repository / "value.txt").read_text() == "approved-on-main\n"


@pytest.mark.anyio
async def test_typed_git_recovers_checked_out_branch_after_ref_only_apply(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "value.txt").write_text("base\n")
    _git(repository, "add", "value.txt")
    _git(repository, "commit", "-m", "base")
    base = _git(repository, "rev-parse", "HEAD").strip()

    worktree = tmp_path / "worktree"
    git = LocalGit()
    await git.add_worktree(repository, worktree, "result", base)
    (worktree / "value.txt").write_text("approved\n")
    await git.stage_paths(worktree, ("value.txt",))
    result = await git.create_commit(worktree, "approved result")

    _git(repository, "fetch", "--no-tags", str(worktree), result)
    _git(repository, "update-ref", "refs/heads/main", result, base)
    assert (repository / "value.txt").read_text() == "base\n"

    assert not await git.apply_result(
        repository,
        worktree,
        target_branch="main",
        expected_head=base,
        result_commit=result,
    )
    assert (repository / "value.txt").read_text() == "approved\n"
    assert not _git(repository, "status", "--porcelain").strip()


@pytest.mark.anyio
async def test_worktree_access_rolls_back_partial_grant_and_detects_git_change(
    tmp_path: Path,
) -> None:
    root = tmp_path / "worktrees"
    worktree = root / "task"
    worktree.mkdir(parents=True)
    _git(worktree, "init", "-b", "main")
    source = worktree / "source.txt"
    source.write_text("source\n")
    resolver = _FixedIdentityResolver()

    class FailingGrant(_TestAccessManager):
        async def _grant_entries(self, lease: Any) -> None:
            await super()._grant_entries(lease)
            raise WorktreeAccessError("simulated partial grant")

    manager = FailingGrant(root, resolver)  # type: ignore[arg-type]
    with pytest.raises(WorktreeAccessError, match="partial grant"):
        await manager.grant(worktree, sandbox_uid=10_001, sandbox_gid=10_001)
    assert f"user:{resolver.host_uid}:" not in _numeric_acl(source)

    verifying_manager = _TestAccessManager(root, resolver)  # type: ignore[arg-type]
    lease = await verifying_manager.grant(worktree, sandbox_uid=10_001, sandbox_gid=10_001)
    (worktree / ".git").chmod(0o700)
    with pytest.raises(WorktreeAccessError, match="Git metadata changed"):
        await lease.revoke()


@pytest.mark.anyio
async def test_worktree_service_prepare_dirty_rejection(tmp_path: Path) -> None:
    """Test dirty source rejection via real git (edge case exercised by prepare)."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "T")
    (repo / "f.txt").write_text("base")
    _git(repo, "add", "f.txt")
    _git(repo, "commit", "-m", "init")

    # Dirty source rejection (directly exercises the check used by prepare)
    (repo / "dirty.txt").write_text("dirty")
    with pytest.raises(GitError):
        await LocalGit().require_clean_source(repo)


@pytest.mark.anyio
async def test_prepare_worktree_handler_cancel_and_missing(tmp_path: Path) -> None:
    """Test handler cancellation and missing project paths."""
    mock_factory = MagicMock()
    mock_reg = MagicMock()
    mock_wts = AsyncMock()
    h = PrepareWorktreeHandler(mock_factory, mock_reg, mock_wts, owner="exec")

    cancel = CancellationContext()
    cancel.request()

    req = MagicMock()
    req.task_id = uuid.uuid4()
    req.run_id = uuid.uuid4()
    outcome = await h.execute(req, cancel)
    assert outcome.kind.value == "cancelled" or "CANCELLED" in str(outcome.kind)

    # Non-canceled but missing task/project path
    cancel2 = CancellationContext()
    mock_sess = AsyncMock()
    mock_sess.get.return_value = None
    mock_factory.begin.return_value.__aenter__.return_value = mock_sess
    req2 = MagicMock()
    req2.task_id = uuid.uuid4()
    req2.run_id = uuid.uuid4()
    outcome2 = await h.execute(req2, cancel2)
    assert "project_required" in str(outcome2.category) or outcome2.kind.value in (
        "permanent_failure",
        "PERMANENT_FAILURE",
    )
