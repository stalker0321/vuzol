"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from vuzol.config.settings import Settings

from ._execution_helpers import (
    Any,
    AsyncMock,
    CancellationContext,
    GitError,
    LocalGit,
    MagicMock,
    Path,
    PrepareWorktreeHandler,
    WorktreeAccessError,
    _FixedIdentityResolver,
    _git,
    _numeric_acl,
    _TestAccessManager,
    asyncio,
    pytest,
    subprocess,
    uuid,
    worktree_branch,
)


def test_local_git_initializes_project_repository_idempotently(tmp_path: Path) -> None:
    async def scenario() -> None:
        repository = tmp_path / "notes"
        git = LocalGit()
        first = await git.initialize_repository(repository, readme="# Notes\n\nA project.\n")
        second = await git.initialize_repository(repository, readme="# Notes\n\nA project.\n")
        assert first == second == await git.resolve_commit(repository, "HEAD")
        assert (repository / "README.md").read_text() == "# Notes\n\nA project.\n"
        makefile = (repository / "Makefile").read_text()
        assert "vuzol-scaffold-gate: true" in makefile
        assert "scaffold: no project tests yet" in makefile
        # Green scaffold: empty/docs-only managed projects may pass make test.
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


def test_local_git_clones_existing_repository_idempotently(tmp_path: Path) -> None:
    async def scenario() -> None:
        source = tmp_path / "source"
        source.mkdir()
        _git(source, "init", "-b", "trunk")
        _git(source, "config", "user.email", "test@example.com")
        _git(source, "config", "user.name", "Test")
        (source / "README.md").write_text("existing project\n")
        _git(source, "add", "README.md")
        _git(source, "commit", "-m", "initial")

        destination = tmp_path / "imported"
        git = LocalGit()
        first = await git.clone_repository(destination, url=str(source))
        second = await git.clone_repository(destination, url=str(source))

        assert first == second
        assert first[1] == "trunk"
        assert (destination / "README.md").read_text() == "existing project\n"

    asyncio.run(scenario())


@pytest.mark.anyio
async def test_diff_check_reports_bounded_git_stderr(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    (repository / "bad.txt").write_text("content\n\n")
    git = LocalGit()
    await git.stage_paths(repository, ("bad.txt",))

    with pytest.raises(GitError, match=r"bad\.txt.*blank line at EOF"):
        await git.require_diff_check(repository)


@pytest.mark.anyio
async def test_artifact_git_check_allows_untracked_output_but_rejects_tracked_changes(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    tracked = repository / "tracked.txt"
    tracked.write_text("base\n")
    _git(repository, "add", "tracked.txt")
    _git(repository, "commit", "-m", "base")
    git = LocalGit()

    (repository / "artifact.bin").write_bytes(b"artifact")
    await git.require_clean_tracked_worktree(repository)
    tracked.write_text("mutated\n")
    with pytest.raises(GitError, match="artifact command changed"):
        await git.require_clean_tracked_worktree(repository)


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
    assert inspection.added_files == ("new-untracked.txt",)
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
    assert committed.added_files == ("new-untracked.txt",)
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
async def test_typed_git_fast_forwards_whole_approved_package_chain(tmp_path: Path) -> None:
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

    worktree = tmp_path / "package-worktree"
    git = LocalGit()
    await git.add_worktree(repository, worktree, "package-result", base)
    (worktree / "first.txt").write_text("first\n")
    await git.stage_paths(worktree, ("first.txt",))
    await git.create_commit(worktree, "first task")
    (worktree / "second.txt").write_text("second\n")
    await git.stage_paths(worktree, ("second.txt",))
    package_head = await git.create_commit(worktree, "second task")

    assert await git.apply_result(
        repository,
        worktree,
        target_branch="main",
        expected_head=base,
        result_commit=package_head,
    )
    assert _git(repository, "rev-parse", "main").strip() == package_head


@pytest.mark.anyio
async def test_typed_git_creates_private_package_ref_with_cas(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "value.txt").write_text("base\n")
    _git(repository, "add", "value.txt")
    _git(repository, "commit", "-m", "base")
    base = _git(repository, "rev-parse", "HEAD").strip()

    worktree = tmp_path / "package-step"
    git = LocalGit()
    await git.add_worktree(repository, worktree, "isolated-result", base)
    (worktree / "value.txt").write_text("preview\n")
    await git.stage_paths(worktree, ("value.txt",))
    result = await git.create_commit(worktree, "package step")

    assert await git.apply_result(
        repository,
        worktree,
        target_branch="vuzol/package/test/revision",
        expected_head=base,
        result_commit=result,
    )
    assert _git(repository, "rev-parse", "vuzol/package/test/revision").strip() == result
    assert _git(repository, "rev-parse", "main").strip() == base


@pytest.mark.anyio
async def test_typed_git_promotes_chain_from_successive_shallow_worktrees(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "base.txt").write_text("base\n")
    _git(repository, "add", "base.txt")
    _git(repository, "commit", "-m", "base")
    base = _git(repository, "rev-parse", "HEAD").strip()
    git = LocalGit()

    first = tmp_path / "first"
    await git.add_worktree(repository, first, "first-task", base)
    (first / "first.txt").write_text("first\n")
    await git.stage_paths(first, ("first.txt",))
    first_head = await git.create_commit(first, "first task")
    assert await git.apply_result(
        repository,
        first,
        target_branch="vuzol/package/test/revision",
        expected_head=base,
        result_commit=first_head,
    )

    second = tmp_path / "second"
    await git.add_worktree(repository, second, "second-task", first_head)
    (second / "second.txt").write_text("second\n")
    await git.stage_paths(second, ("second.txt",))
    final_head = await git.create_commit(second, "second task")
    assert await git.apply_result(
        repository,
        second,
        target_branch="main",
        expected_head=base,
        result_commit=final_head,
    )
    assert _git(repository, "rev-parse", "main").strip() == final_head


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
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "repositories",
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    h = PrepareWorktreeHandler(
        mock_factory,
        mock_reg,
        mock_wts,
        owner="exec",
        settings=settings,
    )

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


def _persisted_worktree(path, **overrides):
    from vuzol.storage.models import Worktree
    from vuzol.storage.types import WorktreeDeliveryState

    return Worktree(
        **(
            dict(
                id=uuid.uuid4(),
                task_id=uuid.uuid4(),
                run_id=uuid.uuid4(),
                project_id="project",
                path=str(path),
                branch="worker",
                base_commit="a" * 40,
                repository_identity_hash="identity",
                owner="executor",
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                lifecycle_generation=1,
            )
            | overrides
        )
    )


@pytest.mark.anyio
@pytest.mark.parametrize("refusal", ["missing", "cleaned_at", "cleaned", "active", "process"])
async def test_cleanup_preserves_unowned_or_in_use_worktrees(tmp_path, refusal):
    from vuzol.execution.worktrees import WorktreeService
    from vuzol.storage.types import WorktreeDeliveryState

    path = tmp_path / "worktrees" / "recorded"
    path.mkdir(parents=True)
    sentinel = path / "result.txt"
    sentinel.write_text("keep me")
    row = _persisted_worktree(path)
    if refusal == "cleaned_at":
        from datetime import UTC, datetime

        row.cleaned_at = datetime.now(UTC)
    if refusal in {"cleaned", "active"}:
        row.delivery_state = (
            WorktreeDeliveryState.CLEANED if refusal == "cleaned" else WorktreeDeliveryState.ACTIVE
        )
    session = AsyncMock()
    session.scalar.side_effect = [
        None if refusal == "missing" else row,
        uuid.uuid4() if refusal == "process" else None,
    ]
    git = AsyncMock()
    service = WorktreeService(path.parent, git, retention_days=3)
    await service.cleanup(session, worktree_id=row.id, repository=tmp_path)
    assert sentinel.read_text() == "keep me"
    git.remove_worktree.assert_not_awaited()
    session.flush.assert_not_awaited()


@pytest.mark.anyio
async def test_cleanup_records_path_escape_without_deleting_external_files(tmp_path):
    from vuzol.execution.worktrees import WorktreeService
    from vuzol.storage.types import WorktreeDeliveryState

    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "result"
    sentinel.write_text("private")
    row = _persisted_worktree(outside)
    session, git = AsyncMock(), AsyncMock()
    session.scalar.side_effect = [row, None]
    await WorktreeService(tmp_path / "worktrees", git, retention_days=3).cleanup(
        session, worktree_id=row.id
    )
    assert row.cleanup_reason == "path_violation"
    assert row.delivery_state == WorktreeDeliveryState.WORKTREE_RETAINED
    assert sentinel.read_text() == "private"
    git.remove_worktree.assert_not_awaited()
    session.flush.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize("metadata", ["gone", "present", "error", "no_repository"])
@pytest.mark.parametrize("path_exists", [False, True])
async def test_cleanup_requires_both_filesystem_and_git_removal(tmp_path, metadata, path_exists):
    from vuzol.execution.worktrees import WorktreeService
    from vuzol.storage.types import WorktreeDeliveryState

    root = tmp_path / "worktrees"
    root.mkdir()
    path = root / "recorded"
    if path_exists:
        path.mkdir()
        (path / ".git").mkdir()
        (path / "result").write_text("result")
    row = _persisted_worktree(path)
    session, git = AsyncMock(), AsyncMock()
    session.scalar.side_effect = [row, None]
    git.remove_worktree.side_effect = RuntimeError("git remove failed")
    if metadata == "error":
        git._run.side_effect = RuntimeError("socket unavailable")
    else:
        git._run.return_value = str(path).encode() if metadata == "present" else b""
    service = WorktreeService(root, git, retention_days=3)
    await service.cleanup(
        session,
        worktree_id=row.id,
        repository=None if metadata == "no_repository" else tmp_path,
        reason="completed",
    )
    assert not path.exists()
    if metadata in {"present", "error"}:
        assert row.delivery_state == WorktreeDeliveryState.WORKTREE_RETAINED
        assert row.cleaned_at is None
        assert row.cleanup_reason == "removal_incomplete"
    else:
        assert row.delivery_state == WorktreeDeliveryState.CLEANED
        assert row.cleaned_at is not None
        assert row.cleanup_reason == "completed"
    session.flush.assert_awaited_once()


@pytest.mark.anyio
async def test_cleanup_does_not_mark_cleaned_when_filesystem_removal_fails(tmp_path, monkeypatch):
    from vuzol.execution.worktrees import WorktreeService
    from vuzol.storage.types import WorktreeDeliveryState

    path = tmp_path / "worktrees" / "recorded"
    path.mkdir(parents=True)
    row = _persisted_worktree(path)
    session = AsyncMock()
    session.scalar.side_effect = [row, None]
    monkeypatch.setattr("vuzol.execution.worktrees.shutil.rmtree", lambda *a, **kw: None)
    await WorktreeService(path.parent, AsyncMock(), retention_days=3).cleanup(
        session, worktree_id=row.id
    )
    assert row.cleanup_reason == "removal_incomplete"
    assert row.delivery_state == WorktreeDeliveryState.WORKTREE_RETAINED
    assert row.cleaned_at is None
    assert path.exists()


@pytest.mark.parametrize(
    "remote,expected",
    [
        (None, None),
        ("/srv/repository", "/srv/repository"),
        ("git@host.test:org/repo.git", "host.test:org/repo.git"),
        (
            "https://user:password@host.test:8443/org/repo?token=secret#fragment",
            "https://host.test:8443/org/repo",
        ),
        ("file:///srv/repo", "file://redacted/srv/repo"),
    ],
)
def test_persisted_remote_redacts_credentials_query_and_fragment(remote, expected):
    from vuzol.execution.worktrees import _safe_remote

    assert _safe_remote(remote) == expected


@pytest.mark.anyio
async def test_retain_refuses_missing_record_without_inspecting_disk(tmp_path):
    from vuzol.execution.worktrees import WorktreeError, WorktreeService

    session, git = AsyncMock(), AsyncMock()
    session.scalar.return_value = None
    with pytest.raises(WorktreeError, match="unknown worktree"):
        await WorktreeService(tmp_path, git, retention_days=3).retain(
            session, worktree_id=uuid.uuid4()
        )
    git.inspect.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["inspect", "identity", "flush"])
async def test_prepare_rolls_back_created_directory_on_failure(tmp_path, failure):
    from types import SimpleNamespace

    from vuzol.execution.paths import worktree_path
    from vuzol.execution.worktrees import WorktreeError, WorktreeService

    root, repo = tmp_path / "worktrees", tmp_path / "repository"
    repo.mkdir()
    task_id, run_id = uuid.uuid4(), uuid.uuid4()
    path = worktree_path(root, "project", run_id)
    session, git = AsyncMock(), AsyncMock()
    session.add = MagicMock()
    session.scalar.side_effect = [None, None]
    git.repository_identity.return_value = ("identity", None)
    git.resolve_commit.return_value = "a" * 40

    async def add(*args):
        path.mkdir()
        (path / "partial").write_text("partial")

    git.add_worktree.side_effect = add
    git.inspect.return_value = SimpleNamespace(
        head="a" * 40, branch="wrong" if failure == "identity" else worktree_branch(task_id, run_id)
    )
    if failure == "inspect":
        git.inspect.side_effect = RuntimeError("inspection failed")
    if failure == "flush":
        session.flush.side_effect = RuntimeError("database failed")
    error = WorktreeError if failure == "identity" else RuntimeError
    with pytest.raises(
        error,
        match={
            "inspect": "inspection failed",
            "identity": "identity is invalid",
            "flush": "database failed",
        }[failure],
    ):
        await WorktreeService(root, git, retention_days=3).prepare(
            session,
            task_id=task_id,
            run_id=run_id,
            project=SimpleNamespace(id="project", repository_path=repo, default_branch="main"),
            owner="executor",
        )
    git.remove_worktree.assert_awaited_once_with(repo, path)
    assert not path.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("missing", ["package", "goal"])
async def test_prepare_refuses_broken_materialization_links(tmp_path, missing):
    from types import SimpleNamespace

    from vuzol.execution.worktrees import WorktreeError, WorktreeService

    session, git = AsyncMock(), AsyncMock()
    materialization = SimpleNamespace(work_package_id=uuid.uuid4())
    package = SimpleNamespace(managed_goal_id=uuid.uuid4(), integration_branch=None)
    session.scalar.side_effect = [None, materialization, None if missing == "package" else package]
    session.get.return_value = None
    git.repository_identity.return_value = ("identity", None)
    git.resolve_commit.return_value = "a" * 40
    with pytest.raises(
        WorktreeError, match="no work package" if missing == "package" else "no goal"
    ):
        await WorktreeService(tmp_path / "worktrees", git, retention_days=3).prepare(
            session,
            task_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            project=SimpleNamespace(id="project", repository_path=tmp_path, default_branch="main"),
            owner="executor",
        )
    git.add_worktree.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("missing_branch", [False, True])
async def test_managed_worktree_pins_goal_branch_and_falls_back_to_default_commit(
    tmp_path, missing_branch
):
    from types import SimpleNamespace

    from vuzol.execution.worktrees import WorktreeService

    root = tmp_path / "worktrees"
    task_id, run_id = uuid.uuid4(), uuid.uuid4()
    package = SimpleNamespace(managed_goal_id=uuid.uuid4(), integration_branch=None)
    materialization = SimpleNamespace(work_package_id=uuid.uuid4())
    session, git = AsyncMock(), AsyncMock()
    session.add = MagicMock()
    session.scalar.side_effect = [None, materialization, package]
    session.get.return_value = SimpleNamespace(integration_branch="vuzol/goal/goal-17")
    git.repository_identity.return_value = (
        "d" * 64,
        "https://user:password@host.test/repo?token=secret",
    )
    git.resolve_commit.side_effect = (
        ["a" * 40, GitError("missing branch") if missing_branch else "b" * 40]
        + (["a" * 40] if missing_branch else [])
        + ["a" * 40 if missing_branch else "b" * 40]
    )
    path = root / "project" / str(run_id)

    async def add(*args):
        path.mkdir()

    git.add_worktree.side_effect = add
    base = "a" * 40 if missing_branch else "b" * 40
    git.inspect.return_value = SimpleNamespace(head=base, branch=worktree_branch(task_id, run_id))

    async def flush():
        session.add.call_args.args[0].id = uuid.uuid4()

    session.flush.side_effect = flush
    reference = await WorktreeService(root, git, retention_days=3).prepare(
        session,
        task_id=task_id,
        run_id=run_id,
        project=SimpleNamespace(id="project", repository_path=tmp_path, default_branch="main"),
        owner="executor",
    )
    assert reference.path == path and reference.base_commit == base
    assert package.integration_branch == "vuzol/goal/goal-17"
    assert package.integration_target_branch == package.integration_branch
    assert package.integration_base_commit == package.integration_head_commit == base
    row = session.add.call_args.args[0]
    assert row.source_remote == "https://host.test/repo"
    assert row.default_branch == package.integration_branch
    assert row.expected_target_head == base
    git.add_worktree.assert_awaited_once_with(
        tmp_path, path, worktree_branch(task_id, run_id), base
    )


@pytest.mark.anyio
@pytest.mark.parametrize("problem", ["mismatched_record", "unrecorded_path"])
async def test_prepare_refuses_mismatched_or_unrecorded_worktree_identity(tmp_path, problem):
    from types import SimpleNamespace

    from vuzol.execution.worktrees import WorktreeError, WorktreeService

    root = tmp_path / "worktrees"
    task_id, run_id = uuid.uuid4(), uuid.uuid4()
    path = root / "project" / str(run_id)
    path.mkdir(parents=True)
    sentinel = path / "result"
    sentinel.write_text("keep existing work")
    existing = (
        _persisted_worktree(path, run_id=run_id, task_id=uuid.uuid4())
        if problem == "mismatched_record"
        else None
    )
    session, git = AsyncMock(), AsyncMock()
    session.scalar.side_effect = [existing, None]
    git.repository_identity.return_value = ("identity", None)
    git.resolve_commit.return_value = "a" * 40
    service = WorktreeService(root, git, retention_days=3)
    # Existing/unrecorded paths must never be overwritten by prepare.
    with pytest.raises(
        WorktreeError,
        match="identity does not match" if existing else "already exists without a record",
    ):
        await service.prepare(
            session,
            task_id=task_id,
            run_id=run_id,
            project=SimpleNamespace(id="project", repository_path=tmp_path, default_branch="main"),
            owner="executor",
        )
    assert sentinel.read_text() == "keep existing work"
    git.add_worktree.assert_not_awaited()
    git.remove_worktree.assert_not_awaited()
