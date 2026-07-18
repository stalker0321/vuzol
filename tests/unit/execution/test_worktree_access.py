"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from ._execution_helpers import *


@pytest.mark.anyio
async def test_worktree_acl_is_bounded_inherited_and_revoked(tmp_path: Path) -> None:
    root = tmp_path / "worktrees"
    worktree = root / "task"
    other = root / "other"
    worktree.mkdir(parents=True)
    other.mkdir()
    _git(worktree, "init", "-b", "main")
    existing = worktree / "existing.txt"
    existing.write_text("before\n")
    await _set_numeric_acl(existing, "u:60003:r--")
    other_file = other / "unrelated.txt"
    other_file.write_text("unrelated\n")
    other_acl = _numeric_acl(other_file)
    resolver = _FixedIdentityResolver()
    manager = _TestAccessManager(root, resolver)  # type: ignore[arg-type]
    await manager.preflight(((10_001, 10_001),))

    lease = await manager.grant(worktree, sandbox_uid=10_001, sandbox_gid=10_001)
    uid = resolver.host_uid
    assert f"user:{uid}:rw-" in _numeric_acl(existing)
    root_acl = _numeric_acl(worktree)
    assert f"user:{uid}:rwx" in root_acl
    assert f"default:user:{uid}:rwx" in root_acl
    assert f"user:{uid}:" not in _numeric_acl(worktree / ".git")
    assert os.access(existing, os.R_OK | os.W_OK)
    assert existing.stat().st_mode & stat.S_IWOTH == 0

    created_directory = worktree / "created"
    created_directory.mkdir()
    created_file = created_directory / "new.txt"
    created_file.write_text("new\n")
    assert f"user:{uid}:" in _numeric_acl(created_directory)
    assert f"default:user:{uid}:rwx" in _numeric_acl(created_directory)
    assert f"user:{uid}:rwx" in _numeric_acl(created_file)
    assert _numeric_acl(other_file) == other_acl

    await lease.revoke()
    await lease.revoke()
    assert lease.revoked
    assert f"user:{uid}:" not in _numeric_acl(existing)
    assert "user:60003:r--" in _numeric_acl(existing)
    assert f"user:{uid}:" not in _numeric_acl(created_file)
    assert f"default:user:{uid}:" not in _numeric_acl(created_directory)
    assert existing.read_text() == "before\n"
    assert created_file.read_text() == "new\n"
    assert _numeric_acl(other_file) == other_acl


@pytest.mark.anyio
async def test_worktree_acl_rejects_symlinks_and_unavailable_support(
    tmp_path: Path,
) -> None:
    root = tmp_path / "worktrees"
    worktree = root / "task"
    worktree.mkdir(parents=True)
    _git(worktree, "init", "-b", "main")
    (worktree / "escape").symlink_to(tmp_path)
    manager = _TestAccessManager(root, _FixedIdentityResolver())  # type: ignore[arg-type]
    with pytest.raises(WorktreeAccessError, match="symbolic link"):
        await manager.grant(worktree, sandbox_uid=10_001, sandbox_gid=10_001)
    with pytest.raises(PathViolation):
        await manager.grant(tmp_path, sandbox_uid=10_001, sandbox_gid=10_001)

    manager._setfacl = tmp_path / "missing-setfacl"
    with pytest.raises(WorktreeAccessError, match="unavailable"):
        await manager.preflight(((10_001, 10_001),))


@pytest.mark.anyio
async def test_worktree_acl_reclaims_entries_before_rejecting_new_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "worktrees"
    worktree = root / "task"
    worktree.mkdir(parents=True)
    _git(worktree, "init", "-b", "main")
    existing = worktree / "existing.txt"
    existing.write_text("before\n")
    resolver = _FixedIdentityResolver()
    verifying_manager = _TestAccessManager(root, resolver)  # type: ignore[arg-type]
    lease = await verifying_manager.grant(worktree, sandbox_uid=10_001, sandbox_gid=10_001)
    (worktree / "escape").symlink_to(tmp_path)
    with pytest.raises(WorktreeAccessError, match="introduced a symbolic link"):
        await lease.revoke()
    assert f"user:{resolver.host_uid}:" not in _numeric_acl(existing)
    assert (worktree / ".git").exists()


@pytest.mark.anyio
async def test_worktree_acl_rejects_changed_mapping_then_revokes_with_original_mapping(
    tmp_path: Path,
) -> None:
    root = tmp_path / "worktrees"
    worktree = root / "task"
    worktree.mkdir(parents=True)
    _git(worktree, "init", "-b", "main")
    (worktree / "file.txt").write_text("value\n")
    resolver = _FixedIdentityResolver()
    verifying_manager = _TestAccessManager(root, resolver)  # type: ignore[arg-type]
    lease = await verifying_manager.grant(worktree, sandbox_uid=10_001, sandbox_gid=10_001)
    original_uid = resolver.host_uid
    resolver.host_uid += 1
    with pytest.raises(WorktreeAccessError, match="mapping changed"):
        await lease.revoke()
    resolver.host_uid = original_uid
    await lease.revoke()
    assert lease.revoked


def test_acl_xattr_errors_are_safe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.touch()

    def denied_getxattr(*_args: Any, **_kwargs: Any) -> bytes:
        raise OSError(errno.EACCES, "denied")

    monkeypatch.setattr(os, "getxattr", denied_getxattr)
    with pytest.raises(WorktreeAccessError, match="inspection failed"):
        _get_xattr(target, "system.posix_acl_access")
    monkeypatch.undo()

    def denied_setxattr(*_args: Any, **_kwargs: Any) -> None:
        raise OSError(errno.EACCES, "denied")

    monkeypatch.setattr(os, "setxattr", denied_setxattr)
    with pytest.raises(WorktreeAccessError, match="restoration failed"):
        _set_xattr(target, "system.posix_acl_access", b"value")


@pytest.mark.anyio
async def test_worktree_access_additional_fail_closed_boundaries(tmp_path: Path) -> None:
    root = tmp_path / "worktrees"
    root.mkdir()
    resolver = _FixedIdentityResolver()
    manager = _TestAccessManager(root, resolver)  # type: ignore[arg-type]
    with pytest.raises(WorktreeAccessError, match="no sandbox identities"):
        await manager.preflight(())

    no_git = root / "no-git"
    no_git.mkdir()
    with pytest.raises(WorktreeAccessError, match="Git metadata is missing"):
        await manager.grant(no_git, sandbox_uid=10_001, sandbox_gid=10_001)

    unsafe_file = root / "unsafe-file"
    unsafe_file.mkdir()
    _git(unsafe_file, "init", "-b", "main")
    tracked = unsafe_file / "tracked.txt"
    tracked.write_text("tracked\n")
    await _set_numeric_acl(tracked, f"u:{resolver.host_uid}:rw-")
    with pytest.raises(WorktreeAccessError, match="pre-existing ACL"):
        await manager.grant(unsafe_file, sandbox_uid=10_001, sandbox_gid=10_001)

    unsafe_acl = root / "unsafe-acl"
    unsafe_acl.mkdir()
    _git(unsafe_acl, "init", "-b", "main")
    await _set_numeric_acl(unsafe_acl / ".git", f"u:{resolver.host_uid}:rw-")
    with pytest.raises(WorktreeAccessError, match="Git metadata already grants"):
        await manager.grant(unsafe_acl, sandbox_uid=10_001, sandbox_gid=10_001)

    special = root / "special"
    special.mkdir()
    _git(special, "init", "-b", "main")
    os.mkfifo(special / "pipe")
    with pytest.raises(WorktreeAccessError, match="non-regular"):
        await manager.grant(special, sandbox_uid=10_001, sandbox_gid=10_001)

    with pytest.raises(WorktreeAccessError, match="command failed"):
        await manager._run("/usr/bin/false")

    plain = tmp_path / "plain"
    plain.write_text("plain\n")
    assert _base_acl_mode(plain) == stat.S_IMODE(plain.stat().st_mode)


@pytest.mark.anyio
async def test_worktree_preflight_rejects_unverified_acl_result(tmp_path: Path) -> None:
    root = tmp_path / "worktrees"
    root.mkdir()

    class MissingAclResult(_TestAccessManager):
        async def _run(self, *argv: object, capture: bool = False) -> str:
            if Path(str(argv[0])).name == "getfacl":
                return ""
            return await super()._run(*argv, capture=capture)

    manager = MissingAclResult(root, _FixedIdentityResolver())  # type: ignore[arg-type]
    with pytest.raises(WorktreeAccessError, match="did not retain"):
        await manager.preflight(((10_001, 10_001),))


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("provider_failure", "cancellation"))
async def test_provider_handler_revokes_acl_on_provider_failure_or_cancellation(
    failure: str,
) -> None:
    from vuzol.providers.handlers import ProviderStepHandler
    from vuzol.workflows.domain import OutcomeKind, StepOutcome

    access = MagicMock()
    access.revoke = AsyncMock()
    handler = ProviderStepHandler(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        worktrees=MagicMock(),
        finalizer=MagicMock(),
        worktree_access=MagicMock(),
    )
    provider_request = MagicMock(task_draft={"step09a_capsule": {}})
    handler._build_request = AsyncMock(  # type: ignore[method-assign]
        return_value=(provider_request, "grok-a", uuid.uuid4(), "revision")
    )
    handler._grant_worktree_access = AsyncMock(return_value=access)  # type: ignore[method-assign]
    error: BaseException = (
        asyncio.CancelledError() if failure == "cancellation" else RuntimeError("provider failure")
    )
    handler._execute_built = AsyncMock(side_effect=error)  # type: ignore[method-assign]
    handler._provider_launch_exists = AsyncMock(return_value=False)  # type: ignore[method-assign]
    handler._unexpected_pre_provider_failure = AsyncMock(  # type: ignore[method-assign]
        return_value=StepOutcome(
            kind=OutcomeKind.PERMANENT_FAILURE,
            result={},
            category="pre_provider_unexpected",
            summary="RuntimeError",
        )
    )
    request = MagicMock(step_type="execute_code")
    if failure == "cancellation":
        with pytest.raises(asyncio.CancelledError):
            await handler.execute(request, CancellationContext())
    else:
        outcome = await handler.execute(request, CancellationContext())
        assert outcome.category == "pre_provider_unexpected"
    access.revoke.assert_awaited_once()


@pytest.mark.anyio
async def test_provider_handler_grants_acl_for_regular_coding_task() -> None:
    from vuzol.providers.handlers import ProviderStepHandler
    from vuzol.workflows.domain import StepOutcome

    access = MagicMock()
    access.revoke = AsyncMock()
    handler = ProviderStepHandler(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        worktrees=MagicMock(),
        worktree_access=MagicMock(),
    )
    provider_request = MagicMock(task_draft={})
    handler._build_request = AsyncMock(  # type: ignore[method-assign]
        return_value=(provider_request, "codex-subscription-prod", uuid.uuid4(), "revision")
    )
    handler._grant_worktree_access = AsyncMock(return_value=access)  # type: ignore[method-assign]
    handler._execute_built = AsyncMock(  # type: ignore[method-assign]
        return_value=StepOutcome.succeeded({"text": "implemented"})
    )

    outcome = await handler.execute(MagicMock(step_type="execute_code"), CancellationContext())

    assert outcome.result == {"text": "implemented"}
    handler._grant_worktree_access.assert_awaited_once()
    access.revoke.assert_awaited_once()
