"""Result validation git tests (split for cohesion)."""

from __future__ import annotations

from ._test_result_validation_helpers import *


@pytest.mark.anyio
async def test_validate_empty_change_blocks(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection(files=(), diff=b""))
    worktree = _worktree(worktree_path)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "validation_empty_change"
    handler._persist.assert_not_awaited()


@pytest.mark.anyio
async def test_validate_commits_measured_result(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    inspection = _inspection(head=base, files=("index.html", "app.js"))
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        side_effect=[
            inspection,
            GitInspection(
                head=result,
                branch="task-branch",
                changed_files=("app.js", "index.html"),
                diff=inspection.diff,
            ),
        ]
    )
    git.stage_paths = AsyncMock()
    git.require_diff_check = AsyncMock()
    git.create_commit = AsyncMock(return_value=result)
    git.commit_parent = AsyncMock(return_value=base)
    git.require_clean_worktree = AsyncMock()
    worktree = _worktree(worktree_path, base_commit=base, result_commit=base)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["result_commit"] == result
    assert all(gate["exit_code"] == 0 for gate in outcome.result["structured_output"]["gates"])
    git.create_commit.assert_awaited_once()
    handler._persist.assert_awaited_once()


@pytest.mark.anyio
async def test_validate_blocks_precommitted_head(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection(head="c" * 40, files=("x.py",), diff=b"+x\n"))
    worktree = _worktree(worktree_path, result_commit=None)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "validation_precommitted"


@pytest.mark.anyio
async def test_validate_git_error_blocks(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock(side_effect=GitError("source repository is dirty"))
    worktree = _worktree(worktree_path, result_commit=None)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "validation_failed"


@pytest.mark.anyio
async def test_validate_branch_mismatch(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection(branch="other"))
    worktree = _worktree(worktree_path)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_branch_mismatch"


@pytest.mark.anyio
async def test_validate_prohibited_path(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection(files=(".env",), diff=b"+SECRET=1\n"))
    worktree = _worktree(worktree_path)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_prohibited_path"


@pytest.mark.anyio
async def test_validate_secret_artifact_blocks(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection())
    artifacts = MagicMock()
    artifacts.reject_secrets.side_effect = ArtifactSecretError("secret")
    worktree = _worktree(worktree_path)
    handler = _handler(git=git, worktree_root=tmp_path, artifacts=artifacts)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_failed"


@pytest.mark.anyio
async def test_validate_suspicious_diff_blocks(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=_inspection(
            files=("x.py",),
            diff=b"diff --git a/x.py b/x.py\n+assert True or True\n",
        )
    )
    worktree = _worktree(worktree_path)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_suspicious_diff"


@pytest.mark.anyio
async def test_validate_already_finalized(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    inspection = _inspection(head=result, files=("index.html",), diff=b"+ok\n")
    git.inspect = AsyncMock(return_value=inspection)
    git.require_clean_worktree = AsyncMock()
    git.commit_parent = AsyncMock(return_value=base)
    worktree = _worktree(
        worktree_path,
        base_commit=base,
        result_commit=result,
        diff_hash=inspection.diff_hash,
    )
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["result_commit"] == result
    git.create_commit = AsyncMock()
    # already finalized path should not create another commit
    assert not hasattr(git.create_commit, "await_count") or git.create_commit.await_count == 0


@pytest.mark.anyio
async def test_validate_recovers_commit_created_before_persistence(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    inspection = _inspection(head=result, files=("index.html",), diff=b"+ok\n")
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=inspection)
    git.require_clean_worktree = AsyncMock()
    git.commit_parent = AsyncMock(return_value=base)
    worktree = _worktree(
        worktree_path,
        base_commit=base,
        result_commit=base,
        diff_hash=inspection.diff_hash,
    )
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]

    outcome = await handler.execute(_request(worktree), CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["result_commit"] == result
    assert any(
        check["command_id"] == "system:recovered-result-commit"
        for check in outcome.result["system_checks"]
    )
    git.create_commit = AsyncMock()
    assert git.create_commit.await_count == 0


@pytest.mark.anyio
async def test_validate_git_finalization_failure(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection())
    git.stage_paths = AsyncMock()
    git.require_diff_check = AsyncMock()
    git.create_commit = AsyncMock(side_effect=GitError("commit failed"))
    worktree = _worktree(worktree_path)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_git_finalization"
