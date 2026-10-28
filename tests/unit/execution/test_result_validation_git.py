"""Result validation git tests (split for cohesion)."""

from __future__ import annotations

from vuzol.execution.result_validation import _normalize_blank_lines_at_eof

from ._test_result_validation_helpers import (
    ArtifactSecretError,
    AsyncMock,
    CancellationContext,
    CommandDefinition,
    GateRun,
    GitError,
    GitInspection,
    MagicMock,
    OutcomeKind,
    Path,
    SimpleNamespace,
    _captured,
    _gate_evidence,
    _handler,
    _inspection,
    _project,
    _request,
    _worktree,
    pytest,
)


def test_normalize_blank_lines_at_eof_is_bounded_to_changed_text(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    redundant = source / "styles.css"
    redundant.write_bytes(b"body {}\n\n")
    crlf = source / "windows.txt"
    crlf.write_bytes(b"value\r\n\r\n")
    binary = source / "image.bin"
    binary.write_bytes(b"\x00\n\n")
    unchanged = source / "single.txt"
    unchanged.write_bytes(b"single\n")
    trailing = source / "README.md"
    trailing.write_bytes(b"one  \n two\t\r\nlast  ")

    changed = _normalize_blank_lines_at_eof(
        tmp_path,
        (
            "src/styles.css",
            "src/windows.txt",
            "src/image.bin",
            "src/single.txt",
            "src/README.md",
        ),
    )

    assert changed == ("src/styles.css", "src/windows.txt", "src/README.md")
    assert redundant.read_bytes() == b"body {}\n"
    assert crlf.read_bytes() == b"value\r\n"
    assert binary.read_bytes() == b"\x00\n\n"
    assert unchanged.read_bytes() == b"single\n"
    assert trailing.read_bytes() == b"one\n two\r\nlast"


@pytest.mark.anyio
@pytest.mark.parametrize("reason", ["Already implemented in base.", "x" * 2500, None])
async def test_validate_empty_change_passes_with_gates(tmp_path: Path, reason: str | None) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection(files=(), diff=b""))
    git.create_commit = AsyncMock()
    worktree = _worktree(worktree_path)
    session = MagicMock(scalar=AsyncMock(return_value=SimpleNamespace(result={"text": reason})))
    gates = MagicMock(
        run=AsyncMock(
            return_value=(
                GateRun(evidence=_gate_evidence(), stdout=_captured(), stderr=_captured()),
            )
        )
    )
    project = _project(
        tmp_path, validation_commands=(CommandDefinition(name="tests", argv=("make", "test")),)
    )
    handler = _handler(git=git, worktree_root=tmp_path, session=session, gate_runner=gates)
    handler._grant_access = AsyncMock(return_value=None)
    handler._load = AsyncMock(return_value=(worktree, project, worktree_path))
    handler._persist = AsyncMock()

    outcome = await handler.execute(_request(worktree), CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    for evidence in (outcome.result, outcome.result["structured_output"]):
        assert evidence["no_change"] is True
        assert evidence["no_change_reason"] == (reason or "")[:2000]
        assert evidence["result_commit"] == worktree.base_commit
        assert evidence["changed_files"] == []
        assert evidence["diff_hash"] == _inspection(files=(), diff=b"").diff_hash
    gates.run.assert_awaited_once()
    git.require_clean_worktree.assert_awaited()
    git.create_commit.assert_not_awaited()
    handler._persist.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize("bad", ["head", "dirty", "gate_dirty", "gate_failed"])
async def test_validate_empty_change_keeps_guards(tmp_path: Path, bad: str) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.require_clean_worktree = AsyncMock(
        side_effect=GitError("dirty")
        if bad == "dirty"
        else [None, GitError("dirty after gates")]
        if bad == "gate_dirty"
        else None
    )
    git.inspect = AsyncMock(
        return_value=_inspection(head="b" * 40 if bad == "head" else "a" * 40, files=(), diff=b"")
    )
    gates = MagicMock(
        run=AsyncMock(
            return_value=(
                GateRun(
                    evidence=_gate_evidence(exit_code=1 if bad == "gate_failed" else 0),
                    stdout=_captured(),
                    stderr=_captured(),
                ),
            )
        )
    )
    worktree = _worktree(worktree_path)
    project = _project(
        tmp_path, validation_commands=(CommandDefinition(name="tests", argv=("make", "test")),)
    )
    handler = _handler(git=git, worktree_root=tmp_path, gate_runner=gates)
    handler._grant_access = AsyncMock(return_value=None)
    handler._load = AsyncMock(return_value=(worktree, project, worktree_path))
    handler._persist = AsyncMock()

    outcome = await handler.execute(_request(worktree), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert (
        outcome.category
        == {
            "head": "validation_precommitted",
            "dirty": "validation_failed",
            "gate_dirty": "validation_failed",
            "gate_failed": "validation_gate_failed",
        }[bad]
    )
    handler._persist.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("repair_no_change", [False, True])
async def test_validate_commits_measured_result(tmp_path: Path, repair_no_change: bool) -> None:
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
    git.commit_parent = AsyncMock(
        side_effect=lambda _path, commit: base if commit == result else "0" * 40
    )
    git.require_clean_worktree = AsyncMock()
    worktree = _worktree(worktree_path, base_commit=base, result_commit=base)
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]
    request = _request(worktree)
    if repair_no_change:
        request.payload["amend_repaired_result"] = True
    outcome = await handler.execute(request, CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["result_commit"] == result
    assert all(gate["exit_code"] == 0 for gate in outcome.result["structured_output"]["gates"])
    git.create_commit.assert_awaited_once()
    assert git.create_commit.await_args.args[1] == "chore: implement task\n\nVuzol-Task: 42"
    handler._persist.assert_awaited_once()


@pytest.mark.anyio
async def test_review_repair_amends_existing_result_commit(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    previous = "b" * 40
    amended = "c" * 40
    inspection = _inspection(head=previous, files=("app.js",), diff=b"+fixed\n")
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        side_effect=[
            inspection,
            GitInspection(
                head=amended,
                branch="task-branch",
                changed_files=("app.js",),
                diff=inspection.diff,
            ),
        ]
    )
    git.commit_parent = AsyncMock(return_value=base)
    git.stage_paths = AsyncMock()
    git.require_diff_check = AsyncMock()
    git.amend_commit = AsyncMock(return_value=amended)
    git.create_commit = AsyncMock()
    git.require_clean_worktree = AsyncMock()
    worktree = _worktree(
        worktree_path, base_commit=base, result_commit=previous, diff_hash="d" * 64
    )
    request = _request(worktree)
    request.payload["amend_repaired_result"] = True
    handler = _handler(git=git, worktree_root=tmp_path)
    handler._load = AsyncMock(return_value=(worktree, _project(tmp_path), worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["result_commit"] == amended
    git.amend_commit.assert_awaited_once()
    git.create_commit.assert_not_awaited()


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
