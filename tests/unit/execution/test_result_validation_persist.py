"""Result validation persist tests (split for cohesion)."""

from __future__ import annotations

from ._test_result_validation_helpers import *


@pytest.mark.anyio
async def test_grant_access_failures(tmp_path: Path) -> None:
    handler = _handler(git=MagicMock(), worktree_root=tmp_path, worktree_access=None)
    with pytest.raises(ResultValidationError) as missing_access:
        await handler._grant_access(tmp_path / "wt", _project(tmp_path))  # type: ignore[arg-type]
    assert missing_access.value.category == "validation_access_unavailable"

    access = MagicMock()
    access.grant = AsyncMock(return_value=MagicMock())
    handler = _handler(git=MagicMock(), worktree_root=tmp_path, worktree_access=access)
    with pytest.raises(ResultValidationError) as missing_profile:
        await handler._grant_access(
            tmp_path / "wt",
            _project(tmp_path, validation_sandbox_profile=None),  # type: ignore[arg-type]
        )
    assert missing_profile.value.category == "validation_sandbox_missing"

    handler._registries.sandboxes.get = MagicMock(  # type: ignore[method-assign]
        return_value=SimpleNamespace(enabled=False, uid=1, gid=1)
    )
    with pytest.raises(ResultValidationError) as disabled:
        await handler._grant_access(
            tmp_path / "wt",
            _project(tmp_path, validation_sandbox_profile="vuzol-validation"),  # type: ignore[arg-type]
        )
    assert disabled.value.category == "validation_sandbox_disabled"


@pytest.mark.anyio
async def test_load_validates_lease_and_worktree(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    lease = _lease()
    worktree = _worktree(worktree_path, task_id=uuid.uuid4(), run_id=uuid.uuid4())
    request = _request(worktree, lease)
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=request.run_id,
    )
    run = SimpleNamespace(task_id=request.task_id)
    session = MagicMock()
    session.get = AsyncMock(side_effect=[step, run])
    session.scalar = AsyncMock(return_value=worktree)
    handler = _handler(git=MagicMock(), worktree_root=tmp_path, session=session)
    loaded = await handler._load(request)
    assert loaded[0] == worktree
    assert loaded[2] == worktree_path

    session.get = AsyncMock(return_value=None)
    with pytest.raises(LookupError):
        await handler._load(request)

    session.get = AsyncMock(
        side_effect=[
            SimpleNamespace(
                status=StepStatus.PENDING,
                lease_owner=lease.owner,
                lease_generation=lease.generation,
                run_id=request.run_id,
            ),
            run,
        ]
    )
    with pytest.raises(ValueError, match="not bound"):
        await handler._load(request)

    session.get = AsyncMock(side_effect=[step, run])
    session.scalar = AsyncMock(return_value=None)
    with pytest.raises(LookupError, match="prepared worktree"):
        await handler._load(request)

    bad_wt = _worktree(worktree_path, delivery_state=WorktreeDeliveryState.CLEANED)
    session.get = AsyncMock(side_effect=[step, run])
    session.scalar = AsyncMock(return_value=bad_wt)
    with pytest.raises(ValueError, match="not available"):
        await handler._load(request)


@pytest.mark.anyio
async def test_persist_writes_validation_results(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    worktree = _worktree(worktree_path, result_commit="a" * 40, lifecycle_generation=2)
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[SimpleNamespace(), worktree])
    session.add = MagicMock()
    session.flush = AsyncMock()
    artifacts = MagicMock()
    artifacts.persist = AsyncMock()
    handler = _handler(
        git=MagicMock(),
        worktree_root=tmp_path,
        session=session,
        artifacts=artifacts,
    )
    empty = _captured(b"out")
    evidence = _success_payload(
        base_commit="a" * 40,
        result_commit="b" * 40,
        branch="task-branch",
        changed_files=("x.py",),
        diff_hash="c" * 64,
        system_checks=[
            SystemCheck(name="git-facts", command_id="system:git-facts", exit_code=0, duration_ms=1)
        ],
        gates=(
            GateEvidence(
                name="tests",
                command_id="make test",
                argv=("/usr/bin/make", "test"),
                exit_code=0,
                duration_ms=1,
                stdout_sha256="e" * 64,
                stdout_bytes=0,
                stdout_truncated=False,
                stderr_sha256="e" * 64,
                stderr_bytes=0,
                stderr_truncated=False,
            ),
        ),
        gate_runs=(
            GateRun(
                evidence=GateEvidence(
                    name="tests",
                    command_id="make test",
                    argv=("/usr/bin/make", "test"),
                    exit_code=0,
                    duration_ms=1,
                    stdout_sha256="e" * 64,
                    stdout_bytes=0,
                    stdout_truncated=False,
                    stderr_sha256="e" * 64,
                    stderr_bytes=0,
                    stderr_truncated=False,
                ),
                stdout=empty,
                stderr=empty,
            ),
        ),
    )
    request = _request(worktree)
    await handler._persist(request, worktree_id=worktree.id, evidence=evidence)
    assert worktree.result_commit == "b" * 40
    assert worktree.diff_hash == "c" * 64
    assert worktree.lifecycle_generation == 3
    assert session.add.call_count >= 2
    assert artifacts.persist.await_count >= 2
    assert "_gate_runs" not in evidence


@pytest.mark.anyio
async def test_persist_rejects_stale_validation_lease(tmp_path: Path) -> None:
    session = MagicMock()
    session.scalar = AsyncMock(return_value=None)
    handler = _handler(git=MagicMock(), worktree_root=tmp_path, session=session)
    worktree = _worktree(tmp_path)

    with pytest.raises(LeaseLost, match="before persistence"):
        await handler._persist(
            _request(worktree),
            worktree_id=worktree.id,
            evidence={"result_commit": "b" * 40, "diff_hash": "c" * 64},
        )
