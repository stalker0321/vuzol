"""Result validation gates tests (split for cohesion)."""

from __future__ import annotations

from ._test_result_validation_helpers import *


def test_prohibited_paths_block_secret_names() -> None:
    assert prohibited_paths(
        ("src/app.py", ".env", "certs/server.pem", "ok.txt", "/abs", "a/../b", ".ssh/config")
    ) == (".env", "certs/server.pem", "/abs", "a/../b", ".ssh/config")


def test_resolve_trusted_gates_allowlists_only() -> None:
    project = SimpleNamespace(
        validation_commands=(
            CommandDefinition(name="tests", argv=("make", "test")),
            CommandDefinition(name="lint", argv=("make", "lint"), required=False),
        )
    )
    gates = resolve_trusted_gates(project)  # type: ignore[arg-type]
    assert len(gates) == 1
    assert gates[0].command_id == "make test"


def test_resolve_trusted_gates_rejects_arbitrary_argv() -> None:
    project = SimpleNamespace(
        validation_commands=(CommandDefinition(name="evil", argv=("curl", "https://x")),)
    )
    with pytest.raises(ResultValidationError) as error:
        resolve_trusted_gates(project)  # type: ignore[arg-type]
    assert error.value.category == "validation_untrusted_command"


def test_success_payload_and_json_bytes() -> None:
    payload = _success_payload(
        base_commit="a" * 40,
        result_commit="b" * 40,
        branch="main",
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
                duration_ms=10,
                stdout_sha256="d" * 64,
                stdout_bytes=0,
                stdout_truncated=False,
                stderr_sha256="d" * 64,
                stderr_bytes=0,
                stderr_truncated=False,
            ),
        ),
    )
    assert payload["schema_version"] == RESULT_VALIDATION_SCHEMA
    assert payload["structured_output"]["gates"]
    assert b"result-validation.v1" in _json_bytes({"schema_version": RESULT_VALIDATION_SCHEMA})


@pytest.mark.anyio
async def test_validate_trusted_gates_pass_and_revoke_access(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    inspection = _inspection(head=base, files=("x.py",), diff=b"+print(1)\n")
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        side_effect=[
            inspection,
            inspection,  # post-gate
            GitInspection(
                head=result,
                branch="task-branch",
                changed_files=("x.py",),
                diff=inspection.diff,
            ),
        ]
    )
    git.stage_paths = AsyncMock()
    git.require_diff_check = AsyncMock()
    git.create_commit = AsyncMock(return_value=result)
    git.commit_parent = AsyncMock(return_value=base)
    git.require_clean_worktree = AsyncMock()

    empty = _captured()
    evidence = GateEvidence(
        name="tests",
        command_id="make test",
        argv=("/usr/bin/make", "test"),
        exit_code=0,
        duration_ms=5,
        stdout_sha256="e" * 64,
        stdout_bytes=0,
        stdout_truncated=False,
        stderr_sha256="e" * 64,
        stderr_bytes=0,
        stderr_truncated=False,
        validation_image_digest="img@sha256:" + "f" * 64,
    )
    gate_runner = MagicMock()
    gate_runner.run = AsyncMock(
        return_value=(GateRun(evidence=evidence, stdout=empty, stderr=empty),)
    )
    lease = MagicMock()
    lease.revoke = AsyncMock()
    access = MagicMock()
    access.grant = AsyncMock(return_value=lease)

    project = _project(
        tmp_path,
        validation_commands=(CommandDefinition(name="tests", argv=("make", "test")),),
        validation_sandbox_profile="vuzol-validation",
    )
    worktree = _worktree(worktree_path, base_commit=base, result_commit=base)
    handler = _handler(
        git=git,
        worktree_root=tmp_path,
        project=project,
        gate_runner=gate_runner,
        worktree_access=access,
        artifacts=MagicMock(persist=AsyncMock()),
    )
    handler._load = AsyncMock(return_value=(worktree, project, worktree_path))  # type: ignore[method-assign]
    handler._persist = AsyncMock()  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert any(g["command_id"] == "make test" for g in outcome.result["gates"])
    access.grant.assert_awaited_once()
    lease.revoke.assert_awaited_once()


@pytest.mark.anyio
async def test_validate_trusted_gate_failure(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    inspection = _inspection()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=inspection)
    empty = _captured()
    evidence = GateEvidence(
        name="tests",
        command_id="make test",
        argv=("/usr/bin/make", "test"),
        exit_code=1,
        duration_ms=5,
        stdout_sha256="e" * 64,
        stdout_bytes=0,
        stdout_truncated=False,
        stderr_sha256="e" * 64,
        stderr_bytes=0,
        stderr_truncated=False,
    )
    gate_runner = MagicMock()
    gate_runner.run = AsyncMock(
        return_value=(GateRun(evidence=evidence, stdout=empty, stderr=empty),)
    )
    access = MagicMock()
    access.grant = AsyncMock(return_value=MagicMock(revoke=AsyncMock()))
    project = _project(
        tmp_path,
        validation_commands=(CommandDefinition(name="tests", argv=("make", "test")),),
        validation_sandbox_profile="vuzol-validation",
    )
    worktree = _worktree(worktree_path)
    handler = _handler(
        git=git,
        worktree_root=tmp_path,
        gate_runner=gate_runner,
        worktree_access=access,
    )
    handler._load = AsyncMock(return_value=(worktree, project, worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_gate_failed"


@pytest.mark.anyio
async def test_validate_gate_runner_missing(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(return_value=_inspection())
    project = _project(
        tmp_path,
        validation_commands=(CommandDefinition(name="tests", argv=("make", "test")),),
        validation_sandbox_profile="vuzol-validation",
    )
    worktree = _worktree(worktree_path)
    access = MagicMock()
    access.grant = AsyncMock(return_value=MagicMock(revoke=AsyncMock()))
    handler = _handler(git=git, worktree_root=tmp_path, worktree_access=access, gate_runner=None)
    handler._load = AsyncMock(return_value=(worktree, project, worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_gate_runner_unavailable"


@pytest.mark.anyio
async def test_validate_gate_mutates_worktree(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    before = _inspection(files=("x.py",), diff=b"+a\n")
    after = _inspection(files=("x.py", "y.py"), diff=b"+a\n+b\n")
    git = MagicMock()
    git.require_clean_source = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(side_effect=[before, after])
    empty = _captured()
    evidence = GateEvidence(
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
    )
    gate_runner = MagicMock()
    gate_runner.run = AsyncMock(
        return_value=(GateRun(evidence=evidence, stdout=empty, stderr=empty),)
    )
    access = MagicMock()
    access.grant = AsyncMock(return_value=MagicMock(revoke=AsyncMock()))
    project = _project(
        tmp_path,
        validation_commands=(CommandDefinition(name="tests", argv=("make", "test")),),
        validation_sandbox_profile="vuzol-validation",
    )
    worktree = _worktree(worktree_path)
    handler = _handler(
        git=git, worktree_root=tmp_path, gate_runner=gate_runner, worktree_access=access
    )
    handler._load = AsyncMock(return_value=(worktree, project, worktree_path))  # type: ignore[method-assign]
    outcome = await handler.execute(_request(worktree), CancellationContext())
    assert outcome.category == "validation_gate_mutated_worktree"
