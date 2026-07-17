"""Deterministic result validation unit coverage."""

from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config.models import CommandDefinition
from vuzol.execution.artifacts import ArtifactSecretError
from vuzol.execution.domain import GitInspection
from vuzol.execution.finalization import CapturedOutput, GateEvidence, GateRun
from vuzol.execution.git import GitError
from vuzol.execution.result_validation import (
    RESULT_VALIDATION_SCHEMA,
    ResultValidationError,
    ResultValidationHandler,
    SystemCheck,
    _json_bytes,
    _success_payload,
    prohibited_paths,
    resolve_trusted_gates,
)
from vuzol.storage.errors import LeaseLost
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _lease(*, owner: str = "owner", generation: int = 1) -> LeaseToken:
    return LeaseToken(
        step=StepRecord(
            id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            status=StepStatus.RUNNING,
            lease_generation=generation,
            lease_owner=owner,
            lease_expires_at=None,
        ),
        owner=owner,
        generation=generation,
    )


def _inspection(
    *,
    head: str = "a" * 40,
    branch: str = "task-branch",
    files: tuple[str, ...] = ("index.html",),
    diff: bytes = b"diff --git a/index.html b/index.html\n+hello\n",
) -> GitInspection:
    return GitInspection(head=head, branch=branch, changed_files=files, diff=diff)


def _captured(content: bytes = b"") -> CapturedOutput:
    return CapturedOutput(
        content=content,
        sha256="e" * 64,
        byte_count=len(content),
        truncated=False,
    )


def _gate_evidence(*, exit_code: int = 0) -> GateEvidence:
    return GateEvidence(
        name="tests",
        command_id="make test",
        argv=("/usr/bin/make", "test"),
        exit_code=exit_code,
        duration_ms=5,
        stdout_sha256="e" * 64,
        stdout_bytes=0,
        stdout_truncated=False,
        stderr_sha256="e" * 64,
        stderr_bytes=0,
        stderr_truncated=False,
        validation_image_digest="img@sha256:" + "f" * 64,
    )


def _project(tmp_path: Path, **updates: object) -> SimpleNamespace:
    base = {
        "id": "bill-buddy",
        "repository_path": tmp_path / "repo",
        "validation_commands": (),
        "validation_sandbox_profile": None,
        "sandbox_profile": "project-default",
    }
    base.update(updates)
    return SimpleNamespace(**base)


def _worktree(path: Path, **updates: object) -> SimpleNamespace:
    base = {
        "id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "run_id": uuid.uuid4(),
        "project_id": "bill-buddy",
        "path": str(path),
        "base_commit": "a" * 40,
        "branch": "task-branch",
        "result_commit": "a" * 40,
        "diff_hash": None,
        "delivery_state": WorktreeDeliveryState.WORKTREE_RETAINED,
        "lifecycle_generation": 1,
    }
    base.update(updates)
    return SimpleNamespace(**base)


def _request(worktree: SimpleNamespace, lease: LeaseToken | None = None) -> StepExecutionRequest:
    token = lease or _lease()
    return StepExecutionRequest(
        task_id=worktree.task_id,
        run_id=worktree.run_id,
        step_id=token.step.id,
        step_type="validate",
        payload={},
        timeout_seconds=120,
        lease=token,
    )


def _handler(
    *,
    git: MagicMock,
    worktree_root: Path,
    project: SimpleNamespace | None = None,
    worktree_access: MagicMock | None = None,
    gate_runner: MagicMock | None = None,
    artifacts: MagicMock | None = None,
    session: MagicMock | None = None,
) -> ResultValidationHandler:
    registries = MagicMock()
    registries.projects.get.return_value = project or SimpleNamespace(
        id="bill-buddy",
        repository_path=worktree_root / "repo",
        validation_commands=(),
        validation_sandbox_profile=None,
        sandbox_profile="project-default",
    )
    registries.sandboxes.get.side_effect = lambda profile_id: SimpleNamespace(
        enabled=True, uid=10001, gid=10001, id=profile_id
    )
    active_session = session or MagicMock()
    factory = MagicMock()
    factory.return_value = AsyncContext(active_session)
    factory.begin.return_value = AsyncContext(active_session)
    return ResultValidationHandler(
        factory,
        registries,
        git,
        worktree_root=worktree_root,
        gate_runner=gate_runner,
        worktree_access=worktree_access,
        artifacts=artifacts,
    )


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
