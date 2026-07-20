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

__all__ = [
    "RESULT_VALIDATION_SCHEMA",
    "ArtifactSecretError",
    "AsyncContext",
    "AsyncMock",
    "CancellationContext",
    "CapturedOutput",
    "CommandDefinition",
    "GateEvidence",
    "GateRun",
    "GitError",
    "GitInspection",
    "LeaseLost",
    "LeaseToken",
    "MagicMock",
    "OutcomeKind",
    "Path",
    "ResultValidationError",
    "ResultValidationHandler",
    "SimpleNamespace",
    "StepExecutionRequest",
    "StepRecord",
    "StepStatus",
    "SystemCheck",
    "WorktreeDeliveryState",
    "_captured",
    "_gate_evidence",
    "_handler",
    "_inspection",
    "_json_bytes",
    "_lease",
    "_project",
    "_request",
    "_success_payload",
    "_worktree",
    "annotations",
    "prohibited_paths",
    "pytest",
    "resolve_trusted_gates",
    "uuid",
]


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
