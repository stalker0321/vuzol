import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config import DeliveryMode, GitDeliveryPolicy
from vuzol.execution.git import GitError
from vuzol.execution.result_apply import ResultApplyHandler
from vuzol.storage.types import ApprovalStatus
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.result_approval import envelope_hash


def project_policy(*, allowed: bool = True, approval_required: bool = True) -> object:
    return SimpleNamespace(
        repository_path=Path("/managed/repository"),
        git_delivery=GitDeliveryPolicy(
            allowed_modes=(
                frozenset({DeliveryMode.APPLY}) if allowed else frozenset({DeliveryMode.RETAIN})
            ),
            approval_required=(
                frozenset({DeliveryMode.APPLY}) if approval_required and allowed else frozenset()
            ),
        ),
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "failure",
    ("not_allowed", "approval_not_required", "revision_changed", "identity_changed", "git"),
)
async def test_result_apply_fails_closed_before_recording_delivery(failure: str) -> None:
    registries = MagicMock(revision="a" * 64)
    registries.projects.get.return_value = project_policy(
        allowed=failure != "not_allowed",
        approval_required=failure != "approval_not_required",
    )
    git = MagicMock()
    git.repository_identity = AsyncMock(
        return_value=(("c" if failure == "identity_changed" else "b") * 64, None)
    )
    git.apply_result = AsyncMock()
    if failure == "git":
        git.apply_result.side_effect = GitError("target branch changed")
    handler = ResultApplyHandler(MagicMock(), registries, git)
    approval_id = uuid.uuid4()
    worktree = SimpleNamespace(id=uuid.uuid4(), project_id="vuzol", path="/retained/result")
    envelope = {
        "configuration_revision": "d" * 64 if failure == "revision_changed" else "a" * 64,
        "repository_identity_hash": "b" * 64,
        "target_branch": "main",
        "expected_target_head": "e" * 40,
        "result_commit": "f" * 40,
    }
    handler._load = AsyncMock(return_value=(approval_id, envelope, worktree))  # type: ignore[method-assign]
    handler._record_applied = AsyncMock()  # type: ignore[method-assign]

    outcome = await handler.execute(MagicMock(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "approved_result_not_applied"
    handler._record_applied.assert_not_awaited()


@pytest.mark.anyio
async def test_result_apply_records_the_exact_successful_operation() -> None:
    registries = MagicMock(revision="a" * 64)
    registries.projects.get.return_value = project_policy()
    git = MagicMock()
    git.repository_identity = AsyncMock(return_value=("b" * 64, None))
    git.apply_result = AsyncMock(return_value=True)
    handler = ResultApplyHandler(MagicMock(), registries, git)
    approval_id = uuid.uuid4()
    worktree = SimpleNamespace(id=uuid.uuid4(), project_id="vuzol", path="/retained/result")
    envelope = {
        "configuration_revision": "a" * 64,
        "repository_identity_hash": "b" * 64,
        "target_branch": "main",
        "expected_target_head": "e" * 40,
        "result_commit": "f" * 40,
    }
    handler._load = AsyncMock(return_value=(approval_id, envelope, worktree))  # type: ignore[method-assign]
    handler._record_applied = AsyncMock()  # type: ignore[method-assign]

    outcome = await handler.execute(MagicMock(), CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    git.apply_result.assert_awaited_once()
    handler._record_applied.assert_awaited_once()


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("step", "identity", "approval", "worktree", "changed"))
async def test_result_apply_load_rejects_stale_persistence(failure: str) -> None:
    approval_id = uuid.uuid4()
    envelope = {
        "step_id": str(uuid.uuid4()),
        "project_id": "vuzol",
        "base_commit": "a" * 40,
        "result_commit": "b" * 40,
        "diff_hash": "c" * 64,
    }
    approval = SimpleNamespace(status=MagicMock(), action_envelope_hash="unused")
    step = SimpleNamespace(
        id=uuid.UUID(envelope["step_id"]),
        payload={
            "approval_id": str(approval_id),
            "action_envelope": envelope,
        },
    )
    approval.status = ApprovalStatus.APPROVED
    approval.action_envelope_hash = envelope_hash(envelope)
    worktree = SimpleNamespace(
        project_id="other" if failure == "changed" else "vuzol",
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash="c" * 64,
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=(
            None if failure == "step" else step,
            None if failure == "approval" else approval,
        )
    )
    if failure == "identity":
        step.payload = {}
    session.scalar = AsyncMock(return_value=None if failure == "worktree" else worktree)
    factory = MagicMock(return_value=AsyncContext(session))
    handler = ResultApplyHandler(factory, MagicMock(), MagicMock())

    with pytest.raises((LookupError, ValueError)):
        await handler._load(MagicMock(step_id=step.id, run_id=uuid.uuid4(), task_id=uuid.uuid4()))
