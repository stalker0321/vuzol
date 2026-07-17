"""Mechanical review step coverage."""

from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.domain import GitInspection
from vuzol.execution.git import GitError
from vuzol.review.domain import ReviewVerdictKind
from vuzol.review.handler import (
    ResultReviewHandler,
    effective_risk,
    mechanical_findings,
    runtime_risk,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import RiskLevel, StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _lease() -> LeaseToken:
    return LeaseToken(
        step=StepRecord(
            id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            status=StepStatus.RUNNING,
            lease_generation=1,
            lease_owner="owner",
            lease_expires_at=None,
        ),
        owner="owner",
        generation=1,
    )


def _request(task_id: uuid.UUID, run_id: uuid.UUID, lease: LeaseToken) -> StepExecutionRequest:
    return StepExecutionRequest(
        task_id=task_id,
        run_id=run_id,
        step_id=lease.step.id,
        step_type="review",
        payload={},
        timeout_seconds=120,
        lease=lease,
    )


def test_mechanical_findings_classifies_blocking_patterns() -> None:
    findings = mechanical_findings(b"+assert True or True\n")
    assert any(item.classification == "forced_success" for item in findings)
    assert any(item.severity.value == "blocker" for item in findings)


def test_effective_risk_uses_draft_when_higher() -> None:
    task = SimpleNamespace(risk=RiskLevel.LOW, task_draft={"suggested_risk": "medium"})
    assert effective_risk(task) is RiskLevel.MEDIUM  # type: ignore[arg-type]
    task = SimpleNamespace(risk=RiskLevel.HIGH, task_draft={"suggested_risk": "low"})
    assert effective_risk(task) is RiskLevel.HIGH  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("paths", "diff", "expected"),
    (
        (("src/app.py",), b"+small\n", RiskLevel.LOW),
        (("src/auth/session.py",), b"+small\n", RiskLevel.HIGH),
        (("deploy/systemd/vuzol.service",), b"+small\n", RiskLevel.PRIVILEGED),
        (tuple(f"src/f{i}.py" for i in range(21)), b"+wide\n", RiskLevel.HIGH),
        (("src/app.py",), b"+" + b"x" * 4_100, RiskLevel.MEDIUM),
    ),
)
def test_runtime_risk_escalates_from_measured_diff(
    paths: tuple[str, ...], diff: bytes, expected: RiskLevel
) -> None:
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=paths,
        diff=diff,
    )
    assert runtime_risk(RiskLevel.LOW, inspection) is expected
    assert runtime_risk(RiskLevel.PRIVILEGED, inspection) is RiskLevel.PRIVILEGED


@pytest.mark.anyio
async def test_review_passes_after_clean_validate(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result,
                "gates": [{"name": "git-facts", "exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    run = SimpleNamespace(task_id=task_id)
    task = SimpleNamespace(risk=RiskLevel.LOW, task_draft={"suggested_risk": "medium"})
    worktree = SimpleNamespace(
        path=str(worktree_path),
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        base_commit=base,
        result_commit=result,
        diff_hash="c" * 64,
        branch="task-branch",
    )
    session = MagicMock()
    session.get = AsyncMock(side_effect=[step, run, task])
    session.scalar = AsyncMock(side_effect=[validate, worktree])
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=GitInspection(
            head=result,
            branch="task-branch",
            changed_files=("index.html",),
            diff=b"+hello\n",
        )
    )
    handler = ResultReviewHandler(factory, git, worktree_root=tmp_path)
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["verdict"] == ReviewVerdictKind.PASSED.value
    assert outcome.result["review_kind"] == "mechanical"


@pytest.mark.anyio
async def test_review_blocks_high_risk_without_independent_reviewer(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result,
                "gates": [{"exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[
            step,
            SimpleNamespace(task_id=task_id),
            SimpleNamespace(risk=RiskLevel.HIGH, task_draft={}),
        ]
    )
    session.scalar = AsyncMock(
        side_effect=[
            validate,
            SimpleNamespace(
                path=str(worktree_path),
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                base_commit=base,
                result_commit=result,
                diff_hash="c" * 64,
                branch="task-branch",
            ),
        ]
    )
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=GitInspection(
            head=result,
            branch="task-branch",
            changed_files=("x.py",),
            diff=b"+x\n",
        )
    )
    handler = ResultReviewHandler(factory, git, worktree_root=tmp_path)
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "independent_review_required"


@pytest.mark.anyio
async def test_review_escalates_sensitive_diff_to_independent_reviewer(tmp_path: Path) -> None:
    from vuzol.review.domain import ReviewVerdict

    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result,
                "gates": [{"name": "git-facts", "exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    task = SimpleNamespace(
        risk=RiskLevel.LOW,
        task_draft={"goal": "Harden auth", "suggested_risk": "low"},
        original_text="x",
    )
    session = MagicMock()
    session.get = AsyncMock(side_effect=[step, SimpleNamespace(task_id=task_id), task])
    session.scalar = AsyncMock(
        side_effect=[
            validate,
            SimpleNamespace(
                path=str(worktree_path),
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                base_commit=base,
                result_commit=result,
                diff_hash="c" * 64,
                branch="task-branch",
            ),
        ]
    )
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=GitInspection(
            head=result,
            branch="task-branch",
            changed_files=("auth.py",),
            diff=b"+def ok():\n+    return 1\n",
        )
    )
    independent = MagicMock()
    independent.review = AsyncMock(
        return_value=ReviewVerdict(
            verdict=ReviewVerdictKind.PASSED,
            review_kind="independent",
            risk="high",
            base_commit=base,
            result_commit=result,
            diff_hash="c" * 64,
            changed_files=("auth.py",),
            findings=(),
            summary="Independent review passed.",
        )
    )
    handler = ResultReviewHandler(
        factory, git, worktree_root=tmp_path, independent_reviewer=independent
    )
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["review_kind"] == "independent"
    independent.review.assert_awaited_once()
    assert independent.review.await_args.kwargs["risk"] is RiskLevel.HIGH


@pytest.mark.anyio
async def test_review_blocks_suspicious_diff(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result,
                "gates": [{"exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[
            step,
            SimpleNamespace(task_id=task_id),
            SimpleNamespace(risk=RiskLevel.MEDIUM, task_draft={}),
        ]
    )
    session.scalar = AsyncMock(
        side_effect=[
            validate,
            SimpleNamespace(
                path=str(worktree_path),
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                base_commit=base,
                result_commit=result,
                diff_hash="c" * 64,
                branch="task-branch",
            ),
        ]
    )
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=GitInspection(
            head=result,
            branch="task-branch",
            changed_files=("x.py",),
            diff=b"+assert True or True\n",
        )
    )
    handler = ResultReviewHandler(factory, git, worktree_root=tmp_path)
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_blocked"


@pytest.mark.anyio
async def test_review_fails_closed_on_git_error(tmp_path: Path) -> None:
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    base = "a" * 40
    result = "b" * 40
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result,
                "gates": [{"exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[
            step,
            SimpleNamespace(task_id=task_id),
            SimpleNamespace(risk=RiskLevel.MEDIUM, task_draft={}),
        ]
    )
    session.scalar = AsyncMock(
        side_effect=[
            validate,
            SimpleNamespace(
                path=str(worktree_path),
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                base_commit=base,
                result_commit=result,
                diff_hash="c" * 64,
                branch="task-branch",
            ),
        ]
    )
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock(side_effect=GitError("dirty"))
    handler = ResultReviewHandler(factory, git, worktree_root=tmp_path)
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"


@pytest.mark.anyio
async def test_review_fails_when_state_missing(tmp_path: Path) -> None:
    lease = _lease()
    session = MagicMock()
    session.get = AsyncMock(return_value=None)
    factory = MagicMock(return_value=AsyncContext(session))
    handler = ResultReviewHandler(factory, MagicMock(), worktree_root=tmp_path)
    outcome = await handler.execute(
        _request(uuid.uuid4(), uuid.uuid4(), lease), CancellationContext()
    )
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"


@pytest.mark.anyio
async def test_review_fails_on_lease_mismatch(tmp_path: Path) -> None:
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    step = SimpleNamespace(
        status=StepStatus.PENDING,
        lease_owner="other",
        lease_generation=9,
        run_id=run_id,
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[step, SimpleNamespace(task_id=task_id), SimpleNamespace(risk=RiskLevel.LOW)]
    )
    factory = MagicMock(return_value=AsyncContext(session))
    handler = ResultReviewHandler(factory, MagicMock(), worktree_root=tmp_path)
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"


@pytest.mark.anyio
async def test_review_fails_without_worktree(tmp_path: Path) -> None:
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": "a" * 40,
                "result_commit": "b" * 40,
                "gates": [{"exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        dependency_metadata={"predecessor_ordinals": [1]},
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[
            step,
            SimpleNamespace(task_id=task_id),
            SimpleNamespace(risk=RiskLevel.LOW, task_draft={}),
        ]
    )
    session.scalar = AsyncMock(side_effect=[validate, None])
    factory = MagicMock(return_value=AsyncContext(session))
    handler = ResultReviewHandler(factory, MagicMock(), worktree_root=tmp_path)
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"
