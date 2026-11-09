"""Mechanical review step coverage."""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.domain import GitInspection
from vuzol.execution.git import GitError
from vuzol.providers.domain import ContextItem
from vuzol.review.domain import ReviewVerdictKind
from vuzol.review.handler import (
    ResultReviewHandler,
    effective_risk,
    mechanical_findings,
    runtime_risk,
    unexpected_file_findings,
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


def test_standalone_success_exit_is_not_forced_success() -> None:
    findings = mechanical_findings(b'if test "$FAIL" -gt 0; then exit 1; fi\nexit 0\n')
    assert not any(item.classification == "forced_success" for item in findings)


def test_shell_failure_suppression_is_forced_success() -> None:
    findings = mechanical_findings(b"run-tests || exit 0\n")
    assert any(item.classification == "forced_success" for item in findings)


def test_docs_request_warns_about_unmentioned_new_verify_script() -> None:
    task = SimpleNamespace(
        original_text="Обнови README",
        task_draft={"goal": "Update README.md with setup notes."},
    )
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=("README.md", "verify.sh"),
        added_files=("verify.sh",),
        diff=b"+documentation\n",
    )

    findings = unexpected_file_findings(task, inspection)  # type: ignore[arg-type]

    assert len(findings) == 1
    assert findings[0].severity.value == "warning"
    assert findings[0].classification == "unexpected_executable_file"
    assert findings[0].path == "verify.sh"


def test_single_file_request_warns_about_unmentioned_new_test_file() -> None:
    task = SimpleNamespace(
        original_text="Fix src/app.py only.",
        task_draft={"goal": "Fix src/app.py only."},
    )
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=("src/app.py", "tests/test_app.py"),
        added_files=("tests/test_app.py",),
        diff=b"+change\n",
    )

    findings = unexpected_file_findings(task, inspection)  # type: ignore[arg-type]

    assert [finding.path for finding in findings] == ["tests/test_app.py"]


@pytest.mark.parametrize(
    ("goal", "changed", "added"),
    (
        ("Implement the feature.", ("src/app.py", "tests/test_app.py"), ("tests/test_app.py",)),
        (
            "Update src/app.py and add tests/test_app.py.",
            ("src/app.py", "tests/test_app.py"),
            ("tests/test_app.py",),
        ),
        ("Update README.md.", ("README.md", "docs/setup.md"), ("docs/setup.md",)),
    ),
)
def test_unexpected_file_warning_stays_narrow(
    goal: str, changed: tuple[str, ...], added: tuple[str, ...]
) -> None:
    task = SimpleNamespace(original_text=goal, task_draft={"goal": goal})
    inspection = GitInspection(
        head="b" * 40,
        branch="task",
        changed_files=changed,
        added_files=added,
        diff=b"+change\n",
    )

    assert unexpected_file_findings(task, inspection) == ()  # type: ignore[arg-type]


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
        payload={},
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    run = SimpleNamespace(task_id=task_id)
    # D0 floors: MEDIUM → L2 → independent reviewer. This mechanical-pass
    # fixture stays L0 (LOW risk, docs-only) so it exercises the L0 path.
    task = SimpleNamespace(risk=RiskLevel.LOW, task_draft={"suggested_risk": "low"})
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
    session.scalar = AsyncMock(side_effect=[validate, worktree, None])
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=GitInspection(
            head=result,
            branch="task-branch",
            changed_files=("docs/guide.md",),
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
        payload={},
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
            None,
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
@pytest.mark.parametrize("context_case", ["legacy", "frozen", "corrupt"])
async def test_review_escalates_sensitive_diff_to_independent_reviewer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    context_case: str,
) -> None:
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
        payload={},
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    task = SimpleNamespace(
        risk=RiskLevel.LOW,
        task_draft={"goal": "Harden auth", "suggested_risk": "low"},
        original_text="x",
    )
    context: tuple[ContextItem, ...] = ()
    if context_case != "legacy":
        task.ingress_source = "chat_work"
        text = "Selected semantic prototype; only local testing; no deployment."
        context = (
            ContextItem(
                source="chat_work_context",
                reference="spec:part-1-of-1",
                content=text,
                content_hash=hashlib.sha256(text.encode()).hexdigest(),
            ),
        )
        loader = AsyncMock(return_value=context)
        if context_case == "corrupt":
            loader.side_effect = ValueError("chat work context source mismatch")
        monkeypatch.setattr("vuzol.discussion.work_context.load_work_context", loader)
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
            None,
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
    if context_case == "corrupt":
        assert outcome.kind is OutcomeKind.BLOCKED
        assert outcome.category == "review_failed"
        independent.review.assert_not_awaited()
        return
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["review_kind"] == "independent"
    independent.review.assert_awaited_once()
    assert independent.review.await_args.kwargs["risk"] is RiskLevel.HIGH
    if context_case == "frozen":
        assert independent.review.await_args.kwargs["work_context"] == context


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
        payload={},
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
            None,
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
        payload={},
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
            None,
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
        payload={},
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


@pytest.mark.anyio
@pytest.mark.parametrize("verdict_kind", ["pass", "blocked", "missing"])
async def test_no_change_handler_requires_independent_review(tmp_path, verdict_kind):
    from vuzol.review.domain import ReviewVerdict
    from vuzol.review.partitions import verify_chunk_receipts

    lease = _lease()
    task_id, run_id = uuid.uuid4(), uuid.uuid4()
    inspection = GitInspection(head="a" * 40, branch="task", changed_files=(), diff=b"")
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": inspection.head,
                "result_commit": inspection.head,
                "gates": [{"exit_code": 0}],
                "no_change": True,
                "no_change_reason": "x" * 2500,
            },
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        payload={},
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    task = SimpleNamespace(risk=RiskLevel.LOW, task_draft={}, original_text="Already done")
    tree = SimpleNamespace(
        path=str(tmp_path),
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        base_commit=inspection.head,
        result_commit=inspection.head,
        diff_hash=inspection.diff_hash,
        branch=inspection.branch,
    )
    session = MagicMock(
        get=AsyncMock(side_effect=[step, SimpleNamespace(task_id=task_id), task]),
        scalar=AsyncMock(side_effect=[validate, tree, None]),
    )
    git = MagicMock(
        require_clean_worktree=AsyncMock(),
        require_no_remotes=AsyncMock(),
        inspect=AsyncMock(return_value=inspection),
    )
    independent = (
        None
        if verdict_kind == "missing"
        else MagicMock(
            review=AsyncMock(
                return_value=ReviewVerdict(
                    verdict=ReviewVerdictKind(verdict_kind),
                    review_kind="independent",
                    risk="low",
                    base_commit=inspection.head,
                    result_commit=inspection.head,
                    diff_hash=inspection.diff_hash,
                    changed_files=(),
                    findings=(),
                    summary="Reviewed base",
                )
            )
        )
    )
    handler = ResultReviewHandler(
        MagicMock(return_value=AsyncContext(session)),
        git,
        worktree_root=tmp_path,
        independent_reviewer=independent,
    )

    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())

    assert outcome.kind is (
        OutcomeKind.SUCCEEDED if verdict_kind == "pass" else OutcomeKind.BLOCKED
    )
    if independent is None:
        assert outcome.category == "independent_review_required"
    else:
        independent.review.assert_awaited_once()
        context = independent.review.await_args.kwargs["work_context"]
        verify_chunk_receipts(context)
        claim = json.loads(
            next(item.content for item in context if item.source == "no_change_result")
        )
        assert claim["no_change"] is True
        assert claim["no_change_reason"] == "x" * 2000


def _review_fixture(tmp_path):
    lease = _lease()
    task_id, run_id = uuid.uuid4(), uuid.uuid4()
    inspection = GitInspection(
        head="b" * 40, branch="task-branch", changed_files=("docs/guide.md",), diff=b"+hello\n"
    )
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": "a" * 40,
                "result_commit": inspection.head,
                "gates": [{"exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        payload={},
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    run = SimpleNamespace(task_id=task_id)
    task = SimpleNamespace(risk=RiskLevel.LOW, task_draft={}, original_text="Update docs")
    worktree = SimpleNamespace(
        path=str(tmp_path),
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        base_commit="a" * 40,
        result_commit=inspection.head,
        diff_hash=inspection.diff_hash,
        branch=inspection.branch,
    )
    session = MagicMock(
        get=AsyncMock(side_effect=[step, run, task]),
        scalar=AsyncMock(side_effect=[validate, worktree]),
    )
    git = MagicMock(
        require_clean_worktree=AsyncMock(),
        require_no_remotes=AsyncMock(),
        inspect=AsyncMock(return_value=inspection),
    )
    handler = ResultReviewHandler(
        MagicMock(return_value=AsyncContext(session)), git, worktree_root=tmp_path
    )
    return SimpleNamespace(
        handler=handler,
        request=_request(task_id, run_id, lease),
        step=step,
        run=run,
        task=task,
        worktree=worktree,
        validate=validate,
        session=session,
        git=git,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "problem,message",
    [
        ("active", "worktree is not retained"),
        ("commit", "measured result commit"),
        ("structured", "no structured validation output"),
        ("gates", "no gate evidence"),
        ("failed_gate", "did not pass all gates"),
        ("malformed_gate", "did not pass all gates"),
        ("result", "validate result commit does not match"),
        ("base", "validate base commit does not match"),
        ("head", "HEAD does not match"),
        ("branch", "branch does not match"),
    ],
)
async def test_review_refuses_unbound_or_failed_validation_evidence(tmp_path, problem, message):
    setup = _review_fixture(tmp_path)
    structured = setup.validate.result["structured_output"]
    if problem == "active":
        setup.worktree.delivery_state = WorktreeDeliveryState.ACTIVE
    elif problem == "commit":
        setup.worktree.result_commit = None
    elif problem == "structured":
        setup.validate.result = {"structured_output": None}
    elif problem == "gates":
        structured["gates"] = []
    elif problem == "failed_gate":
        structured["gates"] = [{"exit_code": 1}]
    elif problem == "malformed_gate":
        structured["gates"] = ["passed"]
    elif problem in {"result", "base"}:
        structured[f"{problem}_commit"] = "c" * 40
    else:
        setup.git.inspect.return_value = setup.git.inspect.return_value.model_copy(
            update={"head" if problem == "head" else "branch": "wrong"}
        )
    outcome = await setup.handler.execute(setup.request, CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"
    assert message in outcome.summary
    assert outcome.unknown_effects is False
    if problem not in {"head", "branch"}:
        setup.git.inspect.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "binding,predecessor,message",
    [
        ("bad-uuid", None, "binding is invalid"),
        (str(uuid.UUID(int=1)), None, "binding is missing"),
        (str(uuid.UUID(int=1)), SimpleNamespace(step_type="execute_code"), "binding is missing"),
        (
            str(uuid.UUID(int=1)),
            SimpleNamespace(step_type="validate", status=StepStatus.RUNNING),
            "not completed",
        ),
        (None, None, "exactly one validate predecessor"),
    ],
)
async def test_review_repair_refuses_invalid_or_unfinished_predecessor(
    tmp_path, binding, predecessor, message
):
    setup = _review_fixture(tmp_path)
    setup.step.payload = {"repair_validation_step_id": binding} if binding else {}
    if binding is None:
        setup.step.dependency_metadata = {"predecessor_ordinals": [1, 2]}
    setup.session.scalar.side_effect = None
    setup.session.scalar.return_value = predecessor
    outcome = await setup.handler.execute(setup.request, CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"
    assert message in outcome.summary
    setup.git.inspect.assert_not_awaited()


@pytest.mark.anyio
async def test_review_repair_uses_completed_bound_validation(tmp_path):
    setup = _review_fixture(tmp_path)
    setup.step.payload = {"repair_validation_step_id": str(uuid.uuid4())}
    outcome = await setup.handler.execute(setup.request, CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["verdict"] == "pass"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "message,category,kind",
    [
        (
            "admission-only launch requires consent",
            "admission_only_consent_required",
            OutcomeKind.BLOCKED,
        ),
        ("cost is unknown", "money_cap_uncertified", OutcomeKind.BLOCKED),
        ("explicit money cap exceeded", "money_cap_reached", OutcomeKind.BLOCKED),
        ("invalid structured output", "reviewer_output_invalid", OutcomeKind.TRANSIENT_FAILURE),
    ],
)
async def test_independent_review_refusals_preserve_budget_and_retry_categories(
    tmp_path, message, category, kind
):
    from vuzol.review.independent import IndependentReviewError

    setup = _review_fixture(tmp_path)
    setup.task.risk = RiskLevel.HIGH
    reviewer = MagicMock(review=AsyncMock(side_effect=IndependentReviewError(message)))
    setup.handler._independent = reviewer
    outcome = await setup.handler.execute(setup.request, CancellationContext())
    assert outcome.kind is kind
    assert outcome.category == category
    assert outcome.unknown_effects is False
    assert outcome.result == {}
    reviewer.review.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize("mutation", ["head", "diff"])
async def test_independent_verdict_is_rejected_if_result_changes_during_review(tmp_path, mutation):
    from vuzol.review.domain import ReviewVerdict

    setup = _review_fixture(tmp_path)
    setup.task.risk = RiskLevel.HIGH
    inspection = setup.git.inspect.return_value
    reviewer = MagicMock(
        review=AsyncMock(
            return_value=ReviewVerdict(
                verdict=ReviewVerdictKind.PASSED,
                review_kind="independent",
                risk="high",
                base_commit="a" * 40,
                result_commit=inspection.head,
                diff_hash=inspection.diff_hash,
                changed_files=inspection.changed_files,
                findings=(),
                summary="Approved",
            )
        )
    )
    setup.handler._independent = reviewer
    setup.git.inspect.side_effect = [
        inspection,
        inspection.model_copy(
            update={"head": "c" * 40} if mutation == "head" else {"diff": b"+mutated"}
        ),
    ]
    outcome = await setup.handler.execute(setup.request, CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"
    assert "mutated during independent review" in outcome.summary
    assert setup.git.inspect.await_count == 2


def test_review_policy_failure_requires_independent_review_instead_of_passing(monkeypatch):
    from vuzol.review import handler
    from vuzol.review.independent import IndependentReviewError

    monkeypatch.setattr(
        handler, "resolve_review_plan", MagicMock(side_effect=ValueError("unknown policy"))
    )
    with pytest.raises(IndependentReviewError, match="review policy failed closed: unknown policy"):
        handler._requires_independent_for(RiskLevel.LOW, ("README.md",))


def test_non_string_scope_fields_and_unknown_risk_do_not_weaken_review():
    from vuzol.review.handler import _task_scope_text

    task = SimpleNamespace(
        risk=RiskLevel.HIGH,
        original_text=17,
        task_draft={
            "goal": None,
            "task_summary": "Update guide",
            "normalized_title": {},
            "requested_outcomes": ["Keep commands", 17],
            "constraints": ("No scripts", None),
            "suggested_risk": "not-a-risk",
        },
        source_turn_id=uuid.UUID(int=1),
        spec_revision="revision-17",
    )
    assert effective_risk(task) is RiskLevel.HIGH
    text = _task_scope_text(task)
    assert (
        text == "Update guide\nKeep commands\nNo scripts\n"
        "[derived from turn 00000000-0000-0000-0000-000000000001]\n"
        "[spec revision revision-17]"
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "predecessor,message",
    [
        (None, "predecessor must be a validate step"),
        (SimpleNamespace(step_type="execute_code"), "predecessor must be a validate step"),
        (
            SimpleNamespace(step_type="validate", status=StepStatus.FAILED),
            "validate predecessor is not completed",
        ),
    ],
)
async def test_review_refuses_missing_or_failed_validate_predecessor(
    tmp_path, predecessor, message
):
    setup = _review_fixture(tmp_path)
    setup.session.scalar.side_effect = None
    setup.session.scalar.return_value = predecessor
    outcome = await setup.handler.execute(setup.request, CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED and outcome.category == "review_failed"
    assert message in outcome.summary
    setup.git.inspect.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "verdict_kind,classification,category",
    [
        ("changes_required", "quality", "review_changes_required"),
        ("blocked", "independent_review_required", "independent_review_required"),
    ],
)
async def test_independent_refusal_returns_precise_blocking_category(
    tmp_path, verdict_kind, classification, category
):
    from vuzol.review.domain import FindingSeverity, ReviewFinding, ReviewVerdict

    setup = _review_fixture(tmp_path)
    setup.task.risk = RiskLevel.HIGH
    inspection = setup.git.inspect.return_value
    setup.handler._independent = MagicMock(
        review=AsyncMock(
            return_value=ReviewVerdict(
                verdict=ReviewVerdictKind(verdict_kind),
                review_kind="independent",
                risk="high",
                base_commit=setup.worktree.base_commit,
                result_commit=inspection.head,
                diff_hash=inspection.diff_hash,
                findings=(
                    ReviewFinding(
                        severity=FindingSeverity.ERROR,
                        classification=classification,
                        summary="Needs review",
                    ),
                ),
                summary="Refused",
            )
        )
    )
    outcome = await setup.handler.execute(setup.request, CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED and outcome.category == category
    assert outcome.result["verdict"] == verdict_kind
    assert outcome.summary == "Refused"


def test_mechanical_findings_keep_warning_severity_and_evidence_location():
    from vuzol.review.domain import FindingSeverity

    (finding,) = mechanical_findings(b"+pytest.skip('not ready')\n")
    assert finding.severity is FindingSeverity.WARNING
    assert finding.classification == "arbitrary_skip"
    assert finding.line == 1 and finding.path is None
    assert "not ready" in finding.summary
