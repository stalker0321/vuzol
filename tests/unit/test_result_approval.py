import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.storage.models import Step
from vuzol.storage.types import StepStatus
from vuzol.workflows.result_approval import (
    ensure_result_approval,
    envelope_hash,
    verified_envelope,
)


def _step(
    *,
    step_type: str,
    result: dict[str, Any] | None = None,
    status: StepStatus = StepStatus.COMPLETED,
    ordinal: int = 0,
) -> Step:
    step = MagicMock()
    step.step_type = step_type
    step.status = status
    step.result = result
    step.ordinal = ordinal
    return cast(Step, step)


@pytest.mark.anyio
async def test_non_apply_approval_is_not_materialized_as_a_result_decision() -> None:
    step = MagicMock(payload={}, dependency_metadata={})
    assert (
        await ensure_result_approval(
            MagicMock(), run=MagicMock(), approval_step=step, steps_by_ordinal={}
        )
        is None
    )


@pytest.mark.anyio
async def test_existing_result_approval_is_reused() -> None:
    existing = object()
    session = MagicMock()
    session.scalar = AsyncMock(return_value=existing)
    step = MagicMock(id=uuid.uuid4(), payload={"requested_action": "apply_result"})
    assert (
        await ensure_result_approval(
            session, run=MagicMock(), approval_step=step, steps_by_ordinal={}
        )
        is existing
    )


@pytest.mark.anyio
async def test_result_approval_requires_retained_worktree() -> None:
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=(None, None))
    step = MagicMock(id=uuid.uuid4(), payload={"requested_action": "apply_result"})
    with pytest.raises(ValueError, match="retained measured worktree"):
        await ensure_result_approval(
            session,
            run=MagicMock(),
            approval_step=step,
            steps_by_ordinal={},
        )


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("missing_validate", "gates"))
async def test_result_approval_requires_validation_evidence(failure: str) -> None:
    base = "a" * 40
    result_commit = "b" * 40
    worktree = SimpleNamespace(
        result_commit=result_commit,
        diff_hash="c" * 64,
        base_commit=base,
        project_id="vuzol",
        repository_identity_hash="d" * 64,
        default_branch="main",
        expected_target_head=base,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=(None, worktree))
    steps = {}
    if failure != "missing_validate":
        steps[5] = _step(
            step_type="validate",
            result={
                "structured_output": {
                    "base_commit": base,
                    "result_commit": result_commit,
                    "gates": [] if failure == "gates" else [{"exit_code": 0}],
                }
            },
        )
    with pytest.raises(ValueError):
        await ensure_result_approval(
            session,
            run=MagicMock(),
            approval_step=MagicMock(id=uuid.uuid4(), payload={"requested_action": "apply_result"}),
            steps_by_ordinal=steps,
        )


@pytest.mark.anyio
async def test_result_approval_prefers_review_summary_and_validate_gates() -> None:
    base = "a" * 40
    result_commit = "b" * 40
    validate = _step(
        step_type="validate",
        ordinal=5,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result_commit,
                "gates": [{"name": "git-facts", "exit_code": 0, "duration_ms": 1}],
            },
            "implementation_summary": "validate summary",
        },
    )
    review = _step(
        step_type="review",
        ordinal=6,
        result={
            "summary": "Mechanical review passed for 2 changed path(s).",
            "structured_output": {
                "schema_version": "result-review.v1",
                "verdict": "pass",
                "review_kind": "mechanical",
                "risk": "medium",
                "base_commit": base,
                "result_commit": result_commit,
                "diff_hash": "c" * 64,
                "findings": [],
            },
        },
    )
    approval_step = MagicMock(
        id=uuid.uuid4(),
        payload={"requested_action": "apply_result"},
    )
    worktree = SimpleNamespace(
        result_commit=result_commit,
        diff_hash="c" * 64,
        base_commit=base,
        project_id="bill-buddy",
        repository_identity_hash="d" * 64,
        default_branch="main",
        expected_target_head=base,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=(None, worktree))
    session.flush = AsyncMock()
    run = MagicMock(
        task_id=uuid.uuid4(),
        id=uuid.uuid4(),
        configuration_revision="e" * 64,
        policy_revision="f" * 64,
    )
    approval = await ensure_result_approval(
        session,
        run=run,
        approval_step=approval_step,
        steps_by_ordinal={5: validate, 6: review},
    )
    assert approval is not None
    assert approval.human_summary == "validate summary"
    assert approval_step.payload["action_envelope"]["project_id"] == "bill-buddy"
    assert approval_step.payload["action_envelope"]["gates"][0]["name"] == "git-facts"
    assert approval_step.payload["action_envelope"]["validation_evidence_hash"]
    assert approval_step.payload["action_envelope"]["review_evidence"]["verdict"] == "pass"
    assert approval_step.payload["action_envelope"]["review_evidence_hash"]


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("mismatch", "verdict", "contradiction"))
async def test_result_approval_rejects_invalid_review_evidence(failure: str) -> None:
    base = "a" * 40
    result_commit = "b" * 40
    validate = _step(
        step_type="validate",
        ordinal=5,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result_commit,
                "gates": [{"name": "tests", "exit_code": 0}],
            }
        },
    )
    review = _step(
        step_type="review",
        ordinal=6,
        result={
            "structured_output": {
                "schema_version": "result-review.v1",
                "verdict": "changes_required" if failure == "verdict" else "pass",
                "review_kind": "independent",
                "risk": "high",
                "base_commit": base,
                "result_commit": "d" * 40 if failure == "mismatch" else result_commit,
                "diff_hash": "c" * 64,
                "findings": (
                    [{"severity": "blocker", "classification": "unsafe"}]
                    if failure == "contradiction"
                    else []
                ),
            }
        },
    )
    worktree = SimpleNamespace(
        result_commit=result_commit,
        diff_hash="c" * 64,
        base_commit=base,
        project_id="vuzol",
        repository_identity_hash="d" * 64,
        default_branch="main",
        expected_target_head=base,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=(None, worktree))

    with pytest.raises(ValueError):
        await ensure_result_approval(
            session,
            run=MagicMock(),
            approval_step=MagicMock(id=uuid.uuid4(), payload={"requested_action": "apply_result"}),
            steps_by_ordinal={5: validate, 6: review},
        )


@pytest.mark.anyio
async def test_result_approval_uses_a_safe_summary_fallback() -> None:
    base = "a" * 40
    result_commit = "b" * 40
    source = _step(
        step_type="validate",
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result_commit,
                "gates": [{"name": "tests", "exit_code": 0}],
            }
        },
    )
    step = MagicMock(
        id=uuid.uuid4(),
        payload={"requested_action": "apply_result"},
    )
    worktree = SimpleNamespace(
        result_commit=result_commit,
        diff_hash="c" * 64,
        base_commit=base,
        project_id="vuzol",
        repository_identity_hash="d" * 64,
        default_branch="main",
        expected_target_head=base,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=(None, worktree))
    session.flush = AsyncMock()
    run = MagicMock(
        task_id=uuid.uuid4(),
        id=uuid.uuid4(),
        configuration_revision="e" * 64,
        policy_revision="f" * 64,
    )
    approval = await ensure_result_approval(
        session,
        run=run,
        approval_step=step,
        steps_by_ordinal={2: source},
    )
    assert approval is not None
    assert approval.human_summary.startswith("The requested change")


def test_verified_envelope_rejects_missing_hash_or_wrong_step() -> None:
    step_id = uuid.uuid4()
    envelope = {"step_id": str(step_id)}
    approval = SimpleNamespace(action_envelope_hash=envelope_hash(envelope))
    with pytest.raises(ValueError, match="missing or has changed"):
        verified_envelope(SimpleNamespace(id=step_id, payload={}), approval)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="another step"):
        verified_envelope(
            SimpleNamespace(id=uuid.uuid4(), payload={"action_envelope": envelope}),  # type: ignore[arg-type]
            approval,  # type: ignore[arg-type]
        )
