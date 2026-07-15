import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.workflows.result_approval import (
    ensure_result_approval,
    envelope_hash,
    verified_envelope,
)


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
@pytest.mark.parametrize("failure", ("predecessor", "manifest"))
async def test_result_approval_rejects_incomplete_finalization(failure: str) -> None:
    session = MagicMock()
    session.scalar = AsyncMock(return_value=None)
    predecessor = MagicMock(result={})
    step = MagicMock(
        id=uuid.uuid4(),
        payload={"requested_action": "apply_result"},
        dependency_metadata={"predecessor_ordinals": [] if failure == "predecessor" else [2]},
    )
    with pytest.raises(ValueError):
        await ensure_result_approval(
            session,
            run=MagicMock(),
            approval_step=step,
            steps_by_ordinal={2: predecessor},
        )


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("worktree", "gates"))
async def test_result_approval_requires_matching_retention_and_passing_gates(failure: str) -> None:
    base = "a" * 40
    result_commit = "b" * 40
    manifest = {
        "base_commit": base,
        "result_commit": result_commit,
        "gates": [] if failure == "gates" else [{"exit_code": 0}],
    }
    source = MagicMock(result={"structured_output": manifest})
    step = MagicMock(
        id=uuid.uuid4(),
        payload={"requested_action": "apply_result"},
        dependency_metadata={"predecessor_ordinals": [2]},
    )
    worktree = SimpleNamespace(
        result_commit=result_commit,
        diff_hash="c" * 64,
        base_commit=base,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=(None, None if failure == "worktree" else worktree))
    with pytest.raises(ValueError):
        await ensure_result_approval(
            session,
            run=MagicMock(),
            approval_step=step,
            steps_by_ordinal={2: source},
        )


@pytest.mark.anyio
async def test_result_approval_uses_a_safe_summary_fallback() -> None:
    base = "a" * 40
    result_commit = "b" * 40
    source = MagicMock(
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result_commit,
                "gates": [{"name": "tests", "exit_code": 0}],
            }
        }
    )
    step = MagicMock(
        id=uuid.uuid4(),
        payload={"requested_action": "apply_result"},
        dependency_metadata={"predecessor_ordinals": [2]},
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
