"""Unit tests for effect operation keys and observation taxonomy (WP05)."""

from __future__ import annotations

import uuid

import pytest

from vuzol.execution.effect import apply_operation_key
from vuzol.execution.effect_reconciliation import (
    EffectObservation,
    classify_effect_observation,
)


def test_apply_operation_key_is_stable_and_reused() -> None:
    approval_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    first = apply_operation_key(
        approval_id=approval_id, result_commit="a" * 40, target_branch="main"
    )
    second = apply_operation_key(
        approval_id=approval_id, result_commit="a" * 40, target_branch="main"
    )
    other = apply_operation_key(
        approval_id=approval_id, result_commit="b" * 40, target_branch="main"
    )
    assert first == second
    assert first != other
    assert first == f"apply:{approval_id}:{'a' * 40}:main"


def test_observation_taxonomy_is_fail_closed() -> None:
    result = "a" * 40
    expected = "b" * 40
    assert (
        classify_effect_observation(
            observed_ref=result, result_commit=result, expected_head=expected
        )
        is EffectObservation.APPLIED
    )
    assert (
        classify_effect_observation(
            observed_ref=expected, result_commit=result, expected_head=expected
        )
        is EffectObservation.NOT_APPLIED
    )
    assert (
        classify_effect_observation(observed_ref=None, result_commit=result, expected_head=expected)
        is EffectObservation.UNCERTAIN
    )
    assert (
        classify_effect_observation(
            observed_ref="c" * 40, result_commit=result, expected_head=expected
        )
        is EffectObservation.UNCERTAIN
    )


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["clean", "drifted", "dirty"])
async def test_no_change_recovery_checks_target_before_settling(tmp_path, state):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.execution.effect_reconciliation import EffectReconciler
    from vuzol.execution.git import GitError
    from vuzol.storage.models import Effect, Step, Worktree
    from vuzol.storage.types import StepStatus, WorktreeDeliveryState

    base = "a" * 40
    tree = Worktree(
        id=uuid.uuid4(), result_commit=base, delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED
    )
    effect = Effect(
        id=uuid.uuid4(),
        operation_key="no-op",
        step_id=uuid.uuid4(),
        status="dispatched",
        target_kind="git_ref",
        target_reference="refs/heads/main",
        payload_hash="c" * 64,
        context={
            "repository_path": str(tmp_path),
            "target_branch": "main",
            "result_commit": base,
            "expected_head": base,
            "worktree_id": str(tree.id),
        },
    )
    step = Step(status=StepStatus.BLOCKED)
    session = MagicMock(
        connection=AsyncMock(),
        scalar=AsyncMock(side_effect=[True, effect]),
        scalars=AsyncMock(return_value=MagicMock(all=lambda: [effect])),
        get=AsyncMock(
            side_effect=lambda model, *_args, **_kwargs: tree if model is Worktree else step
        ),
        execute=AsyncMock(),
        commit=AsyncMock(),
        flush=AsyncMock(),
    )

    @asynccontextmanager
    async def context():
        yield session

    factory = MagicMock(side_effect=context)
    factory.begin.side_effect = context
    git = MagicMock(
        read_ref=AsyncMock(return_value="b" * 40 if state == "drifted" else base),
        require_clean_source=AsyncMock(side_effect=GitError("dirty") if state == "dirty" else None),
    )

    report = await EffectReconciler(factory, git, MagicMock(), owner="test").reconcile_startup()

    assert report.confirmed_count == (1 if state == "clean" else 0)
    assert report.uncertain_count == (0 if state == "clean" else 1)
    assert tree.delivery_state is (
        WorktreeDeliveryState.APPLIED
        if state == "clean"
        else WorktreeDeliveryState.WORKTREE_RETAINED
    )
