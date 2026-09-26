"""Unit tests for effect operation keys and observation taxonomy (WP05)."""

from __future__ import annotations

import uuid

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
        classify_effect_observation(
            observed_ref=None, result_commit=result, expected_head=expected
        )
        is EffectObservation.UNCERTAIN
    )
    assert (
        classify_effect_observation(
            observed_ref="c" * 40, result_commit=result, expected_head=expected
        )
        is EffectObservation.UNCERTAIN
    )
