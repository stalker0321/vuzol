"""Unit tests for the pure WP04 recovery decision table and fingerprints."""

from __future__ import annotations

from vuzol.workflows.recovery_policy import (
    DEFAULT_RECOVERY_POLICY,
    FINGERPRINT_SCHEMA,
    RecoveryAction,
    RecoveryPolicy,
    RecoveryState,
    failure_fingerprint,
    fingerprint_components,
    recovery_attempt_summary,
)


def _repairable(**changes: object) -> RecoveryState:
    values: dict[str, object] = {
        "outcome_kind": "blocked",
        "category": "validation_gate_failed",
        "step_type": "validate",
        "unknown_effects": False,
        "retryable": False,
        "fingerprint": "new",
        "seen_fingerprints": frozenset(),
    }
    values.update(changes)
    return RecoveryState(**values)  # type: ignore[arg-type]


def test_identical_failure_without_new_evidence_is_attention() -> None:
    from vuzol.workflows.recovery_policy import decide_recovery

    state = _repairable(fingerprint="same", seen_fingerprints=frozenset({"same"}))
    assert decide_recovery(state) is RecoveryAction.ATTENTION


def test_changed_failure_fingerprint_may_repair() -> None:
    from vuzol.workflows.recovery_policy import decide_recovery

    state = _repairable(fingerprint="new", seen_fingerprints=frozenset({"old"}))
    assert decide_recovery(state) is RecoveryAction.REPAIR


def test_oscillation_a_b_a_is_attention() -> None:
    from vuzol.workflows.recovery_policy import decide_recovery

    state = _repairable(fingerprint="a", seen_fingerprints=frozenset({"a", "b"}))
    assert decide_recovery(state) is RecoveryAction.ATTENTION


def test_step_and_task_repair_caps_fail_closed() -> None:
    from vuzol.workflows.recovery_policy import decide_recovery

    policy = RecoveryPolicy(step_repair_cap=2, task_repair_cap=4)
    assert (
        decide_recovery(_repairable(repair_count=2), policy) is RecoveryAction.ATTENTION
    )
    assert (
        decide_recovery(_repairable(task_repair_count=4), policy) is RecoveryAction.ATTENTION
    )
    assert decide_recovery(_repairable(repair_count=1, task_repair_count=3), policy) is (
        RecoveryAction.REPAIR
    )


def test_backpressure_waits_then_attention() -> None:
    from vuzol.workflows.recovery_policy import decide_recovery

    policy = RecoveryPolicy(backpressure_wait_cap=2)
    waiting = _repairable(
        category="rate_limited", step_type="execute_model", backpressure_count=1
    )
    assert decide_recovery(waiting, policy) is RecoveryAction.WAIT
    exhausted = _repairable(
        category="rate_limited", step_type="execute_model", backpressure_count=2
    )
    assert decide_recovery(exhausted, policy) is RecoveryAction.ATTENTION


def test_unknown_effects_and_deadline_never_automate() -> None:
    from vuzol.workflows.recovery_policy import decide_recovery

    assert decide_recovery(_repairable(unknown_effects=True)) is RecoveryAction.ATTENTION
    assert decide_recovery(_repairable(deadline_exceeded=True)) is RecoveryAction.ATTENTION


def test_transient_retryable_retries() -> None:
    from vuzol.workflows.recovery_policy import decide_recovery

    state = RecoveryState(
        outcome_kind="transient_failure",
        category="timeout",
        step_type="execute_model",
        unknown_effects=False,
        retryable=True,
    )
    assert decide_recovery(state) is RecoveryAction.RETRY
    blocked = RecoveryState(
        outcome_kind="transient_failure",
        category="timeout",
        step_type="execute_model",
        unknown_effects=False,
        retryable=False,
    )
    assert decide_recovery(blocked) is RecoveryAction.ATTENTION


def test_fingerprint_components_are_versioned_and_stable() -> None:
    base = fingerprint_components(
        step_type="validate",
        category="validation_gate_failed",
        evidence_hash="e1",
        environment_hash="env1",
        result_hash="r1",
        strategy_hash="s1",
    )
    same = fingerprint_components(
        step_type="validate",
        category="validation_gate_failed",
        evidence_hash="e1",
        environment_hash="env1",
        result_hash="r1",
        strategy_hash="s1",
    )
    changed = fingerprint_components(
        step_type="validate",
        category="validation_gate_failed",
        evidence_hash="e2",
        environment_hash="env1",
        result_hash="r1",
        strategy_hash="s1",
    )
    assert base["schema"] == FINGERPRINT_SCHEMA
    assert failure_fingerprint(base) == failure_fingerprint(same)
    assert failure_fingerprint(base) != failure_fingerprint(changed)


def test_attempt_summary_is_operator_visible() -> None:
    summary = recovery_attempt_summary(_repairable(), RecoveryAction.REPAIR)
    assert summary["decision"] == "repair"
    assert summary["fingerprint_schema"] == FINGERPRINT_SCHEMA
    assert summary["category"] == "validation_gate_failed"


def test_default_policy_is_bounded() -> None:
    assert DEFAULT_RECOVERY_POLICY.step_repair_cap == 3
    assert DEFAULT_RECOVERY_POLICY.task_repair_cap >= DEFAULT_RECOVERY_POLICY.step_repair_cap
