"""J5 canary and kill-switch tests."""

from __future__ import annotations

import pytest

from vuzol.experiments.canary import (
    CanaryPolicy,
    KillSwitch,
    WhitelistGate,
    cohort_bucket,
    in_cohort,
)


def test_canary_is_deterministic_and_one_kind_only() -> None:
    assert cohort_bucket("stable-id") == cohort_bucket("stable-id")
    assert in_cohort("stable-id", 100) is True
    assert in_cohort("stable-id", 0) is False

    policy = CanaryPolicy(
        enabled_kind="intake",
        cohort_percent=100,
        allowlist=WhitelistGate(enabled_kinds=frozenset({"intake"})),
    )
    admitted = policy.admit(decision_kind="intake", opportunity_id="op-1")
    assert admitted.allowed is True and admitted.reason == "admitted"
    other = policy.admit(decision_kind="work_shape", opportunity_id="op-1")
    assert other.allowed is False and other.reason == "kind_not_allowlisted"

    not_whitelisted = CanaryPolicy(
        enabled_kind="intake", cohort_percent=100, allowlist=WhitelistGate()
    ).admit(decision_kind="intake", opportunity_id="op-1")
    assert not_whitelisted.allowed is False
    assert not_whitelisted.reason == "not_whitelisted"


def test_kill_switch_and_rollback_block_apply() -> None:
    switch = KillSwitch()
    policy = CanaryPolicy(
        enabled_kind="intake",
        cohort_percent=100,
        allowlist=WhitelistGate(enabled_kinds=frozenset({"intake"})),
        kill_switch=switch,
    )
    assert policy.admit(decision_kind="intake", opportunity_id="op-1").allowed is True
    policy.rollback()
    blocked = policy.admit(decision_kind="intake", opportunity_id="op-1")
    assert blocked.allowed is False and blocked.reason == "kill_switch"
    switch.thaw("intake")
    assert policy.admit(decision_kind="intake", opportunity_id="op-1").allowed is True


def test_whitelist_gate_default_off() -> None:
    gate = WhitelistGate()
    assert gate.allows("repair_triage") is False
    enabled = gate.enable("repair_triage", gate_evidence="preregistration-hash-abc")
    assert gate.allows("repair_triage") is False
    assert enabled.allows("repair_triage") is True
    assert enabled.allows("other_kind") is False
    with pytest.raises(ValueError, match="gate evidence"):
        gate.enable("repair_triage", gate_evidence="  ")
