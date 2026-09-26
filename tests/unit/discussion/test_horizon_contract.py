"""Horizon v1 contract helpers + opt-in flag (WP08, ADR-A01.5)."""

from vuzol.config.settings import Settings
from vuzol.discussion.horizon import (
    HORIZON_STATUS_MAPPING,
    budget_state,
    deadline_exceeded,
    horizon_enabled,
    horizon_status,
    is_horizon,
    needs_approval_gate,
    parse_budget,
    unmet_exit_criteria,
)
from vuzol.storage.types import WorkPackageStatus


def test_flag_default_off() -> None:
    assert Settings().horizon.enabled is False
    assert horizon_enabled(Settings()) is False
    assert horizon_enabled(Settings(horizon={"enabled": True})) is True


def test_empty_exit_criteria_is_not_success() -> None:
    assert unmet_exit_criteria([], frozenset()) == ("__no_exit_criteria__",)
    assert unmet_exit_criteria(None, frozenset()) == ("__no_exit_criteria__",)
    assert unmet_exit_criteria(
        [{"criterion_id": "a"}, {"criterion_id": "b"}], frozenset({"a"})
    ) == ("b",)


def test_is_horizon_requires_goal_or_criteria() -> None:
    assert is_horizon("ship it", None) is True
    assert is_horizon(None, [{"criterion_id": "a"}]) is True
    assert is_horizon(None, None) is False
    assert is_horizon("  ", []) is False


def test_status_mapping_completed_requires_acceptance() -> None:
    assert (
        horizon_status(WorkPackageStatus.COMPLETED, None, True)
        == HORIZON_STATUS_MAPPING["succeeded"]
    )
    assert (
        horizon_status(WorkPackageStatus.COMPLETED, None, False)
        == HORIZON_STATUS_MAPPING["running"]
    )
    assert (
        horizon_status(WorkPackageStatus.RUNNING, "evaluating", False)
        == HORIZON_STATUS_MAPPING["evaluating"]
    )


def test_lifetime_budget_never_resets_on_retry() -> None:
    budget = parse_budget({"max_cost": 10, "max_attempts": 3})
    assert budget is not None
    # Cumulative spend across retry epochs composes; no per-epoch reset.
    assert budget_state(budget, spent_cost=9.9, spent_attempts=2).value == "within"
    assert budget_state(budget, spent_cost=10.0, spent_attempts=2).value == "exhausted"
    assert budget_state(budget, spent_cost=1.0, spent_attempts=3).value == "exhausted"


def test_deadline_and_approval_gate() -> None:
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    assert deadline_exceeded(deadline=now - timedelta(seconds=1), now=now) is True
    assert deadline_exceeded(deadline=now + timedelta(hours=1), now=now) is False
    assert deadline_exceeded(deadline=None, now=now) is False
    assert needs_approval_gate(True) is True
    assert needs_approval_gate(False) is False
