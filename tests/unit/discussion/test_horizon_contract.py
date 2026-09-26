"""Horizon v1 contract helpers + opt-in flag (WP08, ADR-A01.5)."""

import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config.settings import HorizonSettings, Settings
from vuzol.discussion.application import PackageControlIngress
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
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.discussion.service import WorkPackageService
from vuzol.storage.models import WorkPackage
from vuzol.storage.types import WorkPackageStatus


def test_flag_default_off() -> None:
    assert Settings().horizon.enabled is False
    assert horizon_enabled(Settings()) is False
    assert horizon_enabled(Settings(horizon=HorizonSettings(enabled=True))) is True


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


def _sequencer() -> tuple[WorkPackageSequencer, MagicMock]:
    uow = MagicMock()
    uow.session.scalar = AsyncMock(return_value=None)
    uow.session.get = AsyncMock(return_value=None)
    uow.events.append = AsyncMock()
    uow.outbox.enqueue = AsyncMock()
    return WorkPackageSequencer(cast(Any, uow)), uow


def _running_package(*, goal: str | None) -> WorkPackage:
    package = WorkPackage(
        session_id=uuid.uuid4(),
        project_id="test",
        status=WorkPackageStatus.RUNNING,
        title="horizon package",
    )
    package.id = uuid.uuid4()
    package.goal = goal
    package.exit_criteria = None
    package.cursor_ordinal = 1
    package.version = 1
    package.horizon_phase = None
    return package


@pytest.mark.anyio
async def test_exhausted_queue_enters_evaluating_behind_flag() -> None:
    sequencer, _ = _sequencer()
    package = _running_package(goal="ship the horizon")
    revision = SimpleNamespace(id=uuid.uuid4())

    result = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]

    assert result.completed is False
    assert package.status is WorkPackageStatus.RUNNING
    assert package.horizon_phase == "evaluating"


@pytest.mark.anyio
async def test_exhausted_queue_completes_when_flag_off() -> None:
    sequencer, _ = _sequencer()
    package = _running_package(goal="ship the horizon")
    revision = SimpleNamespace(id=uuid.uuid4())

    result = await sequencer._materialize_current(package, revision, horizon_enabled=False)  # type: ignore[arg-type]

    assert result.completed is True
    assert package.status is WorkPackageStatus.COMPLETED
    assert package.horizon_phase is None


def test_ingress_horizon_wiring_defaults_off() -> None:
    default = PackageControlIngress(MagicMock(), enabled=True, authorized_user_ids=frozenset({1}))
    assert default._horizon_enabled is False
    flagged = PackageControlIngress(
        MagicMock(), enabled=True, authorized_user_ids=frozenset({1}), horizon_enabled=True
    )
    assert flagged._horizon_enabled is True


@pytest.mark.anyio
async def test_restart_continues_approved_horizon_without_new_revision() -> None:
    revision_id = uuid.uuid4()
    package = WorkPackage(
        session_id=uuid.uuid4(),
        project_id="test",
        status=WorkPackageStatus.STOPPED,
        title="horizon package",
    )
    package.id = uuid.uuid4()
    package.goal = "ship the horizon"
    package.exit_criteria = None
    package.version = 3
    package.head_revision_id = revision_id
    package.approved_revision_id = revision_id
    package.last_failure_task_id = uuid.uuid4()
    uow = MagicMock()
    uow.work_packages.get_package = AsyncMock(return_value=package)
    uow.work_packages.get_fenced_revision = AsyncMock(
        return_value=SimpleNamespace(id=revision_id, revision_number=2, content_hash="ab" * 32)
    )
    uow.events.append = AsyncMock()
    service = WorkPackageService(cast(Any, uow))

    result = await service.restart_plan(
        package_id=package.id,
        revision_number=2,
        h8="ab" * 8,
        expected_status_generation=3,
        user_id=7,
        horizon_enabled=True,
    )

    assert result.revision_id == revision_id
    assert result.revision_number == 2
    assert result.status_generation == 3
    assert package.version == 3
    assert package.pause_reason is None
    assert package.last_failure_task_id is None
    uow.work_packages.get_head_revision.assert_not_called()
    payload = uow.events.append.call_args.kwargs["payload"]
    assert payload["restart"] is True
    assert payload["horizon_continued"] is True
