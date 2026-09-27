"""Horizon v1 contract helpers + opt-in flag (WP08, ADR-A01.5)."""

import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vuzol.config.settings import HorizonSettings, Settings
from vuzol.discussion.application import PackageControlIngress
from vuzol.discussion.domain import DomainError, PlanDraft, PlanItemDraft
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
from vuzol.storage.types import (
    EstimatedComplexity,
    RiskLevel,
    WorkPackagePauseReason,
    WorkPackageStatus,
)


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
    assert result.status_generation == 4
    assert package.version == 4
    assert package.status is WorkPackageStatus.APPROVED
    assert package.approved_revision_id == revision_id
    assert package.head_revision_id == revision_id
    assert package.pause_reason is None
    assert package.last_failure_task_id is None
    uow.work_packages.get_head_revision.assert_not_called()
    payload = uow.events.append.call_args.kwargs["payload"]
    assert payload["restart"] is True
    assert payload["horizon_continued"] is True


def _approval_item() -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        item_id=uuid.uuid4(),
        summary="gated step",
        goal="gated goal",
        expected_outcome="gated outcome",
        completion_criteria=("check",),
        allowed_scope="src/**",
        out_of_scope=(),
        dependencies=(),
        trusted_checks=(),
        suggested_risk=RiskLevel.LOW,
        needs_approval=True,
        estimated_complexity=EstimatedComplexity.SMALL,
    )


@pytest.mark.anyio
async def test_needs_approval_item_waits_behind_flag() -> None:
    sequencer, uow = _sequencer()
    package = _running_package(goal="ship the horizon")
    revision = SimpleNamespace(id=uuid.uuid4())
    uow.session.scalar = AsyncMock(side_effect=[_approval_item(), None, _approval_item(), None])

    result = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]

    assert result.completed is False
    assert result.task_id is None
    assert package.status is WorkPackageStatus.RUNNING
    assert package.horizon_phase == "waiting_approval"
    event = uow.events.append.call_args.kwargs
    assert event["event_type"] == "work_package.waiting_approval"

    # Repeat observation is idempotent: no new generation or event.
    before = package.version
    uow.events.append.reset_mock()
    repeat = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]
    assert repeat.completed is False
    assert package.version == before
    uow.events.append.assert_not_called()


@pytest.mark.anyio
async def test_needs_approval_item_materializes_when_flag_off() -> None:
    sequencer, uow = _sequencer()
    package = _running_package(goal="ship the horizon")
    revision = SimpleNamespace(id=uuid.uuid4(), approved_by_user_id=42)
    task_id = uuid.uuid4()
    discussion = SimpleNamespace(chat_id=-100, message_thread_id=10)
    task = MagicMock()
    task.id = task_id
    task.source_chat_id = None
    task.source_thread_id = None
    uow.session.scalar = AsyncMock(side_effect=[_approval_item(), None])
    uow.session.get = AsyncMock(side_effect=[discussion, task])
    uow.session.flush = AsyncMock()
    uow.tasks.create = AsyncMock(return_value=SimpleNamespace(id=task_id))
    uow.work_packages.add_materialization = AsyncMock()

    result = await sequencer._materialize_current(package, revision, horizon_enabled=False)  # type: ignore[arg-type]

    assert result.completed is False
    assert result.task_id == task_id
    assert package.horizon_phase is None


def _waiting_package() -> tuple[WorkPackage, uuid.UUID]:
    revision_id = uuid.uuid4()
    package = WorkPackage(
        session_id=uuid.uuid4(),
        project_id="test",
        status=WorkPackageStatus.RUNNING,
        title="horizon package",
    )
    package.id = uuid.uuid4()
    package.goal = "ship the horizon"
    package.exit_criteria = None
    package.version = 5
    package.cursor_ordinal = 1
    package.running_revision_id = revision_id
    package.head_revision_id = revision_id
    package.approved_revision_id = revision_id
    package.horizon_phase = "waiting_approval"
    return package, revision_id


@pytest.mark.anyio
async def test_approve_waiting_item_records_single_use_marker() -> None:
    package, revision_id = _waiting_package()
    item_pk = uuid.uuid4()
    uow = MagicMock()
    uow.work_packages.get_package = AsyncMock(return_value=package)
    uow.work_packages.get_fenced_revision = AsyncMock(
        return_value=SimpleNamespace(id=revision_id, revision_number=1, content_hash="ab" * 32)
    )
    uow.work_packages.resolve_fenced_item = AsyncMock(return_value=(revision_id, item_pk))
    uow.session = MagicMock()
    uow.session.get = AsyncMock(return_value=SimpleNamespace(needs_approval=True))
    uow.events.append = AsyncMock()
    service = WorkPackageService(cast(Any, uow))

    generation = await service.approve_waiting_item(
        package_id=package.id,
        revision_number=1,
        h8="ab" * 8,
        expected_status_generation=5,
        ordinal=1,
        user_id=7,
        horizon_enabled=True,
    )

    assert generation == 6
    assert package.horizon_phase == "item_approved:1"


@pytest.mark.anyio
async def test_approve_waiting_item_rejects_wrong_ordinal_and_flag_off() -> None:
    package, revision_id = _waiting_package()
    uow = MagicMock()
    uow.work_packages.get_package = AsyncMock(return_value=package)
    uow.work_packages.get_fenced_revision = AsyncMock(
        return_value=SimpleNamespace(id=revision_id, revision_number=1, content_hash="ab" * 32)
    )
    uow.session = MagicMock()
    uow.events.append = AsyncMock()
    service = WorkPackageService(cast(Any, uow))

    with pytest.raises(DomainError, match="item_not_waiting_approval"):
        await service.approve_waiting_item(
            package_id=package.id,
            revision_number=1,
            h8="ab" * 8,
            expected_status_generation=5,
            ordinal=2,
            user_id=7,
            horizon_enabled=True,
        )
    with pytest.raises(DomainError, match="horizon_not_enabled"):
        await service.approve_waiting_item(
            package_id=package.id,
            revision_number=1,
            h8="ab" * 8,
            expected_status_generation=5,
            ordinal=1,
            user_id=7,
            horizon_enabled=False,
        )


def _scalars_result(rows: list[object]) -> MagicMock:
    result = MagicMock()
    result.all = MagicMock(return_value=rows)
    return result


@pytest.mark.anyio
async def test_lifetime_budget_counts_shared_retry_history() -> None:
    sequencer, uow = _sequencer()
    package = _running_package(goal="ship the horizon")
    package.lifetime_budget = {"max_attempts": 1}
    revision = SimpleNamespace(id=uuid.uuid4())
    task_a, task_b = uuid.uuid4(), uuid.uuid4()
    uow.session.scalar = AsyncMock(side_effect=[SimpleNamespace(needs_approval=False), None])
    uow.session.scalars = AsyncMock(
        side_effect=[
            _scalars_result([task_a]),
            _scalars_result([{"task_id": str(task_a), "previous_task_id": str(task_b)}]),
        ]
    )
    with patch("vuzol.providers.budgets.usage_totals_by_purpose", new=AsyncMock(return_value=[])):
        result = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]

    # Two attempts lifetime (current link + retried-away task) exhaust max 1.
    assert result.completed is False
    assert result.task_id is None
    assert package.status is WorkPackageStatus.PAUSED
    assert package.pause_reason is WorkPackagePauseReason.ITEM_BLOCKED
    payload = uow.events.append.call_args.kwargs["payload"]
    assert payload["reason"] == "lifetime_budget_exhausted"


@pytest.mark.anyio
async def test_passed_deadline_pauses_without_new_task() -> None:
    from datetime import UTC, datetime, timedelta

    sequencer, uow = _sequencer()
    package = _running_package(goal="ship the horizon")
    package.deadline = datetime.now(UTC) - timedelta(seconds=1)
    revision = SimpleNamespace(id=uuid.uuid4())
    uow.session.scalar = AsyncMock(side_effect=[SimpleNamespace(needs_approval=False)])

    result = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]

    assert result.completed is False
    assert result.task_id is None
    assert package.status is WorkPackageStatus.PAUSED
    payload = uow.events.append.call_args.kwargs["payload"]
    assert payload["reason"] == "deadline_exceeded"


@pytest.mark.anyio
async def test_within_lifetime_budget_materializes() -> None:
    sequencer, uow = _sequencer()
    package = _running_package(goal="ship the horizon")
    package.lifetime_budget = {"max_attempts": 5}
    revision = SimpleNamespace(id=uuid.uuid4(), approved_by_user_id=42)
    task_id = uuid.uuid4()
    item = _approval_item()
    item.needs_approval = False
    discussion = SimpleNamespace(chat_id=-100, message_thread_id=10)
    task = MagicMock()
    task.id = task_id
    task.source_chat_id = None
    task.source_thread_id = None
    uow.session.scalar = AsyncMock(side_effect=[item, None])
    uow.session.scalars = AsyncMock(side_effect=[_scalars_result([]), _scalars_result([])])
    uow.session.get = AsyncMock(side_effect=[discussion, task])
    uow.session.flush = AsyncMock()
    uow.tasks.create = AsyncMock(return_value=SimpleNamespace(id=task_id))
    uow.work_packages.add_materialization = AsyncMock()
    with patch("vuzol.providers.budgets.usage_totals_by_purpose", new=AsyncMock(return_value=[])):
        result = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]

    assert result.completed is False
    assert result.task_id == task_id
    assert package.status is WorkPackageStatus.RUNNING


@pytest.mark.anyio
async def test_queue_end_reaches_evaluating_despite_exhausted_budget() -> None:
    sequencer, uow = _sequencer()
    package = _running_package(goal="ship the horizon")
    package.lifetime_budget = {"max_attempts": 1}
    revision = SimpleNamespace(id=uuid.uuid4())
    task_a, task_b = uuid.uuid4(), uuid.uuid4()
    # Queue-end: no item at the cursor.
    uow.session.scalar = AsyncMock(return_value=None)
    uow.session.scalars = AsyncMock(
        side_effect=[
            _scalars_result([task_a]),
            _scalars_result([{"task_id": str(task_a), "previous_task_id": str(task_b)}]),
        ]
    )
    with patch("vuzol.providers.budgets.usage_totals_by_purpose", new=AsyncMock(return_value=[])):
        result = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]

    # Finished work reaches evaluating even though lifetime is exhausted,
    # so acceptance stays callable.
    assert result.completed is False
    assert package.status is WorkPackageStatus.RUNNING
    assert package.horizon_phase == "evaluating"
    event = uow.events.append.call_args.kwargs
    assert event["event_type"] == "work_package.evaluating"

    artifact_id = uuid.uuid4()
    package.running_revision_id = revision.id
    package.head_revision_id = revision.id
    package.approved_revision_id = revision.id
    service, svc_uow = _acceptance_service(package, revision.id, artifact_id)
    generation = await service.record_acceptance(
        package_id=package.id,
        revision_number=1,
        h8="ab" * 8,
        expected_status_generation=package.version,
        accepted=True,
        artifact_id=artifact_id,
        user_id=7,
        horizon_enabled=True,
    )
    assert generation == package.version
    assert package.status.value == WorkPackageStatus.COMPLETED.value
    event = svc_uow.events.append.call_args.kwargs
    assert event["event_type"] == "work_package.accepted"


@pytest.mark.anyio
async def test_queue_end_reaches_evaluating_despite_passed_deadline() -> None:
    from datetime import UTC, datetime, timedelta

    sequencer, uow = _sequencer()
    package = _running_package(goal="ship the horizon")
    package.deadline = datetime.now(UTC) - timedelta(seconds=1)
    revision = SimpleNamespace(id=uuid.uuid4())
    uow.session.scalar = AsyncMock(return_value=None)

    result = await sequencer._materialize_current(package, revision, horizon_enabled=True)  # type: ignore[arg-type]

    assert result.completed is False
    assert package.status is WorkPackageStatus.RUNNING
    assert package.horizon_phase == "evaluating"


def _evaluating_package() -> tuple[WorkPackage, uuid.UUID]:
    revision_id = uuid.uuid4()
    package = WorkPackage(
        session_id=uuid.uuid4(),
        project_id="test",
        status=WorkPackageStatus.RUNNING,
        title="horizon package",
    )
    package.id = uuid.uuid4()
    package.goal = "ship the horizon"
    package.exit_criteria = None
    package.version = 4
    package.running_revision_id = revision_id
    package.head_revision_id = revision_id
    package.approved_revision_id = revision_id
    package.horizon_phase = "evaluating"
    return package, revision_id


def _acceptance_service(
    package: WorkPackage, revision_id: uuid.UUID, artifact_id: uuid.UUID | None
) -> tuple[WorkPackageService, MagicMock]:
    uow = MagicMock()
    uow.work_packages.get_package = AsyncMock(return_value=package)
    uow.work_packages.get_fenced_revision = AsyncMock(
        return_value=SimpleNamespace(id=revision_id, revision_number=1, content_hash="ab" * 32)
    )
    discussion = SimpleNamespace(active_work_package_id=package.id)
    gets: list[object] = []
    if artifact_id is not None:
        gets.append(SimpleNamespace(id=artifact_id))
    gets.append(discussion)
    uow.session = MagicMock()
    uow.session.get = AsyncMock(side_effect=gets)
    uow.events.append = AsyncMock()
    uow.outbox.enqueue = AsyncMock()
    return WorkPackageService(cast(Any, uow)), uow


@pytest.mark.anyio
async def test_record_acceptance_closes_horizon_with_evidence() -> None:
    package, revision_id = _evaluating_package()
    artifact_id = uuid.uuid4()
    service, uow = _acceptance_service(package, revision_id, artifact_id)

    generation = await service.record_acceptance(
        package_id=package.id,
        revision_number=1,
        h8="ab" * 8,
        expected_status_generation=4,
        accepted=True,
        artifact_id=artifact_id,
        user_id=7,
        horizon_enabled=True,
    )

    assert generation == 5
    assert package.status is WorkPackageStatus.COMPLETED
    assert package.acceptance_artifact_id == artifact_id
    assert package.accepted_at is not None
    assert package.horizon_phase is None
    event = uow.events.append.call_args.kwargs
    assert event["event_type"] == "work_package.accepted"


@pytest.mark.anyio
async def test_rejected_acceptance_stays_evaluating() -> None:
    package, revision_id = _evaluating_package()
    service, uow = _acceptance_service(package, revision_id, None)

    generation = await service.record_acceptance(
        package_id=package.id,
        revision_number=1,
        h8="ab" * 8,
        expected_status_generation=4,
        accepted=False,
        artifact_id=None,
        user_id=7,
        horizon_enabled=True,
    )

    assert generation == 5
    assert package.status is WorkPackageStatus.RUNNING
    assert package.horizon_phase == "evaluating"
    assert package.accepted_at is None
    event = uow.events.append.call_args.kwargs
    assert event["event_type"] == "work_package.acceptance_rejected"


@pytest.mark.anyio
async def test_record_acceptance_rejects_flag_off_and_missing_artifact() -> None:
    package, revision_id = _evaluating_package()
    service, _ = _acceptance_service(package, revision_id, None)

    with pytest.raises(DomainError, match="horizon_not_enabled"):
        await service.record_acceptance(
            package_id=package.id,
            revision_number=1,
            h8="ab" * 8,
            expected_status_generation=4,
            accepted=True,
            artifact_id=None,
            user_id=7,
            horizon_enabled=False,
        )

    missing_id = uuid.uuid4()
    uow_missing = MagicMock()
    uow_missing.work_packages.get_package = AsyncMock(return_value=package)
    uow_missing.work_packages.get_fenced_revision = AsyncMock(
        return_value=SimpleNamespace(id=revision_id, revision_number=1, content_hash="ab" * 32)
    )
    uow_missing.session = MagicMock()
    uow_missing.session.get = AsyncMock(
        side_effect=[None, SimpleNamespace(active_work_package_id=package.id)]
    )
    uow_missing.events.append = AsyncMock()
    uow_missing.outbox.enqueue = AsyncMock()
    service2 = WorkPackageService(cast(Any, uow_missing))
    with pytest.raises(DomainError, match="artifact_missing"):
        await service2.record_acceptance(
            package_id=package.id,
            revision_number=1,
            h8="ab" * 8,
            expected_status_generation=4,
            accepted=True,
            artifact_id=missing_id,
            user_id=7,
            horizon_enabled=True,
        )


def _revise_plan(*, first_summary: str = "Step 1") -> PlanDraft:
    return PlanDraft(
        title="Horizon plan",
        items=tuple(
            PlanItemDraft(
                local_id=f"item-{ordinal}",
                summary=first_summary if ordinal == 1 else f"Step {ordinal}",
                goal=f"Goal {ordinal}",
                expected_outcome=f"Outcome {ordinal}",
                completion_criteria=(f"Check {ordinal}",),
                allowed_scope="src/**",
            )
            for ordinal in (1, 2)
        ),
    )


def _revise_package(*, status: WorkPackageStatus, cursor: int | None) -> WorkPackage:
    package = WorkPackage(
        session_id=uuid.uuid4(),
        project_id="test",
        status=status,
        title="horizon package",
    )
    package.id = uuid.uuid4()
    package.goal = "ship the horizon"
    package.exit_criteria = None
    package.version = 3
    package.cursor_ordinal = cursor
    return package


def _revise_uow(package: WorkPackage, *, item_ids: tuple[uuid.UUID, ...]) -> MagicMock:
    uow = MagicMock()
    uow.work_packages.get_package = AsyncMock(return_value=package)
    uow.work_packages.get_head_revision = AsyncMock(
        return_value=SimpleNamespace(id=uuid.uuid4(), revision_number=1, state="draft")
    )
    uow.work_packages.resolve_item_identities = AsyncMock(return_value=item_ids)
    prev = SimpleNamespace(
        ordinal=1,
        item_id=item_ids[0],
        summary="Step 1",
        goal="Goal 1",
        expected_outcome="Outcome 1",
        completion_criteria=["Check 1"],
    )
    scalars_result = MagicMock()
    scalars_result.all = MagicMock(return_value=[prev])
    uow.session = MagicMock()
    uow.session.scalars = AsyncMock(return_value=scalars_result)
    uow.work_packages.add_revision = AsyncMock()
    uow.work_packages.add_revision_item = AsyncMock()
    uow.work_packages.close_open_edit_sessions = AsyncMock(return_value=[])
    uow.work_packages.clear_open_detail = AsyncMock()
    uow.events.append = AsyncMock()
    uow.outbox.enqueue = AsyncMock()
    return uow


@pytest.mark.anyio
async def test_goal_change_requires_choice() -> None:
    from vuzol.storage.types import PlanRevisionCreatedBy

    package = _revise_package(status=WorkPackageStatus.DRAFT, cursor=None)
    package.version = 1
    uow = _revise_uow(package, item_ids=(uuid.uuid4(), uuid.uuid4()))
    service = WorkPackageService(cast(Any, uow))

    with pytest.raises(DomainError, match="goal_change_requires_choice"):
        await service.revise_draft(
            package_id=package.id,
            expected_status_generation=1,
            plan=_revise_plan(),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
            goal="a different product goal",
            horizon_enabled=True,
        )
    uow.work_packages.add_revision.assert_not_called()


@pytest.mark.anyio
async def test_past_item_rewrite_is_revision_conflict() -> None:
    from vuzol.storage.types import PlanRevisionCreatedBy

    package = _revise_package(status=WorkPackageStatus.RUNNING, cursor=2)
    uow = _revise_uow(package, item_ids=(uuid.uuid4(), uuid.uuid4()))
    service = WorkPackageService(cast(Any, uow))

    with pytest.raises(DomainError, match="revision_conflict"):
        await service.revise_draft(
            package_id=package.id,
            expected_status_generation=3,
            plan=_revise_plan(first_summary="Rewritten history"),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
            horizon_enabled=True,
        )
    uow.work_packages.add_revision.assert_not_called()


@pytest.mark.anyio
async def test_future_only_rolling_revision_passes() -> None:
    from vuzol.storage.types import PlanRevisionCreatedBy

    package = _revise_package(status=WorkPackageStatus.RUNNING, cursor=2)
    uow = _revise_uow(package, item_ids=(uuid.uuid4(), uuid.uuid4()))
    # Past ordinal keeps the stored identity; patch the mock to match.
    prev_id = uuid.uuid4()
    uow.work_packages.resolve_item_identities = AsyncMock(return_value=(prev_id, uuid.uuid4()))
    scalars_result = MagicMock()
    scalars_result.all = MagicMock(
        return_value=[
            SimpleNamespace(
                ordinal=1,
                item_id=prev_id,
                summary="Step 1",
                goal="Goal 1",
                expected_outcome="Outcome 1",
                completion_criteria=["Check 1"],
            )
        ]
    )
    uow.session.scalars = AsyncMock(return_value=scalars_result)
    service = WorkPackageService(cast(Any, uow))

    result = await service.revise_draft(
        package_id=package.id,
        expected_status_generation=3,
        plan=_revise_plan(),
        created_by=PlanRevisionCreatedBy.USER,
        actor_type="user",
        horizon_enabled=True,
    )

    assert result.package_id == package.id
    uow.work_packages.add_revision.assert_called_once()
