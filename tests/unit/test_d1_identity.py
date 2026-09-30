"""D1 identity/revisions unit tests (no PostgreSQL)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.discussion.domain import (
    ITEM_CONTRACT_FIELDS,
    PlanDraft,
    item_contract_dict,
    item_contract_hash,
    item_contract_hash_of,
)
from vuzol.discussion.sequencer import _same_plan_item
from vuzol.storage.errors import LeaseLost


def _draft(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "summary": "Implement step",
        "goal": "Finish goal",
        "expected_outcome": "Outcome",
        "completion_criteria": ["Check passes"],
        "allowed_scope": "src/vuzol/**",
        "out_of_scope": [],
        "dependencies": [],
        "trusted_checks": [],
        "suggested_risk": "low",
        "needs_approval": False,
        "estimated_complexity": "small",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_d1_contract_fields_cover_scope_dependencies_approval() -> None:
    """The unified contract includes the fields the old 4-field guard missed."""

    for field in (
        "allowed_scope",
        "out_of_scope",
        "dependencies",
        "trusted_checks",
        "suggested_risk",
        "needs_approval",
        "estimated_complexity",
    ):
        assert field in ITEM_CONTRACT_FIELDS


def test_d1_guard_carry_forward_parity_on_scope_rewrite() -> None:
    """pp.1: an allowed_scope rewrite changes the hash AND breaks carry-forward."""

    before = _draft()
    after = _draft(allowed_scope="src/other/**")
    assert item_contract_hash_of(before) != item_contract_hash_of(after)
    # carry-forward agrees (same projection by construction)
    assert _same_plan_item(cast(Any, before), cast(Any, before)) is True
    assert _same_plan_item(cast(Any, before), cast(Any, after)) is False
    # mapping shapes hash identically to attribute shapes
    as_mapping = {
        "summary": "Implement step",
        "goal": "Finish goal",
        "expected_outcome": "Outcome",
        "completion_criteria": ["Check passes"],
        "allowed_scope": "src/vuzol/**",
        "out_of_scope": [],
        "dependencies": [],
        "trusted_checks": [],
        "suggested_risk": "low",
        "needs_approval": False,
        "estimated_complexity": "small",
    }
    assert item_contract_hash(as_mapping) == item_contract_hash_of(before)
    assert item_contract_dict(before) == item_contract_dict(as_mapping)


def _revise_uow(package: MagicMock, *, item_ids: tuple[uuid.UUID, ...]) -> MagicMock:
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
        allowed_scope="src/**",
        out_of_scope=[],
        dependencies=[],
        trusted_checks=[],
        suggested_risk="low",
        needs_approval=False,
        estimated_complexity="small",
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


def _running_package(*, cursor: int | None = 1) -> MagicMock:
    from vuzol.storage.types import WorkPackageStatus

    package = MagicMock()
    package.status = WorkPackageStatus.RUNNING
    package.version = 3
    package.cursor_ordinal = cursor
    package.goal = "ship it"
    package.exit_criteria = None
    package.execution_contract_version = None
    package.intent_revision = None
    return package


def _plan_with(*, first_summary: str = "Step 1", scope: str = "src/**") -> PlanDraft:
    from vuzol.discussion.domain import PlanItemDraft

    return PlanDraft(
        title="Plan",
        items=(
            PlanItemDraft(
                local_id="item-1",
                summary=first_summary,
                goal="Goal 1",
                expected_outcome="Outcome 1",
                completion_criteria=("Check 1",),
                allowed_scope=scope,
            ),
        ),
    )


@pytest.mark.anyio
async def test_d1_guard_rejects_scope_rewrite_of_past_item() -> None:
    """pp.1: scope rewrite of a passed item → revision_conflict (was silent)."""

    from vuzol.discussion.domain import DomainError
    from vuzol.discussion.service import WorkPackageService
    from vuzol.storage.types import PlanRevisionCreatedBy

    package = _running_package(cursor=2)
    uow = _revise_uow(package, item_ids=(uuid.uuid4(),))
    service = WorkPackageService(cast(Any, uow))
    with pytest.raises(DomainError, match="revision_conflict"):
        await service.revise_draft(
            package_id=uuid.uuid4(),
            expected_status_generation=3,
            plan=_plan_with(scope="src/elsewhere/**"),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
            horizon_enabled=True,
        )


@pytest.mark.anyio
async def test_d1_guard_off_without_flag_and_on_when_pinned() -> None:
    """pp.2: flag off → no guard (legacy); pinned-enabled + flag off → guard."""

    from vuzol.discussion.domain import DomainError
    from vuzol.discussion.horizon import HORIZON_CONTRACT_ENABLED
    from vuzol.discussion.service import WorkPackageService
    from vuzol.storage.types import PlanRevisionCreatedBy

    package = _running_package()
    uow = _revise_uow(package, item_ids=(uuid.uuid4(),))
    service = WorkPackageService(cast(Any, uow))
    # flag off, unpinned: legacy path, no history guard — succeeds past _require
    result = await service.revise_draft(
        package_id=uuid.uuid4(),
        expected_status_generation=3,
        plan=_plan_with(first_summary="Rewritten history"),
        created_by=PlanRevisionCreatedBy.USER,
        actor_type="user",
        horizon_enabled=False,
    )
    assert result.status_generation == 4

    pinned = _running_package(cursor=2)
    pinned.execution_contract_version = HORIZON_CONTRACT_ENABLED
    uow2 = _revise_uow(pinned, item_ids=(uuid.uuid4(),))
    service2 = WorkPackageService(cast(Any, uow2))
    with pytest.raises(DomainError, match="revision_conflict"):
        await service2.revise_draft(
            package_id=uuid.uuid4(),
            expected_status_generation=3,
            plan=_plan_with(first_summary="Rewritten history"),
            created_by=PlanRevisionCreatedBy.USER,
            actor_type="user",
            horizon_enabled=False,
        )


@pytest.mark.anyio
async def test_d1_pause_closes_commit_path() -> None:
    """pp.6: a live lease cannot commit while the run is PAUSED (soft pause)."""

    from vuzol.storage.models import Run, Step
    from vuzol.storage.records import LeaseToken, StepRecord
    from vuzol.storage.types import RunStatus, StepStatus
    from vuzol.workflows.domain import StepOutcome
    from vuzol.workflows.service import commit_step_outcome

    lease = LeaseToken(
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
    step = MagicMock(spec=Step)
    step.id = lease.step.id
    step.run_id = lease.step.run_id
    step.lease_owner = "owner"
    step.lease_generation = 1
    step.status = StepStatus.RUNNING
    run = MagicMock(spec=Run)
    run.id = lease.step.run_id
    run.status = RunStatus.PAUSED
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[step, run])
    with pytest.raises(LeaseLost, match="paused"):
        await commit_step_outcome(
            session,
            lease,
            StepOutcome.succeeded({"ok": True}),
        )


@pytest.mark.anyio
async def test_d1_task_command_records_outcome_receipt() -> None:
    """L5: consumer writes payload.outcome so duplicates return the receipt."""

    from unittest.mock import patch

    from vuzol.workflows.application import TaskCommandResult
    from vuzol.workflows.controls import WorkflowControlConsumer

    action = SimpleNamespace(
        action_kind="pause",
        task_id=uuid.uuid4(),
        requested_by_user_id=7,
        approval_id=None,
        step_id=None,
        payload={},
    )
    session = MagicMock()
    receipt = TaskCommandResult(
        task_id=action.task_id, version=4, status="paused", applied=True
    )
    consumer = WorkflowControlConsumer.__new__(WorkflowControlConsumer)
    with patch(
        "vuzol.workflows.application.apply_task_command",
        new=AsyncMock(return_value=receipt),
    ):
        await consumer._apply(session, action)  # type: ignore[arg-type]
    assert action.payload["outcome"]["applied"] is True
    assert action.payload["outcome"]["version"] == 4
    assert action.payload["outcome"]["task_id"] == str(action.task_id)


def test_d1_spec_revision_is_content_addressed() -> None:
    """L2: identical drafts share a revision; changed drafts get a new one."""

    from vuzol.storage.attempts import spec_revision_for

    assert spec_revision_for({"a": 1}) == spec_revision_for({"a": 1})
    assert spec_revision_for({"a": 1}) != spec_revision_for({"a": 2})


@pytest.mark.anyio
async def test_d1_duplicate_without_version_is_noop_without_receipt() -> None:
    """pp.5: duplicate pause without CAS → _noop event, no outcome receipt."""

    from vuzol.storage.models import Run, Task
    from vuzol.storage.types import RunStatus
    from vuzol.workflows.controls import pause_task

    task = MagicMock(spec=Task)
    task.id = uuid.uuid4()
    run = MagicMock(spec=Run)
    run.id = uuid.uuid4()
    run.status = RunStatus.PAUSED
    run.task_id = task.id
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    # _locked_context: task, run, steps
    with (
        __import__("unittest.mock", fromlist=["patch"]).patch(
            "vuzol.workflows.controls._locked_context",
            new=AsyncMock(return_value=(task, run, ())),
        ),
    ):
        await pause_task(session, task.id, actor_id="7")
    events = [call.args[0] for call in session.add.call_args_list]
    assert any(
        getattr(event, "event_type", "") == "workflow.control_noop" for event in events
    )
    # _noop writes an event only — never a payload outcome receipt
    assert not any(
        isinstance(getattr(event, "payload", None), dict)
        and "outcome" in event.payload
        for event in events
    )
