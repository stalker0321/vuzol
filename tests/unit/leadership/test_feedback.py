"""Bounded context retains references and signals omissions instead of inventing data."""

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.leadership.feedback import MAX_CONTEXT_BYTES, _plan, _put, _report


def test_projection_is_allowlisted_and_limits_plan_items():
    plan = _plan(
        {
            "title": "bounded",
            "owner_user_id": "PRIVATE_OWNER",
            "items": [
                {
                    "summary": "x" * 10_000,
                    "completion_criteria": ["test"] * 100,
                    "credential": "PRIVATE_CREDENTIAL",
                }
            ]
            * 100,
        }
    )
    assert plan["omitted_items"] == 96
    assert len(plan["items"]) == 4
    assert len(plan["items"][0]["summary"]) < 600
    assert len(plan["items"][0]["completion_criteria"]) == 4
    assert "PRIVATE" not in json.dumps(plan)


def test_global_context_budget_is_in_utf8_bytes_and_keeps_valid_json():
    context = {"binding": {"scope_revision": 1}, "truncated": False}
    for i in range(40):
        _put(context, f"section_{i}", {"summary": "я" * 1000})
    encoded = json.dumps(context, ensure_ascii=False)
    assert len(encoded.encode()) <= MAX_CONTEXT_BYTES
    assert context["truncated"] is True
    assert json.loads(encoded)["binding"]["scope_revision"] == 1


def test_report_reaches_lead_with_sources_and_explicit_omissions():
    report = _report(
        {
            "summary": "Research complete",
            "findings": ["finding"] * 6,
            "sources": ["README.md:1"],
            "limitations": ["No live access"],
            "credential": "PRIVATE",
        }
    )
    assert report["summary"] == "Research complete"
    assert report["sources"] == ["README.md:1"]
    assert report["omitted_findings"] == 2
    assert "PRIVATE" not in json.dumps(report)


@pytest.mark.anyio
@pytest.mark.parametrize("no_change", [True, False])
async def test_feedback_exposes_bounded_no_change_claim_for_its_task(no_change):
    from vuzol.leadership.feedback import result_context
    from vuzol.leadership.persistence import ManagedGoal
    from vuzol.storage.models import WorkPackage
    from vuzol.storage.types import RunStatus, StepStatus, WorkPackageStatus

    goal = ManagedGoal(id=uuid.uuid4(), current_package_id=uuid.uuid4(), scope_revision=1)
    package = WorkPackage(
        id=goal.current_package_id,
        managed_goal_id=goal.id,
        status=WorkPackageStatus.RUNNING,
        version=1,
        integration_head_commit="a" * 40,
    )
    row = SimpleNamespace(
        id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        ordinal=5,
        step_type="validate",
        status=StepStatus.COMPLETED,
        run_status=RunStatus.RUNNING,
        failure_category=None,
        run_failure_category=None,
        failure_summary=None,
        run_failure_summary=None,
        unknown_effects=False,
        implementation_summary="Validated",
        summary=None,
        structured={
            "no_change": no_change,
            "no_change_reason": "я" * 2500,
            "result_commit": "a" * 40,
        },
    )
    session = MagicMock(
        get=AsyncMock(return_value=package),
        scalar=AsyncMock(return_value=0),
        execute=AsyncMock(return_value=MagicMock(all=lambda: [row])),
    )

    context = await result_context(session, goal, package)

    result = context["steps"][0]
    assert result["task_id"] == str(row.task_id)
    if no_change:
        assert result["no_change"] is True
        assert result["no_change_reason"] == "я" * 2000
        assert result["no_change_reason_untrusted"] is True
    else:
        assert "no_change" not in result


@pytest.mark.anyio
@pytest.mark.parametrize("binding", ["current", "stale", "repaired"])
async def test_task_context_exposes_only_current_no_change_validation(binding):
    from vuzol.leadership.persistence import ManagedGoal
    from vuzol.leadership.tasks import task_context
    from vuzol.storage.models import Run, Task, WorkPackage, Worktree
    from vuzol.storage.types import TaskStatus

    goal = ManagedGoal(id=uuid.uuid4(), current_package_id=uuid.uuid4(), scope_revision=1)
    package = WorkPackage(id=goal.current_package_id)
    task = Task(id=uuid.uuid4(), status=TaskStatus.WAITING_APPROVAL)
    run = Run(id=uuid.uuid4(), selected_route={})
    tree = Worktree(result_commit="a" * 40)
    manifest = {
        "no_change": True,
        "no_change_reason": "я" * 2500,
        "result_commit": "b" * 40 if binding == "stale" else tree.result_commit,
    }
    validations = [(run.id, manifest)]
    if binding == "repaired":
        validations.insert(0, (run.id, {"result_commit": tree.result_commit}))
    session = MagicMock(
        get=AsyncMock(return_value=package),
        execute=AsyncMock(
            side_effect=[
                MagicMock(all=lambda: [(task, run, tree)]),
                [],
                validations,
            ]
        ),
    )

    context = await task_context(session, goal)

    result = context["tasks"][0]
    assert result["task_id"] == str(task.id)
    if binding == "current":
        assert result["no_change"] is True
        assert result["no_change_reason"] == "я" * 2000
        assert result["no_change_reason_untrusted"] is True
    else:
        assert "no_change" not in result
