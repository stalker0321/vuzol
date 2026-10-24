"""Explicit managed-task selection and result acceptance within the existing grant."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.leadership.evidence import new_effect_blocker, package_has_unknown_effects
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import (
    Approval,
    Event,
    MaterializationLink,
    PlanRevision,
    Run,
    Step,
    Task,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import ApprovalStatus, RunStatus, StepStatus, TaskStatus, WorkPackageStatus
from vuzol.workflows.result_approval import _validation_evidence, verified_envelope
from vuzol.workflows.transitions import transition_step, transition_task


def spec_digest(spec: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


async def start_spec(
    session: AsyncSession, goal: ManagedGoal, package: WorkPackage | None
) -> dict[str, Any] | None:
    if package is None or package.head_revision_id is None:
        return None
    revision = await session.get(PlanRevision, package.head_revision_id)
    if revision is None:
        return None
    from vuzol.discussion.sequencer import first_unfinished_ordinal

    ordinal = (
        package.cursor_ordinal
        if package.status is WorkPackageStatus.RUNNING
        else await first_unfinished_ordinal(session, package.id, revision)
    )
    return {
        "plan_revision_id": str(revision.id),
        "plan_hash": revision.content_hash,
        "scope_revision": revision.immutable_body.get("scope_revision", goal.scope_revision),
        "ordinal": ordinal,
    }


async def check_start_spec(
    session: AsyncSession, goal: ManagedGoal, package: WorkPackage | None, payload: dict[str, Any]
) -> None:
    expected = await start_spec(session, goal, package)
    if (
        expected is None
        or payload.get("spec") != expected
        or expected["scope_revision"] != goal.scope_revision
    ):
        raise DomainError("task_spec_stale")


async def execution_binding(session: AsyncSession, task: Task) -> dict[str, Any] | None:
    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task.id)
    )
    package = await session.get(WorkPackage, link.work_package_id) if link else None
    if package is None or package.managed_goal_id is None or link is None:
        return None
    goal = await session.get(ManagedGoal, package.managed_goal_id)
    if goal is None:
        raise DomainError("managed_goal_missing")
    return {
        "goal_id": str(goal.id),
        "scope_revision": goal.scope_revision,
        "plan_revision_id": str(link.plan_revision_id),
        "plan_revision_item_id": str(link.plan_revision_item_id),
        "spec_revision": task.spec_revision,
    }


async def scoped_task(session: AsyncSession, goal: ManagedGoal, task_id: object) -> Task:
    try:
        parsed = uuid.UUID(str(task_id))
    except ValueError:
        raise DomainError("task_out_of_scope") from None
    link = await session.scalar(
        select(MaterializationLink).where(
            MaterializationLink.task_id == parsed,
            MaterializationLink.work_package_id == goal.current_package_id,
        )
    )
    task = await session.get(Task, parsed, with_for_update=True) if link else None
    if task is None or task.project_id != goal.project_id:
        raise DomainError("task_out_of_scope")
    return task


async def accept_result(
    session: AsyncSession,
    goal: ManagedGoal,
    payload: dict[str, Any],
    *,
    actor_id: str,
    branch_prefix: str,
) -> dict[str, Any]:
    task = await scoped_task(session, goal, payload.get("task_id"))
    accepted = await session.scalar(
        select(Event.payload).where(
            Event.entity_type == "task",
            Event.entity_id == task.id,
            Event.event_type == "result.accepted_by_lead",
        )
    )
    if accepted is not None:
        # Live pilot: the lead repeated accept_result after the task finished; a
        # refusal parked the goal as if it needed the owner. Accepting is done.
        return {
            "task_id": str(task.id),
            "approval_id": accepted.get("approval_id"),
            "already_accepted": True,
        }
    if task.status is not TaskStatus.WAITING_APPROVAL:
        raise DomainError("result_not_ready")
    run = await session.scalar(
        select(Run)
        .where(Run.task_id == task.id)
        .order_by(Run.created_at.desc())
        .limit(1)
        .with_for_update()
    )
    if run is None or run.status is not RunStatus.RUNNING:
        raise DomainError("result_not_ready")
    binding = run.selected_route.get("managed_task")
    expected = await execution_binding(session, task)
    package = await session.get(WorkPackage, goal.current_package_id)
    if (
        not binding
        or binding != expected
        or package is None
        or package.status is not WorkPackageStatus.RUNNING
        or str(package.running_revision_id) != binding["plan_revision_id"]
        or package.head_revision_id != package.running_revision_id
        or package.approved_revision_id != package.running_revision_id
        or payload.get("scope_revision") != goal.scope_revision
        or payload.get("plan_revision_id") != binding["plan_revision_id"]
        or payload.get("spec_revision") != task.spec_revision
        or task.spec_revision != spec_digest(task.task_draft)
    ):
        raise DomainError("result_revision_stale")
    from vuzol.leadership.rules import integration_branch_rejection

    default_branch = goal.constraints.get("project_default_branch")
    if (
        integration_branch_rejection(
            goal.integration_branch,
            prefix=branch_prefix,
            default_branch=default_branch if isinstance(default_branch, str) else None,
        )
        is not None
    ):
        raise DomainError("integration_branch_forbidden")
    blocker = await new_effect_blocker(session, task.id)
    if blocker or await package_has_unknown_effects(session, package.id):
        raise DomainError(blocker or "unknown_effects_open")
    steps = list(
        await session.scalars(
            select(Step).where(Step.run_id == run.id).order_by(Step.ordinal).with_for_update()
        )
    )
    step = next(
        (
            s
            for s in steps
            if s.step_type == "approval"
            and s.payload.get("requested_action") == "apply_result"
            and s.status is StepStatus.WAITING_APPROVAL
        ),
        None,
    )
    if step is None or any(
        s.unknown_effects
        or s.status
        in {
            StepStatus.RUNNING,
            StepStatus.LEASED,
            StepStatus.QUEUED,
            StepStatus.BLOCKED,
            StepStatus.AWAITING_USER,
        }
        for s in steps
    ):
        raise DomainError("validation_or_repair_pending")
    approval = await session.scalar(
        select(Approval).where(Approval.step_id == step.id).with_for_update()
    )
    worktree = await session.scalar(
        select(Worktree).where(Worktree.run_id == run.id).with_for_update()
    )
    if (
        approval is None
        or approval.status is not ApprovalStatus.PENDING
        or approval.expires_at <= datetime.now(UTC)
        or worktree is None
    ):
        raise DomainError("result_approval_stale")
    try:
        envelope = verified_envelope(step, approval)
        evidence = _validation_evidence({s.ordinal: s for s in steps}, worktree)
    except ValueError as error:
        raise DomainError("result_evidence_stale") from error
    if (
        payload.get("result_commit") != worktree.result_commit
        or (
            envelope.get("result_kind") == "report"
            and payload.get("report_hash") != evidence["report_hash"]
        )
        or envelope.get("result_commit") != worktree.result_commit
        or envelope.get("diff_hash") != worktree.diff_hash
        or envelope.get("base_commit") != worktree.base_commit
        or envelope.get("target_branch") != goal.integration_branch
        or envelope.get("configuration_revision") != run.configuration_revision
        or envelope.get("policy_revision") != run.policy_revision
        or any(
            envelope.get(key) != evidence[key]
            for key in (
                "validation_evidence_hash",
                "review_evidence_hash",
                "static_build_evidence",
                "artifact_evidence",
            )
        )
    ):
        raise DomainError("result_commit_stale")
    # This consumes the owner's existing apply_integration grant for this exact
    # envelope. It cannot approve deployment, installation or a protected ref.
    approval.status = ApprovalStatus.APPROVED
    approval.decided_at = datetime.now(UTC)
    await transition_step(session, step, StepStatus.QUEUED, actor_type="agent", actor_id=actor_id)
    await transition_task(
        session, task, TaskStatus.EXECUTING, actor_type="agent", actor_id=actor_id
    )
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="result.accepted_by_lead",
            actor_type="agent",
            actor_id=actor_id,
            payload={"approval_id": str(approval.id), **binding},
        )
    )
    return {
        "task_id": str(task.id),
        "approval_id": str(approval.id),
        "result_commit": worktree.result_commit,
    }


async def task_context(session: AsyncSession, goal: ManagedGoal) -> dict[str, Any]:
    package = (
        await session.get(WorkPackage, goal.current_package_id) if goal.current_package_id else None
    )
    context: dict[str, Any] = {
        "start_spec": await start_spec(session, goal, package),
    }
    if package is None:
        return context
    revision = (
        await session.get(PlanRevision, package.head_revision_id)
        if package.head_revision_id
        else None
    )
    if revision is not None and context["start_spec"] is not None:
        ordinal = context["start_spec"]["ordinal"]
        context["next_item"] = next(
            (item for item in revision.immutable_body["items"] if item["ordinal"] == ordinal), None
        )
    rows = await session.execute(
        select(Task, Run, Worktree)
        .join(MaterializationLink, MaterializationLink.task_id == Task.id)
        .outerjoin(Run, Run.task_id == Task.id)
        .outerjoin(Worktree, Worktree.run_id == Run.id)
        .where(MaterializationLink.work_package_id == package.id)
        .order_by(Task.created_at.desc())
        .limit(10)
    )
    tasks = rows.all()
    report_hashes = {
        run_id: digest
        for run_id, digest in await session.execute(
            select(Step.run_id, Step.payload["action_envelope"]["report_hash"].as_string()).where(
                Step.run_id.in_([run.id for _task, run, _tree in tasks if run]),
                Step.step_type == "approval",
            )
        )
        if digest is not None
    }
    context["tasks"] = [
        {
            "task_id": str(task.id),
            "status": task.status.value,
            "accept_spec": {
                "task_id": str(task.id),
                "scope_revision": (run.selected_route.get("managed_task") or {}).get(
                    "scope_revision"
                )
                if run
                else None,
                "plan_revision_id": str(package.running_revision_id),
                "spec_revision": task.spec_revision,
                "result_commit": tree.result_commit if tree else None,
                **(
                    {"report_hash": report_hashes[run.id]}
                    if run and run.id in report_hashes
                    else {}
                ),
            },
        }
        for task, run, tree in tasks
    ]
    return context


async def task_turn_ready(session: AsyncSession, package: WorkPackage) -> bool:
    statuses = list(
        await session.scalars(
            select(Task.status)
            .join(
                MaterializationLink,
                MaterializationLink.task_id == Task.id,
            )
            .where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == package.running_revision_id,
                MaterializationLink.ordinal == package.cursor_ordinal,
            )
        )
    )
    return not statuses or all(
        status
        in {
            TaskStatus.WAITING_APPROVAL,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.BLOCKED,
            TaskStatus.QUOTA_EXHAUSTED,
        }
        for status in statuses
    )
