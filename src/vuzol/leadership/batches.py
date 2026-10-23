"""Start one stored plan through the existing sequencer."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.discussion.service import WorkPackageService
from vuzol.leadership.actor import Actor
from vuzol.leadership.commands import CommandStatus, LeadCommand
from vuzol.leadership.evidence import evidence_token, package_has_unknown_effects
from vuzol.leadership.limits import check_batch_limits
from vuzol.leadership.persistence import GoalDelegation, ManagedGoal
from vuzol.leadership.service import plan_draft_from_payload
from vuzol.storage.models import Task, WorkPackage
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    PlanRevisionCreatedBy,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork

_LIVE = frozenset(
    {
        WorkPackageStatus.DRAFT,
        WorkPackageStatus.APPROVED,
        WorkPackageStatus.RUNNING,
        WorkPackageStatus.PAUSED,
    }
)


async def execute_batch(
    uow: UnitOfWork,
    *,
    actor: Actor,
    goal: ManagedGoal,
    delegation: GoalDelegation,
    command: LeadCommand,
    repeat_limit: int,
) -> dict[str, Any]:
    """Create, approve, and start the plan already stored on the goal.

    The model payload is ignored. A refusal before any package row is a
    returned status; a refusal after writes raises so the savepoint rolls back.
    """

    assert uow.session is not None
    stored = goal.proposed_plan
    if not isinstance(stored, dict):
        raise DomainError("plan_required")
    plan = plan_draft_from_payload({"plan": stored})
    current = await _current_package(uow.session, goal)
    from vuzol.leadership.tasks import check_start_spec

    if command.name == "start_task":
        check_start_spec(goal, current, command.payload)
    busy = await uow.session.scalar(
        select(Task.id)
        .where(
            Task.project_id == goal.project_id,
            Task.task_type != "discussion_agent_internal",
            Task.status.not_in(USER_TERMINAL_TASK_STATUSES),
        )
        .limit(1)
    )
    if busy is not None:
        raise DomainError("project_task_running")
    if current is not None and await package_has_unknown_effects(uow.session, current.id):
        raise DomainError("unknown_effects_open")
    if current is not None and current.status in _LIVE:
        if current.status is not WorkPackageStatus.RUNNING:
            raise DomainError("package_still_live")
        await _check_dependencies(uow.session, current)
        observed = await WorkPackageSequencer(uow).materialize_running(package_id=current.id)
        goal.version += 1
        goal.fence_epoch += 1
        return {
            "status": CommandStatus.APPLIED.value,
            "version": goal.version,
            "package_id": str(current.id),
            "revision_id": str(current.running_revision_id),
            "task_id": str(observed.task_id),
        }
    if plan.items[0].dependencies:
        raise DomainError("dependencies_unfinished")
    await check_batch_limits(uow.session, goal, items=len(plan.items))
    token = evidence_token(current)
    fingerprint = _fingerprint(stored)
    stored_token = str(dict(goal.constraints).get("repeat_evidence") or "")
    if goal.repeat_fingerprint == fingerprint and stored_token == token:
        goal.repeat_count += 1
    else:
        goal.repeat_count = 1
        goal.repeat_fingerprint = fingerprint
    if goal.repeat_count > repeat_limit:
        goal.lifecycle = "waiting"
        goal.waiting_reason = "repeat_without_new_evidence"
        goal.lease_owner = None
        goal.lease_until = None
        goal.version += 1
        goal.fence_epoch += 1
        return {
            "status": CommandStatus.WAITING.value,
            "reason": "repeat_without_new_evidence",
            "version": goal.version,
        }
    result = await WorkPackageService(uow).create_draft(
        session_id=goal.discussion_session_id,
        project_id=goal.project_id,
        plan=plan,
        created_by=PlanRevisionCreatedBy.SYSTEM,
        actor_type="agent",
    )
    package = await uow.work_packages.get_package(result.package_id, for_update=True)
    package.managed_goal_id = goal.id
    await uow.session.flush()
    generation = await WorkPackageService(uow).bind_delegated_approval(
        package_id=package.id,
        revision_number=result.revision_number,
        h8=result.content_hash[:8],
        expected_status_generation=result.status_generation,
        delegation_id=delegation.id,
    )
    observed = await WorkPackageSequencer(uow).start(
        package_id=package.id,
        revision_number=result.revision_number,
        h8=result.content_hash[:8],
        expected_status_generation=generation,
        user_id=delegation.owner_user_id,
        horizon_enabled=False,
        actor_type="agent",
        actor_id=actor.subject,
        delegation_id=delegation.id,
        accountable_owner_user_id=delegation.owner_user_id,
    )
    constraints = dict(goal.constraints)
    constraints["repeat_evidence"] = evidence_token(package)
    goal.constraints = constraints
    goal.current_package_id = package.id
    goal.lifecycle = "active"
    goal.waiting_reason = None
    goal.version += 1
    goal.fence_epoch += 1
    return {
        "status": CommandStatus.APPLIED.value,
        "version": goal.version,
        "package_id": str(package.id),
        "revision_id": str(result.revision_id),
        "task_id": str(observed.task_id),
    }


async def _current_package(session: AsyncSession, goal: ManagedGoal) -> WorkPackage | None:
    if goal.current_package_id is None:
        return None
    return await session.get(WorkPackage, goal.current_package_id)


def _fingerprint(plan: dict[str, Any]) -> str:
    encoded = json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


async def _check_dependencies(session: AsyncSession, package: WorkPackage) -> None:
    from vuzol.storage.models import MaterializationLink, PlanRevisionItem, WorkItemDraft
    from vuzol.storage.types import TaskStatus

    item = await session.scalar(
        select(PlanRevisionItem).where(
            PlanRevisionItem.plan_revision_id == package.running_revision_id,
            PlanRevisionItem.ordinal == package.cursor_ordinal,
        )
    )
    if item is None:
        raise DomainError("plan_item_missing")
    for dependency in item.dependencies:
        done = await session.scalar(
            select(Task.id)
            .join(MaterializationLink, MaterializationLink.task_id == Task.id)
            .join(
                PlanRevisionItem, PlanRevisionItem.id == MaterializationLink.plan_revision_item_id
            )
            .join(WorkItemDraft, WorkItemDraft.id == PlanRevisionItem.item_id)
            .where(
                MaterializationLink.plan_revision_id == package.running_revision_id,
                WorkItemDraft.local_id == dependency,
                Task.status == TaskStatus.COMPLETED,
            )
            .limit(1)
        )
        if done is None:
            raise DomainError("dependencies_unfinished")
