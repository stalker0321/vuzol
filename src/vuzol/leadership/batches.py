"""Persist and execute the one authoritative managed plan."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError, PlanDraft, semantic_plan_hash
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.discussion.service import WorkPackageService
from vuzol.execution.git import GitError, LocalGit
from vuzol.leadership.actor import Actor
from vuzol.leadership.commands import CommandStatus, LeadCommand
from vuzol.leadership.evidence import evidence_token, package_has_unknown_effects
from vuzol.leadership.limits import check_batch_limits
from vuzol.leadership.persistence import GoalDelegation, ManagedGoal, has_owner_mandate
from vuzol.storage.models import PlanRevision, Task, WorkPackage
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    PlanRevisionCreatedBy,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork


async def propose_plan(
    uow: UnitOfWork,
    goal: ManagedGoal,
    plan: PlanDraft,
    *,
    repository_path: Path | None,
    expected_source_commit: str | None = None,
    planner_profile: str | None = None,
    prompt_version: str | None = None,
) -> dict[str, Any]:
    assert uow.session is not None
    current = (
        await uow.session.get(WorkPackage, goal.current_package_id)
        if goal.current_package_id
        else None
    )
    commit = None
    if repository_path is not None:
        try:
            git = LocalGit()
            commit = await git.read_ref(repository_path, goal.integration_branch)
            if commit is None:
                commit = await git.read_ref(
                    repository_path, str(goal.constraints.get("project_default_branch", "main"))
                )
        except (OSError, GitError) as error:
            raise DomainError("source_repository_unavailable") from error
    if commit is None:
        raise DomainError("source_repository_unavailable")
    if expected_source_commit is not None and commit != expected_source_commit:
        raise DomainError("planner_head_changed")
    service = WorkPackageService(uow)
    if current is not None and current.status in {
        WorkPackageStatus.DRAFT,
        WorkPackageStatus.APPROVED,
        WorkPackageStatus.RUNNING,
        WorkPackageStatus.PAUSED,
    }:
        await check_batch_limits(uow.session, goal, items=len(plan.items), admitting=False)
        result = await service.revise_draft(
            package_id=current.id,
            expected_status_generation=current.version,
            plan=plan,
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="agent",
            source_repo_commit=commit,
            planner_profile=planner_profile,
            prompt_version=prompt_version,
        )
    else:
        await check_batch_limits(uow.session, goal, items=len(plan.items))
        result = await service.create_draft(
            session_id=goal.discussion_session_id,
            project_id=goal.project_id,
            plan=plan,
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="agent",
            managed_goal_id=goal.id,
            source_repo_commit=commit,
            planner_profile=planner_profile,
            prompt_version=prompt_version,
        )
        goal.current_package_id = result.package_id
    revision = await uow.session.get(PlanRevision, result.revision_id)
    assert revision is not None
    approval_required = not has_owner_mandate(goal) or await revision_needs_owner(
        uow.session, revision
    )
    if approval_required and goal.lifecycle != "paused":
        goal.lifecycle = "waiting"
        goal.waiting_reason = "plan_approval"
        goal.wakeup_condition = {"kind": "plan_approval", "revision_id": str(revision.id)}
    elif goal.waiting_reason == "plan_approval":
        goal.lifecycle = "active"
        goal.waiting_reason = None
        goal.wakeup_condition = None
    goal.constraints = {
        "legacy_plan_revision_v1": {"status": "superseded"},
        **goal.constraints,
    }
    # A new revision changes the decision card; an edit far up the topic goes unseen.
    await service._enqueue_plan_projection(
        result.package_id, result.status_generation, "proposal", repost=True
    )
    return {
        "package_id": str(result.package_id),
        "revision_id": str(result.revision_id),
        "content_hash": result.content_hash,
        "scope_revision": goal.scope_revision,
        "title": plan.title,
        "items": len(plan.items),
        "owner_approval_required": approval_required,
    }


_RISK_RANK = {"low": 0, "medium": 1, "high": 2, "privileged": 3}
# The lead may grow an owner-approved plan by this many items before asking again.
DELEGATED_GROWTH = 2


def owner_approval_required(revision: PlanRevision, baseline: PlanRevision | None = None) -> bool:
    """Whether only the owner can approve this revision.

    ``baseline`` is the owner's last approved revision of the same package. Items the
    owner approved verbatim are already decided; a follow-up item at the risk the owner
    accepted is within the mandate. Live pilot: every review-driven follow-up item asked
    the owner again for a README edit.
    """
    items = revision.immutable_body.get("items", [])
    if baseline is not None:
        approved = baseline.immutable_body.get("items", [])
        if (
            approved
            and len(items) <= len(approved) + DELEGATED_GROWTH
            and revision.immutable_body.get("environment_delta")
            == baseline.immutable_body.get("environment_delta")
        ):
            seen = [_decision_content(item) for item in approved]
            risk = max(_RISK_RANK.get(item.get("suggested_risk", "low"), 3) for item in approved)
            large = any(item.get("estimated_complexity") == "large" for item in approved)
            gated = any(item.get("needs_approval", False) for item in approved)
            return any(
                _decision_content(item) not in seen
                and (
                    _RISK_RANK.get(item.get("suggested_risk", "low"), 3) > risk
                    or (item.get("estimated_complexity") == "large" and not large)
                    or (item.get("needs_approval", False) and not gated)
                )
                for item in items
            )
    return (
        len(items) != 1
        or any(
            item.get("suggested_risk", "low") != "low"
            or item.get("needs_approval", False)
            or item.get("estimated_complexity") == "large"
            for item in items
        )
        or bool(revision.immutable_body.get("environment_delta"))
    )


def _decision_content(item: dict[str, Any]) -> dict[str, Any]:
    # Identity and position are bookkeeping; a new package gets new item ids.
    return {key: value for key, value in item.items() if key not in {"ordinal", "item_id"}}


async def owner_baseline(session: AsyncSession, revision: PlanRevision) -> PlanRevision | None:
    """The owner's last approved revision for this goal under the same scope.

    A finished package is replanned as a new package, so look across the goal.
    """
    goal_id = await session.scalar(
        select(WorkPackage.managed_goal_id).where(WorkPackage.id == revision.work_package_id)
    )
    if goal_id is None:
        return None
    baseline = await session.scalar(
        select(PlanRevision)
        .join(WorkPackage, WorkPackage.id == PlanRevision.work_package_id)
        .where(
            WorkPackage.managed_goal_id == goal_id,
            PlanRevision.id != revision.id,
            PlanRevision.approved_by_user_id.is_not(None),
        )
        .order_by(PlanRevision.approved_at.desc())
        .limit(1)
    )
    if baseline is None or baseline.immutable_body.get("scope_revision") != (
        revision.immutable_body.get("scope_revision")
    ):
        return None
    return baseline


async def revision_needs_owner(session: AsyncSession, revision: PlanRevision) -> bool:
    return owner_approval_required(revision, await owner_baseline(session, revision))


async def execute_batch(
    uow: UnitOfWork,
    *,
    actor: Actor,
    goal: ManagedGoal,
    delegation: GoalDelegation,
    command: LeadCommand,
    repeat_limit: int,
) -> dict[str, Any]:
    """Start exactly one item of the stored revision; payload cannot approve it."""
    assert uow.session is not None
    if not has_owner_mandate(goal):
        raise DomainError("owner_mandate_required")
    package = (
        await uow.session.get(WorkPackage, goal.current_package_id)
        if goal.current_package_id
        else None
    )
    if package is None or package.managed_goal_id != goal.id or package.head_revision_id is None:
        raise DomainError("plan_required")
    revision = await uow.session.get(PlanRevision, package.head_revision_id)
    assert revision is not None
    from vuzol.leadership.tasks import check_start_spec

    if command.name == "start_task":
        await check_start_spec(uow.session, goal, package, command.payload)
    if revision.immutable_body.get("scope_revision", goal.scope_revision) != goal.scope_revision:
        raise DomainError("task_spec_stale")
    if await busy_project_task(uow.session, goal.project_id) is not None:
        raise DomainError("project_task_running")
    if await package_has_unknown_effects(uow.session, package.id):
        raise DomainError("unknown_effects_open")
    await check_batch_limits(
        uow.session, goal, items=len(revision.immutable_body["items"]), admitting=False
    )
    if package.status is WorkPackageStatus.RUNNING:
        observed = await WorkPackageSequencer(uow).materialize_running(package_id=package.id)
    else:
        if package.status not in {WorkPackageStatus.DRAFT, WorkPackageStatus.APPROVED}:
            raise DomainError("package_still_live")
        if package.status is WorkPackageStatus.DRAFT and await revision_needs_owner(
            uow.session, revision
        ):
            goal.lifecycle = "waiting"
            goal.waiting_reason = "plan_approval"
            goal.wakeup_condition = {"kind": "plan_approval", "revision_id": str(revision.id)}
            goal.version += 1
            goal.fence_epoch += 1
            await WorkPackageService(uow)._enqueue_plan_projection(
                package.id, package.version, "approval_required", repost=True
            )
            return {
                "status": CommandStatus.WAITING.value,
                "reason": "owner_plan_approval_required",
                "version": goal.version,
            }
        if package.status is WorkPackageStatus.DRAFT:
            await WorkPackageService(uow).bind_delegated_approval(
                package_id=package.id,
                revision_number=revision.revision_number,
                h8=revision.content_hash[:8],
                expected_status_generation=package.version,
                delegation_id=delegation.id,
            )
        from vuzol.discussion.service import _plan_from_revision_body

        fingerprint = semantic_plan_hash(_plan_from_revision_body(revision.immutable_body))
        token = evidence_token(package)
        stored_token = str(goal.constraints.get("repeat_evidence") or "")
        if goal.repeat_fingerprint == fingerprint and stored_token == token:
            goal.repeat_count += 1
        else:
            goal.repeat_count = 1
            goal.repeat_fingerprint = fingerprint
        if goal.repeat_count > repeat_limit:
            raise DomainError("repeat_without_new_evidence")
        observed = await WorkPackageSequencer(uow).start(
            package_id=package.id,
            revision_number=revision.revision_number,
            h8=revision.content_hash[:8],
            expected_status_generation=package.version,
            user_id=delegation.owner_user_id,
            actor_type="agent",
            actor_id=actor.subject,
            delegation_id=delegation.id,
            accountable_owner_user_id=delegation.owner_user_id,
        )
        goal.constraints = {**goal.constraints, "repeat_evidence": evidence_token(package)}
    goal.lifecycle = "active"
    goal.waiting_reason = None
    goal.version += 1
    goal.fence_epoch += 1
    return {
        "status": CommandStatus.APPLIED.value,
        "version": goal.version,
        "package_id": str(package.id),
        "revision_id": str(revision.id),
        "task_id": str(observed.task_id),
    }


async def busy_project_task(session: AsyncSession, project_id: str) -> Task | None:
    """The unfinished task that stops a new one from starting in the project."""
    task: Task | None = await session.scalar(
        select(Task)
        .where(
            Task.project_id == project_id,
            Task.task_type != "discussion_agent_internal",
            Task.status.not_in(USER_TERMINAL_TASK_STATUSES),
        )
        .order_by(Task.created_at)
        .limit(1)
    )
    return task
