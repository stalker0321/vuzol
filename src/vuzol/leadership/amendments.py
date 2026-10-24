"""Owner-bound scope changes; current task contracts remain immutable."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.leadership.evidence import package_has_unknown_effects
from vuzol.leadership.persistence import POLICY_REVISION, GoalDelegation, ManagedGoal
from vuzol.leadership.rules import delegation_is_expired
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    ProjectDiscussionSession,
    Task,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import USER_TERMINAL_TASK_STATUSES, WorkPackageStatus


async def store_amendment(
    session: AsyncSession,
    goal: ManagedGoal,
    delegation: GoalDelegation,
    payload: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    # Use the same criterion validation as initial admission.
    from vuzol.leadership.service import _criteria

    if (
        delegation.revoked_at is not None
        or delegation_is_expired(delegation.expires_at, now=datetime.now(UTC))
        or delegation.policy_revision != POLICY_REVISION
    ):
        raise DomainError("delegation_inactive")
    scope = str(payload.get("scope") or "").strip()
    criteria = _criteria(payload.get("criteria"))
    policy = payload.get("inflight_policy")
    if not scope or not reason.strip():
        raise DomainError("amendment_scope_and_reason_required")
    if policy not in {"finish_current", "cancel_current", "queue"}:
        raise DomainError("amendment_policy_invalid")
    try:
        source_id = uuid.UUID(str(payload.get("source_event_id")))
    except ValueError:
        raise DomainError("amendment_source_required") from None
    source = await session.get(TransactionalOutbox, source_id)
    if (
        source is None
        or source.destination != "project_lead"
        or source.linked_entity_id != goal.id
        or source.payload.get("wakeup_kind") != "owner_message"
        or source.payload.get("owner_user_id") != goal.owner_user_id
        or source.payload.get("delegation_id") != str(delegation.id)
        or source.payload.get("scope_revision") != goal.scope_revision
    ):
        raise DomainError("amendment_source_mismatch")
    if str(source_id) in goal.constraints.get("amendment_sources", []):
        raise DomainError("amendment_source_consumed")
    if goal.constraints.get("pending_amendment") or goal.constraints.get("queued_amendment"):
        raise DomainError("amendment_pending")
    # Cancellation is a narrower use of the existing cancel_goal grant. Never
    # upgrade old grants, and do not introduce a new model-facing control action.
    if policy == "cancel_current" and "cancel_goal" not in delegation.allowed_actions:
        raise DomainError("action_not_delegated")
    amendment = {
        "scope": scope,
        "criteria": criteria,
        "reason": reason,
        "source_event_id": str(source_id),
        "base_version": goal.version,
        "scope_revision": goal.scope_revision,
        "delegation_id": str(delegation.id),
        "inflight_policy": policy,
    }
    key = "queued_amendment" if policy == "queue" else "pending_amendment"
    goal.constraints = {
        **goal.constraints,
        key: amendment,
        "amendment_sources": [*goal.constraints.get("amendment_sources", []), str(source_id)],
    }
    return amendment


async def apply_pending_amendment(session: AsyncSession, goal: ManagedGoal) -> bool:
    """Caller holds the goal lock, before package/task locks. False means wait."""
    pending = goal.constraints.get("pending_amendment")
    queued = goal.constraints.get("queued_amendment")
    if not pending and not queued:
        return False
    delegation = await session.get(GoalDelegation, goal.delegation_id)
    amendment = pending or queued
    if not isinstance(amendment, dict):
        return False
    if (
        delegation is None
        or delegation.revoked_at is not None
        or delegation_is_expired(delegation.expires_at, now=datetime.now(UTC))
        or delegation.policy_revision != POLICY_REVISION
        or amendment["delegation_id"] != str(goal.delegation_id)
        or amendment["scope_revision"] != goal.scope_revision
        or goal.lifecycle in {"cancelled", "completed"}
    ):
        return False
    package = (
        await session.get(WorkPackage, goal.current_package_id, with_for_update=True)
        if goal.current_package_id
        else None
    )
    if package is not None:
        if not pending and package.status in {
            WorkPackageStatus.DRAFT,
            WorkPackageStatus.APPROVED,
            WorkPackageStatus.RUNNING,
            WorkPackageStatus.PAUSED,
        }:
            return False
        unfinished = await session.scalar(
            select(Task.id)
            .join(MaterializationLink, MaterializationLink.task_id == Task.id)
            .where(
                MaterializationLink.work_package_id == package.id,
                Task.status.not_in(USER_TERMINAL_TASK_STATUSES),
            )
            .limit(1)
        )
        if unfinished is not None or await package_has_unknown_effects(session, package.id):
            return False
        # Keep the package and its integration head as history. Its unstarted
        # suffix no longer has approval, and cannot resume under the new scope.
        if package.status not in {
            WorkPackageStatus.COMPLETED,
            WorkPackageStatus.STOPPED,
            WorkPackageStatus.DISCARDED,
        }:
            package.status = WorkPackageStatus.STOPPED
            package.pause_reason = None
        package.approved_revision_id = None
        package.version += 1
        discussion = await session.get(
            ProjectDiscussionSession, package.session_id, with_for_update=True
        )
        if discussion is not None and discussion.active_work_package_id == package.id:
            discussion.active_work_package_id = None
    constraints = dict(goal.constraints)
    constraints.pop("pending_amendment", None)
    constraints.pop("queued_amendment", None)
    constraints.pop("paused_children", None)
    constraints["last_amendment"] = amendment
    constraints.setdefault("legacy_plan_revision_v1", {"status": "superseded"})
    goal.constraints = constraints
    goal.scope_text = amendment["scope"]
    goal.criteria = amendment["criteria"]
    if goal.lifecycle == "waiting":
        # Every wait belonged to the old scope (a plan approval, a question, a
        # package): none can be satisfied now, so the lead must take a turn.
        # A paused goal stays paused.
        goal.lifecycle = "active"
        goal.waiting_reason = None
        goal.wakeup_condition = None
    if goal.constraints.get("mandate_required"):
        from vuzol.leadership.commands import DRAFT_ACTIONS

        delegation.allowed_actions = sorted(DRAFT_ACTIONS)
        delegation.version += 1
        if goal.lifecycle == "active":
            goal.lifecycle = "draft"
    goal.scope_revision += 1
    goal.version += 1
    goal.fence_epoch += 1
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type="managed_goal.amended",
            actor_type="system",
            actor_id="project-lead",
            payload={**amendment, "scope_revision": goal.scope_revision, "version": goal.version},
        )
    )
    return True
