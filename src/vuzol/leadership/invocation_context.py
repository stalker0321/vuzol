"""Trusted, in-process authority for a model call; never included in its prompt."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.fencing import TurnClaim, claim_current
from vuzol.leadership.persistence import (
    POLICY_REVISION,
    GoalDelegation,
    LeadershipTurn,
    ManagedGoal,
)
from vuzol.leadership.rules import delegation_is_expired


@dataclass(frozen=True, slots=True)
class TurnInvocationContext:
    claim: TurnClaim
    turn_id: uuid.UUID
    project_id: str
    version: int
    fence_epoch: int
    scope_revision: int
    conversational: bool = False
    retrospective: bool = False
    job: bool = False
    enabled: Callable[[], bool] = lambda: True


current_turn: ContextVar[TurnInvocationContext | None] = ContextVar("lead_turn", default=None)


async def invocation_current(
    session: AsyncSession, context: TurnInvocationContext, *, lock: bool = True
) -> bool:
    """Recheck delegation and both leases, including before a process is started."""
    if not context.enabled():
        return False
    if context.job:
        return await _job_current(session, context, lock=lock)
    if context.retrospective:
        return await _retrospective_current(session, context, lock=lock)
    goal = await session.get(ManagedGoal, context.claim.goal_id, with_for_update=lock)
    turn = await session.get(LeadershipTurn, context.turn_id, with_for_update=lock)
    if (
        goal is None
        or turn is None
        or turn.status != "claimed"
        or (
            goal.lifecycle not in {"draft", "active"}
            and not (context.conversational and goal.lifecycle in {"waiting", "paused"})
        )
        or goal.project_id != context.project_id
        or goal.version != context.version
        or goal.fence_epoch != context.fence_epoch
        or goal.scope_revision != context.scope_revision
        or turn.goal_id != goal.id
        or turn.goal_version != context.version
        or turn.fence_epoch != context.fence_epoch
        or turn.scope_revision != context.scope_revision
        or not await claim_current(session, goal, context.claim, turn, lock=lock)
    ):
        return False
    delegation = await session.scalar(
        select(GoalDelegation).where(GoalDelegation.id == goal.delegation_id)
    )
    return (
        delegation is not None
        and delegation.project_id == goal.project_id
        and delegation.policy_revision == POLICY_REVISION
        and delegation.agent_principal_id == server_agent_principal(goal.project_id)
        and delegation.revoked_at is None
        and not delegation_is_expired(delegation.expires_at, now=datetime.now(UTC))
    )


async def _retrospective_current(
    session: AsyncSession, context: TurnInvocationContext, *, lock: bool = True
) -> bool:
    """A terminal goal's read-only inference is authorized solely by its outbox lease."""
    from vuzol.storage.models import TransactionalOutbox
    from vuzol.storage.types import DeliveryStatus

    claim = context.claim
    item = await session.get(TransactionalOutbox, claim.wakeup_id, with_for_update=lock)
    turn = await session.get(LeadershipTurn, context.turn_id, with_for_update=lock)
    goal = await session.get(ManagedGoal, claim.goal_id)
    return (
        item is not None
        and item.destination == "goal_retrospective"
        and item.operation_type == "retrospect_goal"
        and item.linked_entity_id == claim.goal_id
        and item.status == DeliveryStatus.LEASED
        and item.lease_owner == claim.owner
        and item.lease_generation == claim.outbox_generation == claim.lease_epoch
        and item.lease_expires_at is not None
        and item.lease_expires_at > datetime.now(UTC)
        and not item.payload.get("applied")
        and goal is not None
        and goal.project_id == context.project_id
        and (goal.lifecycle == "cancelled" or bool(goal.constraints.get("owner_acceptance")))
        and turn is not None
        and turn.status == "claimed"
        and turn.goal_id == goal.id
        and turn.owner == claim.owner
        and turn.lease_epoch == claim.outbox_generation
        and turn.wakeup_id == uuid.uuid5(item.id, str(claim.outbox_generation))
    )


async def _job_current(
    session: AsyncSession, context: TurnInvocationContext, *, lock: bool = True
) -> bool:
    """Read-only research follows its job lease, independent of lead turn versions."""
    from vuzol.leadership.jobs import JobClaim, claim_matches
    from vuzol.leadership.persistence import LeadJob

    claim = context.claim
    goal = await session.get(ManagedGoal, claim.goal_id, with_for_update=lock)
    turn = await session.get(LeadershipTurn, context.turn_id, with_for_update=lock)
    job = await session.get(LeadJob, claim.wakeup_id, with_for_update=lock)
    if (
        goal is None
        or job is None
        or turn is None
        or goal.project_id != context.project_id
        or goal.lifecycle not in {"draft", "active", "waiting"}
        or not claim_matches(
            job,
            goal,
            JobClaim(job.id, goal.id, claim.owner, claim.lease_epoch),
            now=datetime.now(UTC),
        )
        or turn.id != uuid.uuid5(job.id, f"attempt:{claim.lease_epoch}")
        or turn.wakeup_id != turn.id
        or turn.goal_id != goal.id
        or turn.status != "claimed"
        or turn.owner != claim.owner
        or turn.lease_epoch != claim.lease_epoch
        or claim.outbox_generation != claim.lease_epoch
    ):
        return False
    delegation = await session.get(GoalDelegation, goal.delegation_id)
    return (
        delegation is not None
        and delegation.project_id == goal.project_id
        and delegation.policy_revision == POLICY_REVISION
        and delegation.agent_principal_id == server_agent_principal(goal.project_id)
        and delegation.revoked_at is None
        and not delegation_is_expired(delegation.expires_at, now=datetime.now(UTC))
    )
