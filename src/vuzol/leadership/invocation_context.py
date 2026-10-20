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
    enabled: Callable[[], bool] = lambda: True


current_turn: ContextVar[TurnInvocationContext | None] = ContextVar("lead_turn", default=None)


async def invocation_current(session: AsyncSession, context: TurnInvocationContext) -> bool:
    """Recheck delegation and both leases, including before a process is started."""
    if not context.enabled():
        return False
    goal = await session.get(ManagedGoal, context.claim.goal_id, with_for_update=True)
    turn = await session.get(LeadershipTurn, context.turn_id, with_for_update=True)
    if (
        goal is None
        or turn is None
        or turn.status != "claimed"
        or goal.lifecycle != "active"
        or goal.project_id != context.project_id
        or goal.version != context.version
        or goal.fence_epoch != context.fence_epoch
        or goal.scope_revision != context.scope_revision
        or turn.goal_id != goal.id
        or turn.goal_version != context.version
        or turn.fence_epoch != context.fence_epoch
        or turn.scope_revision != context.scope_revision
        or not await claim_current(session, goal, context.claim, turn)
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
