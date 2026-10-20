"""Keep a live managed goal as the only package authority on its discussion."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import LIVE_GOAL_LIFECYCLES, ManagedGoal
from vuzol.storage.models import WorkPackage


async def lock_project_admission(session: AsyncSession, project_id: str) -> None:
    """Serialize legacy/managed admission without adding another scheduler."""
    await session.execute(
        select(
            func.pg_advisory_xact_lock(func.hashtextextended(f"project-admission:{project_id}", 0))
        )
    )


async def legacy_package_is_fenced(session: AsyncSession, package: WorkPackage) -> bool:
    """True when legacy control must not start this package.

    The flag is not consulted. A live goal keeps the session even after the
    feature is turned off; only its current package remains eligible.
    """

    goal = await session.scalar(
        select(ManagedGoal)
        .where(
            ManagedGoal.discussion_session_id == package.session_id,
            ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
        )
        .limit(1)
    )
    if goal is None:
        return False
    return goal.current_package_id != package.id or goal.lifecycle == "paused"
