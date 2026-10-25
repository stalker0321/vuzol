# ruff: noqa: RUF001
"""The owner's decision on a finished goal: accept it into the project, or ask for more.

Live pilot 2026-10-25: items are accepted by the delegated lead, so the owner's one
decision is the whole goal. Accepting fast-forwards the project's default branch to
the independently reviewed goal head; it never creates a merge commit.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.evidence import GOAL_HEAD_CHECKER, _current_head, assess_completion
from vuzol.leadership.persistence import LIVE_GOAL_LIFECYCLES, ManagedGoal, ManagedGoalEvidence
from vuzol.storage.models import Event

REWORK_QUESTION = "Что доработать в результате? Опиши одним сообщением."
# Telegram ingress has no git: the lead worker performs the merge.
OWNER_ACCEPT_OPERATION = "owner_goal_accept"
_REFUSALS = {
    "target_moved": (
        "Не влил в main: main изменился с начала работы. "
        "Нажми «Доработать», чтобы руководитель подтянул изменения."
    ),
    "goal_not_verified": "Не влил в main: результат на ветке больше не совпадает с проверенным.",
    "merge_failed": "Не влил в main: репозиторий проекта занят или изменён вручную.",
}


def acceptance_refusal_text(outcome: str) -> str:
    return _REFUSALS.get(outcome, "Не влил в main: действие уже недоступно.")


async def queue_accept(session: AsyncSession, goal: ManagedGoal, *, user_id: int) -> str:
    """Check what ingress can check and hand the merge to the lead worker."""
    from vuzol.storage.models import TransactionalOutbox
    from vuzol.storage.types import DeliveryStatus

    if goal.owner_user_id != user_id:
        return "goal_not_available"
    if not await acceptance_ready(session, goal):
        return "goal_not_verified"
    session.add(
        TransactionalOutbox(
            destination="project_lead",
            operation_type=OWNER_ACCEPT_OPERATION,
            linked_entity_type="managed_goal",
            linked_entity_id=goal.id,
            idempotency_key=f"lead:owner-accept:{goal.id}:{goal.version}",
            payload={"user_id": user_id, "version": goal.version},
            status=DeliveryStatus.PENDING,
        )
    )
    return "accept_queued"


def owner_acceptance(goal: ManagedGoal) -> dict[str, Any] | None:
    raw = goal.constraints.get("owner_acceptance")
    return raw if isinstance(raw, dict) else None


async def acceptance_ready(session: AsyncSession, goal: ManagedGoal) -> bool:
    """A reviewed, completed goal the owner has not accepted yet."""
    if goal.lifecycle != "completed" or owner_acceptance(goal) is not None:
        return False
    return (await assess_completion(session, goal)).satisfied


async def accept_goal(session: AsyncSession, goal: ManagedGoal, *, user_id: int) -> str:
    """Return "accepted" or a refusal reason. The caller holds the goal row lock."""
    from vuzol.execution.git import GitError, LocalGit

    if goal.owner_user_id != user_id:
        return "goal_not_available"
    if owner_acceptance(goal) is not None:
        return "accepted"
    if goal.lifecycle != "completed":
        return "goal_not_finished"
    if not (await assess_completion(session, goal, check_live_head=True)).satisfied:
        return "goal_not_verified"
    head = await _current_head(session, goal, check_live_head=True)
    evidence = await session.scalar(
        select(ManagedGoalEvidence)
        .where(
            ManagedGoalEvidence.goal_id == goal.id,
            ManagedGoalEvidence.scope_revision == goal.scope_revision,
            ManagedGoalEvidence.result_commit == head,
            ManagedGoalEvidence.checker == GOAL_HEAD_CHECKER,
        )
        .limit(1)
    )
    repository = evidence.observation.get("repository_path") if evidence else None
    if head is None or not isinstance(repository, str) or not Path(repository).is_absolute():
        return "goal_not_verified"
    branch = str(goal.constraints.get("project_default_branch") or "main")
    try:
        outcome = await LocalGit().fast_forward(Path(repository), branch=branch, commit=head)
    except (GitError, OSError):
        return "merge_failed"
    if outcome == "diverged":
        return "target_moved"
    goal.constraints = {
        **goal.constraints,
        "owner_acceptance": {
            "commit": head,
            "branch": branch,
            "user_id": user_id,
            "at": datetime.now(UTC).isoformat(),
        },
    }
    goal.version += 1
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type="managed_goal.accepted",
            actor_type="user",
            actor_id=str(user_id),
            payload={"commit": head, "branch": branch, "fast_forward": outcome},
        )
    )
    return "accepted"


async def request_rework(session: AsyncSession, goal: ManagedGoal, *, user_id: int) -> str:
    """Reopen a finished goal with one owner question; the answer drives the lead."""
    if goal.owner_user_id != user_id:
        return "goal_not_available"
    if goal.lifecycle != "completed" or owner_acceptance(goal) is not None:
        return "goal_not_finished"
    other = await session.scalar(
        select(ManagedGoal.id)
        .where(
            ManagedGoal.project_id == goal.project_id,
            ManagedGoal.id != goal.id,
            ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
        )
        .limit(1)
    )
    if other is not None:
        return "other_goal_live"
    from vuzol.leadership.service import _enqueue_owner_question

    # The finished head stays reviewed; completion needs new work on top of it.
    goal.constraints = {
        **goal.constraints,
        "rework_from_head": await _current_head(session, goal),
    }
    asked_version = goal.version + 1
    goal.lifecycle = "waiting"
    goal.waiting_reason = "owner"
    goal.wakeup_condition = {
        "kind": "owner",
        "question": REWORK_QUESTION,
        "scope_revision": goal.scope_revision,
        "asked_version": asked_version,
    }
    goal.version = asked_version
    goal.fence_epoch += 1
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type="managed_goal.rework_requested",
            actor_type="user",
            actor_id=str(user_id),
            payload={"version": goal.version},
        )
    )
    await _enqueue_owner_question(session, goal, REWORK_QUESTION, asked_version)
    return "rework_requested"
