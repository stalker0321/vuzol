"""Goal completion reads checker verdicts. It never writes PASS and never waives."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import GoalDelegation, ManagedGoal, ManagedGoalEvidence
from vuzol.leadership.rules import delegation_is_expired
from vuzol.storage.models import MaterializationLink, Step, WorkPackage
from vuzol.storage.types import WorkPackageStatus

GOAL_HEAD_CHECKER = "goal_head_v1"
_WORD = re.compile(r"[A-Za-z][A-Za-z0-9_-]{4,}")
_INACTIVE_GOAL = frozenset({"paused", "cancelled", "completed"})


@dataclass(frozen=True, slots=True)
class CompletionAssessment:
    satisfied: bool
    covered: tuple[str, ...]
    missing: tuple[str, ...]


def content_words(text: str) -> tuple[str, ...]:
    """Distinctive words of a criterion description. Short glue words are ignored."""

    seen: dict[str, None] = {}
    for match in _WORD.finditer(text):
        seen.setdefault(match.group(0).lower(), None)
    return tuple(seen)


def judge_description(
    description: str, files: Mapping[str, str]
) -> tuple[bool, list[dict[str, object]]]:
    """A description is met only when every distinctive word is cited by this tree.

    Plan criterion ids are not an input. A description with no distinctive word
    fails closed. Missing files fail closed.
    """

    words = content_words(description)
    if not words:
        return False, []
    wanted = set(words)
    covered: set[str] = set()
    citations: list[dict[str, object]] = []
    for path, body in files.items():
        haystack = f"{path}\n{body}".lower()
        hits = [word for word in words if word in haystack]
        if not hits:
            continue
        citations.append({"path": path, "words": hits})
        covered.update(hits)
    return covered == wanted, citations


def observe_worktree(path: str) -> dict[str, str]:
    """Text files at the applied worktree. Git metadata is not an artifact."""

    root = Path(path)
    if not root.is_dir():
        return {}
    files: dict[str, str] = {}
    for item in sorted(root.rglob("*")):
        if len(files) >= 200:
            break
        if not item.is_file():
            continue
        relative = item.relative_to(root).as_posix()
        if relative == ".git" or relative.startswith(".git/"):
            continue
        if item.stat().st_size > 200_000:
            continue
        try:
            files[relative] = item.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
    return files


def _digest(files: Mapping[str, str]) -> str:
    payload = json.dumps(files, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


async def record_head_evidence(
    session: AsyncSession,
    *,
    package: WorkPackage,
    worktree_path: str,
    result_commit: str,
) -> None:
    """Bind a checker verdict to the goal revision and this integration head."""

    if package.managed_goal_id is None or not result_commit:
        return
    goal = await session.get(ManagedGoal, package.managed_goal_id)
    if goal is None or package.integration_branch != goal.integration_branch:
        return
    files = observe_worktree(worktree_path)
    digest = _digest(files)
    for item in goal.criteria:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        criterion_id = str(item["id"])
        description = str(item.get("description") or "")
        existing = await session.scalar(
            select(ManagedGoalEvidence.id).where(
                ManagedGoalEvidence.goal_id == goal.id,
                ManagedGoalEvidence.criterion_id == criterion_id,
                ManagedGoalEvidence.scope_revision == goal.scope_revision,
                ManagedGoalEvidence.result_commit == result_commit,
            )
        )
        if existing is not None:
            continue
        satisfied, citations = judge_description(description, files)
        session.add(
            ManagedGoalEvidence(
                goal_id=goal.id,
                criterion_id=criterion_id,
                description=description,
                scope_revision=goal.scope_revision,
                result_commit=result_commit,
                artifact_digest=digest,
                observation={
                    "checker": GOAL_HEAD_CHECKER,
                    "request": goal.original_request,
                    "scope": goal.scope_text,
                    "description": description,
                    "citations": citations,
                    "paths": sorted(files),
                },
                satisfied=satisfied,
                checker=GOAL_HEAD_CHECKER,
            )
        )
    await session.flush()


async def _current_head(session: AsyncSession, goal: ManagedGoal) -> str | None:
    """The integration head of the newest completed package, not the union of old ones."""

    package = await session.scalar(
        select(WorkPackage)
        .where(
            WorkPackage.managed_goal_id == goal.id,
            WorkPackage.status == WorkPackageStatus.COMPLETED,
            WorkPackage.integration_branch == goal.integration_branch,
            WorkPackage.integration_head_commit.is_not(None),
        )
        .order_by(WorkPackage.updated_at.desc(), WorkPackage.created_at.desc())
        .limit(1)
    )
    if package is None or not package.integration_head_commit:
        return None
    if package.integration_head_commit == package.integration_base_commit:
        return None
    return package.integration_head_commit


async def assess_completion(session: AsyncSession, goal: ManagedGoal) -> CompletionAssessment:
    """Cover a criterion only with a stored verdict for the current head.

    The plan's completion_criteria strings are not evidence. An owner waiver
    is not consulted. A missing verdict stays missing.
    """

    wanted = [
        (str(item["id"]), str(item.get("description") or ""))
        for item in goal.criteria
        if isinstance(item, dict) and item.get("id")
    ]
    head = await _current_head(session, goal)
    covered: list[str] = []
    if head is not None:
        rows = (
            await session.scalars(
                select(ManagedGoalEvidence).where(
                    ManagedGoalEvidence.goal_id == goal.id,
                    ManagedGoalEvidence.scope_revision == goal.scope_revision,
                    ManagedGoalEvidence.result_commit == head,
                    ManagedGoalEvidence.checker == GOAL_HEAD_CHECKER,
                    ManagedGoalEvidence.satisfied.is_(True),
                )
            )
        ).all()
        by_id = {row.criterion_id: row for row in rows}
        for criterion_id, description in wanted:
            row = by_id.get(criterion_id)
            if row is not None and row.description == description:
                covered.append(criterion_id)
    covered_ids = set(covered)
    missing = tuple(criterion_id for criterion_id, _ in wanted if criterion_id not in covered_ids)
    return CompletionAssessment(
        satisfied=bool(wanted) and not missing,
        covered=tuple(covered),
        missing=missing,
    )


def evidence_token(package: WorkPackage | None) -> str:
    """Identity of the package head. A stop does not change it; an apply does."""

    if package is None:
        return "none"
    return f"{package.id}:{package.integration_head_commit}"


async def package_has_unknown_effects(session: AsyncSession, package_id: uuid.UUID) -> bool:
    from vuzol.storage.models import Run

    found = await session.scalar(
        select(Step.id)
        .join(Run, Run.id == Step.run_id)
        .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
        .where(
            MaterializationLink.work_package_id == package_id,
            Step.unknown_effects.is_(True),
        )
        .limit(1)
    )
    return found is not None


async def new_effect_blocker(session: AsyncSession, task_id: uuid.UUID) -> str | None:
    """Refuse a new external effect when the managed grant is no longer live."""

    from datetime import UTC, datetime

    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task_id)
    )
    if link is None:
        return None
    package = await session.get(WorkPackage, link.work_package_id)
    if package is None or package.managed_goal_id is None:
        return None
    goal = await session.get(ManagedGoal, package.managed_goal_id, with_for_update=True)
    if goal is None:
        return "managed_goal_missing"
    if goal.lifecycle in _INACTIVE_GOAL:
        return "managed_goal_inactive"
    delegation = await session.get(GoalDelegation, goal.delegation_id)
    if delegation is None or delegation.revoked_at is not None:
        return "delegation_revoked"
    if delegation_is_expired(delegation.expires_at, now=datetime.now(UTC)):
        return "delegation_expired"
    return None
