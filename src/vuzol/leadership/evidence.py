"""Goal completion reads checker verdicts. It never writes PASS and never waives."""

from __future__ import annotations

import hashlib
import io
import json
import uuid
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import GoalDelegation, ManagedGoal, ManagedGoalEvidence
from vuzol.leadership.rules import delegation_is_expired
from vuzol.storage.models import MaterializationLink, Step, WorkPackage
from vuzol.storage.types import WorkPackageStatus

GOAL_HEAD_CHECKER = "goal_review_v2"
PENDING_CHECKER = "goal_snapshot_v2"
_INACTIVE_GOAL = frozenset({"paused", "cancelled", "completed"})


@dataclass(frozen=True, slots=True)
class CompletionAssessment:
    satisfied: bool
    covered: tuple[str, ...]
    missing: tuple[str, ...]


async def observe_commit(path: str, commit: str) -> dict[str, str]:
    """Read a bounded Git object, never dirty/untracked files or symlink targets."""
    from vuzol.execution.git import LocalGit

    archive = await LocalGit().source_archive(Path(path), commit, max_bytes=120_000)
    with zipfile.ZipFile(io.BytesIO(archive)) as source:
        return {
            item.filename: source.read(item).decode("utf-8")
            for item in source.infolist()
            if not item.is_dir()
        }


def _digest(files: Mapping[str, str]) -> str:
    payload = json.dumps(files, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


async def record_head_evidence(
    session: AsyncSession,
    *,
    package: WorkPackage,
    worktree_path: str,
    repository_path: str,
    result_commit: str,
) -> None:
    """Capture evidence only. Independent review, outside this transaction, decides."""

    if package.managed_goal_id is None or not result_commit:
        return
    goal = await session.get(ManagedGoal, package.managed_goal_id)
    if goal is None or package.integration_branch != goal.integration_branch:
        return
    from vuzol.execution.git import GitError

    try:
        files = await observe_commit(worktree_path, result_commit)
        capture_error = None
    except (GitError, UnicodeDecodeError, OSError, zipfile.BadZipFile):
        files = {}
        capture_error = "goal_snapshot_unavailable"
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
        session.add(
            ManagedGoalEvidence(
                goal_id=goal.id,
                criterion_id=criterion_id,
                description=description,
                scope_revision=goal.scope_revision,
                result_commit=result_commit,
                artifact_digest=digest,
                observation={
                    "checker": PENDING_CHECKER,
                    "request": goal.original_request,
                    "scope": goal.scope_text,
                    "description": description,
                    "files": files,
                    "capture_error": capture_error,
                    "repository_path": repository_path,
                },
                satisfied=False,
                checker=PENDING_CHECKER,
            )
        )
    await session.flush()


async def _current_head(
    session: AsyncSession, goal: ManagedGoal, *, check_live_head: bool = False
) -> str | None:
    """The integration head of the newest completed package, not the union of old ones."""

    package = (
        await session.get(WorkPackage, goal.current_package_id)
        if goal.current_package_id is not None
        else None
    )
    if (
        package is None
        or package.managed_goal_id != goal.id
        or package.status is not WorkPackageStatus.COMPLETED
        or package.integration_branch != goal.integration_branch
        or not package.integration_head_commit
        or await package_has_unknown_effects(session, package.id)
    ):
        return None
    if package.integration_head_commit == package.integration_base_commit:
        return None
    if check_live_head:
        from vuzol.execution.git import GitError, LocalGit

        evidence = await session.scalar(
            select(ManagedGoalEvidence)
            .where(
                ManagedGoalEvidence.goal_id == goal.id,
                ManagedGoalEvidence.scope_revision == goal.scope_revision,
                ManagedGoalEvidence.result_commit == package.integration_head_commit,
                ManagedGoalEvidence.checker.in_((PENDING_CHECKER, GOAL_HEAD_CHECKER)),
            )
            .limit(1)
        )
        # The applier records this server-configured source repository. Task
        # worktrees are isolated repositories and do not contain the target ref.
        repository_path = evidence.observation.get("repository_path") if evidence else None
        if not isinstance(repository_path, str) or not Path(repository_path).is_absolute():
            return None
        try:
            live = await LocalGit().read_ref(Path(repository_path), goal.integration_branch)
        except (GitError, OSError):
            return None
        if live != package.integration_head_commit:
            return None
    return package.integration_head_commit


async def assess_completion(
    session: AsyncSession, goal: ManagedGoal, *, check_live_head: bool = False
) -> CompletionAssessment:
    """Cover a criterion only with a stored verdict for the current head.

    The plan's completion_criteria strings are not evidence. An owner waiver
    is not consulted. A missing verdict stays missing.
    """

    wanted = [
        (str(item["id"]), str(item.get("description") or ""))
        for item in goal.criteria
        if isinstance(item, dict) and item.get("id")
    ]
    head = await _current_head(session, goal, check_live_head=check_live_head)
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


async def new_effect_blocker(
    session: AsyncSession,
    task_id: uuid.UUID,
    *,
    feature_enabled: bool | None = None,
    restoring_publication: bool = False,
) -> str | None:
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
    if feature_enabled is False:
        return "project_lead_disabled"
    goal = await session.get(ManagedGoal, package.managed_goal_id, with_for_update=True)
    if goal is None:
        return "managed_goal_missing"
    if goal.lifecycle in _INACTIVE_GOAL and not (
        restoring_publication and goal.lifecycle == "completed"
    ):
        return "managed_goal_inactive"
    delegation = await session.get(GoalDelegation, goal.delegation_id)
    if delegation is None or delegation.revoked_at is not None:
        return "delegation_revoked"
    if delegation_is_expired(delegation.expires_at, now=datetime.now(UTC)):
        return "delegation_expired"
    from vuzol.leadership.persistence import POLICY_REVISION

    if (
        delegation.policy_revision != POLICY_REVISION
        or delegation.project_id != goal.project_id
        or delegation.integration_branch != goal.integration_branch
        or "apply_integration" not in delegation.allowed_actions
    ):
        return "delegation_changed"
    return None
