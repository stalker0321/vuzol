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
from vuzol.storage.models import (
    MaterializationLink,
    Run,
    Step,
    Task,
    ValidationResult,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import IdempotencyClass, StepStatus, TaskStatus, WorkPackageStatus

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


def observation_digest(files: Mapping[str, str], validation: object = None) -> str:
    # Old captured commits have no runtime evidence; retain their original binding.
    if validation is None:
        return _digest(files)
    payload = json.dumps(
        {"files": files, "runtime_validation": validation},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


async def runtime_validation(
    session: AsyncSession, *, package: WorkPackage, result_commit: str, scope_revision: int
) -> list[dict[str, object]]:
    """Only completed system validation of this package's exact applied worktree.

    Do not consume executor claims, arbitrary log files, earlier repair attempts,
    or validation from another run which happens to mention the same commit.
    """
    candidates = await session.execute(
        select(Step, Worktree)
        .join(Run, Run.id == Step.run_id)
        .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
        .join(Worktree, (Worktree.run_id == Run.id) & (Worktree.task_id == Run.task_id))
        .where(
            MaterializationLink.work_package_id == package.id,
            Step.step_type == "validate",
            Step.status == StepStatus.COMPLETED,
            Step.unknown_effects.is_(False),
            Worktree.result_commit == result_commit,
        )
        .order_by(Step.id)
    )
    receipts: list[dict[str, object]] = []
    for step, worktree in candidates:
        result = step.result or {}
        if (
            result.get("schema_version") != "result-validation.v1"
            or result.get("result_commit") != result_commit
            or not worktree.diff_hash
            or result.get("diff_hash") != worktree.diff_hash
            or result.get("base_commit") != worktree.base_commit
        ):
            continue
        gates = result.get("gates", [])
        stored = await session.scalars(
            select(ValidationResult)
            .where(ValidationResult.step_id == step.id)
            .order_by(ValidationResult.id)
        )
        for row in stored:
            gate = row.result
            if (
                not isinstance(gate, dict)
                or gate not in gates
                or not gate.get("argv")
                or not gate.get("validation_image_digest")
                or row.status != "passed"
                or row.exit_code != 0
                or gate.get("exit_code") != 0
                or row.validator_type != gate.get("command_id")
                or row.command_hash != hashlib.sha256("\0".join(gate["argv"]).encode()).hexdigest()
            ):
                continue
            receipts.append(
                {
                    "evidence_path": f"runtime/validation/{row.id}",
                    "validation_result_id": str(row.id),
                    "step_id": str(step.id),
                    "run_id": str(step.run_id),
                    "package_id": str(package.id),
                    "scope_revision": scope_revision,
                    "result_commit": result_commit,
                    "diff_hash": worktree.diff_hash,
                    "base_commit": worktree.base_commit,
                    "command_hash": row.command_hash,
                    "gate": gate,
                }
            )
    return receipts


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
    validation = await runtime_validation(
        session, package=package, result_commit=result_commit, scope_revision=goal.scope_revision
    )
    digest = observation_digest(files, validation)
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
                    "runtime_validation": validation,
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
        package is not None
        and package.status in {WorkPackageStatus.DISCARDED, WorkPackageStatus.STOPPED}
        and package.integration_head_commit in {None, package.integration_base_commit}
    ):
        # A rejected or never-run plan produced nothing; the goal's result is still
        # the newest completed package on its branch (live pilot, 2026-10-24).
        package = await session.scalar(
            select(WorkPackage)
            .where(
                WorkPackage.managed_goal_id == goal.id,
                WorkPackage.status == WorkPackageStatus.COMPLETED,
            )
            .order_by(WorkPackage.created_at.desc())
            .limit(1)
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


# Explicit types: publication and installation can also be idempotent.
# Cancellation never erases the unknown-effects history.
_CONFINED_STEPS = frozenset(
    {
        "prepare_context",
        "plan",
        "prepare_worktree",
        "execute_code",
        "execute_agent",
        "validate",
        "review",
        "build_static",
    }
)


def unknown_effects_block_work(task: Task, run: Run, step: Step) -> bool:
    if not step.unknown_effects:
        return False
    confined = (
        task.status is TaskStatus.CANCELLED
        # fixed_executor is the lead's task workflow (live pilot 2026-10-24).
        and run.workflow_type in {"coding", "architecture", "fixed_executor"}
        and step.step_type in _CONFINED_STEPS
        and step.idempotency_class
        in {
            IdempotencyClass.READ_ONLY,
            IdempotencyClass.ISOLATED_RETRYABLE,
            IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
        }
        and set(step.required_capabilities or []).issubset(
            {
                "repository_read",
                "filesystem_write",
                "code_edit",
                "git",
                "project_shell",
            }
        )
    )
    return not confined


async def package_has_unknown_effects(session: AsyncSession, package_id: uuid.UUID) -> bool:
    rows = await session.execute(
        select(Task, Run, Step)
        .join(Run, Run.task_id == Task.id)
        .join(Step, Step.run_id == Run.id)
        .join(MaterializationLink, MaterializationLink.task_id == Task.id)
        .where(
            MaterializationLink.work_package_id == package_id,
            Step.unknown_effects.is_(True),
        )
    )
    return any(unknown_effects_block_work(task, run, step) for task, run, step in rows)


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
