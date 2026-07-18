"""Crash-recoverable retention sweeper for managed worktrees and artifacts.

Two-phase external cleanup (never claimed atomic with PostgreSQL):

1. Persist a cleanup **intent** under row locks after last-moment safety checks.
2. Perform Git/FS (or quarantine move) outside the finalization transaction.
3. Re-lock, re-validate, and finalize DB state; incomplete intents reconcile on the
   next run from durable intent markers + filesystem observation.

Dry-run is strictly read-only: no root creation, no row field writes, no events,
and no filesystem side effects.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.attributes import flag_modified

from vuzol.config.settings import RetentionDefaults
from vuzol.execution.git import GitError, LocalGit
from vuzol.execution.paths import PathViolation, contained, trusted_root
from vuzol.storage.models import (
    Approval,
    Artifact,
    Event,
    Run,
    Step,
    SupervisedProcess,
    Task,
    Worktree,
)
from vuzol.storage.types import (
    ApprovalStatus,
    ArtifactStorageState,
    ProcessStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)

RETENTION_SWEEP_LOCK_KEY = 8_946_527_105
QUARANTINE_DIR_NAME = ".quarantine"
ARTIFACT_URI_PREFIX = "artifact:"
WORKTREE_INTENT = "retention_intent"
WORKTREE_EXTERNAL_DONE = "retention_external_done"
ARTIFACT_META_KEY = "retention_cleanup"
ARTIFACT_PHASE_INTENT = "intent"
ARTIFACT_PHASE_EXTERNAL = "external_done"

CLEANABLE_TASK_STATUSES = frozenset(
    {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.ROLLED_BACK,
    }
)
FAILED_LIKE_TASK_STATUSES = frozenset(
    {
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.ROLLED_BACK,
    }
)
ACTIVE_PROCESS_STATUSES = frozenset(
    {
        ProcessStatus.STARTING,
        ProcessStatus.RUNNING,
        ProcessStatus.TERMINATING,
        ProcessStatus.UNKNOWN,
    }
)
# Includes BLOCKED: unknown-effect / unresolved steps must retain evidence.
PROTECTED_STEP_STATUSES = frozenset(
    {
        StepStatus.PENDING,
        StepStatus.QUEUED,
        StepStatus.LEASED,
        StepStatus.RUNNING,
        StepStatus.WAITING_APPROVAL,
        StepStatus.AWAITING_USER,
        StepStatus.BLOCKED,
    }
)
PROTECTED_WORKTREE_STATES = frozenset(
    {
        WorktreeDeliveryState.ACTIVE,
        WorktreeDeliveryState.CLEANED,
    }
)
INTENT_CLEANUP_REASONS = frozenset({WORKTREE_INTENT, WORKTREE_EXTERNAL_DONE})


class ProjectRepositoryLookup(Protocol):
    def get(self, project_id: str) -> object: ...


class RetentionSweepMode(StrEnum):
    DRY_RUN = "dry_run"
    APPLY = "apply"


class RetentionOutcome(StrEnum):
    WOULD_CLEAN = "would_clean"
    CLEANED = "cleaned"
    INTENT_RECORDED = "intent_recorded"
    EXTERNAL_DONE = "external_done"
    RECONCILED = "reconciled"
    WOULD_QUARANTINE = "would_quarantine"
    QUARANTINED = "quarantined"
    WOULD_MARK_MISSING = "would_mark_missing"
    MARKED_MISSING = "marked_missing"
    WOULD_MARK_DELETED = "would_mark_deleted"
    MARKED_DELETED = "marked_deleted"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RetentionAction:
    resource_type: str
    resource_id: str
    outcome: RetentionOutcome
    reason: str
    detail: dict[str, object] = field(default_factory=dict)


class _RetentionActionAbort(Exception):
    def __init__(self, action: RetentionAction) -> None:
        super().__init__(action.reason)
        self.action = action


@dataclass(frozen=True, slots=True)
class RetentionSweepReport:
    mode: RetentionSweepMode
    lock_acquired: bool
    started_at: datetime
    finished_at: datetime
    actions: tuple[RetentionAction, ...]

    @property
    def cleaned_count(self) -> int:
        return sum(
            1
            for action in self.actions
            if action.outcome
            in {
                RetentionOutcome.CLEANED,
                RetentionOutcome.RECONCILED,
                RetentionOutcome.MARKED_DELETED,
                RetentionOutcome.QUARANTINED,
                RetentionOutcome.MARKED_MISSING,
            }
        )

    @property
    def skipped_count(self) -> int:
        return sum(1 for action in self.actions if action.outcome is RetentionOutcome.SKIPPED)

    @property
    def failure_count(self) -> int:
        return sum(1 for action in self.actions if action.outcome is RetentionOutcome.FAILED)

    def to_operational_payload(self) -> dict[str, object]:
        counts: dict[str, int] = {}
        for action in self.actions:
            key = action.outcome.value
            counts[key] = counts.get(key, 0) + 1
        return {
            "schema_version": "retention-sweep-report.v2",
            "mode": self.mode.value,
            "lock_acquired": self.lock_acquired,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "action_count": len(self.actions),
            "cleaned_count": self.cleaned_count,
            "skipped_count": self.skipped_count,
            "failure_count": self.failure_count,
            "outcome_counts": counts,
            "actions": [
                {
                    "resource_type": action.resource_type,
                    "resource_id": action.resource_id,
                    "outcome": action.outcome.value,
                    "reason": action.reason,
                    "detail": action.detail,
                }
                for action in self.actions
            ],
        }


def effective_worktree_retention_until(
    *,
    task_status: TaskStatus,
    retention_until: datetime,
    task_updated_at: datetime,
    completed_days: int,
    failed_days: int,
) -> datetime | None:
    if task_status is TaskStatus.COMPLETED:
        completed_deadline = task_updated_at + timedelta(days=completed_days)
        return min(retention_until, completed_deadline)
    if task_status in FAILED_LIKE_TASK_STATUSES:
        failed_floor = task_updated_at + timedelta(days=max(failed_days, completed_days))
        return max(retention_until, failed_floor)
    return None


class RetentionSweeper:
    """Single-instance retention sweeper with crash-recoverable external phases."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        worktree_root: Path,
        artifact_root: Path,
        repository_root: Path,
        retention: RetentionDefaults,
        owner: str,
        projects: ProjectRepositoryLookup | None = None,
        git: LocalGit | None = None,
        lock_timeout_seconds: float | None = None,
        lock_poll_seconds: float = 0.1,
        scan_limit: int | None = None,
    ) -> None:
        self._factory = factory
        self._worktree_root_path = worktree_root
        self._artifact_root_path = artifact_root
        self._repository_root_path = repository_root
        self._worktree_root: Path | None = None
        self._artifact_root: Path | None = None
        self._quarantine_root: Path | None = None
        self._repository_root: Path | None = None
        self._retention = retention
        self._owner = owner
        self._projects = projects
        self._git = git or LocalGit()
        self._lock_timeout_seconds = (
            retention.sweep_lock_timeout_seconds
            if lock_timeout_seconds is None
            else lock_timeout_seconds
        )
        self._lock_poll_seconds = lock_poll_seconds
        self._batch_size = retention.sweep_batch_size
        self._scan_limit = scan_limit or max(retention.sweep_batch_size * 20, 200)

    def _bind_roots(self, *, mode: RetentionSweepMode) -> None:
        create = mode is RetentionSweepMode.APPLY
        self._worktree_root = trusted_root(self._worktree_root_path, create=create)
        self._artifact_root = trusted_root(self._artifact_root_path, create=create)
        self._repository_root = trusted_root(self._repository_root_path, create=False)
        if mode is RetentionSweepMode.APPLY:
            self._quarantine_root = trusted_root(
                self._artifact_root / QUARANTINE_DIR_NAME, create=True
            )
        else:
            candidate = self._artifact_root_path / QUARANTINE_DIR_NAME
            self._quarantine_root = candidate if candidate.is_dir() else None

    async def run(self, *, mode: RetentionSweepMode) -> RetentionSweepReport:
        started = datetime.now(UTC)
        try:
            self._bind_roots(mode=mode)
        except (PathViolation, OSError, FileNotFoundError) as error:
            finished = datetime.now(UTC)
            return RetentionSweepReport(
                mode=mode,
                lock_acquired=False,
                started_at=started,
                finished_at=finished,
                actions=(
                    RetentionAction(
                        "sweep",
                        "roots",
                        RetentionOutcome.FAILED,
                        "roots_unavailable",
                        {"error": type(error).__name__},
                    ),
                ),
            )

        # Dry-run uses a short exclusive lock only for serialization against APPLY;
        # it performs no mutations while holding the lock.
        async with self._factory() as lock_session:
            connection = await lock_session.connection()
            if not await self._acquire_lock(lock_session):
                finished = datetime.now(UTC)
                return RetentionSweepReport(
                    mode=mode,
                    lock_acquired=False,
                    started_at=started,
                    finished_at=finished,
                    actions=(
                        RetentionAction(
                            "sweep",
                            "lock",
                            RetentionOutcome.SKIPPED,
                            "advisory_lock_unavailable",
                        ),
                    ),
                )
            actions: list[RetentionAction] = []
            try:
                actions.extend(await self._sweep_worktrees(mode=mode))
                actions.extend(await self._sweep_artifacts(mode=mode))
                actions.extend(await self._reconcile_orphan_artifacts(mode=mode))
                actions.extend(await self._reconcile_quarantine_intents(mode=mode))
                if mode is RetentionSweepMode.APPLY:
                    await self._record_summary(actions, mode=mode, started_at=started)
                finished = datetime.now(UTC)
                return RetentionSweepReport(
                    mode=mode,
                    lock_acquired=True,
                    started_at=started,
                    finished_at=finished,
                    actions=tuple(actions),
                )
            finally:
                try:
                    await lock_session.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": RETENTION_SWEEP_LOCK_KEY},
                    )
                    await lock_session.commit()
                except Exception:  # pragma: no cover - connection already dead
                    await connection.invalidate()
                    raise

    async def _acquire_lock(self, session: AsyncSession) -> bool:
        deadline = time.monotonic() + self._lock_timeout_seconds
        while True:
            acquired = await session.scalar(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": RETENTION_SWEEP_LOCK_KEY},
            )
            if acquired is True:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(self._lock_poll_seconds)

    # --- worktrees -----------------------------------------------------------------

    async def _sweep_worktrees(self, *, mode: RetentionSweepMode) -> list[RetentionAction]:
        assert self._worktree_root is not None
        actions: list[RetentionAction] = []
        scanned = 0
        acted = 0
        cursor_until: datetime | None = None
        cursor_id: uuid.UUID | None = None
        while scanned < self._scan_limit and acted < self._batch_size:
            page = await self._fetch_worktree_page(cursor_until, cursor_id)
            if not page:
                break
            for worktree in page:
                scanned += 1
                cursor_until = worktree.retention_until
                cursor_id = worktree.id
                action = await self._process_worktree(worktree.id, mode=mode)
                actions.append(action)
                # Skips do not consume batch capacity — pagination continues past them.
                if action.outcome is not RetentionOutcome.SKIPPED:
                    acted += 1
                if acted >= self._batch_size:
                    break
            if len(page) < self._batch_size:
                break
        return actions

    async def _fetch_worktree_page(
        self, cursor_until: datetime | None, cursor_id: uuid.UUID | None
    ) -> tuple[Worktree, ...]:
        async with self._factory() as session, session.begin():
            conditions = [
                Worktree.cleaned_at.is_(None),
                Worktree.delivery_state != WorktreeDeliveryState.CLEANED,
                or_(
                    Worktree.cleanup_reason.in_(tuple(INTENT_CLEANUP_REASONS)),
                    and_(
                        Worktree.delivery_state != WorktreeDeliveryState.ACTIVE,
                        Worktree.retention_until < func.now(),
                    ),
                ),
            ]
            if cursor_until is not None and cursor_id is not None:
                conditions.append(
                    or_(
                        Worktree.retention_until > cursor_until,
                        and_(
                            Worktree.retention_until == cursor_until,
                            Worktree.id > cursor_id,
                        ),
                    )
                )
            rows = tuple(
                (
                    await session.scalars(
                        select(Worktree)
                        .where(*conditions)
                        .order_by(Worktree.retention_until, Worktree.id)
                        .limit(self._batch_size)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            return rows

    async def _process_worktree(
        self, worktree_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        assert self._worktree_root is not None
        # Phase A: evaluate / record intent (APPLY) or report (DRY_RUN).
        phase_a = await self._worktree_phase_declare_or_report(worktree_id, mode=mode)
        if phase_a.outcome is not RetentionOutcome.INTENT_RECORDED:
            if phase_a.outcome is RetentionOutcome.EXTERNAL_DONE:
                # Intent already external-done; finalize only.
                return await self._worktree_phase_finalize(worktree_id, mode=mode)
            if (
                phase_a.outcome is RetentionOutcome.RECONCILED
                or phase_a.outcome is RetentionOutcome.CLEANED
            ):
                return phase_a
            return phase_a

        if mode is RetentionSweepMode.DRY_RUN:
            return phase_a

        # Phase B: external Git/FS (outside finalization transaction).
        external = await self._worktree_phase_external(worktree_id)
        if external.outcome is RetentionOutcome.FAILED:
            return external

        # Phase C: re-lock and finalize.
        return await self._worktree_phase_finalize(worktree_id, mode=mode)

    async def _worktree_phase_declare_or_report(
        self, worktree_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        assert self._worktree_root is not None
        async with self._factory() as session, session.begin():
            worktree = await session.scalar(
                select(Worktree).where(Worktree.id == worktree_id).with_for_update()
            )
            if worktree is None:
                return RetentionAction(
                    "worktree", str(worktree_id), RetentionOutcome.SKIPPED, "missing_row"
                )
            task = await session.scalar(
                select(Task).where(Task.id == worktree.task_id).with_for_update()
            )
            run = await session.scalar(
                select(Run).where(Run.id == worktree.run_id).with_for_update()
            )
            if task is None or run is None or run.task_id != task.id:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.SKIPPED,
                    "ambiguous_identity",
                )

            # Resume incomplete intents before ordinary eligibility.
            if worktree.cleanup_reason == WORKTREE_EXTERNAL_DONE:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.EXTERNAL_DONE,
                    "resume_external_done",
                )
            if worktree.cleanup_reason == WORKTREE_INTENT:
                # Re-check safety before re-driving external phase.
                skip = await self._worktree_skip_reason(
                    session, worktree, task, run, ignore_retention=True
                )
                if skip is not None:
                    return RetentionAction(
                        "worktree", str(worktree_id), RetentionOutcome.SKIPPED, skip[0], skip[1]
                    )
                if mode is RetentionSweepMode.DRY_RUN:
                    return RetentionAction(
                        "worktree",
                        str(worktree_id),
                        RetentionOutcome.WOULD_CLEAN,
                        "resume_intent",
                    )
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.INTENT_RECORDED,
                    "resume_intent",
                )

            skip = await self._worktree_skip_reason(session, worktree, task, run)
            if skip is not None:
                return RetentionAction(
                    "worktree", str(worktree_id), RetentionOutcome.SKIPPED, skip[0], skip[1]
                )

            path = Path(worktree.path)
            try:
                contained(self._worktree_root, path, must_exist=False)
            except PathViolation as error:
                # Dry-run must not write cleanup_reason.
                if mode is RetentionSweepMode.APPLY:
                    worktree.cleanup_reason = "path_violation"
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "path_violation",
                    {"error": str(error)},
                )
            if path.exists() and (path.is_symlink() or not path.is_dir()):  # noqa: ASYNC240
                if mode is RetentionSweepMode.APPLY:
                    worktree.cleanup_reason = "invalid_worktree_path"
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "invalid_worktree_path",
                )

            if mode is RetentionSweepMode.DRY_RUN:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.WOULD_CLEAN,
                    "retention_expired",
                    {
                        "path": str(path),
                        "task_status": task.status.value,
                        "delivery_state": worktree.delivery_state.value,
                    },
                )

            worktree.cleanup_reason = WORKTREE_INTENT
            session.add(
                Event(
                    entity_type="worktree",
                    entity_id=worktree.id,
                    event_type="ops.retention.worktree_intent",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={
                        "phase": WORKTREE_INTENT,
                        "path": str(path),
                        "task_id": str(task.id),
                        "run_id": str(run.id),
                        "repository_identity_hash": worktree.repository_identity_hash,
                        "project_id": worktree.project_id,
                    },
                )
            )
            return RetentionAction(
                "worktree",
                str(worktree_id),
                RetentionOutcome.INTENT_RECORDED,
                "retention_expired",
                {"path": str(path)},
            )

    async def _worktree_phase_external(self, worktree_id: uuid.UUID) -> RetentionAction:
        assert self._worktree_root is not None
        assert self._repository_root is not None
        async with self._factory() as session, session.begin():
            worktree = await session.scalar(
                select(Worktree).where(Worktree.id == worktree_id).with_for_update()
            )
            if worktree is None or worktree.cleanup_reason not in INTENT_CLEANUP_REASONS:
                return RetentionAction(
                    "worktree", str(worktree_id), RetentionOutcome.SKIPPED, "intent_missing"
                )
            path = Path(worktree.path)
            project_id = worktree.project_id
            expected_identity = worktree.repository_identity_hash
            # Snapshot fields then release transaction before long Git/FS work.
        try:
            repository = self._resolve_repository(project_id)
            identity, _remote = await self._git.repository_identity(repository)
            if identity != expected_identity:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "repository_identity_mismatch",
                    {"project_id": project_id},
                )
            try:
                contained(self._worktree_root, path, must_exist=False)
            except PathViolation as error:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "path_violation",
                    {"error": str(error)},
                )
            if path.is_symlink():  # noqa: ASYNC240
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "invalid_worktree_path",
                )
            if path.exists():  # noqa: ASYNC240
                # Canonical Git unregister when linked; standalone shallow trees skip registration.
                await self._git.remove_worktree(repository, path)
                if path.exists():  # noqa: ASYNC240
                    # Only after intent: path-contained residual tree removal.
                    if path.is_dir() and not path.is_symlink():  # noqa: ASYNC240
                        shutil.rmtree(path)
                    else:
                        return RetentionAction(
                            "worktree",
                            str(worktree_id),
                            RetentionOutcome.FAILED,
                            "removal_incomplete",
                        )
                listing = await self._git._run(repository, "worktree", "list", "--porcelain")
                if str(path) in listing.decode("utf-8", "ignore"):
                    return RetentionAction(
                        "worktree",
                        str(worktree_id),
                        RetentionOutcome.FAILED,
                        "git_cleanup_failed",
                    )
            # Path already absent: crash window after FS, before EXTERNAL_DONE — continue.
        except (GitError, OSError, PathViolation) as error:
            return RetentionAction(
                "worktree",
                str(worktree_id),
                RetentionOutcome.FAILED,
                "git_cleanup_failed",
                {"error": type(error).__name__},
            )

        async with self._factory() as session, session.begin():
            worktree = await session.scalar(
                select(Worktree).where(Worktree.id == worktree_id).with_for_update()
            )
            if worktree is None:
                return RetentionAction(
                    "worktree", str(worktree_id), RetentionOutcome.SKIPPED, "missing_row"
                )
            worktree.cleanup_reason = WORKTREE_EXTERNAL_DONE
            session.add(
                Event(
                    entity_type="worktree",
                    entity_id=worktree.id,
                    event_type="ops.retention.worktree_external_done",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={"phase": WORKTREE_EXTERNAL_DONE, "path": str(path)},
                )
            )
        return RetentionAction(
            "worktree",
            str(worktree_id),
            RetentionOutcome.EXTERNAL_DONE,
            "external_cleanup_done",
            {"path": str(path)},
        )

    async def _worktree_phase_finalize(
        self, worktree_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        assert self._worktree_root is not None
        async with self._factory() as session, session.begin():
            worktree = await session.scalar(
                select(Worktree).where(Worktree.id == worktree_id).with_for_update()
            )
            if worktree is None:
                return RetentionAction(
                    "worktree", str(worktree_id), RetentionOutcome.SKIPPED, "missing_row"
                )
            task = await session.scalar(
                select(Task).where(Task.id == worktree.task_id).with_for_update()
            )
            run = await session.scalar(
                select(Run).where(Run.id == worktree.run_id).with_for_update()
            )
            if task is None or run is None:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.SKIPPED,
                    "ambiguous_identity",
                )
            # Last-moment re-check; if unsafe after external, preserve evidence (block finalize).
            skip = await self._worktree_skip_reason(
                session, worktree, task, run, ignore_retention=True, ignore_intent=True
            )
            if skip is not None and worktree.cleanup_reason != WORKTREE_EXTERNAL_DONE:
                return RetentionAction(
                    "worktree", str(worktree_id), RetentionOutcome.SKIPPED, skip[0], skip[1]
                )

            path = Path(worktree.path)
            path_missing = not path.exists() and not path.is_symlink()  # noqa: ASYNC240
            if worktree.cleanup_reason == WORKTREE_EXTERNAL_DONE and path_missing:
                if mode is RetentionSweepMode.DRY_RUN:
                    return RetentionAction(
                        "worktree",
                        str(worktree_id),
                        RetentionOutcome.WOULD_CLEAN,
                        "would_finalize_external_done",
                    )
                previous = worktree.delivery_state.value
                worktree.delivery_state = WorktreeDeliveryState.CLEANED
                worktree.cleanup_reason = "retention"
                worktree.cleaned_at = func.now()
                session.add(
                    Event(
                        entity_type="worktree",
                        entity_id=worktree.id,
                        event_type="ops.retention.worktree_cleaned",
                        actor_type="retention",
                        actor_id=self._owner,
                        previous_state=previous,
                        new_state=WorktreeDeliveryState.CLEANED.value,
                        payload={"phase": "finalized", "path": str(path)},
                    )
                )
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.RECONCILED,
                    "finalized_after_external",
                    {"path": str(path)},
                )
            if (
                worktree.cleanup_reason == WORKTREE_EXTERNAL_DONE and not path_missing
            ):  # pragma: no cover
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "removal_incomplete",
                    {"path": str(path)},
                )
            if path_missing and worktree.cleanup_reason == WORKTREE_INTENT:
                # External phase may have removed path but crashed before external_done marker.
                if mode is RetentionSweepMode.DRY_RUN:
                    return RetentionAction(
                        "worktree",
                        str(worktree_id),
                        RetentionOutcome.WOULD_CLEAN,
                        "would_finalize_missing_after_intent",
                    )
                previous = worktree.delivery_state.value
                worktree.delivery_state = WorktreeDeliveryState.CLEANED
                worktree.cleanup_reason = "retention"
                worktree.cleaned_at = func.now()
                session.add(
                    Event(
                        entity_type="worktree",
                        entity_id=worktree.id,
                        event_type="ops.retention.worktree_cleaned",
                        actor_type="retention",
                        actor_id=self._owner,
                        previous_state=previous,
                        new_state=WorktreeDeliveryState.CLEANED.value,
                        payload={
                            "phase": "finalized_missing_after_intent",
                            "path": str(path),
                        },
                    )
                )
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.RECONCILED,
                    "finalized_missing_after_intent",
                )
            return RetentionAction(  # pragma: no cover
                "worktree",
                str(worktree_id),
                RetentionOutcome.FAILED,
                "finalize_precondition",
                {"cleanup_reason": worktree.cleanup_reason},
            )

    def _resolve_repository(self, project_id: str) -> Path:  # pragma: no cover
        assert self._repository_root is not None
        if self._projects is not None:
            project = self._projects.get(project_id)
            path = getattr(project, "repository_path", None)
            if isinstance(path, Path):
                return path.resolve()
        return (self._repository_root / project_id).resolve()

    async def _worktree_skip_reason(
        self,
        session: AsyncSession,
        worktree: Worktree,
        task: Task,
        run: Run,
        *,
        ignore_retention: bool = False,
        ignore_intent: bool = False,
    ) -> tuple[str, dict[str, object]] | None:
        if (
            worktree.cleaned_at is not None
            or worktree.delivery_state is WorktreeDeliveryState.CLEANED
        ):
            return "already_cleaned", {}
        if worktree.delivery_state in PROTECTED_WORKTREE_STATES:
            return "protected_delivery_state", {"delivery_state": worktree.delivery_state.value}
        if task.status is TaskStatus.BLOCKED:
            return "blocked_task", {}
        if task.status not in CLEANABLE_TASK_STATUSES:
            return "non_terminal_task", {"task_status": task.status.value}

        if not ignore_retention and worktree.cleanup_reason not in INTENT_CLEANUP_REASONS:
            effective = effective_worktree_retention_until(
                task_status=task.status,
                retention_until=worktree.retention_until,
                task_updated_at=task.updated_at,
                completed_days=self._retention.completed_worktree_days,
                failed_days=self._retention.failed_worktree_days,
            )
            now = datetime.now(UTC)
            if effective is None or effective > now:
                return (
                    "within_retention",
                    {
                        "effective_until": None if effective is None else effective.isoformat(),
                        "retention_until": worktree.retention_until.isoformat(),
                    },
                )

        protected = await self._run_has_protected_runtime(session, run.id, worktree.id)
        if protected is not None:
            return protected
        return None

    async def _run_has_protected_runtime(
        self, session: AsyncSession, run_id: uuid.UUID, worktree_id: uuid.UUID | None
    ) -> tuple[str, dict[str, object]] | None:
        locked_step = await session.scalar(
            select(Step)
            .where(Step.run_id == run_id, Step.status.in_(tuple(PROTECTED_STEP_STATUSES)))
            .order_by(Step.ordinal, Step.id)
            .limit(1)
            .with_for_update()
        )
        if locked_step is not None:
            return "protected_step", {
                "step_id": str(locked_step.id),
                "step_status": locked_step.status.value,
            }

        pending_approval = await session.scalar(
            select(Approval)
            .join(Step, Step.id == Approval.step_id)
            .where(Step.run_id == run_id, Approval.status == ApprovalStatus.PENDING)
            .limit(1)
            .with_for_update()
        )
        if pending_approval is not None:
            return "pending_approval", {"approval_id": str(pending_approval.id)}

        if worktree_id is not None:
            active_process = await session.scalar(
                select(SupervisedProcess.id)
                .where(
                    SupervisedProcess.worktree_id == worktree_id,
                    SupervisedProcess.status.in_(tuple(ACTIVE_PROCESS_STATUSES)),
                )
                .limit(1)
                .with_for_update()
            )
            if active_process is not None:
                return "active_supervised_process", {"process_id": str(active_process)}
        return None

    # --- artifacts -----------------------------------------------------------------

    async def _sweep_artifacts(self, *, mode: RetentionSweepMode) -> list[RetentionAction]:
        actions: list[RetentionAction] = []
        scanned = 0
        acted = 0
        cursor_until: datetime | None = None
        cursor_id: uuid.UUID | None = None
        while scanned < self._scan_limit and acted < self._batch_size:
            page = await self._fetch_artifact_page(cursor_until, cursor_id)
            if not page:
                break
            for artifact in page:
                scanned += 1
                cursor_until = artifact.retention_until
                cursor_id = artifact.id
                action = await self._process_artifact(artifact.id, mode=mode)
                actions.append(action)
                if action.outcome not in {RetentionOutcome.SKIPPED, RetentionOutcome.FAILED}:
                    acted += 1
                if acted >= self._batch_size:
                    break
            if len(page) < self._batch_size:
                break
        return actions

    async def _fetch_artifact_page(
        self, cursor_until: datetime | None, cursor_id: uuid.UUID | None
    ) -> tuple[Artifact, ...]:
        async with self._factory() as session, session.begin():
            conditions = [
                Artifact.storage_state == ArtifactStorageState.AVAILABLE,
                or_(
                    Artifact.retention_until < func.now(),
                    Artifact.metadata_json.has_key(ARTIFACT_META_KEY),
                ),
            ]
            if cursor_until is not None and cursor_id is not None:
                conditions.append(
                    or_(
                        Artifact.retention_until > cursor_until,
                        and_(
                            Artifact.retention_until == cursor_until,
                            Artifact.id > cursor_id,
                        ),
                    )
                )
            # Prefer AVAILABLE expired; STAGING used only if we parked intent there.
            rows = tuple(
                (
                    await session.scalars(
                        select(Artifact)
                        .where(*conditions)
                        .order_by(Artifact.retention_until, Artifact.id)
                        .limit(self._batch_size)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            return rows

    async def _process_artifact(
        self, artifact_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        try:
            phase = await self._artifact_phase_declare_or_report(artifact_id, mode=mode)
            if phase.outcome is RetentionOutcome.EXTERNAL_DONE:
                return await self._artifact_phase_finalize(artifact_id, mode=mode)
            if phase.outcome is not RetentionOutcome.INTENT_RECORDED:
                return phase
            if mode is RetentionSweepMode.DRY_RUN:
                return phase
            external = await self._artifact_phase_external(artifact_id)
            if external.outcome is RetentionOutcome.FAILED:
                return external
            return await self._artifact_phase_finalize(artifact_id, mode=mode)
        except _RetentionActionAbort as abort:
            return abort.action

    def _artifact_phase(self, artifact: Artifact) -> str | None:
        meta = artifact.metadata_json or {}
        block = meta.get(ARTIFACT_META_KEY)
        if isinstance(block, Mapping):
            phase = block.get("phase")
            return str(phase) if phase is not None else None
        return None

    async def _artifact_phase_declare_or_report(
        self, artifact_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        assert self._artifact_root is not None
        async with self._factory() as session, session.begin():
            artifact = await session.scalar(
                select(Artifact).where(Artifact.id == artifact_id).with_for_update()
            )
            if artifact is None:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, "missing_row"
                )
            phase = self._artifact_phase(artifact)
            if phase == ARTIFACT_PHASE_EXTERNAL:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.EXTERNAL_DONE,
                    "resume_external_done",
                )
            if phase == ARTIFACT_PHASE_INTENT:
                skip = await self._artifact_skip_reason(session, artifact, ignore_retention=True)
                if skip is not None:
                    return RetentionAction(
                        "artifact", str(artifact_id), RetentionOutcome.SKIPPED, skip[0], skip[1]
                    )
                if mode is RetentionSweepMode.DRY_RUN:
                    return RetentionAction(
                        "artifact",
                        str(artifact_id),
                        RetentionOutcome.WOULD_MARK_DELETED,
                        "resume_intent",
                    )
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.INTENT_RECORDED,
                    "resume_intent",
                )

            if artifact.storage_state is not ArtifactStorageState.AVAILABLE:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.SKIPPED,
                    "not_available",
                    {"storage_state": artifact.storage_state.value},
                )
            skip = await self._artifact_skip_reason(session, artifact)
            if skip is not None:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, skip[0], skip[1]
                )

            try:
                absolute = self._artifact_path(artifact)
            except (PathViolation, ValueError) as error:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.FAILED,
                    "path_violation",
                    {"error": str(error)},
                )

            exists = absolute.exists()
            if exists and absolute.is_symlink():
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.FAILED,
                    "symlink_refused",
                    {"path": str(absolute)},
                )
            if exists:
                try:
                    link_count = absolute.lstat().st_nlink
                except OSError as error:
                    return RetentionAction(
                        "artifact",
                        str(artifact_id),
                        RetentionOutcome.FAILED,
                        "stat_failed",
                        {"error": type(error).__name__},
                    )
                if link_count > 1:
                    return RetentionAction(
                        "artifact",
                        str(artifact_id),
                        RetentionOutcome.FAILED,
                        "hardlink_refused",
                        {"nlink": link_count},
                    )

            if not exists:
                if mode is RetentionSweepMode.DRY_RUN:
                    return RetentionAction(
                        "artifact",
                        str(artifact_id),
                        RetentionOutcome.WOULD_MARK_MISSING,
                        "filesystem_missing",
                    )
                artifact.storage_state = ArtifactStorageState.MISSING
                session.add(
                    Event(
                        entity_type="artifact",
                        entity_id=artifact.id,
                        event_type="ops.retention.artifact_missing",
                        actor_type="retention",
                        actor_id=self._owner,
                        payload={"content_hash": artifact.content_hash},
                    )
                )
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.MARKED_MISSING,
                    "filesystem_missing",
                )

            if mode is RetentionSweepMode.DRY_RUN:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.WOULD_MARK_DELETED,
                    "retention_expired",
                    {"path": str(absolute)},
                )

            meta = dict(artifact.metadata_json or {})
            meta[ARTIFACT_META_KEY] = {
                "phase": ARTIFACT_PHASE_INTENT,
                "owner": self._owner,
                "content_hash": artifact.content_hash,
            }
            artifact.metadata_json = meta
            flag_modified(artifact, "metadata_json")
            session.add(
                Event(
                    entity_type="artifact",
                    entity_id=artifact.id,
                    event_type="ops.retention.artifact_intent",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={"phase": ARTIFACT_PHASE_INTENT, "content_hash": artifact.content_hash},
                )
            )
            return RetentionAction(
                "artifact",
                str(artifact_id),
                RetentionOutcome.INTENT_RECORDED,
                "retention_expired",
                {"path": str(absolute)},
            )

    async def _artifact_phase_external(self, artifact_id: uuid.UUID) -> RetentionAction:
        assert self._artifact_root is not None
        async with self._factory() as session, session.begin():
            artifact = await session.scalar(
                select(Artifact).where(Artifact.id == artifact_id).with_for_update()
            )
            if artifact is None or self._artifact_phase(artifact) != ARTIFACT_PHASE_INTENT:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, "intent_missing"
                )
            skip = await self._artifact_skip_reason(session, artifact, ignore_retention=True)
            if skip is not None:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, skip[0], skip[1]
                )
            content_hash = artifact.content_hash
            try:
                absolute = self._artifact_path(artifact)
            except (PathViolation, ValueError) as error:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.FAILED,
                    "path_violation",
                    {"error": str(error)},
                )
            remaining = await session.scalar(
                select(Artifact.id)
                .where(
                    Artifact.content_hash == content_hash,
                    Artifact.id != artifact.id,
                    Artifact.storage_state.in_(
                        (
                            ArtifactStorageState.AVAILABLE,
                            ArtifactStorageState.STAGING,
                            ArtifactStorageState.QUARANTINED,
                        )
                    ),
                )
                .limit(1)
            )
        # External FS outside the transaction.
        file_removed = False
        if remaining is None and absolute.exists() and not absolute.is_symlink():
            try:
                info = absolute.lstat()
                if info.st_nlink > 1:
                    return RetentionAction(
                        "artifact",
                        str(artifact_id),
                        RetentionOutcome.FAILED,
                        "hardlink_refused",
                        {"nlink": info.st_nlink},
                    )
                absolute.unlink()
                file_removed = True
                parent = absolute.parent
                if parent != self._artifact_root and parent.is_dir():
                    with contextlib.suppress(OSError):
                        parent.rmdir()
            except OSError as error:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.FAILED,
                    "filesystem_delete_failed",
                    {"error": type(error).__name__},
                )

        async with self._factory() as session, session.begin():
            artifact = await session.scalar(
                select(Artifact).where(Artifact.id == artifact_id).with_for_update()
            )
            if artifact is None:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, "missing_row"
                )
            meta = dict(artifact.metadata_json or {})
            meta[ARTIFACT_META_KEY] = {
                "phase": ARTIFACT_PHASE_EXTERNAL,
                "owner": self._owner,
                "content_hash": artifact.content_hash,
                "file_removed": file_removed,
            }
            artifact.metadata_json = meta
            flag_modified(artifact, "metadata_json")
            session.add(
                Event(
                    entity_type="artifact",
                    entity_id=artifact.id,
                    event_type="ops.retention.artifact_external_done",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={
                        "phase": ARTIFACT_PHASE_EXTERNAL,
                        "file_removed": file_removed,
                    },
                )
            )
        return RetentionAction(
            "artifact",
            str(artifact_id),
            RetentionOutcome.EXTERNAL_DONE,
            "external_cleanup_done",
            {"file_removed": file_removed},
        )

    async def _artifact_phase_finalize(
        self, artifact_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        assert self._artifact_root is not None
        async with self._factory() as session, session.begin():
            artifact = await session.scalar(
                select(Artifact).where(Artifact.id == artifact_id).with_for_update()
            )
            if artifact is None:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, "missing_row"
                )
            phase = self._artifact_phase(artifact)
            if phase not in {ARTIFACT_PHASE_EXTERNAL, ARTIFACT_PHASE_INTENT}:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, "intent_missing"
                )
            skip = await self._artifact_skip_reason(session, artifact, ignore_retention=True)
            # If external already deleted the blob, still allow finalize even if new
            # protection appears — evidence may already be gone; mark DELETED.
            try:
                absolute = self._artifact_path(artifact)
            except (PathViolation, ValueError):
                absolute = None
            path_missing = absolute is None or (not absolute.exists() and not absolute.is_symlink())
            if not path_missing and phase == ARTIFACT_PHASE_EXTERNAL:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.FAILED,
                    "removal_incomplete",
                )
            if mode is RetentionSweepMode.DRY_RUN:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.WOULD_MARK_DELETED,
                    "would_finalize",
                )
            if skip is not None and phase == ARTIFACT_PHASE_INTENT and not path_missing:
                return RetentionAction(
                    "artifact", str(artifact_id), RetentionOutcome.SKIPPED, skip[0], skip[1]
                )
            meta = dict(artifact.metadata_json or {})
            meta.pop(ARTIFACT_META_KEY, None)
            artifact.metadata_json = meta
            flag_modified(artifact, "metadata_json")
            artifact.storage_state = ArtifactStorageState.DELETED
            session.add(
                Event(
                    entity_type="artifact",
                    entity_id=artifact.id,
                    event_type="ops.retention.artifact_deleted",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={"content_hash": artifact.content_hash, "phase": "finalized"},
                )
            )
            return RetentionAction(
                "artifact",
                str(artifact_id),
                RetentionOutcome.MARKED_DELETED
                if phase == ARTIFACT_PHASE_EXTERNAL
                else RetentionOutcome.RECONCILED,
                "finalized",
            )

    async def _artifact_skip_reason(
        self,
        session: AsyncSession,
        artifact: Artifact,
        *,
        ignore_retention: bool = False,
    ) -> tuple[str, dict[str, object]] | None:
        if artifact.task_id is None:
            return "ambiguous_identity", {"reason": "missing_task_id"}
        task = await session.scalar(
            select(Task).where(Task.id == artifact.task_id).with_for_update()
        )
        if task is None:
            return "ambiguous_identity", {"reason": "task_missing"}
        if task.status is TaskStatus.BLOCKED:
            return "blocked_task", {}
        if task.status not in CLEANABLE_TASK_STATUSES:
            return "non_terminal_task", {"task_status": task.status.value}
        if (
            not ignore_retention
            and artifact.retention_until > datetime.now(UTC)
            and self._artifact_phase(artifact) is None
        ):
            return "within_retention", {}

        # Align with worktree protection (A1): open/blocked steps, approvals, live worktrees.
        if artifact.run_id is not None:
            protected = await self._run_has_protected_runtime(session, artifact.run_id, None)
            if protected is not None:
                return protected
            live_worktree = await session.scalar(
                select(Worktree.id)
                .where(
                    Worktree.run_id == artifact.run_id,
                    Worktree.cleaned_at.is_(None),
                    Worktree.delivery_state != WorktreeDeliveryState.CLEANED,
                )
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if live_worktree is not None:
                return "referenced_by_worktree", {"worktree_id": str(live_worktree)}
        else:
            live_worktree = await session.scalar(
                select(Worktree.id)
                .where(
                    Worktree.task_id == artifact.task_id,
                    Worktree.cleaned_at.is_(None),
                    Worktree.delivery_state != WorktreeDeliveryState.CLEANED,
                    (
                        (Worktree.patch_artifact_id == artifact.id)
                        | (Worktree.changed_files_artifact_id == artifact.id)
                    ),
                )
                .limit(1)
            )
            if live_worktree is not None:
                return "referenced_by_worktree", {"worktree_id": str(live_worktree)}
        return None

    def _artifact_path(self, artifact: Artifact) -> Path:
        assert self._artifact_root is not None
        if not artifact.content_uri.startswith(ARTIFACT_URI_PREFIX):
            raise ValueError("unsupported artifact content_uri")
        relative = Path(artifact.content_uri.removeprefix(ARTIFACT_URI_PREFIX))
        if relative.is_absolute() or ".." in relative.parts:
            raise PathViolation("artifact uri escapes root")
        candidate = self._artifact_root / relative
        return contained(self._artifact_root, candidate, must_exist=False)

    # --- orphans / quarantine ------------------------------------------------------

    async def _reconcile_orphan_artifacts(
        self, *, mode: RetentionSweepMode
    ) -> list[RetentionAction]:
        assert self._artifact_root is not None
        known_hashes = await self._known_content_hashes()
        actions: list[RetentionAction] = []
        for path in self._iter_managed_artifact_files():
            if not self._entry_is_under_root(path):
                actions.append(
                    RetentionAction(
                        "artifact_file",
                        str(path),
                        RetentionOutcome.FAILED,
                        "path_violation",
                    )
                )
                continue
            if path.is_symlink():
                actions.append(
                    await self._quarantine_path(path, mode=mode, reason="symlink_refused")
                )
                continue
            try:
                contained_path = contained(self._artifact_root, path, must_exist=True)
            except PathViolation:
                actions.append(
                    RetentionAction(
                        "artifact_file",
                        str(path),
                        RetentionOutcome.FAILED,
                        "path_violation",
                    )
                )
                continue
            if not contained_path.is_file():
                continue
            try:
                nlink = contained_path.lstat().st_nlink
            except OSError:
                actions.append(
                    RetentionAction(
                        "artifact_file",
                        str(path),
                        RetentionOutcome.FAILED,
                        "stat_failed",
                    )
                )
                continue
            if nlink > 1:
                actions.append(
                    await self._quarantine_path(
                        contained_path, mode=mode, reason="hardlink_ambiguous"
                    )
                )
                continue
            digest = contained_path.name
            if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                actions.append(
                    await self._quarantine_path(
                        contained_path, mode=mode, reason="malformed_identity"
                    )
                )
                continue
            prefix = contained_path.parent.name
            if len(prefix) != 2 or prefix != digest[:2]:
                actions.append(
                    await self._quarantine_path(
                        contained_path, mode=mode, reason="malformed_identity"
                    )
                )
                continue
            if digest in known_hashes:
                continue
            actions.append(
                await self._quarantine_path(
                    contained_path, mode=mode, reason="orphan_filesystem_object"
                )
            )
        return actions

    def _entry_is_under_root(self, path: Path) -> bool:
        assert self._artifact_root is not None
        try:
            parent = path.parent.resolve(strict=True)
            parent.relative_to(self._artifact_root)
            path.relative_to(self._artifact_root)
        except (OSError, ValueError):
            return False
        return True

    async def _known_content_hashes(self) -> set[str]:
        async with self._factory() as session:
            rows = await session.scalars(
                select(Artifact.content_hash).where(
                    Artifact.storage_state.in_(
                        (
                            ArtifactStorageState.AVAILABLE,
                            ArtifactStorageState.STAGING,
                            ArtifactStorageState.QUARANTINED,
                            ArtifactStorageState.MISSING,
                            # Include DELETED so recreated hash paths are not
                            # spuriously treated as orphans when a row still names them.
                            ArtifactStorageState.DELETED,
                        )
                    )
                )
            )
            return set(rows.all())

    def _iter_managed_artifact_files(self) -> list[Path]:
        """Walk artifact root depth-first without following symlinks or leaving root."""

        assert self._artifact_root is not None
        results: list[Path] = []
        stack = [self._artifact_root]
        while stack:
            current = stack.pop()
            try:
                if current.is_symlink():
                    if current != self._artifact_root:
                        results.append(current)
                    continue
                if not current.is_dir():
                    if current.is_file():
                        results.append(current)
                    continue
                if current.name == QUARANTINE_DIR_NAME and current.parent == self._artifact_root:
                    continue
                with os.scandir(current) as entries:
                    for entry in entries:
                        child = Path(entry.path)
                        if not self._entry_is_under_root(child):
                            continue
                        if entry.is_symlink():
                            results.append(child)
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            # Do not cross mount points.
                            try:
                                if (
                                    child.stat().st_dev != self._artifact_root.stat().st_dev
                                ):  # pragma: no cover - mount edge
                                    continue
                            except OSError:  # pragma: no cover
                                continue
                            stack.append(child)
                        elif entry.is_file(follow_symlinks=False):
                            results.append(child)
            except OSError:  # pragma: no cover - unreadable entry
                continue
        return results

    async def _quarantine_path(
        self, path: Path, *, mode: RetentionSweepMode, reason: str
    ) -> RetentionAction:
        assert self._artifact_root is not None
        resource_id = str(path.relative_to(self._artifact_root))
        if mode is RetentionSweepMode.DRY_RUN:
            return RetentionAction(
                "artifact_file",
                resource_id,
                RetentionOutcome.WOULD_QUARANTINE,
                reason,
            )
        assert self._quarantine_root is not None
        token = uuid.uuid4().hex
        destination = self._quarantine_root / token / path.name
        # Phase 1: durable intent (planned destination).
        async with self._factory() as session, session.begin():
            session.add(
                Event(
                    entity_type="artifact_file",
                    entity_id=uuid.uuid5(uuid.NAMESPACE_URL, f"intent:{resource_id}:{token}"),
                    event_type="ops.retention.quarantine_intent",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={
                        "reason": reason,
                        "source": resource_id,
                        "token": token,
                        "destination": str(destination.relative_to(self._artifact_root)),
                    },
                )
            )
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            contained(self._quarantine_root, destination.parent, must_exist=True)
            if path.is_symlink():  # noqa: ASYNC240
                os.rename(path, destination)
            else:
                shutil.move(str(path), str(destination))
        except (OSError, PathViolation) as error:
            return RetentionAction(
                "artifact_file",
                resource_id,
                RetentionOutcome.FAILED,
                "quarantine_failed",
                {"error": type(error).__name__, "reason": reason},
            )
        # Phase 2: finalize event.
        async with self._factory() as session, session.begin():
            session.add(
                Event(
                    entity_type="artifact_file",
                    entity_id=uuid.uuid5(uuid.NAMESPACE_URL, f"done:{resource_id}:{token}"),
                    event_type="ops.retention.quarantined",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={
                        "reason": reason,
                        "source": resource_id,
                        "token": token,
                        "quarantine_path": str(destination.relative_to(self._artifact_root)),
                    },
                )
            )
        return RetentionAction(
            "artifact_file",
            resource_id,
            RetentionOutcome.QUARANTINED,
            reason,
            {"quarantine_path": str(destination.relative_to(self._artifact_root))},
        )

    async def _reconcile_quarantine_intents(  # pragma: no cover
        self, *, mode: RetentionSweepMode
    ) -> list[RetentionAction]:
        """Finalize quarantine moves that crashed after FS move but before commit."""

        assert self._artifact_root is not None
        actions: list[RetentionAction] = []
        async with self._factory() as session:
            intents = tuple(
                (
                    await session.scalars(
                        select(Event)
                        .where(Event.event_type == "ops.retention.quarantine_intent")
                        .order_by(Event.created_at.desc())
                        .limit(self._batch_size)
                    )
                ).all()
            )
        for intent in intents:
            payload = intent.payload or {}
            token = payload.get("token")
            source = payload.get("source")
            destination = payload.get("destination")
            if not isinstance(token, str) or not isinstance(destination, str):
                continue
            async with self._factory() as session:
                done_events = tuple(
                    (
                        await session.scalars(
                            select(Event).where(Event.event_type == "ops.retention.quarantined")
                        )
                    ).all()
                )
            if any((event.payload or {}).get("token") == token for event in done_events):
                continue
            dest_path = self._artifact_root / destination
            if dest_path.exists() or dest_path.is_symlink():
                if mode is RetentionSweepMode.DRY_RUN:
                    actions.append(
                        RetentionAction(
                            "artifact_file",
                            str(source or destination),
                            RetentionOutcome.WOULD_QUARANTINE,
                            "would_finalize_quarantine_intent",
                            {"token": token},
                        )
                    )
                    continue
                async with self._factory() as session, session.begin():
                    session.add(
                        Event(
                            entity_type="artifact_file",
                            entity_id=uuid.uuid5(uuid.NAMESPACE_URL, f"done:{source}:{token}"),
                            event_type="ops.retention.quarantined",
                            actor_type="retention",
                            actor_id=self._owner,
                            payload={
                                "reason": "reconcile_after_crash",
                                "source": source,
                                "token": token,
                                "quarantine_path": destination,
                            },
                        )
                    )
                actions.append(
                    RetentionAction(
                        "artifact_file",
                        str(source or destination),
                        RetentionOutcome.RECONCILED,
                        "quarantine_intent_finalized",
                        {"token": token},
                    )
                )
        return actions

    async def _record_summary(
        self,
        actions: list[RetentionAction],
        *,
        mode: RetentionSweepMode,
        started_at: datetime,
    ) -> None:
        report = RetentionSweepReport(
            mode=mode,
            lock_acquired=True,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            actions=tuple(actions),
        )
        payload = report.to_operational_payload()
        summary = {
            key: payload[key]
            for key in (
                "schema_version",
                "mode",
                "lock_acquired",
                "started_at",
                "finished_at",
                "action_count",
                "cleaned_count",
                "skipped_count",
                "failure_count",
                "outcome_counts",
            )
        }
        async with self._factory() as session, session.begin():
            session.add(
                Event(
                    entity_type="runtime_resource",
                    entity_id=uuid.uuid5(uuid.NAMESPACE_DNS, f"retention:{started_at.isoformat()}"),
                    event_type="ops.retention.sweep_completed",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload=summary,
                )
            )
