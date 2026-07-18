"""Idempotent retention sweeper for managed worktrees and content-addressed artifacts.

Eligibility is always derived from PostgreSQL state plus ``retention_until``. Active,
leased, awaiting-review/approval, blocked, and other non-terminal resources are never
removed. Orphan filesystem objects are quarantined under the artifact root before any
later operator disposal; they are never deleted in-place by this module.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.settings import RetentionDefaults
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

# Terminal outcomes that may eventually release retained resources. BLOCKED is
# intentionally excluded: unknown-effect recovery requires manual resolution.
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
OPEN_STEP_STATUSES = frozenset(
    {
        StepStatus.PENDING,
        StepStatus.QUEUED,
        StepStatus.LEASED,
        StepStatus.RUNNING,
        StepStatus.WAITING_APPROVAL,
        StepStatus.AWAITING_USER,
    }
)
PROTECTED_WORKTREE_STATES = frozenset(
    {
        WorktreeDeliveryState.ACTIVE,
        WorktreeDeliveryState.CLEANED,
    }
)


class RetentionSweepMode(StrEnum):
    DRY_RUN = "dry_run"
    APPLY = "apply"


class RetentionOutcome(StrEnum):
    WOULD_CLEAN = "would_clean"
    CLEANED = "cleaned"
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
    """Abort the current transaction and surface a deterministic action result."""

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
        """Structured summary suitable for a future System projection."""

        counts: dict[str, int] = {}
        for action in self.actions:
            key = action.outcome.value
            counts[key] = counts.get(key, 0) + 1
        return {
            "schema_version": "retention-sweep-report.v1",
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
    """Return the earliest cleanup time, or None when the worktree must be retained.

    Unresolved and blocked tasks never become eligible through this function.
    Completed tasks may become eligible after ``completed_days`` without shortening
    the longer failed-default ``retention_until`` stamped at prepare time for
    unresolved work: the effective deadline is the earlier of the stamped value and
    ``task_updated_at + completed_days`` only when the task is already completed.
    """

    if task_status is TaskStatus.COMPLETED:
        completed_deadline = task_updated_at + timedelta(days=completed_days)
        return min(retention_until, completed_deadline)
    if task_status in FAILED_LIKE_TASK_STATUSES:
        # Failed/cancelled retention must not be shorter than the completed window.
        failed_floor = task_updated_at + timedelta(days=max(failed_days, completed_days))
        return (
            max(retention_until, failed_floor)
            if retention_until < failed_floor
            else retention_until
        )
    return None


class RetentionSweeper:
    """Single-instance retention sweeper guarded by a PostgreSQL advisory lock."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        worktree_root: Path,
        artifact_root: Path,
        retention: RetentionDefaults,
        owner: str,
        lock_timeout_seconds: float | None = None,
        lock_poll_seconds: float = 0.1,
    ) -> None:
        self._factory = factory
        self._worktree_root = trusted_root(worktree_root, create=True)
        self._artifact_root = trusted_root(artifact_root, create=True)
        self._quarantine_root = trusted_root(self._artifact_root / QUARANTINE_DIR_NAME, create=True)
        self._retention = retention
        self._owner = owner
        self._lock_timeout_seconds = (
            retention.sweep_lock_timeout_seconds
            if lock_timeout_seconds is None
            else lock_timeout_seconds
        )
        self._lock_poll_seconds = lock_poll_seconds
        self._batch_size = retention.sweep_batch_size

    async def run(self, *, mode: RetentionSweepMode) -> RetentionSweepReport:
        started = datetime.now(UTC)
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
                            resource_type="sweep",
                            resource_id="lock",
                            outcome=RetentionOutcome.SKIPPED,
                            reason="advisory_lock_unavailable",
                        ),
                    ),
                )
            actions: list[RetentionAction] = []
            try:
                actions.extend(await self._sweep_worktrees(mode=mode))
                actions.extend(await self._sweep_artifacts(mode=mode))
                actions.extend(await self._reconcile_orphan_artifacts(mode=mode))
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
                except Exception:
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

    async def _sweep_worktrees(self, *, mode: RetentionSweepMode) -> list[RetentionAction]:
        actions: list[RetentionAction] = []
        async with self._factory() as session:
            async with session.begin():
                rows = tuple(
                    (
                        await session.scalars(
                            select(Worktree)
                            .where(
                                Worktree.cleaned_at.is_(None),
                                Worktree.delivery_state != WorktreeDeliveryState.CLEANED,
                            )
                            .order_by(Worktree.retention_until, Worktree.id)
                            .limit(self._batch_size)
                            .with_for_update(skip_locked=True)
                        )
                    ).all()
                )
            for worktree in rows:
                actions.append(await self._process_worktree(worktree.id, mode=mode))
        return actions

    async def _process_worktree(
        self, worktree_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        async with self._factory() as session, session.begin():
            worktree = await session.scalar(
                select(Worktree).where(Worktree.id == worktree_id).with_for_update()
            )
            if worktree is None:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.SKIPPED,
                    "missing_row",
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
                    {"reason": "task_or_run_missing"},
                )
            if run.task_id != task.id:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.SKIPPED,
                    "ambiguous_identity",
                    {"reason": "run_task_mismatch"},
                )

            skip = await self._worktree_skip_reason(session, worktree, task, run)
            if skip is not None:
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.SKIPPED,
                    skip[0],
                    skip[1],
                )

            path = Path(worktree.path)
            try:
                contained(self._worktree_root, path, must_exist=False)
            except PathViolation as error:
                worktree.cleanup_reason = "path_violation"
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "path_violation",
                    {"error": str(error)},
                )
            if path.exists() and (path.is_symlink() or not path.is_dir()):  # noqa: ASYNC240
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

            removed = self._remove_worktree_tree(path)
            if not removed:
                worktree.cleanup_reason = "removal_incomplete"
                return RetentionAction(
                    "worktree",
                    str(worktree_id),
                    RetentionOutcome.FAILED,
                    "removal_incomplete",
                    {"path": str(path)},
                )
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
                    previous_state=None,
                    new_state=WorktreeDeliveryState.CLEANED.value,
                    payload={
                        "reason": "retention",
                        "path": str(path),
                        "task_id": str(task.id),
                        "run_id": str(run.id),
                    },
                )
            )
            return RetentionAction(
                "worktree",
                str(worktree_id),
                RetentionOutcome.CLEANED,
                "retention_expired",
                {"path": str(path)},
            )

    async def _worktree_skip_reason(
        self,
        session: AsyncSession,
        worktree: Worktree,
        task: Task,
        run: Run,
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

        open_step = await session.scalar(
            select(Step.id)
            .where(Step.run_id == run.id, Step.status.in_(tuple(OPEN_STEP_STATUSES)))
            .limit(1)
        )
        if open_step is not None:
            return "open_step", {"step_id": str(open_step)}

        pending_approval = await session.scalar(
            select(Approval.id)
            .join(Step, Step.id == Approval.step_id)
            .where(
                Step.run_id == run.id,
                Approval.status == ApprovalStatus.PENDING,
            )
            .limit(1)
        )
        if pending_approval is not None:
            return "pending_approval", {"approval_id": str(pending_approval)}

        active_process = await session.scalar(
            select(SupervisedProcess.id)
            .where(
                SupervisedProcess.worktree_id == worktree.id,
                SupervisedProcess.status.in_(tuple(ACTIVE_PROCESS_STATUSES)),
            )
            .limit(1)
        )
        if active_process is not None:
            return "active_supervised_process", {"process_id": str(active_process)}
        return None

    @staticmethod
    def _remove_worktree_tree(path: Path) -> bool:
        # Check the directory entry itself first: broken symlinks report
        # exists()=False while still being unsafe to treat as "already gone".
        if path.is_symlink():
            return False
        if not path.exists():
            return True
        try:
            shutil.rmtree(path)
        except OSError:
            return False
        return not path.exists()

    async def _sweep_artifacts(self, *, mode: RetentionSweepMode) -> list[RetentionAction]:
        actions: list[RetentionAction] = []
        async with self._factory() as session:
            async with session.begin():
                rows = tuple(
                    (
                        await session.scalars(
                            select(Artifact)
                            .where(
                                Artifact.storage_state == ArtifactStorageState.AVAILABLE,
                                Artifact.retention_until < func.now(),
                            )
                            .order_by(Artifact.retention_until, Artifact.id)
                            .limit(self._batch_size)
                            .with_for_update(skip_locked=True)
                        )
                    ).all()
                )
            for artifact in rows:
                actions.append(await self._process_artifact(artifact.id, mode=mode))
        return actions

    async def _process_artifact(
        self, artifact_id: uuid.UUID, *, mode: RetentionSweepMode
    ) -> RetentionAction:
        try:
            async with self._factory() as session, session.begin():
                return await self._process_artifact_in_session(session, artifact_id, mode=mode)
        except _RetentionActionAbort as abort:
            return abort.action

    async def _process_artifact_in_session(
        self,
        session: AsyncSession,
        artifact_id: uuid.UUID,
        *,
        mode: RetentionSweepMode,
    ) -> RetentionAction:
        artifact = await session.scalar(
            select(Artifact).where(Artifact.id == artifact_id).with_for_update()
        )
        if artifact is None:
            return RetentionAction(
                "artifact",
                str(artifact_id),
                RetentionOutcome.SKIPPED,
                "missing_row",
            )
        if artifact.storage_state is not ArtifactStorageState.AVAILABLE:
            return RetentionAction(
                "artifact",
                str(artifact_id),
                RetentionOutcome.SKIPPED,
                "not_available",
                {"storage_state": artifact.storage_state.value},
            )
        if artifact.retention_until > datetime.now(UTC):
            return RetentionAction(
                "artifact",
                str(artifact_id),
                RetentionOutcome.SKIPPED,
                "within_retention",
            )

        if artifact.task_id is not None:
            task = await session.scalar(select(Task).where(Task.id == artifact.task_id))
            if task is None:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.SKIPPED,
                    "ambiguous_identity",
                    {"reason": "task_missing"},
                )
            if task.status is TaskStatus.BLOCKED:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.SKIPPED,
                    "blocked_task",
                )
            if task.status not in CLEANABLE_TASK_STATUSES:
                return RetentionAction(
                    "artifact",
                    str(artifact_id),
                    RetentionOutcome.SKIPPED,
                    "non_terminal_task",
                    {"task_status": task.status.value},
                )
        else:
            return RetentionAction(
                "artifact",
                str(artifact_id),
                RetentionOutcome.SKIPPED,
                "ambiguous_identity",
                {"reason": "missing_task_id"},
            )

        referenced = await self._artifact_still_required(session, artifact)
        if referenced is not None:
            return RetentionAction(
                "artifact",
                str(artifact_id),
                RetentionOutcome.SKIPPED,
                referenced[0],
                referenced[1],
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

        remaining = await session.scalar(
            select(Artifact.id)
            .where(
                Artifact.content_hash == artifact.content_hash,
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
        file_removed = False
        if remaining is None:
            try:
                absolute.unlink()
                file_removed = True
            except OSError as error:
                raise _RetentionActionAbort(
                    RetentionAction(
                        "artifact",
                        str(artifact_id),
                        RetentionOutcome.FAILED,
                        "filesystem_delete_failed",
                        {"error": type(error).__name__},
                    )
                ) from error
            parent = absolute.parent
            if parent != self._artifact_root and parent.is_dir():
                with contextlib.suppress(OSError):
                    parent.rmdir()
        artifact.storage_state = ArtifactStorageState.DELETED
        session.add(
            Event(
                entity_type="artifact",
                entity_id=artifact.id,
                event_type="ops.retention.artifact_deleted",
                actor_type="retention",
                actor_id=self._owner,
                payload={
                    "content_hash": artifact.content_hash,
                    "file_removed": file_removed,
                },
            )
        )
        return RetentionAction(
            "artifact",
            str(artifact_id),
            RetentionOutcome.MARKED_DELETED,
            "retention_expired",
            {"path": str(absolute), "file_removed": file_removed},
        )

    async def _artifact_still_required(
        self, session: AsyncSession, artifact: Artifact
    ) -> tuple[str, dict[str, object]] | None:
        active_worktree = await session.scalar(
            select(Worktree.id)
            .where(
                Worktree.cleaned_at.is_(None),
                Worktree.delivery_state != WorktreeDeliveryState.CLEANED,
                (
                    (Worktree.patch_artifact_id == artifact.id)
                    | (Worktree.changed_files_artifact_id == artifact.id)
                ),
            )
            .limit(1)
        )
        if active_worktree is not None:
            return "referenced_by_worktree", {"worktree_id": str(active_worktree)}
        return None

    def _artifact_path(self, artifact: Artifact) -> Path:
        if not artifact.content_uri.startswith(ARTIFACT_URI_PREFIX):
            raise ValueError("unsupported artifact content_uri")
        relative = Path(artifact.content_uri.removeprefix(ARTIFACT_URI_PREFIX))
        if relative.is_absolute() or ".." in relative.parts:
            raise PathViolation("artifact uri escapes root")
        candidate = self._artifact_root / relative
        return contained(self._artifact_root, candidate, must_exist=False)

    async def _reconcile_orphan_artifacts(
        self, *, mode: RetentionSweepMode
    ) -> list[RetentionAction]:
        known_hashes = await self._known_content_hashes()
        actions: list[RetentionAction] = []
        for path in self._iter_managed_artifact_files():
            # Inspect the directory entry itself before resolve(): resolve() would
            # follow a symlink escape and look like a path_violation instead of a
            # quarantineable symlink node under the artifact root.
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
                    await self._quarantine_path(
                        path,
                        mode=mode,
                        reason="symlink_refused",
                    )
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
            digest = contained_path.name
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                actions.append(
                    await self._quarantine_path(
                        contained_path,
                        mode=mode,
                        reason="malformed_identity",
                    )
                )
                continue
            prefix = contained_path.parent.name
            if len(prefix) != 2 or prefix != digest[:2]:
                actions.append(
                    await self._quarantine_path(
                        contained_path,
                        mode=mode,
                        reason="malformed_identity",
                    )
                )
                continue
            if digest in known_hashes:
                continue
            actions.append(
                await self._quarantine_path(
                    contained_path,
                    mode=mode,
                    reason="orphan_filesystem_object",
                )
            )
        return actions

    def _entry_is_under_root(self, path: Path) -> bool:
        """Return True when the entry's parent chain stays inside the artifact root."""

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
                        )
                    )
                )
            )
            return set(rows.all())

    def _iter_managed_artifact_files(self) -> list[Path]:
        results: list[Path] = []
        try:
            children = list(self._artifact_root.iterdir())
        except OSError:
            return results
        for child in children:
            if child.name == QUARANTINE_DIR_NAME:
                continue
            if child.is_symlink():
                results.append(child)
                continue
            if not child.is_dir():
                if child.is_file() or child.is_symlink():
                    results.append(child)
                continue
            try:
                for entry in child.iterdir():
                    if entry.is_file() or entry.is_symlink():
                        results.append(entry)
            except OSError:
                continue
        return results

    async def _quarantine_path(
        self, path: Path, *, mode: RetentionSweepMode, reason: str
    ) -> RetentionAction:
        resource_id = str(path.relative_to(self._artifact_root))
        if mode is RetentionSweepMode.DRY_RUN:
            return RetentionAction(
                "artifact_file",
                resource_id,
                RetentionOutcome.WOULD_QUARANTINE,
                reason,
            )
        token = uuid.uuid4().hex
        destination = self._quarantine_root / token / path.name
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            contained(self._quarantine_root, destination.parent, must_exist=True)
            if path.is_symlink():  # noqa: ASYNC240
                # Move the symlink node itself without following it.
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
        async with self._factory() as session, session.begin():
            session.add(
                Event(
                    entity_type="artifact_file",
                    entity_id=uuid.uuid5(uuid.NAMESPACE_URL, resource_id),
                    event_type="ops.retention.quarantined",
                    actor_type="retention",
                    actor_id=self._owner,
                    payload={
                        "reason": reason,
                        "source": resource_id,
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
        # Keep the durable event bounded; full action lists stay in the CLI report.
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
