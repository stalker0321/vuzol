"""Persisted idempotent Git worktree lifecycle."""

import contextlib
import hashlib
import shutil
import uuid
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProjectConfig
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.domain import WorktreeReference
from vuzol.execution.paths import (
    PathViolation,
    contained,
    trusted_root,
    worktree_branch,
    worktree_path,
)
from vuzol.execution.ports import GitPort
from vuzol.storage.models import SupervisedProcess, Worktree
from vuzol.storage.types import ProcessStatus, WorktreeDeliveryState

WORKTREE_LOCK_KEY = 8_946_527_102


class WorktreeError(RuntimeError):
    """A worktree lifecycle invariant was not satisfied."""


class WorktreeService:
    def __init__(self, root: Path, git: GitPort, *, retention_days: int) -> None:
        self._root = trusted_root(root, create=True)
        self._git = git
        self._retention_days = retention_days

    async def prepare(
        self,
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        run_id: uuid.UUID,
        project: ProjectConfig,
        owner: str,
    ) -> WorktreeReference:
        await session.execute(select(func.pg_advisory_xact_lock(WORKTREE_LOCK_KEY)))
        existing = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
        repository = project.repository_path.resolve(strict=True)
        identity, remote = await self._git.repository_identity(repository)
        base = await self._git.resolve_commit(repository, project.default_branch)
        path = worktree_path(self._root, project.id, run_id)
        branch = worktree_branch(task_id, run_id)
        if existing is not None:
            if (
                existing.task_id != task_id
                or existing.project_id != project.id
                or existing.repository_identity_hash != identity
                or existing.base_commit != base
                or existing.branch != branch
                or Path(existing.path) != path
            ):
                raise WorktreeError("persisted worktree identity does not match the request")
            contained(self._root, path)
            return _reference(existing)

        await self._git.require_clean_source(repository)
        path.parent.mkdir(parents=True, exist_ok=True)
        contained(self._root, path.parent)
        if path.exists():
            raise WorktreeError("derived worktree path already exists without a record")
        await self._git.add_worktree(repository, path, branch, base)
        try:
            contained(self._root, path)
            inspection = await self._git.inspect(path)
            if inspection.head != base or inspection.branch != branch:
                raise WorktreeError("created worktree identity is invalid")
            row = Worktree(
                task_id=task_id,
                run_id=run_id,
                project_id=project.id,
                source_remote=_safe_remote(remote),
                source_remote_hash=(
                    hashlib.sha256(remote.encode()).hexdigest() if remote else None
                ),
                repository_identity_hash=identity,
                base_commit=base,
                default_branch=project.default_branch,
                expected_target_head=base,
                branch=branch,
                path=str(path),
                owner=owner,
                delivery_state=WorktreeDeliveryState.ACTIVE,
                retention_until=func.now() + timedelta(days=self._retention_days),
            )
            session.add(row)
            await session.flush()
            return _reference(row)
        except BaseException:
            await self._git.remove_worktree(repository, path)
            raise

    async def retain(
        self,
        session: AsyncSession,
        *,
        worktree_id: uuid.UUID,
        artifacts: ArtifactStore | None = None,
        step_id: uuid.UUID | None = None,
    ) -> WorktreeReference:
        row = await session.scalar(
            select(Worktree).where(Worktree.id == worktree_id).with_for_update()
        )
        if row is None:
            raise WorktreeError("unknown worktree")
        path = contained(self._root, Path(row.path))
        inspection = await self._git.inspect(path)
        if artifacts is not None:
            artifacts.reject_secrets(inspection.diff)
        row.diff_hash = inspection.diff_hash
        row.result_commit = inspection.head
        row.delivery_state = WorktreeDeliveryState.WORKTREE_RETAINED
        row.last_inspected_at = func.now()
        row.lifecycle_generation += 1

        if artifacts is not None:
            # Persist diff and changed files list as artifacts for evidence
            use_step = step_id or row.task_id  # fallback; caller should supply real step
            diff_art = await artifacts.persist(
                session,
                task_id=row.task_id,
                run_id=row.run_id,
                step_id=use_step,
                artifact_type="git_diff",
                content=inspection.diff,
                media_type="text/x-diff",
            )
            changed_content = "\n".join(inspection.changed_files).encode()
            changed_art = await artifacts.persist(
                session,
                task_id=row.task_id,
                run_id=row.run_id,
                step_id=use_step,
                artifact_type="changed_files",
                content=changed_content or b"",
                media_type="text/plain",
            )
            row.patch_artifact_id = diff_art.id
            row.changed_files_artifact_id = changed_art.id

        await session.flush()
        return _reference(row)

    async def cleanup(
        self,
        session: AsyncSession,
        *,
        worktree_id: uuid.UUID,
        repository: Path | None = None,
        reason: str = "retention",
    ) -> None:
        """Idempotent safe cleanup of a worktree after retention or completion.

        Never deletes unrecorded, active, or escaping paths.
        """
        row = await session.scalar(
            select(Worktree).where(Worktree.id == worktree_id).with_for_update()
        )
        if row is None:
            return
        if row.cleaned_at is not None or row.delivery_state == WorktreeDeliveryState.CLEANED:
            return

        # Reject active or in-use worktrees
        if row.delivery_state == WorktreeDeliveryState.ACTIVE:
            return
        # Check for any non-terminal supervised process
        active = await session.scalar(
            select(SupervisedProcess.id)
            .where(
                SupervisedProcess.worktree_id == worktree_id,
                SupervisedProcess.status.in_(
                    [ProcessStatus.STARTING, ProcessStatus.RUNNING, ProcessStatus.TERMINATING]
                ),
            )
            .limit(1)
        )
        if active is not None:
            return

        path = Path(row.path)
        try:
            contained(self._root, path, must_exist=False)
        except PathViolation:
            row.cleanup_reason = "path_violation"
            await session.flush()
            return

        # Remove from git metadata if possible
        if repository is not None:
            with contextlib.suppress(Exception):
                await self._git.remove_worktree(repository, path)

        # Remove fs
        fs_removed = False
        if path.exists():  # noqa: ASYNC240
            with contextlib.suppress(Exception):
                for child in list(path.iterdir()):  # noqa: ASYNC240
                    if child.name == ".git":
                        continue
                shutil.rmtree(str(path), ignore_errors=True)
            if not path.exists():  # noqa: ASYNC240
                fs_removed = True
        else:
            fs_removed = True

        # Verify git metadata gone if we had repo
        git_meta_gone = True
        if repository is not None:
            try:
                out = await self._git._run(repository, "worktree", "list", "--porcelain")  # type: ignore[attr-defined]
                if str(path) in out.decode("utf-8", "ignore"):
                    git_meta_gone = False
            except Exception:
                git_meta_gone = False  # conservative

        if not (fs_removed and git_meta_gone):
            row.cleanup_reason = "removal_incomplete"
            await session.flush()
            return

        row.delivery_state = WorktreeDeliveryState.CLEANED
        row.cleanup_reason = reason
        row.cleaned_at = func.now()
        await session.flush()


def _safe_remote(remote: str | None) -> str | None:
    if remote is None:
        return None
    if "://" not in remote:
        if "@" in remote:
            return remote.split("@", 1)[1]
        return remote
    parsed = urlsplit(remote)
    host = parsed.hostname or "redacted"
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _reference(row: Worktree) -> WorktreeReference:
    return WorktreeReference(
        id=row.id,
        task_id=row.task_id,
        run_id=row.run_id,
        project_id=row.project_id,
        path=Path(row.path),
        branch=row.branch,
        base_commit=row.base_commit,
        repository_identity_hash=row.repository_identity_hash,
    )
