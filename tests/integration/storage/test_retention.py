"""PostgreSQL integration coverage for the retention sweeper."""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from vuzol.config.settings import RetentionDefaults
from vuzol.ops.retention import (
    RETENTION_SWEEP_LOCK_KEY,
    RetentionOutcome,
    RetentionSweeper,
    RetentionSweepMode,
)
from vuzol.storage.models import Artifact, Event, Task, Worktree
from vuzol.storage.types import (
    ArtifactStorageState,
    IdempotencyClass,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.storage.unit_of_work import UnitOfWork


def _artifact_bytes(content: bytes) -> tuple[str, Path]:
    digest = hashlib.sha256(content).hexdigest()
    return digest, Path(digest[:2]) / digest


async def _seed_task(
    factory: async_sessionmaker[AsyncSession],
    *,
    status: TaskStatus,
    run_status: RunStatus = RunStatus.COMPLETED,
    step_status: StepStatus = StepStatus.COMPLETED,
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        task = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="retention fixture",
            task_type="coding",
        )
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=run_status,
        )
        step = await uow.steps.create(
            run_id=run_id,
            ordinal=1,
            step_type="execute_code",
            idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
            status=step_status,
            max_attempts=3,
        )
        task_id = task.id
        step_id = step.id
    async with factory.begin() as session:
        await session.execute(update(Task).where(Task.id == task_id).values(status=status))
        if status is TaskStatus.COMPLETED:
            await session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(updated_at=datetime.now(UTC) - timedelta(days=10))
            )
    return task_id, run_id, step_id


async def _add_worktree(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    root: Path,
    delivery_state: WorktreeDeliveryState = WorktreeDeliveryState.WORKTREE_RETAINED,
    retention_until: datetime | None = None,
) -> Worktree:
    path = root / "project" / str(run_id)
    path.mkdir(parents=True, exist_ok=True)
    (path / "README").write_text("retained\n")
    row = Worktree(
        task_id=task_id,
        run_id=run_id,
        project_id="project",
        repository_identity_hash="r" * 64,
        base_commit="b" * 40,
        default_branch="main",
        expected_target_head="b" * 40,
        branch=f"vuzol/task-{task_id}-run-{str(run_id)[:12]}",
        path=str(path),
        owner="test",
        delivery_state=delivery_state,
        retention_until=retention_until or (datetime.now(UTC) - timedelta(days=1)),
    )
    session.add(row)
    await session.flush()
    return row


async def _add_artifact(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    root: Path,
    content: bytes,
    retention_until: datetime | None = None,
) -> Artifact:
    digest, relative = _artifact_bytes(content)
    destination = root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    row = Artifact(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        artifact_type="git_diff",
        content_uri=f"artifact:{relative.as_posix()}",
        storage_key=f"{task_id}/{run_id}/{step_id}/git_diff/{digest}",
        size_bytes=len(content),
        content_hash=digest,
        media_type="text/plain",
        sensitivity="internal",
        visibility="private",
        retention_until=retention_until or (datetime.now(UTC) - timedelta(days=1)),
        storage_state=ArtifactStorageState.AVAILABLE,
    )
    session.add(row)
    await session.flush()
    return row


def _sweeper(
    factory: async_sessionmaker[AsyncSession], worktree_root: Path, artifact_root: Path
) -> RetentionSweeper:
    return RetentionSweeper(
        factory,
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        retention=RetentionDefaults(
            completed_worktree_days=3,
            failed_worktree_days=14,
            artifact_days=14,
            sweep_batch_size=50,
            sweep_lock_timeout_seconds=1.0,
        ),
        owner="test-retention",
    )


@pytest.mark.postgresql
def test_expired_completed_worktree_and_artifact_are_cleaned(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session, task_id=task_id, run_id=run_id, root=worktree_root
            )
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"diff-a",
            )
            worktree_path = Path(worktree.path)
            artifact_path = artifact_root / artifact.content_uri.removeprefix("artifact:")

        dry = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        assert dry.lock_acquired
        assert any(
            action.outcome is RetentionOutcome.WOULD_CLEAN and action.resource_type == "worktree"
            for action in dry.actions
        )
        assert any(
            action.outcome is RetentionOutcome.WOULD_MARK_DELETED
            and action.resource_type == "artifact"
            for action in dry.actions
        )
        assert worktree_path.exists()  # noqa: ASYNC240
        assert artifact_path.exists()

        applied = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        assert applied.lock_acquired
        assert not worktree_path.exists()  # noqa: ASYNC240
        assert not artifact_path.exists()
        async with factory() as session:
            cleaned = await session.get(Worktree, worktree.id)
            deleted = await session.get(Artifact, artifact.id)
            assert cleaned is not None
            assert cleaned.delivery_state is WorktreeDeliveryState.CLEANED
            assert cleaned.cleaned_at is not None
            assert deleted is not None
            assert deleted.storage_state is ArtifactStorageState.DELETED
            events = tuple(
                (
                    await session.scalars(
                        select(Event.event_type).where(
                            Event.event_type.in_(
                                (
                                    "ops.retention.worktree_cleaned",
                                    "ops.retention.artifact_deleted",
                                    "ops.retention.sweep_completed",
                                )
                            )
                        )
                    )
                ).all()
            )
            assert "ops.retention.worktree_cleaned" in events
            assert "ops.retention.artifact_deleted" in events
            assert "ops.retention.sweep_completed" in events

        repeat = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        assert repeat.cleaned_count == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_active_blocked_and_pending_resources_are_preserved(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()

        active_task, active_run, _ = await _seed_task(
            factory,
            status=TaskStatus.EXECUTING,
            run_status=RunStatus.RUNNING,
            step_status=StepStatus.RUNNING,
        )
        blocked_task, blocked_run, blocked_step = await _seed_task(
            factory, status=TaskStatus.BLOCKED, run_status=RunStatus.BLOCKED
        )
        async with factory.begin() as session:
            active = await _add_worktree(
                session,
                task_id=active_task,
                run_id=active_run,
                root=worktree_root,
                delivery_state=WorktreeDeliveryState.ACTIVE,
            )
            blocked = await _add_worktree(
                session,
                task_id=blocked_task,
                run_id=blocked_run,
                root=worktree_root,
            )
            blocked_art = await _add_artifact(
                session,
                task_id=blocked_task,
                run_id=blocked_run,
                step_id=blocked_step,
                root=artifact_root,
                content=b"blocked",
            )

        report = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        reasons = {action.reason for action in report.actions}
        assert "protected_delivery_state" in reasons or "non_terminal_task" in reasons
        assert "blocked_task" in reasons
        assert Path(active.path).exists()  # noqa: ASYNC240
        assert Path(blocked.path).exists()  # noqa: ASYNC240
        async with factory() as session:
            active_row = await session.get(Worktree, active.id)
            blocked_row = await session.get(Worktree, blocked.id)
            blocked_artifact = await session.get(Artifact, blocked_art.id)
            assert active_row is not None and active_row.cleaned_at is None
            assert blocked_row is not None and blocked_row.cleaned_at is None
            assert (
                blocked_artifact is not None
                and blocked_artifact.storage_state is ArtifactStorageState.AVAILABLE
            )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_orphan_file_is_quarantined_and_symlink_escape_refused(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()
        orphan_dir = artifact_root / "aa"
        orphan_dir.mkdir()
        orphan = orphan_dir / ("a" * 64)
        orphan.write_bytes(b"orphan-bytes")
        outside = tmp_path / "outside-secret"
        outside.write_text("keep-me\n")
        link = artifact_root / "bb"
        link.mkdir()
        escape = link / ("b" * 64)
        escape.symlink_to(outside)

        report = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        outcomes = {(action.reason, action.outcome) for action in report.actions}
        assert ("orphan_filesystem_object", RetentionOutcome.QUARANTINED) in outcomes
        assert ("symlink_refused", RetentionOutcome.QUARANTINED) in outcomes
        assert not orphan.exists()
        assert not escape.exists()
        assert outside.read_text() == "keep-me\n"
        quarantine_files = list((artifact_root / ".quarantine").rglob("*"))
        assert any(path.is_file() or path.is_symlink() for path in quarantine_files)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_concurrent_sweep_serializes_on_advisory_lock(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()
        holder_started = asyncio.Event()
        release_holder = asyncio.Event()

        async def hold_lock() -> None:
            async with factory() as session:
                acquired = await session.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"),
                    {"key": RETENTION_SWEEP_LOCK_KEY},
                )
                assert acquired is True
                holder_started.set()
                await release_holder.wait()
                await session.execute(
                    text("SELECT pg_advisory_unlock(:key)"),
                    {"key": RETENTION_SWEEP_LOCK_KEY},
                )
                await session.commit()

        holder = asyncio.create_task(hold_lock())
        await holder_started.wait()
        report = await RetentionSweeper(
            factory,
            worktree_root=worktree_root,
            artifact_root=artifact_root,
            retention=RetentionDefaults(sweep_lock_timeout_seconds=0.2),
            owner="contender",
        ).run(mode=RetentionSweepMode.DRY_RUN)
        assert report.lock_acquired is False
        assert report.actions[0].reason == "advisory_lock_unavailable"
        release_holder.set()
        await holder
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_partial_failure_does_not_block_other_resources(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()
        good_task, good_run, _ = await _seed_task(factory, status=TaskStatus.COMPLETED)
        bad_task, bad_run, _ = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            good = await _add_worktree(
                session, task_id=good_task, run_id=good_run, root=worktree_root
            )
            bad = await _add_worktree(session, task_id=bad_task, run_id=bad_run, root=worktree_root)
            # Escape the configured root after insert.
            bad.path = str(tmp_path / "foreign" / "worktree")
            Path(bad.path).mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240

        report = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            action.resource_id == str(good.id) and action.outcome is RetentionOutcome.CLEANED
            for action in report.actions
        )
        assert any(
            action.resource_id == str(bad.id) and action.outcome is RetentionOutcome.FAILED
            for action in report.actions
        )
        assert not Path(good.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_open_step_and_referenced_artifact_are_skipped(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()
        open_task, open_run, _open_step = await _seed_task(
            factory,
            status=TaskStatus.COMPLETED,
            run_status=RunStatus.COMPLETED,
            step_status=StepStatus.WAITING_APPROVAL,
        )
        ref_task, ref_run, ref_step = await _seed_task(
            factory,
            status=TaskStatus.COMPLETED,
            run_status=RunStatus.COMPLETED,
            step_status=StepStatus.COMPLETED,
        )
        async with factory.begin() as session:
            # completed_days is computed from task.updated_at; keep the referencing
            # worktree inside that window so it is not cleaned first.
            await session.execute(
                update(Task).where(Task.id == ref_task).values(updated_at=datetime.now(UTC))
            )
            open_wt = await _add_worktree(
                session, task_id=open_task, run_id=open_run, root=worktree_root
            )
            ref_wt = await _add_worktree(
                session,
                task_id=ref_task,
                run_id=ref_run,
                root=worktree_root,
                retention_until=datetime.now(UTC) + timedelta(days=30),
            )
            artifact = await _add_artifact(
                session,
                task_id=ref_task,
                run_id=ref_run,
                step_id=ref_step,
                root=artifact_root,
                content=b"still-needed",
            )
            ref_wt.patch_artifact_id = artifact.id

        report = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        reasons = {action.reason for action in report.actions}
        assert "open_step" in reasons
        assert "referenced_by_worktree" in reasons
        assert Path(open_wt.path).exists()  # noqa: ASYNC240
        assert Path(ref_wt.path).exists()  # noqa: ASYNC240
        async with factory() as session:
            row = await session.get(Artifact, artifact.id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.AVAILABLE
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_invalid_worktree_path_and_already_cleaned_are_deterministic(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()
        task_id, run_id, _step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session, task_id=task_id, run_id=run_id, root=worktree_root
            )
            # Replace the directory with a plain file so cleanup fails closed.
            import shutil

            path = Path(worktree.path)
            shutil.rmtree(path)
            path.write_text("not-a-directory\n")  # noqa: ASYNC240

        report = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            action.resource_id == str(worktree.id)
            and action.outcome is RetentionOutcome.FAILED
            and action.reason == "invalid_worktree_path"
            for action in report.actions
        )
        async with factory.begin() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            row.delivery_state = WorktreeDeliveryState.CLEANED
            row.cleaned_at = datetime.now(UTC)
        report = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        # Already cleaned rows are ignored by the candidate query.
        assert not any(action.resource_id == str(worktree.id) for action in report.actions)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_missing_artifact_file_is_marked_missing(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        worktree_root.mkdir()
        artifact_root.mkdir()
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"gone",
            )
            path = artifact_root / artifact.content_uri.removeprefix("artifact:")
            path.unlink()
        report = await _sweeper(factory, worktree_root, artifact_root).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            action.resource_id == str(artifact.id)
            and action.outcome is RetentionOutcome.MARKED_MISSING
            for action in report.actions
        )
        async with factory() as session:
            row = await session.get(Artifact, artifact.id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.MISSING
        await engine.dispose()

    asyncio.run(scenario())
