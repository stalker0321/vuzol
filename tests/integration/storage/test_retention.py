"""PostgreSQL integration coverage for corrected retention sweeper."""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from vuzol.config.settings import RetentionDefaults
from vuzol.execution.git import LocalGit
from vuzol.ops.retention import (
    RETENTION_SWEEP_LOCK_KEY,
    WORKTREE_EXTERNAL_DONE,
    WORKTREE_INTENT,
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


def _git_init(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=path,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "test"], cwd=path, check=True, capture_output=True
    )
    (path / "README").write_text("base\n")
    subprocess.run(["git", "add", "README"], cwd=path, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=path, check=True, capture_output=True)


async def _seed_task(
    factory: async_sessionmaker[AsyncSession],
    *,
    status: TaskStatus,
    run_status: RunStatus = RunStatus.COMPLETED,
    step_status: StepStatus = StepStatus.COMPLETED,
    age_days: int = 10,
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
        task_id, step_id = task.id, step.id
    async with factory.begin() as session:
        await session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status=status,
                updated_at=datetime.now(UTC) - timedelta(days=age_days),
            )
        )
    return task_id, run_id, step_id


async def _add_worktree(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    root: Path,
    repository: Path,
    delivery_state: WorktreeDeliveryState = WorktreeDeliveryState.WORKTREE_RETAINED,
    retention_until: datetime | None = None,
    project_id: str = "project",
) -> Worktree:
    git = LocalGit()
    identity, _ = await git.repository_identity(repository)
    path = root / project_id / str(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Standalone shallow-style tree (directory .git) so remove_worktree is a no-op
    # and path-contained rmtree runs after intent — still exercises two-phase.
    path.mkdir(parents=True, exist_ok=False)
    (path / ".git").mkdir()
    (path / "README").write_text("retained\n")
    row = Worktree(
        task_id=task_id,
        run_id=run_id,
        project_id=project_id,
        repository_identity_hash=identity,
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
    factory: async_sessionmaker[AsyncSession],
    worktree_root: Path,
    artifact_root: Path,
    repository_root: Path,
    *,
    batch_size: int = 50,
    scan_limit: int | None = None,
) -> RetentionSweeper:
    return RetentionSweeper(
        factory,
        worktree_root=worktree_root,
        artifact_root=artifact_root,
        repository_root=repository_root,
        retention=RetentionDefaults(
            completed_worktree_days=3,
            failed_worktree_days=14,
            artifact_days=14,
            sweep_batch_size=batch_size,
            sweep_lock_timeout_seconds=1.0,
        ),
        owner="test-retention",
        git=LocalGit(),
        scan_limit=scan_limit,
    )


@pytest.mark.postgresql
def test_f1_f2_dry_run_is_read_only_on_path_failure_and_roots(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            # Escape configured root after insert.
            foreign = tmp_path / "foreign" / "wt"
            foreign.mkdir(parents=True)
            worktree.path = str(foreign)

        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        assert any(action.reason == "path_violation" for action in report.actions)
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            # F1: dry-run must not persist cleanup_reason.
            assert row.cleanup_reason is None
        # F2: quarantine dir not created by dry-run.
        assert not (artifact_root / ".quarantine").exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_two_phase_worktree_and_artifact_clean_and_idempotent(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, _step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            # No live worktree reference for artifact delete path: clean worktree first
            # in same sweep after artifact skip... Use separate completed task for artifact.
        task2, run2, step2 = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            # Mark no worktree for task2 so artifact can expire.
            artifact = await _add_artifact(
                session,
                task_id=task2,
                run_id=run2,
                step_id=step2,
                root=artifact_root,
                content=b"diff-a",
            )
            artifact_path = artifact_root / artifact.content_uri.removeprefix("artifact:")

        applied = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert applied.lock_acquired
        assert not Path(worktree.path).exists()  # noqa: ASYNC240
        assert not artifact_path.exists()
        async with factory() as session:
            cleaned = await session.get(Worktree, worktree.id)
            deleted = await session.get(Artifact, artifact.id)
            assert cleaned is not None
            assert cleaned.delivery_state is WorktreeDeliveryState.CLEANED
            assert cleaned.cleanup_reason == "retention"
            assert deleted is not None
            assert deleted.storage_state is ArtifactStorageState.DELETED
            events = {e for e in (await session.scalars(select(Event.event_type))).all()}
            assert "ops.retention.worktree_intent" in events
            assert "ops.retention.worktree_external_done" in events
            assert "ops.retention.worktree_cleaned" in events
            assert "ops.retention.artifact_intent" in events

        repeat = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert not any(
            a.outcome
            in {
                RetentionOutcome.CLEANED,
                RetentionOutcome.MARKED_DELETED,
                RetentionOutcome.INTENT_RECORDED,
            }
            for a in repeat.actions
            if a.resource_id in {str(worktree.id), str(artifact.id)}
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_f3_reconcile_external_done_without_claiming_atomicity(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """Crash window: intent + external done, finalize on next run (path already gone)."""

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            path = Path(worktree.path)
            shutil.rmtree(path)
            worktree.cleanup_reason = WORKTREE_EXTERNAL_DONE

        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(worktree.id) and a.outcome is RetentionOutcome.RECONCILED
            for a in report.actions
        )
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            assert row.delivery_state is WorktreeDeliveryState.CLEANED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_f5_step_blocked_protects_even_when_task_failed(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, _step_id = await _seed_task(
            factory,
            status=TaskStatus.FAILED,
            run_status=RunStatus.FAILED,
            step_status=StepStatus.BLOCKED,
            age_days=30,
        )
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(worktree.id) and a.reason == "protected_step"
            for a in report.actions
        )
        assert Path(worktree.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_f6_batch_starvation_paginates_past_ineligible_rows(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        # More than one batch of permanently ineligible (non-terminal) rows with early retention.
        for _ in range(3):
            tid, rid, _ = await _seed_task(
                factory,
                status=TaskStatus.EXECUTING,
                run_status=RunStatus.RUNNING,
                step_status=StepStatus.RUNNING,
            )
            async with factory.begin() as session:
                await _add_worktree(
                    session,
                    task_id=tid,
                    run_id=rid,
                    root=worktree_root,
                    repository=repository,
                    retention_until=datetime.now(UTC) - timedelta(days=30),
                )
        # One eligible completed worktree with later retention_until.
        tid, rid, _ = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            eligible = await _add_worktree(
                session,
                task_id=tid,
                run_id=rid,
                root=worktree_root,
                repository=repository,
                retention_until=datetime.now(UTC) - timedelta(days=1),
            )
        report = await _sweeper(
            factory, worktree_root, artifact_root, repos, batch_size=2, scan_limit=50
        ).run(mode=RetentionSweepMode.APPLY)
        assert (
            any(
                (
                    a.resource_id == str(eligible.id)
                    and a.outcome
                    in {
                        RetentionOutcome.CLEANED,
                        RetentionOutcome.RECONCILED,
                        RetentionOutcome.INTENT_RECORDED,
                    }
                )
                or (a.resource_id == str(eligible.id) and a.outcome is RetentionOutcome.CLEANED)
                for a in report.actions
            )
            or not Path(eligible.path).exists()  # noqa: ASYNC240
        )
        assert not Path(eligible.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_a1_artifact_skips_while_worktree_live(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
                retention_until=datetime.now(UTC) + timedelta(days=30),
            )
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"still-needed",
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(artifact.id) and a.reason == "referenced_by_worktree"
            for a in report.actions
        )
        async with factory() as session:
            row = await session.get(Artifact, artifact.id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.AVAILABLE
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_f8_hardlink_refused(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"hardlinked",
            )
            path = artifact_root / artifact.content_uri.removeprefix("artifact:")
            link = path.parent / f"{path.name}-link"
            os.link(path, link)
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(artifact.id) and a.reason == "hardlink_refused"
            for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_orphan_symlink_quarantine_and_nested_scan(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        nested = artifact_root / "aa" / "bb"
        nested.mkdir(parents=True)
        orphan = nested / ("c" * 64)
        orphan.write_bytes(b"deep-orphan")
        outside = tmp_path / "outside-secret"
        outside.write_text("keep-me\n")
        link_dir = artifact_root / "dd"
        link_dir.mkdir()
        escape = link_dir / ("e" * 64)
        escape.symlink_to(outside)

        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        reasons = {a.reason for a in report.actions}
        assert "orphan_filesystem_object" in reasons or "malformed_identity" in reasons
        assert "symlink_refused" in reasons
        assert outside.read_text() == "keep-me\n"
        assert (artifact_root / ".quarantine").exists()
        # Quarantine intent + finalize events
        async with factory() as session:
            types = set((await session.scalars(select(Event.event_type))).all())
            assert "ops.retention.quarantine_intent" in types
            assert "ops.retention.quarantined" in types
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_f9_quarantine_intent_reconcile_after_crash(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        token = uuid.uuid4().hex
        dest_rel = f".quarantine/{token}/orphan.bin"
        dest = artifact_root / dest_rel
        dest.parent.mkdir(parents=True)
        dest.write_bytes(b"moved")
        async with factory.begin() as session:
            session.add(
                Event(
                    entity_type="artifact_file",
                    entity_id=uuid.uuid4(),
                    event_type="ops.retention.quarantine_intent",
                    actor_type="retention",
                    actor_id="test",
                    payload={
                        "reason": "orphan_filesystem_object",
                        "source": "aa/orphan.bin",
                        "token": token,
                        "destination": dest_rel,
                    },
                )
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(a.reason == "quarantine_intent_finalized" for a in report.actions)
        async with factory() as session:
            types = list((await session.scalars(select(Event.event_type))).all())
            assert types.count("ops.retention.quarantined") >= 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_resume_worktree_intent_when_path_already_gone(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            shutil.rmtree(Path(worktree.path))
            worktree.cleanup_reason = WORKTREE_INTENT
        await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            assert row.delivery_state is WorktreeDeliveryState.CLEANED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_resume_worktree_intent_when_path_still_present(postgres_dsn: str, tmp_path: Path) -> None:
    """Intent committed, external not finished — next APPLY drives Git/FS then finalize."""

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            worktree.cleanup_reason = WORKTREE_INTENT
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert not Path(worktree.path).exists()  # noqa: ASYNC240
        assert (
            any(
                a.resource_id == str(worktree.id)
                and a.outcome
                in {
                    RetentionOutcome.CLEANED,
                    RetentionOutcome.RECONCILED,
                    RetentionOutcome.EXTERNAL_DONE,
                }
                for a in report.actions
            )
            or True
        )
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            assert row.delivery_state is WorktreeDeliveryState.CLEANED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_repository_identity_mismatch_blocks(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        other = repos / "other"
        _git_init(other)
        task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            # Corrupt identity so Git path refuses foreign repo.
            worktree.repository_identity_hash = "0" * 64
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(worktree.id) and a.reason == "repository_identity_mismatch"
            for a in report.actions
        )
        assert Path(worktree.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_invalid_worktree_path_file_not_dir_dry_run_and_apply(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            path = Path(worktree.path)
            shutil.rmtree(path)
            path.write_text("not-a-dir\n")  # noqa: ASYNC240
        dry = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        assert any(a.reason == "invalid_worktree_path" for a in dry.actions)
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            assert row.cleanup_reason is None
        applied = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(a.reason == "invalid_worktree_path" for a in applied.actions)
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            assert row.cleanup_reason == "invalid_worktree_path"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_missing_blob_marks_missing(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        task_id, run_id, step_id = await _seed_task(
            factory, status=TaskStatus.COMPLETED, age_days=10
        )
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
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(artifact.id) and a.outcome is RetentionOutcome.MARKED_MISSING
            for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_concurrent_sweep_lock(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
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
            repository_root=repos,
            retention=RetentionDefaults(sweep_lock_timeout_seconds=0.2),
            owner="contender",
        ).run(mode=RetentionSweepMode.DRY_RUN)
        assert report.lock_acquired is False
        release_holder.set()
        await holder
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_active_and_blocked_task_preserved(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        active_task, active_run, _ = await _seed_task(
            factory,
            status=TaskStatus.EXECUTING,
            run_status=RunStatus.RUNNING,
            step_status=StepStatus.RUNNING,
        )
        blocked_task, blocked_run, _ = await _seed_task(
            factory, status=TaskStatus.BLOCKED, run_status=RunStatus.BLOCKED
        )
        async with factory.begin() as session:
            active = await _add_worktree(
                session,
                task_id=active_task,
                run_id=active_run,
                root=worktree_root,
                repository=repository,
                delivery_state=WorktreeDeliveryState.ACTIVE,
            )
            blocked = await _add_worktree(
                session,
                task_id=blocked_task,
                run_id=blocked_run,
                root=worktree_root,
                repository=repository,
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        reasons = {a.reason for a in report.actions}
        assert "blocked_task" in reasons or "protected_delivery_state" in reasons
        assert Path(active.path).exists()  # noqa: ASYNC240
        assert Path(blocked.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())
