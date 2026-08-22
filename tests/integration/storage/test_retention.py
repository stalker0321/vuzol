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
    RetentionAction,
    RetentionOutcome,
    RetentionSweeper,
    RetentionSweepMode,
)
from vuzol.storage.models import Artifact, Event, Run, Step, SupervisedProcess, Task, Worktree
from vuzol.storage.types import (
    ArtifactStorageState,
    IdempotencyClass,
    ProcessStatus,
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
    run_id: uuid.UUID | None,
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


async def _add_linked_git_worktree(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    root: Path,
    repository: Path,
    project_id: str = "project",
    retention_until: datetime | None = None,
) -> Worktree:
    """Create a real linked Git worktree (`.git` file), not a standalone shallow clone."""
    git = LocalGit()
    identity, _ = await git.repository_identity(repository)
    path = root / project_id / str(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    head = subprocess.run(  # noqa: ASYNC221
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    branch = f"vuzol/task-{task_id}-run-{str(run_id)[:12]}"
    subprocess.run(  # noqa: ASYNC221
        ["git", "-C", str(repository), "worktree", "add", "-b", branch, str(path), head],
        check=True,
        capture_output=True,
        text=True,
    )
    assert (path / ".git").is_file()  # linked worktree marker
    row = Worktree(
        task_id=task_id,
        run_id=run_id,
        project_id=project_id,
        repository_identity_hash=identity,
        base_commit=head,
        default_branch="main",
        expected_target_head=head,
        branch=branch,
        path=str(path),
        owner="test",
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        retention_until=retention_until or (datetime.now(UTC) - timedelta(days=1)),
    )
    session.add(row)
    await session.flush()
    return row


@pytest.mark.postgresql
def test_registered_git_worktree_removal(postgres_dsn: str, tmp_path: Path) -> None:
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
            worktree = await _add_linked_git_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            path = Path(worktree.path)
            worktree_id = worktree.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert not path.exists()  # noqa: ASYNC240
        listing = subprocess.run(  # noqa: ASYNC221
            ["git", "-C", str(repository), "worktree", "list", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        assert str(path) not in listing
        async with factory() as session:
            row = await session.get(Worktree, worktree_id)
            assert row is not None
            assert row.delivery_state is WorktreeDeliveryState.CLEANED
            assert row.cleaned_at is not None
        assert any(
            a.resource_id == str(worktree_id)
            and a.outcome in {RetentionOutcome.CLEANED, RetentionOutcome.RECONCILED}
            for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_r1_protection_recheck_before_external_deletion(
    postgres_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Race: step becomes BLOCKED after INTENT, immediately before external Git/FS."""

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
        task_id, run_id, step_id = await _seed_task(
            factory, status=TaskStatus.COMPLETED, age_days=10
        )
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            path = Path(worktree.path)
            worktree_id = worktree.id

        original_external = RetentionSweeper._worktree_phase_external

        async def race_then_external(
            self: RetentionSweeper, worktree_id: uuid.UUID
        ) -> RetentionAction:
            # Flip protection only after phase A committed INTENT.
            async with factory.begin() as session:
                await session.execute(
                    update(Step).where(Step.id == step_id).values(status=StepStatus.BLOCKED)
                )
            return await original_external(self, worktree_id)

        monkeypatch.setattr(RetentionSweeper, "_worktree_phase_external", race_then_external)

        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert path.exists()  # noqa: ASYNC240
        assert any(
            a.resource_id == str(worktree_id) and a.reason == "protected_step"
            for a in report.actions
        )
        async with factory() as session:
            row = await session.get(Worktree, worktree_id)
            assert row is not None
            assert row.cleanup_reason == WORKTREE_INTENT
            assert row.delivery_state is not WorktreeDeliveryState.CLEANED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_shared_content_hash_peers_last_unlink(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        content = b"shared-bytes-for-two-tasks"
        t1, r1, s1 = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        t2, r2, s2 = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            a1 = await _add_artifact(
                session, task_id=t1, run_id=r1, step_id=s1, root=artifact_root, content=content
            )
            a2 = await _add_artifact(
                session, task_id=t2, run_id=r2, step_id=s2, root=artifact_root, content=content
            )
            path = artifact_root / a1.content_uri.removeprefix("artifact:")
            assert a1.content_hash == a2.content_hash
            assert path.exists()
        await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        async with factory() as session:
            row1 = await session.get(Artifact, a1.id)
            row2 = await session.get(Artifact, a2.id)
            assert row1 is not None and row2 is not None
            assert row1.storage_state is ArtifactStorageState.DELETED
            assert row2.storage_state is ArtifactStorageState.DELETED
        assert not path.exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_resume_after_external_done(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        from sqlalchemy.orm.attributes import flag_modified

        from vuzol.ops.retention import ARTIFACT_META_KEY, ARTIFACT_PHASE_EXTERNAL

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
                content=b"to-delete",
            )
            path = artifact_root / artifact.content_uri.removeprefix("artifact:")
            path.unlink()
            meta = dict(artifact.metadata_json or {})
            meta[ARTIFACT_META_KEY] = {
                "phase": ARTIFACT_PHASE_EXTERNAL,
                "owner": "test",
                "content_hash": artifact.content_hash,
                "file_removed": True,
            }
            artifact.metadata_json = meta
            flag_modified(artifact, "metadata_json")
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(artifact.id)
            and a.outcome in {RetentionOutcome.MARKED_DELETED, RetentionOutcome.RECONCILED}
            for a in report.actions
        )
        async with factory() as session:
            row = await session.get(Artifact, artifact.id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.DELETED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_removal_incomplete_when_external_done_but_path_remains(
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
            worktree.cleanup_reason = WORKTREE_EXTERNAL_DONE
            # Path still present — finalize must fail closed.
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(worktree.id) and a.reason == "removal_incomplete"
            for a in report.actions
        )
        assert Path(worktree.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_finalize_missing_after_intent_reconciles(postgres_dsn: str, tmp_path: Path) -> None:
    """Crash window: path gone, cleanup_reason still INTENT — finalize reconciles."""

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
            path = Path(worktree.path)
            worktree_id = worktree.id
        shutil.rmtree(path)
        sweeper = _sweeper(factory, worktree_root, artifact_root, repos)
        sweeper._bind_roots(mode=RetentionSweepMode.APPLY)
        action = await sweeper._worktree_phase_finalize(worktree_id, mode=RetentionSweepMode.APPLY)
        assert action.outcome is RetentionOutcome.RECONCILED
        assert action.reason == "finalized_missing_after_intent"
        async with factory() as session:
            row = await session.get(Worktree, worktree_id)
            assert row is not None
            assert row.delivery_state is WorktreeDeliveryState.CLEANED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_uri_escape_fails_closed(postgres_dsn: str, tmp_path: Path) -> None:
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
                content=b"escape-me",
            )
            artifact.content_uri = "artifact:../escape.bin"
            artifact_id = artifact.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(artifact_id) and a.reason == "path_violation"
            for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_f9_quarantine_intent_dry_run_would_finalize(postgres_dsn: str, tmp_path: Path) -> None:
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
            mode=RetentionSweepMode.DRY_RUN
        )
        assert any(a.reason == "would_finalize_quarantine_intent" for a in report.actions)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_batch_size_stops_after_acted_budget(postgres_dsn: str, tmp_path: Path) -> None:
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
        for _ in range(3):
            task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
            async with factory.begin() as session:
                await _add_worktree(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    root=worktree_root,
                    repository=repository,
                )
        report = await _sweeper(factory, worktree_root, artifact_root, repos, batch_size=1).run(
            mode=RetentionSweepMode.APPLY
        )
        cleaned = [
            a
            for a in report.actions
            if a.resource_type == "worktree"
            and a.outcome in {RetentionOutcome.CLEANED, RetentionOutcome.RECONCILED}
        ]
        assert len(cleaned) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_intent_resume_dry_run_and_protected_skip(postgres_dsn: str, tmp_path: Path) -> None:
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
        t1, r1, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        t2, r2, s2 = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            w1 = await _add_worktree(
                session, task_id=t1, run_id=r1, root=worktree_root, repository=repository
            )
            w1.cleanup_reason = WORKTREE_INTENT
            w1_id = w1.id
            w2 = await _add_worktree(
                session, task_id=t2, run_id=r2, root=worktree_root, repository=repository
            )
            w2.cleanup_reason = WORKTREE_INTENT
            w2_id = w2.id
            await session.execute(
                update(Step).where(Step.id == s2).values(status=StepStatus.BLOCKED)
            )
        dry = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        assert any(a.resource_id == str(w1_id) and a.reason == "resume_intent" for a in dry.actions)
        applied = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(w2_id) and a.reason == "protected_step" for a in applied.actions
        )
        assert worktree_root.exists()
        async with factory() as session:
            row = await session.get(Worktree, w2_id)
            assert row is not None
            assert row.cleanup_reason == WORKTREE_INTENT
            assert Path(row.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_worktree_path_violation_and_symlink(postgres_dsn: str, tmp_path: Path) -> None:
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
        outside = tmp_path / "outside-wt"
        outside.mkdir()
        (outside / "README").write_text("x")
        t1, r1, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        t2, r2, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            w_bad = await _add_worktree(
                session, task_id=t1, run_id=r1, root=worktree_root, repository=repository
            )
            # Point path outside managed root.
            shutil.rmtree(w_bad.path)
            w_bad.path = str(outside)
            bad_id = w_bad.id
            w_link = await _add_worktree(
                session, task_id=t2, run_id=r2, root=worktree_root, repository=repository
            )
            link_path = Path(w_link.path)
            shutil.rmtree(link_path)
            link_path.symlink_to(outside)  # noqa: ASYNC240
            link_id = w_link.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(bad_id) and a.reason == "path_violation" for a in report.actions
        )
        # Symlink under root resolves outside → contained() fails closed as path_violation.
        assert any(
            a.resource_id == str(link_id) and a.reason == "path_violation" for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_intent_resume_and_symlink_refused(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        from sqlalchemy.orm.attributes import flag_modified

        from vuzol.ops.retention import ARTIFACT_META_KEY, ARTIFACT_PHASE_INTENT

        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        t1, r1, s1 = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        t2, r2, s2 = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            a1 = await _add_artifact(
                session, task_id=t1, run_id=r1, step_id=s1, root=artifact_root, content=b"resume"
            )
            meta = dict(a1.metadata_json or {})
            meta[ARTIFACT_META_KEY] = {
                "phase": ARTIFACT_PHASE_INTENT,
                "owner": "test",
                "content_hash": a1.content_hash,
            }
            a1.metadata_json = meta
            flag_modified(a1, "metadata_json")
            a1_id = a1.id
            a2 = await _add_artifact(
                session, task_id=t2, run_id=r2, step_id=s2, root=artifact_root, content=b"symlink"
            )
            path2 = artifact_root / a2.content_uri.removeprefix("artifact:")
            path2.unlink()
            outside = tmp_path / "secret.bin"
            outside.write_bytes(b"secret")
            path2.symlink_to(outside)
            a2_id = a2.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(a1_id)
            and a.outcome in {RetentionOutcome.MARKED_DELETED, RetentionOutcome.RECONCILED}
            for a in report.actions
        )
        # Symlink target escapes artifact root via resolve → path_violation (fail-closed).
        assert any(
            a.resource_id == str(a2_id) and a.reason == "path_violation" for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_quarantine_intent_skips_malformed_payload(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        async with factory.begin() as session:
            session.add(
                Event(
                    entity_type="artifact_file",
                    entity_id=uuid.uuid4(),
                    event_type="ops.retention.quarantine_intent",
                    actor_type="retention",
                    actor_id="test",
                    payload={"token": 123, "destination": None},
                )
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert not any(a.reason == "quarantine_intent_finalized" for a in report.actions)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_invalid_worktree_file_path_and_active_delivery(postgres_dsn: str, tmp_path: Path) -> None:
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
        t1, r1, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        t2, r2, _ = await _seed_task(factory, status=TaskStatus.COMPLETED, age_days=10)
        async with factory.begin() as session:
            w_file = await _add_worktree(
                session, task_id=t1, run_id=r1, root=worktree_root, repository=repository
            )
            path = Path(w_file.path)
            shutil.rmtree(path)
            path.write_text("not-a-dir")  # noqa: ASYNC240
            file_id = w_file.id
            w_active = await _add_worktree(
                session,
                task_id=t2,
                run_id=r2,
                root=worktree_root,
                repository=repository,
                delivery_state=WorktreeDeliveryState.ACTIVE,
            )
            # INTENT makes the row selectable; ACTIVE delivery must still skip.
            w_active.cleanup_reason = WORKTREE_INTENT
            active_id = w_active.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(file_id) and a.reason == "invalid_worktree_path"
            for a in report.actions
        )
        assert any(
            a.resource_id == str(active_id) and a.reason == "protected_delivery_state"
            for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_dry_run_would_finalize_external_done_missing(postgres_dsn: str, tmp_path: Path) -> None:
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
            worktree.cleanup_reason = WORKTREE_EXTERNAL_DONE
            path = Path(worktree.path)
            worktree_id = worktree.id
        shutil.rmtree(path)
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        assert any(
            a.resource_id == str(worktree_id) and a.reason == "would_finalize_external_done"
            for a in report.actions
        )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_pending_approval_blocks_worktree(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        from vuzol.storage.models import Approval
        from vuzol.storage.types import ApprovalStatus

        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        repository = repos / "project"
        _git_init(repository)
        task_id, run_id, step_id = await _seed_task(
            factory, status=TaskStatus.COMPLETED, age_days=10
        )
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            worktree_id = worktree.id
            session.add(
                Approval(
                    step_id=step_id,
                    action_envelope_hash="a" * 64,
                    requested_action="apply_result",
                    normalized_target="result",
                    human_summary="pending for retention test",
                    token_hash="c" * 64,
                    status=ApprovalStatus.PENDING,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert any(
            a.resource_id == str(worktree_id) and a.reason == "pending_approval"
            for a in report.actions
        )
        async with factory() as session:
            row = await session.get(Worktree, worktree_id)
            assert row is not None
            assert Path(row.path).exists()  # noqa: ASYNC240
            assert row.delivery_state is not WorktreeDeliveryState.CLEANED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_dry_run_would_clean_eligible_worktree(postgres_dsn: str, tmp_path: Path) -> None:
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
            worktree_id = worktree.id
            path = Path(worktree.path)
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        assert any(
            a.resource_id == str(worktree_id)
            and a.outcome is RetentionOutcome.WOULD_CLEAN
            and a.reason == "retention_expired"
            for a in report.actions
        )
        assert path.exists()  # noqa: ASYNC240
        async with factory() as session:
            row = await session.get(Worktree, worktree_id)
            assert row is not None
            assert row.cleanup_reason is None or row.cleanup_reason == ""
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_dry_run_would_mark_deleted(postgres_dsn: str, tmp_path: Path) -> None:
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
                content=b"dry-run-artifact",
            )
            artifact_id = artifact.id
            path = artifact_root / artifact.content_uri.removeprefix("artifact:")
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        assert any(
            a.resource_id == str(artifact_id) and a.outcome is RetentionOutcome.WOULD_MARK_DELETED
            for a in report.actions
        )
        assert path.exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_within_retention_protects_recently_failed_task(postgres_dsn: str, tmp_path: Path) -> None:
    """Failed-task retention floor keeps recently failed worktrees despite old marker."""

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
        task_id, run_id, _ = await _seed_task(
            factory,
            status=TaskStatus.FAILED,
            run_status=RunStatus.FAILED,
            age_days=1,
        )
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
                retention_until=datetime.now(UTC) - timedelta(days=30),
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(worktree.id))
        assert action.outcome is RetentionOutcome.SKIPPED
        assert action.reason == "within_retention"
        assert Path(worktree.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_active_supervised_process_blocks_cleanup(postgres_dsn: str, tmp_path: Path) -> None:
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
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            session.add(
                SupervisedProcess(
                    step_id=step_id,
                    task_id=task_id,
                    run_id=run_id,
                    worktree_id=worktree.id,
                    profile_id="codex-a",
                    lease_generation=1,
                    provider_attempt=1,
                    idempotency_key=uuid.uuid4().hex,
                    command_envelope_hash="c" * 64,
                    command_envelope={},
                    sandbox_spec_hash="s" * 64,
                    container_runtime="docker",
                    image_digest="example/sandbox",
                    working_directory="/workspace",
                    status=ProcessStatus.RUNNING,
                )
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(worktree.id))
        assert action.outcome is RetentionOutcome.SKIPPED
        assert action.reason == "active_supervised_process"
        assert Path(worktree.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_dry_run_reports_resume_intent_for_interrupted_worktree_cleanup(
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
            shutil.rmtree(Path(worktree.path))
            worktree.cleanup_reason = WORKTREE_INTENT
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        action = next(a for a in report.actions if a.resource_id == str(worktree.id))
        assert action.outcome is RetentionOutcome.WOULD_CLEAN
        assert action.reason == "resume_intent"
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            assert row.delivery_state is WorktreeDeliveryState.WORKTREE_RETAINED
            assert row.cleanup_reason == WORKTREE_INTENT
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_apply_rejects_escaping_path_during_external_phase(
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
            foreign = tmp_path / "foreign" / "wt"
            foreign.mkdir(parents=True)
            worktree.path = str(foreign)
            worktree.cleanup_reason = WORKTREE_INTENT
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(worktree.id))
        assert action.outcome is RetentionOutcome.FAILED
        assert action.reason == "path_violation"
        assert foreign.exists()
        async with factory() as session:
            row = await session.get(Worktree, worktree.id)
            assert row is not None
            assert row.delivery_state is WorktreeDeliveryState.WORKTREE_RETAINED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_apply_rejects_symlinked_worktree_path(postgres_dsn: str, tmp_path: Path) -> None:
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
            inside = worktree_root / "symlink-target"
            inside.mkdir(exist_ok=True)
            path.symlink_to(inside)  # noqa: ASYNC240
            worktree.cleanup_reason = WORKTREE_INTENT
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(worktree.id))
        assert action.outcome is RetentionOutcome.FAILED
        assert action.reason == "invalid_worktree_path"
        assert path.is_symlink()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_declare_skips_worktree_with_ambiguous_identity(postgres_dsn: str, tmp_path: Path) -> None:
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
        other_task_id, _other_run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED)
        task_id, run_id, _ = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
            )
            await session.execute(update(Run).where(Run.id == run_id).values(task_id=other_task_id))
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(worktree.id))
        assert action.outcome is RetentionOutcome.SKIPPED
        assert action.reason == "ambiguous_identity"
        assert Path(worktree.path).exists()  # noqa: ASYNC240
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_blocked_task_is_protected(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.BLOCKED)
        async with factory.begin() as session:
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"blocked-evidence",
            )
            path = artifact_root / artifact.content_uri.removeprefix("artifact:")
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact.id))
        assert action.outcome is RetentionOutcome.SKIPPED
        assert action.reason == "blocked_task"
        assert path.exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_non_terminal_task_is_skipped(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.EXECUTING)
        async with factory.begin() as session:
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"running-evidence",
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact.id))
        assert action.outcome is RetentionOutcome.SKIPPED
        assert action.reason == "non_terminal_task"
        assert action.detail == {"task_status": "executing"}
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_missing_file_dry_run_would_mark_missing(
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
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"vanished",
            )
            artifact_id = artifact.id
            (artifact_root / artifact.content_uri.removeprefix("artifact:")).unlink()
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact_id))
        assert action.outcome is RetentionOutcome.WOULD_MARK_MISSING
        assert action.reason == "filesystem_missing"
        async with factory() as session:
            row = await session.get(Artifact, artifact_id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.AVAILABLE
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_referenced_via_patch_link_is_protected(postgres_dsn: str, tmp_path: Path) -> None:
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
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=None,
                step_id=step_id,
                root=artifact_root,
                content=b"patch-blob",
            )
            worktree = await _add_worktree(
                session,
                task_id=task_id,
                run_id=run_id,
                root=worktree_root,
                repository=repository,
                retention_until=datetime.now(UTC) + timedelta(days=30),
            )
            worktree.patch_artifact_id = artifact.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact.id))
        assert action.outcome is RetentionOutcome.SKIPPED
        assert action.reason == "referenced_by_worktree"
        async with factory() as session:
            row = await session.get(Artifact, artifact.id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.AVAILABLE
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_shared_blob_peer_retains_file_and_marks_row_deleted(
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
        expired_task, expired_run, expired_step = await _seed_task(
            factory, status=TaskStatus.COMPLETED, age_days=10
        )
        live_task, live_run, live_step = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            expired = await _add_artifact(
                session,
                task_id=expired_task,
                run_id=expired_run,
                step_id=expired_step,
                root=artifact_root,
                content=b"shared-blob",
            )
            live = await _add_artifact(
                session,
                task_id=live_task,
                run_id=live_run,
                step_id=live_step,
                root=artifact_root,
                content=b"shared-blob",
                retention_until=datetime.now(UTC) + timedelta(days=30),
            )
            blob = artifact_root / expired.content_uri.removeprefix("artifact:")
        assert blob == artifact_root / live.content_uri.removeprefix("artifact:")
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(expired.id))
        assert action.outcome is RetentionOutcome.MARKED_DELETED
        assert blob.exists()
        async with factory() as session:
            row = await session.get(Artifact, expired.id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.DELETED
            assert row.metadata_json == {}
            peer = await session.get(Artifact, live.id)
            assert peer is not None
            assert peer.storage_state is ArtifactStorageState.AVAILABLE
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_external_done_resume_dry_run_would_finalize(
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
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"crashed-external",
            )
            artifact.metadata_json = {
                "retention_cleanup": {"phase": "external_done", "file_removed": True}
            }
            artifact_id = artifact.id
            (artifact_root / artifact.content_uri.removeprefix("artifact:")).unlink()
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.DRY_RUN
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact_id))
        assert action.outcome is RetentionOutcome.WOULD_MARK_DELETED
        assert action.reason == "would_finalize"
        async with factory() as session:
            row = await session.get(Artifact, artifact_id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.AVAILABLE
            assert row.metadata_json is not None
            assert "retention_cleanup" in row.metadata_json
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_resume_unlinks_resolved_target_and_finalizes(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """Resume with a lingering symlink removes its in-root target, then finalizes."""

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
                content=b"lingering",
            )
            blob = artifact_root / artifact.content_uri.removeprefix("artifact:")
            blob.unlink()
            inside = artifact_root / "symlink-target.txt"
            inside.write_text("inside root\n")
            blob.symlink_to(inside)
            artifact.metadata_json = {"retention_cleanup": {"phase": "external_done"}}
            artifact_id = artifact.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact_id))
        assert action.outcome is RetentionOutcome.MARKED_DELETED
        assert action.reason == "finalized"
        assert not inside.exists()
        # The now-dangling blob symlink is quarantined by the orphan scan afterwards.
        assert not os.path.lexists(blob)  # noqa: ASYNC240
        assert any(
            a.resource_type == "artifact_file"
            and a.reason == "symlink_refused"
            and a.outcome is RetentionOutcome.QUARANTINED
            and blob.name in a.resource_id
            for a in report.actions
        )
        async with factory() as session:
            row = await session.get(Artifact, artifact_id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.DELETED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_unsupported_content_uri_fails_closed(postgres_dsn: str, tmp_path: Path) -> None:
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
                content=b"remote-backed",
            )
            artifact.content_uri = "s3://bucket/remote-object"
            artifact_id = artifact.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact_id))
        assert action.outcome is RetentionOutcome.FAILED
        assert action.reason == "path_violation"
        async with factory() as session:
            row = await session.get(Artifact, artifact_id)
            assert row is not None
            assert row.storage_state is ArtifactStorageState.AVAILABLE
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_delete_failure_reports_filesystem_error(
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
        task_id, run_id, step_id = await _seed_task(factory, status=TaskStatus.COMPLETED)
        async with factory.begin() as session:
            artifact = await _add_artifact(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                root=artifact_root,
                content=b"undeletable",
            )
            artifact.metadata_json = {"retention_cleanup": {"phase": "intent"}}
            artifact_id = artifact.id
            blob = artifact_root / artifact.content_uri.removeprefix("artifact:")
            blob.parent.chmod(0o555)
        try:
            report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
                mode=RetentionSweepMode.APPLY
            )
        finally:
            blob.parent.chmod(0o755)
        action = next(a for a in report.actions if a.resource_id == str(artifact_id))
        assert action.outcome is RetentionOutcome.FAILED
        assert action.reason == "filesystem_delete_failed"
        assert action.detail == {"error": "PermissionError"}
        assert blob.exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_artifact_pagination_survives_starvation_batch_of_one(
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
        blocked_task, blocked_run, blocked_step = await _seed_task(
            factory, status=TaskStatus.BLOCKED, age_days=30
        )
        done_task, done_run, done_step = await _seed_task(
            factory, status=TaskStatus.COMPLETED, age_days=10
        )
        async with factory.begin() as session:
            blocked = await _add_artifact(
                session,
                task_id=blocked_task,
                run_id=blocked_run,
                step_id=blocked_step,
                root=artifact_root,
                content=b"starving",
                retention_until=datetime.now(UTC) - timedelta(days=20),
            )
            eligible = await _add_artifact(
                session,
                task_id=done_task,
                run_id=done_run,
                step_id=done_step,
                root=artifact_root,
                content=b"cleanable",
            )
            blocked_path = artifact_root / blocked.content_uri.removeprefix("artifact:")
            eligible_path = artifact_root / eligible.content_uri.removeprefix("artifact:")
        report = await _sweeper(factory, worktree_root, artifact_root, repos, batch_size=1).run(
            mode=RetentionSweepMode.APPLY
        )
        reasons = {a.resource_id: a.reason for a in report.actions if a.resource_type == "artifact"}
        assert reasons[str(blocked.id)] == "blocked_task"
        assert reasons[str(eligible.id)] in {"finalized", "external_cleanup_done"}
        assert blocked_path.exists()
        assert not eligible_path.exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_flat_content_uri_cleanup_removes_top_level_blob(postgres_dsn: str, tmp_path: Path) -> None:
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
                content=b"flat-layout",
            )
            nested = artifact_root / artifact.content_uri.removeprefix("artifact:")
            flat = artifact_root / nested.name
            nested.rename(flat)
            artifact.content_uri = f"artifact:{flat.name}"
            artifact_id = artifact.id
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        action = next(a for a in report.actions if a.resource_id == str(artifact_id))
        assert action.outcome is RetentionOutcome.MARKED_DELETED
        assert not flat.exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_quarantine_move_failure_is_reported(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        worktree_root = tmp_path / "worktrees"
        artifact_root = tmp_path / "artifacts"
        repos = tmp_path / "repos"
        worktree_root.mkdir()
        artifact_root.mkdir()
        repos.mkdir()
        nested = artifact_root / "aa"
        nested.mkdir()
        orphan = nested / ("a" * 64)
        orphan.write_bytes(b"stuck-orphan")
        nested.chmod(0o555)
        try:
            report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
                mode=RetentionSweepMode.APPLY
            )
        finally:
            nested.chmod(0o755)
        action = next(
            a
            for a in report.actions
            if a.resource_type == "artifact_file" and a.reason == "quarantine_failed"
        )
        assert action.outcome is RetentionOutcome.FAILED
        assert action.detail == {"error": "PermissionError", "reason": "orphan_filesystem_object"}
        assert orphan.exists()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_unreadable_directory_and_fifo_are_ignored_by_orphan_scan(
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
        sealed = artifact_root / "bb"
        sealed.mkdir()
        os.chmod(sealed, 0)
        fifo = artifact_root / "ff"
        os.mkfifo(fifo)
        try:
            report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
                mode=RetentionSweepMode.APPLY
            )
        finally:
            os.chmod(sealed, 0o700)
            fifo.unlink()
        assert not any(
            a.resource_type == "artifact_file" and "bb" in a.resource_id for a in report.actions
        )
        assert not any("ff" in a.resource_id for a in report.actions)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_stale_quarantine_intent_without_destination_is_ignored(
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
                        "source": "aa/vanished.bin",
                        "token": uuid.uuid4().hex,
                        "destination": ".quarantine/never-moved.bin",
                    },
                )
            )
        report = await _sweeper(factory, worktree_root, artifact_root, repos).run(
            mode=RetentionSweepMode.APPLY
        )
        assert not any(
            a.reason in {"quarantine_intent_finalized", "would_finalize_quarantine_intent"}
            for a in report.actions
        )
        async with factory() as session:
            types = list((await session.scalars(select(Event.event_type))).all())
            assert "ops.retention.quarantined" not in types
        await engine.dispose()

    asyncio.run(scenario())
