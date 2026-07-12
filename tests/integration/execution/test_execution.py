import asyncio
import subprocess
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from vuzol.config import Capability, ProjectConfig
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.git import LocalGit
from vuzol.execution.worktrees import WorktreeService
from vuzol.storage.models import Artifact, Worktree
from vuzol.storage.types import IdempotencyClass, RetryClass, RunStatus, StepStatus
from vuzol.storage.unit_of_work import UnitOfWork

from ..storage.helpers import storage


async def seed(factory: object) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    async with UnitOfWork(typed) as uow:
        task = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            project_id="project-a",
            original_text="change tracked file",
            task_type="coding",
        )
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        step = await uow.steps.create(
            run_id=run_id,
            ordinal=1,
            step_type="prepare_worktree",
            idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
            retry_class=RetryClass.POLICY,
            status=StepStatus.COMPLETED,
        )
    return task.id, run_id, step.id


def test_worktree_and_artifact_lifecycle_is_persisted(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        repository = tmp_path / "repositories" / "project-a"
        repository.mkdir(parents=True)
        _git(repository, "init", "-b", "main")
        _git(repository, "config", "user.email", "test@example.com")
        _git(repository, "config", "user.name", "Test")
        (repository / "tracked.txt").write_text("base\n")
        _git(repository, "add", "tracked.txt")
        _git(repository, "commit", "-m", "base")
        task_id, run_id, step_id = await seed(factory)
        project = ProjectConfig(
            id="project-a",
            display_name="Project A",
            repository_path=repository,
            default_branch="main",
            allowed_capabilities=frozenset(
                {Capability.REPOSITORY_READ, Capability.FILESYSTEM_WRITE, Capability.GIT}
            ),
            sandbox_profile="default",
        )
        service = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            reference = await service.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=project,
                owner="executor-a",
            )
        (reference.path / "tracked.txt").write_text("changed\n")
        art_store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000, retention_days=14)
        async with factory.begin() as session:
            retained = await service.retain(
                session, worktree_id=reference.id, artifacts=art_store, step_id=step_id
            )
            manifest = await art_store.persist(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                artifact_type="execution_manifest",
                content=b'{"safe":true}',
                media_type="application/json",
            )
        assert retained.id == reference.id
        assert (repository / "tracked.txt").read_text() == "base\n"
        assert retained is not None
        async with factory() as session:
            row = await session.scalar(select(Worktree).where(Worktree.id == reference.id))
            stored = await session.get(Artifact, manifest.id)
            assert row is not None and row.diff_hash is not None
            assert stored is not None and stored.content_hash == manifest.content_hash
            artifact_path = tmp_path / "artifacts" / stored.content_uri.removeprefix("artifact:")
            assert artifact_path.read_bytes() == b'{"safe":true}'
        await engine.dispose()

    asyncio.run(scenario())


def _git(repository: Path, *args: str) -> str:
    result = subprocess.run(
        ("/usr/bin/git", *args),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def test_docker_i_flag_passes_stdin_to_container() -> None:
    """Real (non-mocked) Docker integration smoke test.

    Proves that -i (interactive) in docker run allows stdin to reach the
    process inside the container. A mocked executable is not used.
    """
    input_data = b"stdin-reaches-container-test-12345\n"
    # Use a minimal image; alpine will be pulled if not present (test env supports it)
    proc = subprocess.run(
        ["docker", "run", "--rm", "-i", "alpine", "cat"],  # noqa: S607
        input=input_data,
        capture_output=True,
        timeout=60,
    )
    assert proc.returncode == 0, "docker run failed"
    assert input_data in proc.stdout, "stdin not received inside container"
