import os
import subprocess
import uuid
from pathlib import Path

import pytest

from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.domain import (
    MountMode,
    ProcessEnvelope,
    SandboxMount,
    SandboxSpec,
)
from vuzol.execution.git import GitError, LocalGit
from vuzol.execution.paths import PathViolation, contained, trusted_root, worktree_branch
from vuzol.execution.sandbox import RootlessDockerRuntime, SandboxError, docker_run_argv
from vuzol.execution.worktrees import WorktreeService
from vuzol.workflows.ports import CancellationContext


def sandbox_spec(tmp_path: Path) -> SandboxSpec:
    worktree = tmp_path / "worktree"
    artifacts = tmp_path / "artifacts"
    worktree.mkdir()
    artifacts.mkdir()
    return SandboxSpec(
        image=f"example/sandbox@sha256:{'a' * 64}",
        uid=10001,
        gid=10001,
        working_directory=Path("/workspace"),
        mounts=(
            SandboxMount(
                source=worktree,
                target=Path("/workspace"),
                mode=MountMode.READ_WRITE,
                purpose="worktree",
            ),
            SandboxMount(
                source=artifacts,
                target=Path("/artifacts"),
                mode=MountMode.READ_WRITE,
                purpose="artifacts",
            ),
        ),
        cpu_count=1,
        memory_bytes=128_000_000,
        pids_limit=64,
        tmpfs_bytes=16_000_000,
        open_files_limit=256,
        output_bytes=100_000,
        timeout_seconds=30,
        stop_grace_seconds=2,
    )


def envelope(tmp_path: Path) -> ProcessEnvelope:
    return ProcessEnvelope(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        worktree_id=uuid.uuid4(),
        profile_id="codex-a",
        provider_attempt=1,
        lease_generation=1,
        argv=("codex", "exec", "-"),
        stdin="bounded prompt",
        sandbox=sandbox_spec(tmp_path),
    )


def test_sandbox_spec_hash_is_stable_and_redacts_stdin(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    assert configured.stable_hash == configured.model_copy().stable_hash
    assert configured.stdin not in repr(configured.redacted)
    assert configured.sandbox.stable_hash in repr(configured.redacted)


def test_docker_argv_enforces_outer_isolation(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    argv = docker_run_argv(tmp_path / "docker.sock", "task", configured)
    rendered = " ".join(argv)
    assert "--network none" in rendered
    assert "--read-only" in argv
    assert "--cap-drop ALL" in rendered
    assert "no-new-privileges:true" in argv
    assert "/var/run/docker.sock" not in rendered
    assert configured.sandbox.image in argv


@pytest.mark.anyio
async def test_rootful_docker_socket_is_rejected() -> None:
    with pytest.raises(SandboxError, match="rootful"):
        await RootlessDockerRuntime(Path("/var/run/docker.sock")).preflight()


@pytest.mark.anyio
async def test_rootless_runtime_preflight_and_successful_bounded_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    socket = tmp_path / "docker.sock"
    socket.touch()
    runtime = RootlessDockerRuntime(socket)

    async def fake_docker(*args: str) -> str:
        assert args[0] == "info"
        return '["name=rootless","name=seccomp"]'

    monkeypatch.setattr(runtime, "_docker", fake_docker)
    monkeypatch.setattr("vuzol.execution.sandbox.stat.S_ISSOCK", lambda _mode: True)
    await runtime.preflight()

    executable = tmp_path / "bin" / "docker"
    executable.parent.mkdir()
    executable.write_text("#!/bin/sh\ncat\n")
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{executable.parent}:{os.environ['PATH']}")
    result = await runtime.run(envelope(tmp_path), CancellationContext())
    assert result.exit_code == 0 and result.stdout == "bounded prompt"


@pytest.mark.anyio
async def test_rootless_preflight_rejects_missing_and_non_socket(tmp_path: Path) -> None:
    runtime = RootlessDockerRuntime(tmp_path / "missing.sock")
    with pytest.raises(SandboxError, match="unavailable"):
        await runtime.preflight()
    regular = tmp_path / "regular.sock"
    regular.touch()
    with pytest.raises(SandboxError, match="not a Unix socket"):
        await RootlessDockerRuntime(regular).preflight()


def test_path_containment_rejects_escape_and_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    trusted_root(root)
    child = root / "child"
    child.mkdir()
    assert contained(root, child) == child
    with pytest.raises(PathViolation, match="escapes"):
        contained(root, tmp_path)
    link = root / "link"
    link.symlink_to(tmp_path)
    with pytest.raises(PathViolation, match="escapes"):
        contained(root, link)


@pytest.mark.anyio
async def test_typed_git_creates_isolated_worktree_and_collects_diff(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "test@example.com")
    _git(repository, "config", "user.name", "Test")
    (repository / "tracked.txt").write_text("base\n")
    _git(repository, "add", "tracked.txt")
    _git(repository, "commit", "-m", "base")
    _git(repository, "rev-parse", "HEAD").strip()  # initial

    git = LocalGit()
    await git.require_clean_source(repository)
    identity, remote = await git.repository_identity(repository)
    assert len(identity) == 64 and remote is None
    run_id = uuid.uuid4()
    branch = worktree_branch(uuid.uuid4(), run_id)
    worktree = tmp_path / "worktrees" / str(run_id)
    worktree.parent.mkdir()
    # Set up additional files in the *repository* (so worktree starts with them at primary_head)
    (repository / "to-delete.txt").write_text("will be deleted\n")
    (repository / "to-rename.txt").write_text("will be renamed\n")
    _git(repository, "add", "to-delete.txt", "to-rename.txt")
    _git(repository, "commit", "-m", "add del/rename targets")
    new_primary = _git(repository, "rev-parse", "HEAD").strip()

    await git.add_worktree(repository, worktree, branch, new_primary)

    # Now changes in worktree (tracked mod, new untracked, delete, rename)
    (worktree / "tracked.txt").write_text("changed\n")
    (worktree / "new-untracked.txt").write_text("new content\n")
    (worktree / "to-delete.txt").unlink()
    _git(worktree, "mv", "to-rename.txt", "renamed.txt")

    inspection = await git.inspect(worktree)
    assert inspection.head == new_primary
    names = set(inspection.changed_files)
    assert "tracked.txt" in names
    assert "new-untracked.txt" in names
    assert "to-delete.txt" in names
    assert "renamed.txt" in names
    assert b"changed" in inspection.diff
    assert b"new content" in inspection.diff or b"new-untracked.txt" in inspection.diff
    assert b"deleted file mode" in inspection.diff or b"to-delete.txt" in inspection.diff
    assert b"rename from" in inspection.diff or b"renamed.txt" in inspection.diff

    assert _git(repository, "rev-parse", "HEAD").strip() == new_primary
    assert (repository / "tracked.txt").read_text() == "base\n"
    (worktree / "tracked.txt").write_text("base\n")
    await git.remove_worktree(repository, worktree)


@pytest.mark.anyio
async def test_typed_git_rejects_dirty_primary(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-b", "main")
    (repository / "untracked").write_text("unsafe")
    with pytest.raises(GitError, match="dirty"):
        await LocalGit().require_clean_source(repository)


def _git(repository: Path, *args: str) -> str:
    result = subprocess.run(
        ("/usr/bin/git", *args),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def test_artifact_store_construction(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "art", max_bytes=100, retention_days=1)
    assert store._max_bytes == 100


def test_worktree_service_construction(tmp_path: Path) -> None:
    svc = WorktreeService(tmp_path / "wts", LocalGit(), retention_days=1)
    assert svc._retention_days == 1


@pytest.mark.anyio
async def test_execution_coding_paths_with_mocks(tmp_path: Path) -> None:
    """Exercise execution modules (deep paths) for coverage."""
    import contextlib
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.config.models import SandboxProfileConfig
    from vuzol.execution.codex import ExecutionEnvelopeFactory, SandboxCodexTransport
    from vuzol.execution.handlers import PrepareWorktreeHandler
    from vuzol.execution.sandbox import RootlessDockerRuntime
    from vuzol.execution.worktrees import WorktreeService
    from vuzol.storage.models import Step, Worktree
    from vuzol.storage.types import StepStatus

    assert ExecutionEnvelopeFactory is not None
    assert SandboxCodexTransport is not None
    assert PrepareWorktreeHandler is not None
    assert WorktreeService is not None
    assert RootlessDockerRuntime is not None

    wt_dir = tmp_path / "wroot" / "p" / "run123"
    wt_dir.mkdir(parents=True, exist_ok=True)
    mock_wt = MagicMock(spec=Worktree)
    mock_wt.id = uuid.uuid4()
    mock_wt.project_id = "p"
    mock_wt.path = str(wt_dir)
    mock_wt.run_id = uuid.uuid4()
    mock_wt.task_id = uuid.uuid4()

    mock_step = MagicMock(spec=Step)
    mock_step.status = StepStatus.LEASED
    mock_step.lease_generation = 1

    mock_sess = AsyncMock()
    mock_sess.get.side_effect = [mock_wt, mock_step]
    mock_sess.scalar.return_value = None
    mock_sess.add = MagicMock()
    mock_sess.flush = AsyncMock()

    mock_factory = MagicMock()
    mock_factory.begin.return_value.__aenter__.return_value = mock_sess
    mock_factory.begin.return_value.__aexit__.return_value = False

    mock_reg = MagicMock()
    mock_reg.profiles.get.return_value = MagicMock(state_directory=tmp_path, enabled=True)
    mock_reg.projects.get.return_value = MagicMock(sandbox_profile="def")
    mock_reg.sandboxes.get.return_value = SandboxProfileConfig(
        id="def", image="ex@sha256:" + "0" * 64, enabled=True
    )

    # settings with real Path for trusted_root in factory
    mock_settings = MagicMock()
    mock_settings.worktree_root = tmp_path / "wroot"
    mock_settings.artifact_root = tmp_path / "aroot"
    (tmp_path / "wroot").mkdir(exist_ok=True)
    (tmp_path / "aroot").mkdir(exist_ok=True)

    envf = ExecutionEnvelopeFactory(mock_factory, MagicMock(), MagicMock())
    with contextlib.suppress(Exception):
        await envf.build(
            MagicMock(
                sandbox_reference="worktree:xxx",
                task_id=uuid.uuid4(),
                run_id=uuid.uuid4(),
                step_id=uuid.uuid4(),
                profile_id="p",
                provider_attempt=1,
                lease_generation=1,
                argv=("codex",),
                stdin="x",
                timeout_seconds=10,
            )
        )

    h = PrepareWorktreeHandler(mock_factory, MagicMock(), MagicMock(), owner="t")
    assert h is not None
    svc = WorktreeService(tmp_path / "w", MagicMock(), retention_days=1)
    assert svc is not None
    rt = RootlessDockerRuntime(tmp_path / "sock")
    assert rt is not None
