from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config import StaticDeploymentConfig
from vuzol.execution.static_build import StaticBuildHandler
from vuzol.storage.types import StepStatus
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext


def _handler(tmp_path: Path, deployment: StaticDeploymentConfig | None):
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / "index.html").write_text("<h1>Demo</h1>")
    project = SimpleNamespace(
        static_deployment=deployment,
        sandbox_profile="sandbox",
    )
    registries = SimpleNamespace(
        projects=SimpleNamespace(get=lambda _project_id: project),
        sandboxes=SimpleNamespace(get=lambda _profile: SimpleNamespace(uid=1000, gid=1000)),
    )
    git_facts = SimpleNamespace(head="a", diff_hash="b", changed_files=("index.html",))
    git = SimpleNamespace(inspect=AsyncMock(return_value=git_facts))
    evidence = SimpleNamespace(exit_code=0, model_dump=lambda **_kwargs: {"exit_code": 0})
    gates = SimpleNamespace(run=AsyncMock(return_value=(SimpleNamespace(evidence=evidence),)))
    lease = SimpleNamespace(revoke=AsyncMock())
    access = SimpleNamespace(grant=AsyncMock(return_value=lease))
    handler = StaticBuildHandler(
        MagicMock(), registries, git, gates, access, worktree_root=tmp_path
    )
    task = SimpleNamespace(project_id="demo")
    tree = SimpleNamespace(
        id="tree", path=str(worktree), base_commit="base", result_commit="result"
    )
    step = SimpleNamespace(timeout_seconds=60)
    handler._load = AsyncMock(return_value=(task, tree, step, worktree))
    request = SimpleNamespace(
        task_id="task",
        run_id="run",
        step_id="step",
        lease=SimpleNamespace(generation=1),
    )
    return handler, request, gates, lease


@pytest.mark.anyio
async def test_static_build_measures_artifact_and_binds_commit(tmp_path: Path) -> None:
    deployment = StaticDeploymentConfig(url_path="demo", build_command="make build")
    handler, request, gates, lease = _handler(tmp_path, deployment)

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "built"
    assert outcome.result["source_commit"] == "result"
    assert len(outcome.result["artifact_hash"]) == 64
    gates.run.assert_awaited_once()
    lease.revoke.assert_awaited_once()


@pytest.mark.anyio
async def test_static_build_skips_unconfigured_project(tmp_path: Path) -> None:
    handler, request, gates, _lease = _handler(tmp_path, None)

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.result == {"status": "skipped", "reason": "not_configured"}
    gates.run.assert_not_awaited()


@pytest.mark.anyio
async def test_static_build_requires_trusted_command(tmp_path: Path) -> None:
    handler, request, _gates, _lease = _handler(
        tmp_path, StaticDeploymentConfig(url_path="demo")
    )

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "static_build_not_configured"


@pytest.mark.anyio
async def test_static_build_blocks_failed_gate_and_revokes_access(tmp_path: Path) -> None:
    deployment = StaticDeploymentConfig(url_path="demo", build_command="make build")
    handler, request, gates, lease = _handler(tmp_path, deployment)
    evidence = SimpleNamespace(exit_code=2, model_dump=lambda **_kwargs: {"exit_code": 2})
    gates.run.return_value = (SimpleNamespace(evidence=evidence),)

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.category == "static_build_failed"
    lease.revoke.assert_awaited_once()


@pytest.mark.anyio
async def test_static_build_fails_closed_on_invalid_state(tmp_path: Path) -> None:
    deployment = StaticDeploymentConfig(url_path="demo", build_command="make build")
    handler, request, _gates, _lease = _handler(tmp_path, deployment)
    handler._load.side_effect = LookupError("missing worktree")

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.category == "static_build_invalid"
    assert outcome.summary == "missing worktree"


@pytest.mark.anyio
async def test_static_build_rejects_tracked_changes_from_build(tmp_path: Path) -> None:
    deployment = StaticDeploymentConfig(url_path="demo", build_command="make build")
    handler, request, _gates, lease = _handler(tmp_path, deployment)
    handler._git.inspect.side_effect = [
        SimpleNamespace(head="a", diff_hash="b", changed_files=("index.html",)),
        SimpleNamespace(head="a", diff_hash="changed", changed_files=("index.html",)),
    ]

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.category == "static_build_invalid"
    assert "changed tracked Git facts" in outcome.summary
    lease.revoke.assert_awaited_once()


@pytest.mark.anyio
async def test_static_build_loads_active_database_state(tmp_path: Path) -> None:
    deployment = StaticDeploymentConfig(url_path="demo", build_command="make build")
    handler, request, _gates, _lease = _handler(tmp_path, deployment)
    task = SimpleNamespace(project_id="demo")
    run = SimpleNamespace()
    step = SimpleNamespace(status=StepStatus.RUNNING)
    worktree = SimpleNamespace(path=str(tmp_path / "worktree"))
    session = AsyncMock()
    session.get.side_effect = [task, run, step]
    session.scalar.return_value = worktree
    context = AsyncMock()
    context.__aenter__.return_value = session
    handler._factory = MagicMock(return_value=context)
    del handler._load

    loaded = await StaticBuildHandler._load(handler, request)

    assert loaded == (task, worktree, step, tmp_path / "worktree")


@pytest.mark.anyio
async def test_static_build_load_rejects_inactive_step(tmp_path: Path) -> None:
    deployment = StaticDeploymentConfig(url_path="demo", build_command="make build")
    handler, request, _gates, _lease = _handler(tmp_path, deployment)
    session = AsyncMock()
    session.get.side_effect = [
        SimpleNamespace(project_id="demo"),
        SimpleNamespace(),
        SimpleNamespace(status=StepStatus.COMPLETED),
    ]
    session.scalar.return_value = SimpleNamespace(path=str(tmp_path / "worktree"))
    context = AsyncMock()
    context.__aenter__.return_value = session
    handler._factory = MagicMock(return_value=context)

    with pytest.raises(ValueError, match="not active"):
        await StaticBuildHandler._load(handler, request)
