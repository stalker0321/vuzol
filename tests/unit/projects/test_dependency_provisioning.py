import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config import DependencyProvisioningSettings
from vuzol.projects.dependencies import DEPENDENCY_APPROVAL_SCHEMA
from vuzol.projects.dependency_provisioning import DependencyProvisioningHandler
from vuzol.storage.types import ApprovalStatus, StepStatus
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import envelope_hash


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _request() -> StepExecutionRequest:
    return cast(
        StepExecutionRequest,
        SimpleNamespace(
            task_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            step_id=uuid.uuid4(),
            lease=SimpleNamespace(owner="worker", generation=1),
        ),
    )


def _state(
    tmp_path: Path, request: StepExecutionRequest
) -> tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    worktree_path = tmp_path / "worktrees" / "demo" / str(request.run_id)
    worktree_path.mkdir(parents=True)
    (worktree_path / "pyproject.toml").write_text(
        '[project]\nname="demo"\ndependencies=["fastapi>=1"]\n'
    )
    task = SimpleNamespace(id=request.task_id, project_id="demo")
    step = SimpleNamespace(
        id=request.step_id,
        run_id=request.run_id,
        status=StepStatus.RUNNING,
        lease_owner="worker",
        lease_generation=1,
        payload={},
        external_idempotency_key=None,
    )
    worktree = SimpleNamespace(
        id=uuid.uuid4(),
        task_id=request.task_id,
        run_id=request.run_id,
        path=worktree_path,
    )
    return task, step, worktree


@pytest.mark.anyio
async def test_dependency_manifest_requests_separate_hash_bound_approval(tmp_path: Path) -> None:
    request = _request()
    task, step, worktree = _state(tmp_path, request)
    read = MagicMock()
    read.get = AsyncMock(side_effect=(task, step))
    read.scalar = AsyncMock(side_effect=(worktree, None))
    read.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: []))
    write = MagicMock()
    write.scalar = AsyncMock(side_effect=(step, None))
    write.flush = AsyncMock()
    factory = MagicMock(return_value=AsyncContext(read))
    factory.begin.return_value = AsyncContext(write)
    settings = DependencyProvisioningSettings(
        enabled=True, environment_root=tmp_path / "environments"
    )
    builder = MagicMock()
    handler = DependencyProvisioningHandler(
        cast(Any, factory),
        settings,
        builder,
        worktree_root=tmp_path / "worktrees",
    )

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.NEEDS_APPROVAL
    approval = write.add.call_args.args[0]
    assert approval.requested_action == "install_dependencies"
    envelope = step.payload["action_envelope"]
    assert envelope["schema_version"] == DEPENDENCY_APPROVAL_SCHEMA
    assert envelope["requirements"][0]["direct_dependencies"] == ["fastapi>=1"]
    builder.build.assert_not_called()


@pytest.mark.anyio
async def test_approved_dependency_environment_is_built_and_consumed(tmp_path: Path) -> None:
    request = _request()
    task, step, worktree = _state(tmp_path, request)
    settings = DependencyProvisioningSettings(
        enabled=True, environment_root=tmp_path / "environments"
    )
    from vuzol.projects.dependencies import inspect_dependency_requests
    from vuzol.projects.source_catalog import SourceCatalog

    dependency = inspect_dependency_requests(
        worktree.path, SourceCatalog.builtin(), maximum_direct_dependencies=10
    )[0]
    envelope = {
        "schema_version": DEPENDENCY_APPROVAL_SCHEMA,
        "requested_action": "install_dependencies",
        "task_id": str(task.id),
        "run_id": str(request.run_id),
        "step_id": str(step.id),
        "project_id": "demo",
        "worktree_id": str(worktree.id),
        "environment_root": str(settings.environment_root),
        "requirements": [dependency.approval_record()],
    }
    approval = SimpleNamespace(
        id=uuid.uuid4(),
        status=ApprovalStatus.APPROVED,
        action_envelope_hash=envelope_hash(envelope),
        consumed_at=None,
    )
    step.payload = {"action_envelope": envelope}
    read = MagicMock()
    read.get = AsyncMock(side_effect=(task, step))
    read.scalar = AsyncMock(side_effect=(worktree, approval))
    read.scalars = AsyncMock(return_value=SimpleNamespace(all=lambda: []))
    lease = MagicMock()
    lease.scalar = AsyncMock(return_value=step)
    consume = MagicMock()
    consume.scalar = AsyncMock(side_effect=(step, approval))
    factory = MagicMock(side_effect=(AsyncContext(read), AsyncContext(lease)))
    factory.begin.return_value = AsyncContext(consume)
    builder = MagicMock()
    builder.build = AsyncMock(return_value=SimpleNamespace(request=dependency))
    handler = DependencyProvisioningHandler(
        cast(Any, factory),
        settings,
        builder,
        worktree_root=tmp_path / "worktrees",
    )

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "installed"
    assert approval.status is ApprovalStatus.CONSUMED
    builder.build.assert_awaited_once()
