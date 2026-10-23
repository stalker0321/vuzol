"""Real PostgreSQL publication fencing and publisher-startup recovery."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from tests.integration.leadership.test_lead_review_regressions import StartModel, control
from tests.integration.leadership.test_managed_goal_flow import (
    _enable,
    _factory,
    _session,
    _worker,
)
from tests.integration.storage.helpers import seed_task_run_step
from tests.integration.workflows.test_runtime_preview_publish import (
    _environment,
    _git_repository,
    _handler,
)
from vuzol.app.preview_gateway import create_preview_gateway
from vuzol.cli.static_publisher_worker import initialize_previews, publisher_handlers
from vuzol.storage.models import Run, Step, Task, Worktree
from vuzol.storage.records import LeaseToken
from vuzol.storage.repositories.core import step_record
from vuzol.storage.types import IdempotencyClass, RunStatus, StepStatus, TaskStatus
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.publication import PublicationDenied, publication_authority
from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def request_for(factory, task_id=None):
    if task_id is None:
        task, _, record = await seed_task_run_step(factory, step_type="publish_preview")
        task_id, step_id = task.id, record.id
    else:
        async with factory.begin() as session:
            run = Run(
                task_id=task_id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.RUNNING,
            )
            session.add(run)
            await session.flush()
            step = Step(
                run_id=run.id,
                ordinal=1,
                step_type="publish_preview",
                timeout_seconds=60,
                idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
            )
            session.add(step)
            await session.flush()
            step_id = step.id
    async with factory.begin() as session:
        step = await session.get(Step, step_id)
        task = await session.get(Task, task_id)
        task.status = TaskStatus.EXECUTING
        step.status = StepStatus.RUNNING
        step.lease_owner, step.lease_generation = "publisher-test", 1
        step.lease_expires_at = datetime.now(UTC) + timedelta(minutes=10)
        return StepExecutionRequest(
            task_id=task.id,
            run_id=step.run_id,
            step_id=step.id,
            step_type=step.step_type,
            payload={},
            timeout_seconds=60,
            lease=LeaseToken(step_record(step), "publisher-test", 1),
        )


@pytest.mark.parametrize("change", ["pause", "cancel", "takeover", "expire"])
async def test_stale_publication_cannot_enter_effect_boundary(postgres_dsn, change):
    async with _factory(postgres_dsn) as factory:
        request = await request_for(factory)
        async with publication_authority(
            factory, request, CancellationContext(), feature_enabled=False
        ):
            pass
        async with factory.begin() as session:
            task = await session.get(Task, request.task_id)
            step = await session.get(Step, request.step_id)
            if change == "pause":
                task.status = TaskStatus.PAUSED
            elif change == "cancel":
                task.status = TaskStatus.CANCELLED
            elif change == "takeover":
                step.lease_generation += 1
            else:
                step.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        with pytest.raises(PublicationDenied):
            async with publication_authority(
                factory, request, CancellationContext(), feature_enabled=False
            ):
                pytest.fail("stale effect admitted")


@pytest.mark.parametrize("action", ["pause_goal", "revoke_delegation"])
async def test_managed_stop_wins_before_new_publication(postgres_dsn, action):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await _worker(factory, StartModel()).process_one()
        async with factory() as session:
            task_id = await session.scalar(select(Task.id))
            assert task_id is not None
        request = await request_for(factory, task_id)
        await control(factory, goal_id, action)
        with pytest.raises(PublicationDenied):
            async with publication_authority(
                factory, request, CancellationContext(), feature_enabled=True
            ):
                pytest.fail("managed effect admitted after stop")


async def test_task_stop_serializes_with_publication_boundary(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        request = await request_for(factory)
        stopping = asyncio.Event()

        async def stop():
            async with factory.begin() as session:
                stopping.set()
                task = await session.get(Task, request.task_id, with_for_update=True)
                task.status = TaskStatus.PAUSED

        async with publication_authority(
            factory, request, CancellationContext(), feature_enabled=False
        ):
            pending = asyncio.create_task(stop())
            await stopping.wait()
            await asyncio.sleep(0.02)
            assert not pending.done()
        await asyncio.wait_for(pending, 2)
        with pytest.raises(PublicationDenied):
            async with publication_authority(
                factory, request, CancellationContext(), feature_enabled=False
            ):
                pytest.fail("second effect after stop")


@pytest.mark.parametrize("change", ["cancel", "source", "contract", "legacy", "newer"])
async def test_real_publisher_restores_completed_preview_after_restart(
    postgres_dsn,
    tmp_path,
    monkeypatch,
    change,
):
    import shutil

    from vuzol.security import landlock

    if shutil.which("node") is None or landlock.landlock_abi_version() < 1:
        pytest.skip("real confined Node runtime unavailable")
    repository, commit = _git_repository(tmp_path)
    config_handler, _, _ = _handler(tmp_path)
    runtime = config_handler._runtime
    runtime.settings.project_lead = SimpleNamespace(enabled=False)
    monkeypatch.setattr(
        "vuzol.workflows.runtime_preview.current_environment",
        AsyncMock(return_value=_environment()),
    )
    async with _factory(postgres_dsn) as factory:
        request = await request_for(factory)
        async with factory.begin() as session:
            task = await session.get(Task, request.task_id)
            task.project_id = "demo"
            session.add(
                Worktree(
                    task_id=task.id,
                    run_id=request.run_id,
                    project_id="demo",
                    repository_identity_hash="a" * 64,
                    base_commit=commit,
                    expected_target_head=commit,
                    default_branch="main",
                    branch="fixture",
                    path=str(repository),
                    owner="test",
                    result_commit=commit,
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
        registry = PreviewRuntimeRegistry()
        try:
            result = await publisher_handlers(factory, runtime, registry)[
                "publish_preview"
            ].execute(request, CancellationContext())
            assert result.result.get("status") == "published", result
            original = registry.targets["demo"]
            async with factory.begin() as session:
                step = await session.get(Step, request.step_id)
                step.status, step.result = StepStatus.COMPLETED, result.result
                task = await session.get(Task, request.task_id)
                task.status = TaskStatus.COMPLETED
                run = await session.get(Run, request.run_id)
                run.status = RunStatus.COMPLETED
            await registry.close()
            assert original.process.returncode is not None
            registry = await initialize_previews(factory, runtime)
            assert registry.targets["demo"].source_commit == commit
            assert registry.targets["demo"].process.pid != original.process.pid
            gateway = create_preview_gateway(
                registry, static_root=runtime.settings.preview_site_root
            )
            async with (
                gateway.router.lifespan_context(gateway),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=gateway), base_url="http://preview.test"
                ) as client,
            ):
                response = await client.get("/demo/")
                assert response.status_code == 200
                assert response.text == "ok"
            await registry.close()
            # A stop, drift or a successor must prevent resurrection on another restart.
            async with factory.begin() as session:
                task = await session.get(Task, request.task_id)
                step = await session.get(Step, request.step_id)
                if change == "cancel":
                    task.status = TaskStatus.CANCELLED
                elif change == "source":
                    worktree = await session.scalar(select(Worktree))
                    worktree.result_commit = "f" * 40
                elif change == "contract":
                    changed_environment = _environment()
                    changed_environment.contract["components"]["web"]["run_command"] = [
                        "node",
                        "different.js",
                    ]
                    monkeypatch.setattr(
                        "vuzol.workflows.runtime_preview.current_environment",
                        AsyncMock(return_value=changed_environment),
                    )
                elif change == "legacy":
                    step.result = {
                        key: value for key, value in step.result.items() if key != "runtime_spec"
                    }
                else:
                    session.add(
                        Step(
                            run_id=request.run_id,
                            ordinal=2,
                            step_type="publish_preview",
                            status=StepStatus.COMPLETED,
                            timeout_seconds=60,
                            result={"status": "published", "artifact_kind": "static"},
                            idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                        )
                    )
            registry = await initialize_previews(factory, runtime)
            assert not registry.targets
            if change != "newer":
                assert registry.unavailable["demo"] == (
                    "preview_restore_stale"
                    if change == "cancel"
                    else "preview_restore_source_changed"
                )
                # A stale static preview must not hide failed runtime recovery.
                stale = runtime.settings.preview_site_root / "demo" / "current"
                stale.mkdir(parents=True)
                (stale / "index.html").write_text("obsolete static preview")
                gateway = create_preview_gateway(
                    registry, static_root=runtime.settings.preview_site_root
                )
                async with (
                    gateway.router.lifespan_context(gateway),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=gateway), base_url="http://preview.test"
                    ) as client,
                ):
                    response = await client.get("/demo/")
                    assert response.status_code == 503
                    assert "obsolete static preview" not in response.text
        finally:
            await registry.close()
