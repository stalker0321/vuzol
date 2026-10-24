"""Upgrade compatibility is checked against stored steps, never compiled templates."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import storage
from tests.integration.workflows._test_runtime_helpers import seed_interpreted, simple_draft
from tests.integration.workflows.stored_workflows import stored_workflow
from vuzol.config import Settings
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import Approval, Run, Step, Task, Worktree
from vuzol.storage.types import (
    ApprovalStatus,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.telegram.projections import build_status_card
from vuzol.workflows.controls import cancel_task, decide_result, pause_task, resume_task
from vuzol.workflows.domain import StepOutcome
from vuzol.workflows.recovery import recover_expired_steps
from vuzol.workflows.service import commit_step_outcome, materialize_run
from vuzol.workflows.upgrade import SUPPORTED_STEP_TYPES, check_stored_handlers

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def seed(factory, name):
    task_id, interpretation_id = await seed_interpreted(factory)
    workflow = stored_workflow(
        simple_draft(), interpretation_id=interpretation_id, configured_workflow=name
    )
    async with factory.begin() as session:
        run = await materialize_run(
            session,
            task_id=task_id,
            workflow=workflow,
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            prompt_revision=None,
            automatic_start=True,
        )
        if name == "discussion_agent.v1":
            task = await session.get(Task, task_id)
            task.task_type = "discussion_agent_internal"
        return task_id, run.id


@pytest.mark.parametrize("name", ["coding.v4", "discussion_agent.v1", "adaptive_worker_trial.v1"])
@pytest.mark.parametrize("status", [RunStatus.PAUSED, RunStatus.BLOCKED])
async def test_stored_runs_render_recover_and_cancel(postgres_dsn, name, status):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id = await seed(factory, name)
        async with factory.begin() as session:
            run = await session.get(Run, run_id)
            task = await session.get(Task, task_id)
            run.status = status
            task.status = TaskStatus(status.value)
            await check_stored_handlers(session)
            card = await build_status_card(session, task_id)
            assert card.html
        async with factory.begin() as session:
            # Pausing/resuming an existing blocker must preserve its cause.
            await pause_task(session, task_id, actor_id="1")
            await resume_task(session, task_id, actor_id="1")
            await cancel_task(session, task_id, actor_id="1")
        async with factory() as session:
            assert (await session.get(Run, run_id)).status is RunStatus.CANCELLED
            assert (await session.get(Task, task_id)).status is TaskStatus.CANCELLED
            steps = (await session.scalars(select(Step).where(Step.run_id == run_id))).all()
            assert all(s.status in {StepStatus.COMPLETED, StepStatus.CANCELLED} for s in steps)
            await check_stored_handlers(session)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("step_status", [StepStatus.PENDING, StepStatus.BLOCKED, StepStatus.FAILED])
async def test_upgrade_refuses_removal_needed_by_paused_run(postgres_dsn, step_status):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id = await seed(factory, "coding.v4")
        async with factory.begin() as session:
            await pause_task(session, task_id, actor_id="1")
            step = await session.scalar(
                select(Step).where(Step.run_id == run_id, Step.step_type == "acceptance")
            )
            step.status = step_status
            await session.flush()
            with pytest.raises(RuntimeError, match=f"run={run_id}.*handler=acceptance"):
                await check_stored_handlers(
                    session, supported=SUPPORTED_STEP_TYPES - {"acceptance"}
                )
            await check_stored_handlers(session)
            step.step_type = "plan"
            await session.flush()
            with pytest.raises(RuntimeError, match="handler=plan"):
                await check_stored_handlers(session)
            # Terminal history does not prevent an upgrade.
            (await session.get(Run, run_id)).status = RunStatus.CANCELLED
            await session.flush()
            await check_stored_handlers(session)
    finally:
        await engine.dispose()


async def test_stored_coding_v4_resumes_and_finishes_with_saved_steps(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id = await seed(factory, "coding.v4")
        async with factory.begin() as session:
            session.add(
                Worktree(
                    task_id=task_id,
                    run_id=run_id,
                    project_id="vuzol",
                    repository_identity_hash="c" * 64,
                    base_commit="a" * 40,
                    default_branch="main",
                    expected_target_head="a" * 40,
                    branch="stored/result",
                    path=str(tmp_path / "worktree"),
                    owner="fixture",
                    delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                    result_commit="b" * 40,
                    diff_hash="d" * 64,
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
            await pause_task(session, task_id, actor_id="1")
            await check_stored_handlers(session)
            await resume_task(session, task_id, actor_id="1")
        seen = []
        while True:
            async with factory.begin() as session:
                token = await claim_step(
                    session,
                    owner="upgrade-test",
                    settings=Settings(environment="test"),
                    lease_seconds=60,
                    capabilities=frozenset(
                        {
                            "host_admin",
                            "git",
                            "filesystem_write",
                            "repository_read",
                            "project_shell",
                            "code_edit",
                        }
                    ),
                )
            if token is None:
                async with factory.begin() as session:
                    approval = await session.scalar(
                        select(Approval).where(
                            Approval.status == ApprovalStatus.PENDING,
                        )
                    )
                    if approval is None:
                        break
                    await decide_result(
                        session, approval.id, decision="approve", deciding_user_id=1
                    )
                continue
            async with factory.begin() as session:
                kind = (await session.get(Step, token.step.id)).step_type
                seen.append(kind)
                result = {}
                if kind == "validate":
                    result = {
                        "structured_output": {
                            "base_commit": "a" * 40,
                            "result_commit": "b" * 40,
                            "gates": [{"name": "tests", "exit_code": 0}],
                        }
                    }
                elif kind == "review":
                    result = {
                        "structured_output": {
                            "base_commit": "a" * 40,
                            "result_commit": "b" * 40,
                            "diff_hash": "d" * 64,
                            "verdict": "pass",
                            "findings": [],
                        }
                    }
                elif kind in {"build_static", "produce_artifacts"}:
                    result = {"status": "skipped"}
                await start_step(session, token)
                await commit_step_outcome(session, token, StepOutcome.succeeded(result))
        assert "plan" not in seen
        assert {"acceptance", "publish_preview", "approval"} <= set(seen)
        async with factory() as session:
            assert (await session.get(Run, run_id)).status is RunStatus.COMPLETED
            assert (await session.get(Task, task_id)).status is TaskStatus.COMPLETED
    finally:
        await engine.dispose()


@pytest.mark.parametrize("name", ["coding.v4", "discussion_agent.v1", "adaptive_worker_trial.v1"])
async def test_stored_run_retains_lease_recovery(postgres_dsn, name):
    engine, factory = storage(postgres_dsn)
    try:
        _, run_id = await seed(factory, name)
        async with factory.begin() as session:
            step = await session.scalar(
                select(Step).where(Step.run_id == run_id, Step.status == StepStatus.QUEUED)
            )
            step.status = StepStatus.LEASED
            step.lease_owner = "old-worker"
            step.lease_generation = 1
            step.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            step_id = step.id
        async with factory.begin() as session:
            await check_stored_handlers(session)
            await recover_expired_steps(session, batch_size=20)
        async with factory() as session:
            step = await session.get(Step, step_id)
            assert step.status in {StepStatus.QUEUED, StepStatus.BLOCKED}
            assert step.lease_owner is None
    finally:
        await engine.dispose()


async def test_stored_coding_v4_failed_task_can_cancel_paused_run(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id = await seed(factory, "coding.v4")
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            task.status = TaskStatus.FAILED
            run.status = RunStatus.PAUSED
            version = task.version
            await check_stored_handlers(session)
            with pytest.raises(ValueError, match="no automatic repair"):
                await resume_task(session, task_id, actor_id="1")
            await cancel_task(session, task_id, actor_id="1")
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task.status is TaskStatus.FAILED and task.version == version + 1
            assert (await session.get(Run, run_id)).status is RunStatus.CANCELLED
            steps = await session.scalars(select(Step).where(Step.run_id == run_id))
            assert all(s.status in {StepStatus.COMPLETED, StepStatus.CANCELLED} for s in steps)
    finally:
        await engine.dispose()


async def test_upgrade_refuses_live_pinned_horizon_without_rewriting_history(postgres_dsn):
    from tests.integration.test_d2_acceptance_pg import _horizon_package
    from vuzol.storage.models import WorkPackage
    from vuzol.storage.types import WorkPackageStatus

    engine, factory = storage(postgres_dsn)
    try:
        package_id, _, _ = await _horizon_package(factory, start=False)
        async with factory.begin() as session:
            with pytest.raises(RuntimeError, match=f"horizon package {package_id}"):
                await check_stored_handlers(session)
            package = await session.get(WorkPackage, package_id)
            package.status = WorkPackageStatus.STOPPED
            await session.flush()
            with pytest.raises(RuntimeError, match="needs conversion"):
                await check_stored_handlers(session)
            package.status = WorkPackageStatus.COMPLETED
            await session.flush()
            await check_stored_handlers(session)
            assert package.execution_contract_version == "horizon-v1:enabled"
    finally:
        await engine.dispose()
