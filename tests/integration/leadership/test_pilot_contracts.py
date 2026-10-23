"""Regressions from the real pilot: admission ceilings and running-step pause."""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import select
from tests.integration.leadership.test_lead_review_regressions import StartModel
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    PROJECT,
    _enable,
    _factory,
    _lead_service,
    _package_count,
    _plan,
    _session,
    _worker,
)
from tests.integration.storage.helpers import seed_task_run_step

from vuzol.config import Settings
from vuzol.config.settings import WorkflowSettings
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import claim_step, heartbeat_step, start_step
from vuzol.storage.models import Step, WorkPackage
from vuzol.storage.types import IdempotencyClass, StepStatus, WorkPackageStatus
from vuzol.workflows.application import Principal
from vuzol.workflows.controls import pause_task, resume_task
from vuzol.workflows.domain import StepOutcome
from vuzol.workflows.service import commit_step_outcome
from vuzol.workflows.worker import WorkflowWorker

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def owner_command(factory, goal_id, name, payload):
    async with factory() as session:
        goal = await session.get(ManagedGoal, goal_id)
        version = goal.version
    command = LeadCommand(
        name=name,
        project_id=PROJECT,
        goal_id=goal_id,
        expected_version=version,
        idempotency_key=f"pilot:{name}:{version}",
        payload=payload,
    )
    return await _lead_service(factory, with_executor=True).apply_human(
        Principal(OWNER, "telegram"), command
    ), command


async def test_package_ceiling_survives_failure_restart_and_receipt_replay(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        result, limit_command = await owner_command(
            factory,
            goal_id,
            "set_work_limits",
            {
                "work_limits": {"max_packages": 1, "max_items_per_package": 1},
            },
        )
        assert result.status == "applied"
        await _worker(factory, StartModel()).process_one()
        await _worker(factory, StartModel()).process_one()
        assert await _package_count(factory) == 1
        # Fault injection: an unsuccessful package still consumes its admission.
        async with factory.begin() as session:
            package = await session.scalar(select(WorkPackage))
            package.status = WorkPackageStatus.STOPPED
            from vuzol.storage.models import Task
            from vuzol.storage.types import TaskStatus

            task = await session.scalar(select(Task))
            task.status = TaskStatus.FAILED
        result, start = await owner_command(
            factory,
            goal_id,
            "propose_batch",
            {
                "plan": _plan("Package B", "Write the second marker", ["marker-b"], "SECOND.txt"),
            },
        )
        assert result.status == "waiting"
        assert result.reason == "work_package_limit_reached"
        assert await _package_count(factory) == 1
        restarted = _lead_service(factory, with_executor=True)
        replay = await restarted.apply_human(Principal(OWNER, "telegram"), start)
        assert replay.status == "duplicate"
        assert await _package_count(factory) == 1
        denied = await restarted.apply_agent(
            f"agent:project-lead:{PROJECT}",
            "worker",
            replace(
                limit_command,
                idempotency_key="agent-unlimited",
                expected_version=result.version,
                payload={"work_limits": {}},
            ),
        )
        assert denied.status == "denied"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.wakeup_condition["kind"] == "owner"
            assert goal.constraints["work_limits"]["max_packages"] == 1


@pytest.mark.parametrize(
    "raw",
    [None, {"max_packages": 0}, {"max_packages": True}, {"max_packages": "1"}, {"invented": 1}],
)
async def test_invalid_limits_do_not_mutate_the_grant(postgres_dsn, raw):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        result, _ = await owner_command(factory, goal_id, "set_work_limits", {"work_limits": raw})
        assert result.status == "denied" and result.reason == "invalid_work_limits"
        assert await _package_count(factory) == 0


async def test_item_limit_blocks_before_package_creation(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await owner_command(
            factory,
            goal_id,
            "set_work_limits",
            {
                "work_limits": {"max_items_per_package": 1},
            },
        )
        decision = await StartModel().decide(None)
        payload = decision.commands[0].payload
        payload["plan"]["items"].append({**payload["plan"]["items"][0], "local_id": "second"})
        result, _ = await owner_command(factory, goal_id, "propose_batch", payload)
        assert result.status == "waiting" and result.reason == "work_item_limit_reached"
        assert await _package_count(factory) == 0


async def test_batch_refusal_after_local_mutation_is_a_durable_receipt(postgres_dsn, monkeypatch):
    from vuzol.discussion.domain import DomainError
    from vuzol.discussion.service import WorkPackageService

    async def refuse(*args, **kwargs):
        raise DomainError("fixture_admission_refused")

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        decision = await StartModel().decide(None)
        monkeypatch.setattr(WorkPackageService, "create_draft", refuse)
        result, command = await owner_command(
            factory, goal_id, "propose_batch", decision.commands[0].payload
        )
        assert result.status == "denied" and result.reason == "fixture_admission_refused"
        replay = await _lead_service(factory, with_executor=True).apply_human(
            Principal(OWNER, "telegram"),
            command,
        )
        assert replay.status == "duplicate"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.repeat_count == 0
        assert await _package_count(factory) == 0


async def test_telegram_limits_are_explicit_and_do_not_resume_work(postgres_dsn, tmp_path):
    from tests.integration.leadership.test_lead_surface import _message
    from tests.integration.leadership.test_managed_goal_flow import _runtime

    from vuzol.telegram.ingress import TelegramIngressService

    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=True)
    async with _factory(postgres_dsn) as factory:
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(_message(101, "/lead --max-packages 1 --max-items 1 ship it"))
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal.constraints["work_limits"] == {
                "max_packages": 1,
                "max_items_per_package": 1,
            }
            assert goal.original_request == "ship it"
        await ingress.accept_message(_message(102, "/lead_pause"))
        await ingress.accept_message(_message(103, "/lead_limits 2 3"))
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal.lifecycle == "paused"
            assert goal.constraints["work_limits"] == {
                "max_packages": 2,
                "max_items_per_package": 3,
            }


@pytest.mark.parametrize("mutating", [False, True])
async def test_pause_revokes_running_lease_even_after_fast_resume(postgres_dsn, mutating):
    async with _factory(postgres_dsn) as factory:
        task, _, _ = await seed_task_run_step(
            factory,
            idempotency_class=(
                IdempotencyClass.ISOLATED_RETRYABLE if mutating else IdempotencyClass.READ_ONLY
            ),
        )
        async with factory.begin() as session:
            token = await claim_step(
                session, owner="first", lease_seconds=60, capabilities=frozenset()
            )
            await start_step(session, token)
        async with factory.begin() as session:
            await pause_task(session, task.id)
        async with factory.begin() as session:
            if mutating:
                with pytest.raises(ValueError, match="unknown external effects"):
                    await resume_task(session, task.id)
            else:
                await resume_task(session, task.id)
        with pytest.raises(LeaseLost):
            async with factory.begin() as session:
                await heartbeat_step(session, token, lease_seconds=60)
        with pytest.raises(LeaseLost):
            async with factory.begin() as session:
                await commit_step_outcome(session, token, StepOutcome.succeeded({"late": True}))
        async with factory() as session:
            step = await session.get(Step, token.step.id)
            assert step.unknown_effects is mutating
            assert step.status is (StepStatus.BLOCKED if mutating else StepStatus.QUEUED)
            assert not step.result


async def test_pause_reaches_the_running_handler_cancellation_boundary(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        task, _, stored = await seed_task_run_step(factory, step_type="fixture_cli")
        entered, stopped = asyncio.Event(), asyncio.Event()

        class Cli:
            async def execute(self, request, cancellation):
                entered.set()
                await cancellation.wait()
                stopped.set()
                return StepOutcome.succeeded({"must_not_commit": True})

        worker = WorkflowWorker(
            Settings(
                environment="test", workflow=WorkflowSettings(lease_seconds=15, heartbeat_seconds=1)
            ),
            factory,
            owner="running-cli",
            handlers={"fixture_cli": Cli()},
        )
        running = asyncio.create_task(worker.process_one())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            async with factory.begin() as session:
                await pause_task(session, task.id)
            await asyncio.wait_for(stopped.wait(), 5)
            assert await asyncio.wait_for(running, 5)
            async with factory() as session:
                step = await session.get(Step, stored.id)
                assert step.status is StepStatus.BLOCKED and step.unknown_effects
                assert not step.result
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
