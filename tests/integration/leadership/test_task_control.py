"""PR4: explicit task decisions, workflow events and owner command corrections."""

import uuid

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_lead_review_regressions import StartModel, control
from tests.integration.leadership.test_lead_surface import owner_message
from tests.integration.leadership.test_managed_goal_flow import (
    PROJECT,
    _enable,
    _factory,
    _lead_service,
    _session,
    _start_decision,
    _worker,
)
from tests.integration.leadership.test_stage2_contracts import amendment_command
from tests.integration.storage.helpers import seed_task_run_step

from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.model import LeadCommandDraft, LeadDecision
from vuzol.leadership.persistence import ManagedGoal
from vuzol.leadership.tasks import task_context
from vuzol.storage.models import (
    MaterializationLink,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import IdempotencyClass, StepStatus, TaskStatus, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.transitions import transition_task

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def command(factory, goal_id, name, payload, *, key=None):
    async with factory() as session:
        goal = await session.get(ManagedGoal, goal_id)
        version = goal.version
    return await _lead_service(factory, with_executor=True).apply_agent(
        server_agent_principal(PROJECT),
        "worker",
        LeadCommand(
            name=name,
            project_id=PROJECT,
            goal_id=goal_id,
            expected_version=version,
            idempotency_key=key or str(uuid.uuid4()),
            payload=payload,
        ),
    )


class TwoItems:
    @property
    def planner_plan(self):
        from tests.integration.leadership.planning_helpers import fixture_plan

        plan = fixture_plan()
        plan["items"][0]["local_id"] = "first"
        plan["items"].append({**plan["items"][0], "local_id": "second", "dependencies": ["first"]})
        return plan

    async def decide(self, snapshot):
        return _start_decision()


async def test_each_item_needs_lead_dispatch_and_old_events_are_harmless(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, TwoItems()).process_one()
        from tests.integration.leadership.test_plan_revisions import approve_plan, start

        await approve_plan(factory, goal_id)
        assert (await start(factory, goal_id)).status == "applied"
        async with UnitOfWork(factory) as uow:
            first = await uow.session.scalar(select(Task))
            first_id = first.id
            await transition_task(uow.session, first, TaskStatus.COMPLETED, actor_type="test")
            seq = WorkPackageSequencer(uow)
            await seq.observe_terminal(task_id=first.id)
            await seq.observe_terminal(task_id=first.id)
            assert await uow.session.scalar(select(func.count()).select_from(Task)) == 1
            goal = await uow.session.get(ManagedGoal, goal_id)
            spec = (await task_context(uow.session, goal))["start_spec"]
        stale = await command(factory, goal_id, "start_task", {"spec": {**spec, "ordinal": 1}})
        assert stale.reason == "task_spec_stale"
        started = await command(factory, goal_id, "start_task", {"spec": spec}, key="second")
        assert started.status == "applied"
        assert (
            await command(factory, goal_id, "start_task", {"spec": spec})
        ).reason == "project_task_running"
        async with UnitOfWork(factory) as uow:
            seq = WorkPackageSequencer(uow)
            await seq.observe_terminal(task_id=first_id)
            second = await uow.session.get(Task, uuid.UUID(started.result["task_id"]))
            await transition_task(uow.session, second, TaskStatus.COMPLETED, actor_type="test")
            await seq.observe_terminal(task_id=second.id)
            await seq.observe_terminal(task_id=first_id)
            goal = await uow.session.get(ManagedGoal, goal_id)
            package = await uow.session.get(WorkPackage, goal.current_package_id)
            assert package.status is WorkPackageStatus.COMPLETED
            assert goal.lifecycle == "active"  # accepting a task never completes the goal
            assert await uow.session.scalar(select(func.count()).select_from(Task)) == 2


@pytest.mark.parametrize(
    "step_type,blocked", [("execute_code", False), ("approval", True), ("publish_static", True)]
)
async def test_live_pause_cancel_amendment_recovers_on_next_tick(
    postgres_dsn, tmp_path, step_type, blocked
):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await _worker(factory, StartModel()).process_one()
        _, run_id, record = await seed_task_run_step(
            factory,
            step_status=StepStatus.RUNNING,
            step_type=step_type,
            idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
        )
        async with factory.begin() as session:
            link = await session.scalar(select(MaterializationLink))
            run = await session.get(Run, run_id)
            run.task_id = link.task_id
            task = await session.get(Task, link.task_id)
            task.status = TaskStatus.EXECUTING
        assert (await control(factory, goal_id, "pause_goal")).status == "applied"
        event = await owner_message(factory, tmp_path)
        amendment = await amendment_command(factory, goal_id, event.id, policy="cancel_current")
        assert (
            await _lead_service(factory).apply_agent(
                server_agent_principal(PROJECT), "worker", amendment
            )
        ).status == "applied"
        worker = _worker(factory, StartModel())
        async with factory.begin() as session:
            await worker._reconcile(session)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            step = await session.get(Step, record.id)
            assert step.status is StepStatus.CANCELLED and step.unknown_effects
            assert goal.lifecycle == "paused"
            assert goal.original_request == "ship the marker"
            assert goal.scope_revision == (1 if blocked else 2)
            assert ("pending_amendment" in goal.constraints) is blocked
            if not blocked:
                assert goal.scope_text == "README only"


@pytest.mark.parametrize("name", ["start_task", "not_supported", "amend_goal"])
async def test_owner_gets_receipt_bound_followup_without_inference_retry(
    postgres_dsn, tmp_path, name
):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await _worker(factory, StartModel()).process_one()
        await control(factory, goal_id, "pause_goal")
        await owner_message(factory, tmp_path)

        class Model:
            calls = 0

            async def decide(self, snapshot):
                self.calls += 1
                payload = {"spec": snapshot.result_context["task_control"]["start_spec"]}
                if name == "amend_goal":
                    payload = {
                        "scope": "README only",
                        "criteria": [{"id": "readme", "description": "README updated"}],
                        "source_event_id": snapshot.owner_event["source_event_id"],
                        "inflight_policy": "finish_current",
                    }
                return LeadDecision(
                    reply="Принято: меняю цель",
                    commands=(
                        LeadCommandDraft(name=name, payload=payload, reason="Owner request"),
                    ),
                )

        model = Model()
        worker = _worker(factory, model)
        for _ in range(4):
            await worker.process_one()
        assert model.calls == 1
        async with factory() as session:
            notices = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key.like("lead:command-notice:%")
                    )
                )
            )
            assert len(notices) == 1
            assert notices[0].payload["chat_id"] == -100
            assert notices[0].payload["message_thread_id"] == 10
            assert "Уточнение" in notices[0].payload["html"]
            assert ("ещё не применено" if name == "amend_goal" else "не выполнено") in notices[
                0
            ].payload["html"]


async def test_quota_event_and_reconciliation_share_one_key(postgres_dsn):
    from vuzol.workflows.lead_events import publish_task_event

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await _worker(factory, StartModel()).process_one()
        async with factory.begin() as session:
            task = await session.scalar(select(Task))
            await transition_task(session, task, TaskStatus.QUOTA_EXHAUSTED, actor_type="test")
            await publish_task_event(session, task)
            rows = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "project_lead",
                        TransactionalOutbox.payload["wakeup_kind"].as_string() == "limit_hit",
                    )
                )
            )
            assert len(rows) == 1 and rows[0].payload["task_id"] == str(task.id)
            await _worker(factory, StartModel())._reconcile(session)
            assert (
                len(
                    list(
                        await session.scalars(
                            select(TransactionalOutbox).where(
                                TransactionalOutbox.destination == "project_lead",
                                TransactionalOutbox.payload["wakeup_kind"].as_string()
                                == "limit_hit",
                            )
                        )
                    )
                )
                == 1
            )
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"


@pytest.mark.parametrize("cancel", [False, True])
async def test_acceptance_waits_for_repair_binds_commit_and_runs_applier_once(
    postgres_dsn, tmp_path, monkeypatch, cancel
):
    from tests.integration.leadership.test_managed_goal_flow import (
        _drain,
        _git_init,
        _runtime,
        _workflow_workers,
    )

    from vuzol.execution.git import LocalGit
    from vuzol.execution.worktrees import WorktreeService
    from vuzol.storage.models import Approval, Worktree
    from vuzol.storage.types import ApprovalStatus
    from vuzol.workflows.dispatch import WorkflowDispatcher

    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    base = _git_init(repository)
    runtime = _runtime(tmp_path, repository, lead=True)
    applied = []
    original_apply = LocalGit.apply_result

    async def counting_apply(self, *args, **kwargs):
        applied.append(kwargs)
        return await original_apply(self, *args, **kwargs)

    monkeypatch.setattr(LocalGit, "apply_result", counting_apply)
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await _worker(factory, StartModel()).process_one()
        git = LocalGit()
        workers = (
            WorkflowDispatcher(runtime, factory, owner="dispatch"),
            *_workflow_workers(
                runtime,
                factory,
                git,
                WorktreeService(runtime.settings.worktree_root, git, retention_days=1),
            ),
        )
        spec = None
        # Check after every single step: one round can queue and finish the repair.
        for _ in range(30):
            for worker in workers:
                await worker.process_one()
                async with factory() as session:
                    repair = await session.scalar(
                        select(Step).where(
                            Step.step_type == "execute_code",
                            Step.status == StepStatus.QUEUED,
                            Step.payload.has_key("repair_for_step_id"),
                        )
                    )
                    if repair is not None:
                        goal = await session.get(ManagedGoal, goal_id)
                        spec = (await task_context(session, goal))["tasks"][0]["accept_spec"]
                        break
            if spec is not None:
                break
        else:
            pytest.fail("fixture did not reach review-before-repair")
        assert (await command(factory, goal_id, "accept_result", spec)).reason == "result_not_ready"
        async with factory() as session:
            assert await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.payload["wakeup_kind"].as_string() == "review_result"
                )
            )
        await _drain(workers)
        async with factory() as session:
            task = await session.scalar(select(Task))
            assert task.status is TaskStatus.WAITING_APPROVAL
            goal = await session.get(ManagedGoal, goal_id)
            spec = (await task_context(session, goal))["tasks"][0]["accept_spec"]
            approval = await session.scalar(
                select(Approval).where(Approval.requested_action == "apply_result")
            )
            assert approval.status is ApprovalStatus.PENDING
            assert not applied
        for field, value, reason in [
            ("result_commit", "0" * 40, "result_commit_stale"),
            ("scope_revision", 99, "result_revision_stale"),
            ("spec_revision", "0" * 64, "result_revision_stale"),
            ("plan_revision_id", str(uuid.uuid4()), "result_revision_stale"),
        ]:
            assert (
                await command(factory, goal_id, "accept_result", {**spec, field: value})
            ).reason == reason
        if cancel:
            assert (
                await command(factory, goal_id, "cancel_task", {"task_id": spec["task_id"]})
            ).status == "applied"
            assert (
                await command(factory, goal_id, "accept_result", spec)
            ).reason == "result_not_ready"
            await _drain(workers)
            assert not applied
        else:
            async with factory() as session:
                goal = await session.get(ManagedGoal, goal_id)
                version = goal.version
            receipt_command = LeadCommand(
                name="accept_result",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key="accept-once",
                payload=spec,
            )
            service = _lead_service(factory)
            for expected in ("applied", "duplicate"):
                result = await service.apply_agent(
                    server_agent_principal(PROJECT), "worker", receipt_command
                )
                assert result.status == expected
            await _drain(workers)
            await _drain(workers)
            assert len(applied) == 1
            async with factory() as session:
                assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"
                assert (await session.scalar(select(Worktree))).result_commit == spec[
                    "result_commit"
                ]
            # The lead asking again after the task finished is a no-op success.
            again = await command(factory, goal_id, "accept_result", spec, key="accept-again")
            assert again.status == "applied"
            await _drain(workers)
            assert len(applied) == 1
        assert await git.resolve_commit(repository, "main") == base
