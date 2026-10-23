"""Fixed-contract repair bounds, cancellation and stored lease recovery."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import storage
from tests.integration.workflows._test_runtime_helpers import planned_coding_draft, seed_interpreted
from vuzol.config import Settings
from vuzol.interpretation.domain import TaskType
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import Run, Step, Task
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.controls import cancel_task
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.recovery import recover_expired_steps
from vuzol.workflows.recovery_policy import RecoveryPolicy
from vuzol.workflows.service import commit_step_outcome, materialize_run

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.mark.parametrize("report", [False, True])
@pytest.mark.parametrize("repairs", [1, 3])
@pytest.mark.parametrize("passes", [True, False])
@pytest.mark.parametrize("failed_stage", ["validate", "review"])
async def test_frozen_budget_bounds_successful_and_unsuccessful_repairs(
    postgres_dsn: str, repairs: int, passes: bool, failed_stage: str, report: bool
) -> None:
    engine, factory = storage(postgres_dsn)
    draft = planned_coding_draft()
    if report:
        draft = draft.model_copy(update={"task_type": TaskType.RESEARCH})
    task_id, interpretation_id = await seed_interpreted(factory, draft)
    full = compile_workflow(draft, interpretation_id=interpretation_id)
    # Exercise the executor through review; the real apply boundary is covered by
    # test_coding_runtime_composition with the same factory and real Git handlers.
    workflow = replace(full, steps=tuple(s for s in full.steps if s.ordinal <= 7))
    pinned = RecoveryPolicy(step_repair_cap=repairs, task_repair_cap=repairs)
    async with factory.begin() as session:
        run = await materialize_run(
            session,
            task_id=task_id,
            workflow=workflow,
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            prompt_revision=None,
            automatic_start=True,
            recovery_policy=pinned,
        )
        run_id = run.id
        task = await session.get(Task, task_id)
        assert task is not None
        task.status = TaskStatus.VALIDATING if failed_stage == "validate" else TaskStatus.REVIEWING
        steps = list(await session.scalars(select(Step).where(Step.run_id == run_id)))
        for step in steps:
            step.status = (
                StepStatus.COMPLETED if step.step_type != failed_stage else StepStatus.QUEUED
            )
        # Leave the review behind validation pending when testing validation repairs.
        if failed_stage == "validate":
            next(s for s in steps if s.step_type == "review").status = StepStatus.PENDING

    async def finish(outcome: StepOutcome, expected: str) -> None:
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="fixed",
                lease_seconds=60,
                capabilities=frozenset(
                    {"project_shell", "repository_read", "code_edit", "web_research"}
                ),
                settings=Settings(environment="test"),
                step_types=frozenset({expected}),
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
            # A settings change after start must not increase N.
            await commit_step_outcome(
                session,
                token,
                outcome,
                recovery_policy=RecoveryPolicy(step_repair_cap=20, task_repair_cap=50),
            )

    def failure(index: int) -> StepOutcome:
        return StepOutcome(
            kind=OutcomeKind.BLOCKED,
            result={"finding": index},
            category="validation_failed" if failed_stage == "validate" else "review_failed",
            summary=f"finding {index}",
        )

    for index in range(repairs):
        await finish(failure(index), failed_stage)
        await finish(StepOutcome.succeeded(), "execute_agent" if report else "execute_code")
        if failed_stage == "review":
            await finish(StepOutcome.succeeded(), "validate")
    await finish(StepOutcome.succeeded() if passes else failure(repairs), failed_stage)
    if passes and failed_stage == "validate":
        await finish(StepOutcome.succeeded(), "review")
    async with factory() as session:
        run = await session.get(Run, run_id)
        assert run is not None
        assert run.status is (RunStatus.COMPLETED if passes else RunStatus.BLOCKED)
        steps = list(await session.scalars(select(Step).where(Step.run_id == run_id)))
        assert len([s for s in steps if "repair_for_step_id" in s.payload]) == repairs
        assert run.selected_route["recovery_policy"]["task_repair_cap"] == repairs
        assert all(s.step_type != "plan" for s in steps)
    await engine.dispose()


async def test_fixed_run_lease_recovery_and_cancellation_fence(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    task_id, interpretation_id = await seed_interpreted(factory, planned_coding_draft())
    async with factory.begin() as session:
        run = await materialize_run(
            session,
            task_id=task_id,
            workflow=compile_workflow(planned_coding_draft(), interpretation_id=interpretation_id),
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            prompt_revision=None,
            automatic_start=True,
        )
        run_id = run.id
        token = await claim_step(
            session, owner="crashed", lease_seconds=60, capabilities=frozenset({"host_admin"})
        )
        assert token is not None
        step = await session.get(Step, token.step.id)
        assert step is not None
        step.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    async with factory.begin() as session:
        assert await recover_expired_steps(session, batch_size=10) == 1
        recovered = await claim_step(
            session, owner="recovered", lease_seconds=60, capabilities=frozenset({"host_admin"})
        )
        assert recovered is not None and recovered.generation > token.generation
        await start_step(session, recovered)
    async with factory.begin() as session:
        task = await session.get(Task, task_id)
        assert task is not None
        await cancel_task(session, task_id, actor_id="1")
    async with factory.begin() as session:
        with pytest.raises(LeaseLost):
            await commit_step_outcome(session, recovered, StepOutcome.succeeded())
    async with factory() as session:
        run = await session.get(Run, run_id)
        assert run is not None and run.status is RunStatus.CANCELLED
    await engine.dispose()


async def test_legacy_planning_request_reaches_waiting_lead_once(postgres_dsn: str) -> None:
    from tests.integration.leadership.test_managed_goal_flow import _enable, _session
    from vuzol.leadership.model import LeadDecision
    from vuzol.leadership.worker import ProjectLeadWorker
    from vuzol.storage.models import TransactionalOutbox
    from vuzol.workflows.lead_events import publish_planning_request

    engine, factory = storage(postgres_dsn)
    await _enable(factory, await _session(factory))
    snapshots = []

    class Lead:
        async def decide(self, snapshot):
            snapshots.append(snapshot)
            return LeadDecision(profile_id="fixture")

    worker = ProjectLeadWorker(factory, owner="lead", model=Lead(), enabled=lambda: True)
    assert await worker.process_one()
    task_id, interpretation_id = await seed_interpreted(factory, planned_coding_draft())
    async with factory.begin() as session:
        task = await session.get(Task, task_id)
        assert task is not None
        task.project_id = "vuzol"
        await publish_planning_request(session, task, interpretation_id)
        await publish_planning_request(session, task, interpretation_id)
    assert await worker.process_one()
    assert snapshots[-1].result_context["event"]["wakeup_kind"] == "planning_requested"
    assert snapshots[-1].result_context["event"]["source_task_id"] == str(task_id)
    async with factory() as session:
        requests = list(
            await session.scalars(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["wakeup_kind"].as_string() == "planning_requested",
                )
            )
        )
        assert len(requests) == 1
        assert await session.scalar(select(Step.id).where(Step.step_type == "plan")) is None
    await engine.dispose()
