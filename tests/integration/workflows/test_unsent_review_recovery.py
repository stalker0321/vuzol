"""Explicit terminal-review recovery never regenerates source or erases charges."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config import Settings
from vuzol.storage.models import Event, ProviderBudgetReservation, Run, Step, Task, Worktree
from vuzol.storage.types import (
    BudgetReservationStatus,
    IdempotencyClass,
    QueueClass,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.workflows.application import Principal, TaskControlService
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.worker import CompleteHandler, WorkflowWorker

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def seed(factory, tmp_path, *, conservative=False):
    task, run_id, step = await seed_task_run_step(factory, step_type="review")
    async with factory.begin() as session:
        model = await session.get(Task, task.id)
        model.status = TaskStatus.FAILED
        model.project_id = "p1"
        model.budget_epoch = 3
        run = await session.get(Run, run_id)
        run.status = RunStatus.FAILED
        target = await session.get(Step, step.id)
        target.ordinal = 3
        target.status = StepStatus.FAILED
        target.failure_category = "handler_exception"
        target.failure_summary = "LeaseLost"
        target.attempt_count = target.max_attempts = 3
        target.lease_generation = 3
        target.executor_profile_id = "grok-a"
        target.payload = {
            "cli_reviewer_pin": {"profile_id": "grok-a", "profile_revision": "old"},
            "executor_model_override": "old",
            "keep": "raw context",
        }
        session.add(
            Worktree(
                task_id=task.id,
                run_id=run_id,
                project_id="p1",
                repository_identity_hash="c" * 64,
                base_commit="a" * 40,
                result_commit="b" * 40,
                default_branch="main",
                expected_target_head="a" * 40,
                branch="retained",
                path=str(tmp_path / "worktree"),
                owner="executor",
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                retention_until=datetime.now(UTC) + timedelta(days=1),
            )
        )
        for ordinal, kind, status in (
            (1, "execute_code", StepStatus.COMPLETED),
            (2, "validate", StepStatus.COMPLETED),
            (4, "produce_artifacts", StepStatus.PENDING),
        ):
            session.add(
                Step(
                    run_id=run_id,
                    ordinal=ordinal,
                    step_type=kind,
                    status=status,
                    idempotency_class=IdempotencyClass.READ_ONLY,
                    retry_class="never",
                    queue_class=QueueClass.LIGHT,
                    timeout_seconds=30,
                    attempt_count=1 if ordinal < 3 else 0,
                    dependency_metadata={"predecessor_ordinals": [3]} if ordinal == 4 else {},
                )
            )
        reservation = ProviderBudgetReservation(
            task_id=task.id,
            run_id=run_id,
            step_id=step.id,
            profile_id="grok-a",
            budget_epoch=3,
            provider_attempt=19,
            reserved_input_tokens=100,
            reserved_output_tokens=100,
            reserved_cost_units=0,
            reserved_quota_units=1,
            status=BudgetReservationStatus.CONSERVATIVE
            if conservative
            else BudgetReservationStatus.RESERVED,
        )
        session.add(reservation)
        await session.flush()
        return task.id, run_id, step.id, model.version, reservation.id


@pytest.mark.parametrize("conservative", [False, True])
@pytest.mark.parametrize("review_passes", [False, True])
async def test_recovers_only_review_once_and_retains_history(
    postgres_dsn, tmp_path, conservative, review_passes
):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id, step_id, version, reservation_id = await seed(
            factory, tmp_path, conservative=conservative
        )
        service = TaskControlService(factory)
        kwargs = dict(
            task_id=task_id,
            command="rereview",
            principal=Principal(1, "cli"),
            expected_task_version=version,
            step_id=step_id,
        )
        result = await service.execute(**kwargs)
        assert result.status == "retrying" and result.version == version + 1
        with pytest.raises(ValueError, match="stale"):
            await service.execute(**kwargs)
        async with factory() as session:
            target = await session.get(Step, step_id)
            assert target.payload == {"keep": "raw context", "review_only_recovery": True}
            assert (target.attempt_count, target.max_attempts, target.lease_generation) == (3, 4, 3)
            assert (await session.get(Task, task_id)).budget_epoch == 4
            reservation = await session.get(ProviderBudgetReservation, reservation_id)
            assert reservation.status is (
                BudgetReservationStatus.CONSERVATIVE
                if conservative
                else BudgetReservationStatus.RELEASED
            )
            events = (
                await session.scalars(
                    select(Event).where(Event.event_type == "step.cli_reviewer_rebind_requested")
                )
            ).all()
            assert (
                len(events) == 1 and events[0].payload["previous_pin"]["profile_revision"] == "old"
            )

        class FindingsHandler:
            async def execute(self, request, cancellation):
                return StepOutcome(
                    kind=OutcomeKind.BLOCKED,
                    result={},
                    category="review_changes_required",
                    summary="needs changes",
                    unknown_effects=False,
                )

        worker = WorkflowWorker(
            Settings(environment="test"),
            factory,
            owner="reviewer",
            handlers={"review": CompleteHandler() if review_passes else FindingsHandler()},
            profile_limits={"grok-a": 1},
            queue_classes=frozenset({QueueClass.LIGHT}),
        )
        assert await worker.process_one()
        assert not await worker.process_one()
        async with factory() as session:
            steps = (
                await session.scalars(
                    select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
                )
            ).all()
            assert [s.status for s in steps] == [StepStatus.COMPLETED] * 2 + (
                [StepStatus.COMPLETED, StepStatus.QUEUED]
                if review_passes
                else [StepStatus.BLOCKED, StepStatus.PENDING]
            )
            assert [s.attempt_count for s in steps] == [1, 1, 4, 0]
    finally:
        await engine.dispose()


async def test_existing_owner_retry_rebinds_blocked_review(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id, step_id, version, _ = await seed(factory, tmp_path)
        async with factory.begin() as session:
            (await session.get(Task, task_id)).status = TaskStatus.BLOCKED
            (await session.get(Run, run_id)).status = RunStatus.BLOCKED
            target = await session.get(Step, step_id)
            target.status = StepStatus.BLOCKED
            target.failure_category = "independent_review_required"
        result = await TaskControlService(factory).execute(
            task_id=task_id,
            command="retry",
            principal=Principal(1, "telegram"),
            expected_task_version=version,
            step_id=step_id,
        )
        assert result.status == "retrying"
        async with factory() as session:
            target = await session.get(Step, step_id)
            assert target.payload == {"keep": "raw context"}
            assert target.executor_profile_id == "grok-a"
            assert target.max_attempts == 4 and target.attempt_count == 3
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("nonowner", "owner"),
        ("stale", "stale"),
        ("telegram", "operator CLI"),
        ("unknown", "prelaunch"),
        ("unvalidated", "validated source"),
        ("unpinned", "prelaunch"),
        ("active", "active work"),
        ("delivered", "validated source"),
        ("other_failure", "prelaunch"),
        ("transport", "transport evidence"),
    ],
)
async def test_refuses_unsafe_recovery(postgres_dsn, tmp_path, mutation, match):
    from vuzol.storage.models import SupervisedProcess

    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id, step_id, version, _ = await seed(factory, tmp_path)
        async with factory.begin() as session:
            target = await session.get(Step, step_id)
            steps = (
                await session.scalars(
                    select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
                )
            ).all()
            if mutation == "unknown":
                target.unknown_effects = True
            elif mutation == "unvalidated":
                steps[1].status = StepStatus.PENDING
            elif mutation == "unpinned":
                target.payload = {}
            elif mutation == "active":
                steps[0].status = StepStatus.RUNNING
            elif mutation == "delivered":
                steps[3].attempt_count = 1
            elif mutation == "other_failure":
                target.failure_summary = "RuntimeError"
            elif mutation == "transport":
                tree = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
                session.add(
                    SupervisedProcess(
                        task_id=task_id,
                        run_id=run_id,
                        step_id=step_id,
                        worktree_id=tree.id,
                        profile_id="grok-a",
                        lease_generation=3,
                        provider_attempt=19,
                        idempotency_key="d" * 64,
                        command_envelope_hash="e" * 64,
                        command_envelope={},
                        sandbox_spec_hash="f" * 64,
                        container_runtime="rootless-docker",
                        image_digest="pinned",
                        working_directory="/workspace",
                        status="exited",
                    )
                )
        with pytest.raises(ValueError, match=match):
            await TaskControlService(factory).execute(
                task_id=task_id,
                command="rereview",
                principal=Principal(
                    2 if mutation == "nonowner" else 1,
                    "telegram" if mutation == "telegram" else "cli",
                ),
                expected_task_version=version - 1 if mutation == "stale" else version,
                step_id=step_id,
            )
        async with factory() as session:
            assert (await session.get(Task, task_id)).status is TaskStatus.FAILED
            target = await session.get(Step, step_id)
            assert target.status is StepStatus.FAILED and target.max_attempts == 3
    finally:
        await engine.dispose()
