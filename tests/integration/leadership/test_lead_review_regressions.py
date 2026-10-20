"""Review probes. Assertions express the required behavior, not current bugs."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    PROJECT,
    _enable,
    _factory,
    _lead_service,
    _package_count,
    _session,
    _start_decision,
    _worker,
)

from vuzol.execution.result_apply import ResultApplyHandler
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.evidence import new_effect_blocker
from vuzol.leadership.model import LeadDecision
from vuzol.leadership.persistence import LeadershipCommandReceipt, ManagedGoal
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.storage.models import (
    Approval,
    Effect,
    MaterializationLink,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import (
    ApprovalStatus,
    DeliveryStatus,
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RunStatus,
    StepStatus,
    WorkPackagePauseReason,
    WorkPackageStatus,
)
from vuzol.workflows.application import Principal
from vuzol.workflows.ports import StepExecutionRequest

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def control(factory: object, goal_id: uuid.UUID, name: str) -> object:
    async with factory() as session:  # type: ignore[operator]
        goal = await session.get(ManagedGoal, goal_id)
        version = goal.version
    return await _lead_service(factory).apply_human(  # type: ignore[arg-type]
        Principal(OWNER, "telegram"),
        LeadCommand(
            name=name,
            project_id=PROJECT,
            goal_id=goal_id,
            expected_version=version,
            idempotency_key=f"review:{name}:{version}",
            payload={},
        ),
    )


class StartModel:
    async def decide(self, snapshot: object) -> LeadDecision:
        del snapshot
        return _start_decision()


async def test_pause_propagates_to_running_package(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        assert (await control(factory, goal_id, "pause_goal")).status == "applied"  # type: ignore[attr-defined]
        async with factory() as session:
            package = await session.scalar(select(WorkPackage))
            assert package.status.value == "paused", (
                f"goal paused but package is {package.status.value}"
            )


async def test_saved_response_invalidated_by_pause_resume(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        class CrashApply(ProjectLeadWorker):
            async def _apply_if_current(self, **kwargs: object) -> None:
                raise RuntimeError("crash-before-apply")

        first = CrashApply(factory, owner="before", model=StartModel(), enabled=lambda: True)
        with pytest.raises(RuntimeError, match="crash-before-apply"):
            await first.process_one()
        assert (await control(factory, goal_id, "pause_goal")).status == "applied"  # type: ignore[attr-defined]
        assert (await control(factory, goal_id, "resume_goal")).status == "applied"  # type: ignore[attr-defined]
        await _worker(factory, StartModel(), owner="after").process_one()
        assert await _package_count(factory) == 0, (
            "pre-pause response started a package after resume"
        )


async def test_ack_retry_does_not_repeat_mutations(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        class CrashAck(ProjectLeadWorker):
            raised = False

            async def _complete(self, session: object, token: object) -> None:
                if not self.raised:
                    self.raised = True
                    raise RuntimeError("crash-before-ack")
                await super()._complete(session, token)  # type: ignore[arg-type]

        worker = CrashAck(factory, owner="crash", model=StartModel(), enabled=lambda: True)
        with pytest.raises(RuntimeError, match="crash-before-ack"):
            await worker.process_one()
        async with factory() as session:
            before = (await session.get(ManagedGoal, goal_id)).version
        await worker.process_one()
        async with factory() as session:
            after = (await session.get(ManagedGoal, goal_id)).version
            count = await session.scalar(
                select(func.count())
                .select_from(LeadershipCommandReceipt)
                .where(LeadershipCommandReceipt.command_name == "propose_batch")
            )
        assert (after, count) == (before, 1), (
            f"retry mutated goal: versions {before}->{after}, proposals={count}"
        )


async def test_resume_wakes_parked_goal(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        class Idle:
            async def decide(self, snapshot: object) -> LeadDecision:
                del snapshot
                return LeadDecision()

        await _worker(factory, Idle()).process_one()
        await control(factory, goal_id, "pause_goal")
        await control(factory, goal_id, "resume_goal")
        worker = _worker(factory, StartModel())
        for _ in range(3):
            await worker.process_one()
        assert await _package_count(factory) == 1, "resume did not create a fresh wakeup"


async def test_goal_description_requires_actual_evidence(
    postgres_dsn: str, tmp_path: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.integration.leadership.test_managed_goal_flow import (
        run_managed_goal_flow,
    )

    # Fake lead mistakenly plans marker files, not OAuth. Existing execution,
    # review, apply and completion all run unchanged; no status is forged here.
    await run_managed_goal_flow(
        postgres_dsn,
        tmp_path,
        monkeypatch,  # type: ignore[arg-type]
        request="Implement working OAuth login",
        expected_completed=False,
    )
    async with _factory(postgres_dsn) as factory, factory() as session:
        goal = await session.scalar(select(ManagedGoal))
        assert goal.lifecycle != "completed", "OAuth goal completed by writing marker files"


async def _pause_for_outcome(factory: object, reason: WorkPackagePauseReason) -> None:
    async with factory.begin() as session:  # type: ignore[attr-defined]
        package = await session.scalar(select(WorkPackage))
        assert package is not None
        package.status = WorkPackageStatus.PAUSED
        package.pause_reason = reason


async def _assert_outcome_wakes(postgres_dsn: str, reason: WorkPackagePauseReason) -> None:
    class Noted:
        calls = 0

        async def decide(self, snapshot: object) -> LeadDecision:
            self.calls += 1
            del snapshot
            return LeadDecision()

    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await _pause_for_outcome(factory, reason)
        model = Noted()
        worker = _worker(factory, model, owner="wake")
        for _ in range(3):
            await worker.process_one()
        assert model.calls == 1, f"a {reason.value} package did not wake the lead"


async def test_failed_package_wakes_the_lead(postgres_dsn: str) -> None:
    await _assert_outcome_wakes(postgres_dsn, WorkPackagePauseReason.ITEM_FAILED)


async def test_blocked_package_wakes_the_lead(postgres_dsn: str) -> None:
    await _assert_outcome_wakes(postgres_dsn, WorkPackagePauseReason.ITEM_BLOCKED)


async def test_late_response_after_lease_takeover(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))

        class Idle:
            async def decide(self, snapshot: object) -> LeadDecision:
                del snapshot
                return LeadDecision()

        class Late:
            async def decide(self, snapshot: object) -> LeadDecision:
                del snapshot
                past = datetime.now(UTC) - timedelta(seconds=5)
                async with factory.begin() as session:
                    goal = await session.scalar(select(ManagedGoal))
                    assert goal is not None
                    goal.lease_until = past
                    item = await session.scalar(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "project_lead",
                            TransactionalOutbox.status == DeliveryStatus.LEASED,
                        )
                    )
                    assert item is not None
                    item.lease_expires_at = past
                await _worker(factory, Idle(), owner="takeover").process_one()
                return _start_decision()

        await _worker(factory, Late(), owner="first").process_one()
        assert await _package_count(factory) == 0
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            proposals = await session.scalar(
                select(func.count())
                .select_from(LeadershipCommandReceipt)
                .where(LeadershipCommandReceipt.command_name == "propose_batch")
            )
            assert goal is not None and goal.lease_epoch == 2
            assert proposals == 0


async def test_revoke_before_effect_intent_records_nothing(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        async with factory() as session:
            link = await session.scalar(select(MaterializationLink))
            assert link is not None
            task_id = link.task_id
            assert await new_effect_blocker(session, task_id) is None
        assert (await control(factory, goal_id, "revoke_delegation")).status == "applied"  # type: ignore[attr-defined]
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            run_id = uuid.uuid4()
            step_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            session.add(
                Run(
                    id=run_id,
                    task_id=task.id,
                    workflow_type="coding",
                    workflow_version="1",
                    status=RunStatus.RUNNING,
                    budget_mode="balanced",
                    configuration_revision="c" * 64,
                    policy_revision="d" * 64,
                )
            )
            await session.flush()
            session.add(
                Step(
                    id=step_id,
                    run_id=run_id,
                    ordinal=4,
                    dependency_metadata={"predecessor_ordinals": [3]},
                    step_type="approval",
                    queue_class=QueueClass.PRIVILEGED,
                    status=StepStatus.RUNNING,
                    required_capabilities=["git"],
                    payload={"approval_id": str(approval_id)},
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.IDEMPOTENT,
                    max_attempts=2,
                    timeout_seconds=120,
                    lease_owner="applier",
                    lease_generation=1,
                )
            )
            await session.flush()
            session.add(
                Approval(
                    id=approval_id,
                    step_id=step_id,
                    action_envelope_hash="a" * 64,
                    requested_action="apply_result",
                    normalized_target="vuzol:managed",
                    human_summary="reviewed change",
                    token_hash="b" * 64,
                    status=ApprovalStatus.APPROVED,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
        request = StepExecutionRequest(
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            step_type="approval",
            payload={"approval_id": str(approval_id)},
            timeout_seconds=120,
            lease=LeaseToken(
                step=StepRecord(
                    id=step_id,
                    run_id=run_id,
                    status=StepStatus.RUNNING,
                    lease_generation=1,
                    lease_owner="applier",
                    lease_expires_at=None,
                ),
                owner="applier",
                generation=1,
            ),
        )
        handler = ResultApplyHandler(factory, SimpleNamespace(), SimpleNamespace())
        with pytest.raises(ValueError, match=r"delegation_revoked|managed_goal_inactive"):
            await handler._record_intent(
                request,
                approval_id=approval_id,
                envelope={
                    "result_commit": "c" * 40,
                    "target_branch": "vuzol-managed/vuzol/goal",
                    "expected_target_head": "d" * 40,
                },
                worktree=SimpleNamespace(project_id=PROJECT, id=uuid.uuid4()),  # type: ignore[arg-type]
            )
        async with factory() as session:
            effects = await session.scalar(select(func.count()).select_from(Effect))
            assert effects == 0
