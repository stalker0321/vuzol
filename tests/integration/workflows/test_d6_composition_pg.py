"""D6 delivery-criterion composition tests on PostgreSQL.

Read-only research plan and coding multi-task plan traverse
goal -> candidate -> approval -> effects -> verified goal with crash
injection and reproducible cost attribution. No live models: deterministic
test handlers execute steps; measurement comes from recorded rows.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config import LaunchMode
from vuzol.config.models import Capability, CostClass, ProviderProfileConfig
from vuzol.config.settings import HardLimits
from vuzol.interpretation.domain import (
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.providers.budgets import (
    AccountingContext,
    estimate_reservation,
    reserve_invocation_budget,
    settle_invocation_budget,
    usage_totals_by_purpose,
)
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.leasing import claim_step
from vuzol.storage.models import (
    Approval,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    Worktree,
)
from vuzol.storage.types import (
    ApprovalStatus,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.controls import decide_result
from vuzol.workflows.worker import CompleteHandler, WorkflowWorker

from ._test_runtime_helpers import (
    RegistryDocument,
    RuntimeConfiguration,
    Settings,
    WorkflowDispatcher,
    build_bundle,
    seed_interpreted,
)

pytestmark = pytest.mark.postgresql

CODING_HANDLERS = {
    "plan": CompleteHandler(),
    "ensure_capabilities": CompleteHandler(),
    "prepare_context": CompleteHandler(),
    "prepare_worktree": CompleteHandler(),
    "execute_code": CompleteHandler(),
    "ensure_dependencies": CompleteHandler(),
    "validate": CompleteHandler(),
    "review": CompleteHandler(),
    "produce_artifacts": CompleteHandler(),
    "build_static": CompleteHandler(),
    "publish_preview": CompleteHandler(),
    "acceptance": CompleteHandler(),
    "publish_static": CompleteHandler(),
    "finalize": CompleteHandler(),
}

RESEARCH_HANDLERS = {
    "research_execute": CompleteHandler(),
    "synthesize": CompleteHandler(),
    "finalize": CompleteHandler(),
}

BASE_COMMIT = "a" * 40
RESULT_COMMIT = "b" * 40
DIFF_HASH = "c" * 64


def _research_draft() -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.RESEARCH,
        operation=TaskOperation.INSPECT,
        goal="Survey the repository structure",
        task_summary="Survey the repository structure",
        suggested_complexity=SuggestedComplexity.SMALL,
        suggested_risk=RiskLevel.LOW,
        needs_planning=False,
        needs_clarification=False,
        normalized_title="Survey repository",
    )


def _coding_draft() -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.CODING,
        operation=TaskOperation.MODIFY,
        project_id="vuzol",
        goal="Implement the requested change",
        task_summary="Implement the requested change",
        suggested_complexity=SuggestedComplexity.MEDIUM,
        suggested_risk=RiskLevel.MEDIUM,
        needs_planning=True,
        needs_clarification=False,
        normalized_title="Implement change",
    )


async def _dispatch(factory: object, task_id: uuid.UUID, interpretation_id: uuid.UUID) -> None:
    from pathlib import Path as _Path

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    async with typed.begin() as session:
        session.add(
            TransactionalOutbox(
                destination="workflow_dispatch",
                operation_type="dispatch_interpretation",
                linked_entity_type="interpretation",
                linked_entity_id=interpretation_id,
                idempotency_key=f"workflow:dispatch:{interpretation_id}",
                payload={"task_id": str(task_id)},
            )
        )
    settings = Settings(environment="test")
    auto = settings.model_copy(
        update={
            "interpretation": settings.interpretation.model_copy(
                update={
                    "automatic_execution_enabled": True,
                    "evaluation_report_file": _Path("tests/fixtures/experiments/corpus.v1.json"),
                }
            )
        }
    )
    runtime = RuntimeConfiguration(
        settings=auto, registries=build_bundle(RegistryDocument(), settings)
    )
    dispatcher = WorkflowDispatcher(runtime, typed, owner="dispatcher")
    assert await dispatcher.process_one() is True


async def _drain(factory: object, handlers: dict[str, CompleteHandler], *, limit: int = 60) -> int:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    worker = WorkflowWorker(Settings(environment="test"), typed, owner="worker", handlers=handlers)
    processed = 0
    for _ in range(limit):
        if not await worker.process_one():
            break
        processed += 1
    return processed


def test_research_plan_reaches_verified_goal(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory, _research_draft())
        await _dispatch(factory, task_id, interpretation_id)
        assert await _drain(factory, RESEARCH_HANDLERS) >= 3
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task is not None and task.status is TaskStatus.COMPLETED
            (run,) = tuple((await session.scalars(select(Run))).all())
            assert run.status is RunStatus.COMPLETED
            assert run.workflow_type == "research"
        await engine.dispose()

    asyncio.run(scenario())


def test_coding_plan_survives_worker_crash_and_approves(postgres_dsn: str) -> None:
    async def scenario() -> None:

        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory, _coding_draft())
        await _dispatch(factory, task_id, interpretation_id)

        # Crash injection: a foreign worker claims a step and dies holding
        # the lease (lease expiry simulates the process kill).
        from vuzol.workflows.recovery import recover_expired_steps

        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="crashed-worker",
                lease_seconds=60,
                capabilities=frozenset(value.value for value in Capability),
            )
            assert token is not None
            doomed = await session.get(Step, token.step.id)
            assert doomed is not None
            crashed_key = doomed.step_type
            doomed = await session.get(Step, token.step.id)
            assert doomed is not None
            doomed.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        async with factory.begin() as session:
            recovered = await recover_expired_steps(session, batch_size=10)
            assert recovered >= 1

        # Drive to the approval gate, recording measured validation first.
        worker_ready = False
        for _ in range(60):
            async with factory.begin() as session:
                validate = await session.scalar(
                    select(Step).where(
                        Step.status == StepStatus.COMPLETED, Step.step_type == "validate"
                    )
                )
                run = await session.scalar(select(Run))
                assert run is not None
                prior = validate.result if validate is not None else None
                manifest = prior.get("structured_output") if isinstance(prior, dict) else None
                if validate is not None and not isinstance(manifest, dict):
                    validate.result = {
                        "structured_output": {
                            "result_commit": RESULT_COMMIT,
                            "base_commit": BASE_COMMIT,
                            "gates": [{"exit_code": 0}],
                        }
                    }
                review = await session.scalar(
                    select(Step).where(
                        Step.status == StepStatus.COMPLETED, Step.step_type == "review"
                    )
                )
                prior_review = review.result if review is not None else None
                review_manifest = (
                    prior_review.get("structured_output")
                    if isinstance(prior_review, dict)
                    else None
                )
                if review is not None and not isinstance(review_manifest, dict):
                    review.result = {
                        "structured_output": {
                            "base_commit": BASE_COMMIT,
                            "result_commit": RESULT_COMMIT,
                            "diff_hash": DIFF_HASH,
                            "verdict": "pass",
                            "findings": [],
                        }
                    }
                build = await session.scalar(
                    select(Step).where(
                        Step.status == StepStatus.COMPLETED, Step.step_type == "build_static"
                    )
                )
                build_result = build.result if build is not None else None
                if (
                    build is not None
                    and isinstance(build_result, dict)
                    and build_result.get("status") != "built"
                ):
                    build.result = {
                        "status": "built",
                        "source_commit": RESULT_COMMIT,
                        "artifact_hash": "d" * 64,
                    }
                produced = await session.scalar(
                    select(Step).where(
                        Step.status == StepStatus.COMPLETED,
                        Step.step_type == "produce_artifacts",
                    )
                )
                produced_result = produced.result if produced is not None else None
                if (
                    produced is not None
                    and isinstance(produced_result, dict)
                    and produced_result.get("status") != "produced"
                ):
                    produced.result = {
                        "status": "produced",
                        "source_commit": RESULT_COMMIT,
                        "artifacts": [
                            {
                                "artifact_id": str(uuid.uuid4()),
                                "artifact_type": "static_bundle",
                                "content_hash": "d" * 64,
                                "size_bytes": 10,
                            }
                        ],
                    }
                worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
                if worktree is None and validate is not None:
                    session.add(
                        Worktree(
                            task_id=task_id,
                            run_id=run.id,
                            project_id="vuzol",
                            repository_identity_hash="r" * 64,
                            base_commit=BASE_COMMIT,
                            default_branch="main",
                            expected_target_head=BASE_COMMIT,
                            branch="wt-1",
                            path=f"memory-wt-{uuid.uuid4()}",
                            owner="test",
                            delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                            result_commit=RESULT_COMMIT,
                            diff_hash=DIFF_HASH,
                            retention_until=datetime.now(UTC) + timedelta(days=1),
                        )
                    )
                approval = await session.scalar(select(Step).where(Step.step_type == "approval"))
                if approval is not None and approval.status is StepStatus.WAITING_APPROVAL:
                    worker_ready = True
                    break
            if not await _drain(factory, CODING_HANDLERS, limit=1):
                break
        assert worker_ready, "approval gate was never reached"
        assert crashed_key in CODING_HANDLERS

        async with factory.begin() as session:
            approval = await session.scalar(select(Step).where(Step.step_type == "approval"))
            assert approval is not None
            record = await session.scalar(select(Approval).where(Approval.step_id == approval.id))
            assert record is not None and record.status is ApprovalStatus.PENDING
            await decide_result(session, record.id, decision="approve", deciding_user_id=1)
        # The applier chain (ResultApplyHandler, not run here) executes the
        # approved step through the standard claim/start/commit path, which
        # activates its successors; emulate that boundary.
        from vuzol.storage.leasing import start_step
        from vuzol.workflows.domain import OutcomeKind, StepOutcome
        from vuzol.workflows.service import commit_step_outcome

        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="applier",
                lease_seconds=60,
                capabilities=frozenset(value.value for value in Capability),
                step_types=frozenset({"approval"}),
            )
            assert token is not None
            await start_step(session, token)
            await commit_step_outcome(
                session, token, StepOutcome(kind=OutcomeKind.SUCCEEDED, result={})
            )
        assert await _drain(factory, CODING_HANDLERS) >= 1
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task is not None and task.status is TaskStatus.COMPLETED
            (run,) = tuple((await session.scalars(select(Run))).all())
            assert run.status is RunStatus.COMPLETED
            runs = tuple((await session.scalars(select(Run))).all())
            assert len(runs) == 1, "crash recovery must not duplicate the run"
        await engine.dispose()

    asyncio.run(scenario())


def test_old_pending_approval_completes_only_its_action(postgres_dsn: str) -> None:
    async def scenario() -> None:
        import hashlib
        import json

        engine, factory = storage(postgres_dsn)
        _task, _run, step_record = await seed_task_run_step(factory)
        envelope = {"step_id": str(step_record.id), "action": "apply_result"}
        digest = hashlib.sha256(
            json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

        async def _waiting_approval() -> uuid.UUID:
            async with factory.begin() as session:
                step = await session.get(Step, step_record.id, with_for_update=True)
                assert step is not None
                step.status = StepStatus.WAITING_APPROVAL
                task = await session.get(Task, _task.id, with_for_update=True)
                assert task is not None
                task.status = TaskStatus.WAITING_APPROVAL
                step.payload = {**step.payload, "action_envelope": dict(envelope)}
                record = Approval(
                    step_id=step.id,
                    action_envelope_hash=digest,
                    requested_action="apply_result",
                    normalized_target="vuzol:main",
                    human_summary="apply",
                    token_hash=f"aa{uuid.uuid4().hex[:56]}",
                    status=ApprovalStatus.PENDING,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
                session.add(record)
                await session.flush()
                return record.id

        approval_id = await _waiting_approval()
        async with factory.begin() as session:
            await decide_result(session, approval_id, decision="approve", deciding_user_id=1)
            record = await session.get(Approval, approval_id)
            assert record is not None and record.status is ApprovalStatus.APPROVED
            with pytest.raises(ValueError, match="already decided"):
                await decide_result(session, approval_id, decision="approve", deciding_user_id=1)

        # A new action under the old approval fails closed: the stale pending
        # approval cannot complete anything but its own envelope.
        approval_id = await _waiting_approval()
        async with factory.begin() as session:
            step = await session.get(Step, step_record.id, with_for_update=True)
            assert step is not None
            step.payload = {
                **step.payload,
                "action_envelope": {"step_id": str(step.id), "action": "replan"},
            }
            with pytest.raises(ValueError, match="missing or has changed"):
                await decide_result(session, approval_id, decision="approve", deciding_user_id=1)
        await engine.dispose()

    asyncio.run(scenario())


def _cheap_profile() -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id="test-cheap",
        provider="test",
        model="cheap-1",
        launch_mode=LaunchMode.TOOL,
        credential_required=False,
        capabilities=frozenset({Capability.REPOSITORY_READ}),
        concurrency_limit=1,
        context_limit=10_000,
        output_limit=1_000,
        cost_class=CostClass.CHEAP,
        supported_task_types=frozenset({"general"}),
        input_cost_units_per_million=100.0,
        output_cost_units_per_million=200.0,
        quota_units_per_call=1.0,
    )


def test_cost_attribution_is_reproducible(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        profile = _cheap_profile()
        limits = HardLimits()
        totals = []
        task_ids = []
        async with UnitOfWork(factory) as uow:
            for _ in range(2):
                task_ids.append(
                    (
                        await uow.tasks.create(
                            user_id=1,
                            chat_id=-100,
                            original_text="cost probe",
                            task_type="general",
                        )
                    ).id
                )
        for task_id in task_ids:
            async with factory.begin() as session:
                estimate = estimate_reservation(profile, input_tokens=1_000, output_tokens=500)
                reservation = await reserve_invocation_budget(
                    session,
                    invocation_id=uuid.uuid4(),
                    profile=profile,
                    estimate=estimate,
                    limits=limits,
                    task_id=task_id,
                    accounting=AccountingContext(purpose="execute", pricing_revision="t1"),
                )
                await settle_invocation_budget(
                    session,
                    reservation=reservation,
                    profile=profile,
                    usage=NormalizedUsage(input_tokens=1_000, output_tokens=500, duration_ms=5),
                    provider_request_id="req-1",
                    outcome="succeeded",
                )
        async with factory() as session:
            for task_id in task_ids:
                totals.append(await usage_totals_by_purpose(session, task_id=task_id))
        assert totals[0] == totals[1]
        assert sum(cost for _purpose, cost, _count in totals[0]) > Decimal("0")
        await engine.dispose()

    asyncio.run(scenario())
