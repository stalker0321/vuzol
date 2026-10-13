"""Same-task environmental recovery without starting another model repair."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tests.integration.projects.test_capability_dependency_policy import (
    _approve,
    _claim_applier,
    _installer,
    _lease,
)
from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config import DependencyProvisioningSettings
from vuzol.project_environment import environment_hash
from vuzol.projects.capability_provisioning import CapabilityProvisioningHandler
from vuzol.projects.dependency_provisioning import DependencyProvisioningHandler
from vuzol.storage.models import Event, ProjectEnvironmentRevision, Run, Step, Task, Worktree
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import (
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.workflows.application import Principal, TaskControlService
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.service import commit_step_outcome

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize(
    "case",
    (
        "ok",
        "legacy_base",
        "finalized",
        "wrong_owner",
        "stale",
        "repair_started",
        "unknown_effects",
        "api",
    ),
)
def test_operator_recovery_is_fenced_and_does_not_spend_another_model_attempt(
    postgres_dsn: str,
    tmp_path: Path,
    case: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_record, run_id, validation_record = await seed_task_run_step(
            factory,
            step_type="validate",
            step_status=StepStatus.BLOCKED,
        )
        root = tmp_path / "worktrees"
        path = root / "site"
        path.mkdir(parents=True)
        (path / "Makefile").write_text("test:\n\tnode --test\n")
        started = datetime.now(UTC) - timedelta(hours=1)
        contract: dict[str, object] = {"components": {}, "capabilities": {}}
        async with factory.begin() as session:
            task = await session.get(Task, task_record.id)
            run = await session.get(Run, run_id)
            target = await session.get(Step, validation_record.id)
            assert task is not None and run is not None and target is not None
            task.status = TaskStatus.QUOTA_EXHAUSTED
            task.project_id = "site"
            task.version = 19
            task.budget_epoch = 1
            task.spec_revision = "c" * 64
            run.status = RunStatus.BLOCKED
            run.started_at = started
            target.failure_category = "validation_gate_failed"
            target.failure_summary = "node: not found"
            target.unknown_effects = case == "unknown_effects"
            for ordinal, kind, status in (
                (0, "execute_code", StepStatus.COMPLETED),
                (2, "execute_code", StepStatus.BLOCKED),
            ):
                step = Step(
                    run_id=run_id,
                    ordinal=ordinal,
                    step_type=kind,
                    status=status,
                    queue_class=QueueClass.HEAVY,
                    required_capabilities=[],
                    dependency_metadata={"template_key": "repair_code", "repair_for_ordinal": 1},
                    payload={"repair_for_step_id": str(target.id)} if ordinal == 2 else {},
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                    max_attempts=1,
                    timeout_seconds=60,
                    attempt_count=1 if ordinal == 0 or case == "repair_started" else 0,
                )
                session.add(step)
                await session.flush()
                if ordinal == 2:
                    repair_id = step.id
                    target.payload = {"repair_scheduled_step_id": str(step.id), "repair_count": 1}
            session.add(
                ProjectEnvironmentRevision(
                    project_id="site",
                    revision_number=1,
                    source="detected",
                    contract=contract,
                    content_hash=environment_hash(contract),
                )
            )
            session.add(
                Worktree(
                    task_id=task.id,
                    run_id=run_id,
                    project_id="site",
                    repository_identity_hash="a" * 64,
                    base_commit="b" * 40,
                    result_commit=(
                        "b" * 40
                        if case == "legacy_base"
                        else "d" * 40
                        if case == "finalized"
                        else None
                    ),
                    expected_target_head="b" * 40,
                    default_branch="main",
                    branch="site-work",
                    path=str(path),
                    owner="test",
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
        service = TaskControlService(factory)
        args = {
            "task_id": task_record.id,
            "command": "revalidate",
            "principal": Principal(
                2 if case == "wrong_owner" else 1, "api" if case == "api" else "cli"
            ),
            "expected_task_version": 18 if case == "stale" else 19,
            "step_id": validation_record.id,
        }
        if case not in {"ok", "legacy_base"}:
            with pytest.raises(ValueError):
                await service.execute(**args)
            async with factory() as session:
                repair = await session.get(Step, repair_id)
                assert repair is not None and repair.status is StepStatus.BLOCKED
            await engine.dispose()
            return
        result = await service.execute(**args)
        assert result.applied
        with pytest.raises(ValueError):
            await service.execute(**args)
        async with factory() as session:
            task = await session.get(Task, task_record.id)
            run = await session.get(Run, run_id)
            repair = await session.get(Step, repair_id)
            assert task is not None and task.budget_epoch == 1 and task.spec_revision == "c" * 64
            assert run is not None and run.started_at == started
            assert (
                repair is not None
                and repair.status is StepStatus.CANCELLED
                and repair.attempt_count == 0
            )
            event = await session.scalar(
                select(Event).where(Event.event_type == "task.validation_environment_recovery")
            )
            assert (
                event is not None
                and event.actor_type == "operator"
                and event.payload["ingress_source"] == "cli"
            )
            preparation = await session.scalar(
                select(Step).where(Step.run_id == run_id, Step.step_type == "ensure_dependencies")
            )
            assert preparation is not None
            prep_id = preparation.id
        # Simulate only a scheduler claim, then use real handlers/DB transitions.
        async with factory.begin() as session:
            prep = await session.get(Step, prep_id)
            assert prep is not None
            prep.status = StepStatus.RUNNING
            prep.lease_owner = "test"
            prep.lease_generation = 1
            record = StepRecord(prep.id, run_id, prep.status, 1, "test", None)
        request = StepExecutionRequest(
            task_record.id,
            run_id,
            prep_id,
            "ensure_dependencies",
            {},
            1800,
            LeaseToken(record, "test", 1),
        )
        caps = CapabilityProvisioningHandler(factory, _installer(tmp_path), worktree_root=root)
        handler = DependencyProvisioningHandler(
            factory,
            DependencyProvisioningSettings(enabled=False),
            None,
            worktree_root=root,
            capabilities=caps,
        )
        outcome = await handler.execute(request, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, request.lease, outcome)
        preflight = await _claim_applier(factory, request)
        approval = await caps.execute(preflight, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, preflight.lease, approval)
        async with factory() as session:
            task = await session.get(Task, task_record.id)
            assert task is not None and task.status is TaskStatus.WAITING_APPROVAL
        # TEST-only approval/bundle, then the real handoff back to validation.
        # Production recovery must stop at the owner's Telegram approval.
        await _approve(factory, approval.result["approval_id"])
        preflight = await _lease(factory, preflight)
        installed = await caps.execute(preflight, CancellationContext())
        assert installed.kind is OutcomeKind.SUCCEEDED
        async with factory.begin() as session:
            await commit_step_outcome(session, preflight.lease, installed)
        request = await _lease(factory, request)
        prepared = await handler.execute(request, CancellationContext())
        assert prepared.kind is OutcomeKind.SUCCEEDED
        async with factory.begin() as session:
            await commit_step_outcome(session, request.lease, prepared)
        async with factory() as session:
            task = await session.get(Task, task_record.id)
            target = await session.get(Step, validation_record.id)
            repair = await session.get(Step, repair_id)
            assert task is not None and task.budget_epoch == 1 and task.spec_revision == "c" * 64
            assert target is not None and target.status is StepStatus.QUEUED
            assert repair is not None and repair.status is StepStatus.CANCELLED
        await engine.dispose()

    asyncio.run(scenario())
