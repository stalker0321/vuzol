"""Explicit executor incompatibility blocks before work and never bypasses prerequisites."""

import asyncio

import pytest
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from tests.integration.workflows._test_runtime_helpers import seed_interpreted, simple_draft
from tests.unit.providers.test_pi_adapter import profile
from vuzol.config import Capability, RegistryDocument, RuntimeConfiguration, Settings, build_bundle
from vuzol.interpretation.domain import TaskType
from vuzol.storage.models import (
    Event,
    ProviderBudgetReservation,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    Worktree,
)
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
from vuzol.workflows.controls import retry_blocked_step
from vuzol.workflows.dispatch import WorkflowDispatcher

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize(
    "kind,compatible", [("architecture", False), ("architecture", True), ("coding", True)]
)
def test_assigned_executor_is_checked_before_any_work(postgres_dsn, kind, compatible):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        assigned = profile().model_copy(
            update={
                "id": "assigned",
                "supported_task_types": frozenset({kind if compatible else "coding"}),
                "capabilities": frozenset(
                    {Capability.REPOSITORY_READ, Capability.CODE_EDIT, Capability.PROJECT_SHELL}
                ),
            }
        )
        settings = Settings(environment="test")
        settings = settings.model_copy(
            update={
                "execution": settings.execution.model_copy(
                    update={"default_executor_profile_id": "assigned"}
                ),
            }
        )
        runtime = RuntimeConfiguration(
            settings=settings,
            registries=build_bundle(
                RegistryDocument(profiles=(assigned,)),
                settings,
                validate_profile_credentials=False,
            ),
        )
        draft = simple_draft().model_copy(update={"task_type": TaskType(kind)})
        task_id, interpretation_id = await seed_interpreted(factory, draft)
        try:
            async with factory.begin() as session:
                task = await session.get(Task, task_id)
                task.task_type = kind
                session.add(
                    TransactionalOutbox(
                        destination="workflow_dispatch",
                        operation_type="dispatch_interpretation",
                        linked_entity_type="interpretation",
                        linked_entity_id=interpretation_id,
                        idempotency_key=f"dispatch:{interpretation_id}",
                        payload={},
                    )
                )
            assert await WorkflowDispatcher(runtime, factory, owner="preflight").process_one()
            async with factory() as session:
                run = (await session.scalars(select(Run))).one()
                task = await session.get(Task, task_id)
                steps = (await session.scalars(select(Step).order_by(Step.ordinal))).all()
                assert all(step.attempt_count == 0 for step in steps)
                assert run.status is (RunStatus.CREATED if compatible else RunStatus.BLOCKED)
                assert task.status is (TaskStatus.INTERPRETED if compatible else TaskStatus.BLOCKED)
                assert await session.scalar(select(func.count()).select_from(Worktree)) == 0
                assert (
                    await session.scalar(
                        select(func.count()).select_from(ProviderBudgetReservation)
                    )
                    == 0
                )
                if not compatible:
                    blocked = next(s for s in steps if s.status is StepStatus.BLOCKED)
                    assert blocked.step_type == "prepare_context"
                    assert blocked.failure_category == "executor_route_unavailable"
                    assert (
                        "assigned" in blocked.failure_summary
                        and "architecture" in blocked.failure_summary
                    )
                    assert (
                        await session.scalar(
                            select(Event.id).where(
                                Event.event_type == "workflow.executor_preflight_failed"
                            )
                        )
                        is not None
                    )
                    blocked_id = blocked.id
            if not compatible:
                async with factory.begin() as session:
                    await retry_blocked_step(session, blocked_id, actor_id="1")
                    entry = await session.get(Step, blocked_id)
                    execute = await session.scalar(
                        select(Step).where(Step.step_type == "execute_agent")
                    )
                    assert entry.status is StepStatus.QUEUED
                    assert execute.status is StepStatus.PENDING
        finally:
            await engine.dispose()

    asyncio.run(scenario())
