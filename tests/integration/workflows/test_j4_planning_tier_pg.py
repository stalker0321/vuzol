"""J4 service-level dispatcher test: tier drives workflow and run budget mode."""

from __future__ import annotations

import pytest

from vuzol.interpretation.planning import (
    PLANNING_POLICY_VERSION,
    PLANNING_TIER_EVENT,
    PlanningTier,
)
from vuzol.storage.attempts import snapshot_task_spec
from vuzol.storage.models import Event, Run, Step, Task, TransactionalOutbox

from ._test_runtime_helpers import (
    RegistryDocument,
    RuntimeConfiguration,
    Settings,
    WorkflowDispatcher,
    asyncio,
    build_bundle,
    planned_coding_draft,
    seed_interpreted,
    select,
    storage,
)

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize(
    ("tier", "expected_budget", "expected_plan"),
    [
        (PlanningTier.DIRECT, "cheap", False),
        (PlanningTier.LIGHT, "balanced", True),
        (PlanningTier.STRONG, "strong", True),
    ],
)
def test_dispatcher_materializes_tier_specific_workflow_and_budget(
    postgres_dsn: str, tier: PlanningTier, expected_budget: str, expected_plan: bool
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, interpretation_id = await seed_interpreted(factory, planned_coding_draft())
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            revision = await snapshot_task_spec(session, task)
            session.add(
                Event(
                    entity_type="task",
                    entity_id=task.id,
                    event_type=PLANNING_TIER_EVENT,
                    actor_type="system",
                    payload={
                        "tier": tier.value,
                        "policy_version": PLANNING_POLICY_VERSION,
                        "spec_revision": revision,
                        "requires_scout": False,
                        "reasons": [],
                    },
                )
            )
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
        runtime = RuntimeConfiguration(
            settings=settings,
            registries=build_bundle(RegistryDocument(), settings),
        )
        dispatcher = WorkflowDispatcher(runtime, factory, owner="dispatcher-j4")
        assert await dispatcher.process_one()
        async with factory() as session:
            run = await session.scalar(select(Run).where(Run.task_id == task_id))
            assert run is not None
            assert run.budget_mode == expected_budget
            steps = tuple(
                (
                    await session.scalars(
                        select(Step).where(Step.run_id == run.id).order_by(Step.ordinal)
                    )
                ).all()
            )
            plan_present = any(step.step_type == "plan" for step in steps)
            assert plan_present is expected_plan
            if expected_plan:
                plan_step = next(step for step in steps if step.step_type == "plan")
                assert plan_step.required_capabilities == []
        await engine.dispose()

    asyncio.run(scenario())
