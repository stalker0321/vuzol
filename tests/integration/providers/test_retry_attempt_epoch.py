"""Owner Retry admits bounded calls without deleting lifetime attempt history."""

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from tests.integration.providers._test_routing_helpers import bundle, profile, seed_provider_step
from tests.integration.storage.helpers import storage
from vuzol.providers.budgets import reconcile_usage
from vuzol.providers.domain import NormalizedUsage
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.leasing import start_step
from vuzol.storage.models import Event, ProviderBudgetReservation, RoutingDecision, Run, Step, Task
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
from vuzol.workflows.application import Principal, TaskControlService

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("first_failure", ["provider_unavailable", "provider_attempts_exhausted"])
def test_explicit_retry_gets_new_bounded_attempt_epoch(postgres_dsn, tmp_path, first_failure):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"provider_attempts": 1})}
        )
        task_id, run_id, step_id = await seed_provider_step(factory)

        async def claim():
            async with factory.begin() as session:
                return await claim_routed_step(
                    session,
                    settings=settings,
                    registries=registries,
                    owner="epoch-test",
                    lease_seconds=60,
                    candidate_limit=20,
                )

        async def settle_and_requeue(token):
            async with factory.begin() as session:
                await start_step(session, token)
                step = await session.get(Step, step_id)
                reservation = await session.scalar(
                    select(ProviderBudgetReservation).where(
                        ProviderBudgetReservation.step_id == step_id,
                        ProviderBudgetReservation.provider_attempt
                        == step.payload["provider_attempt"],
                    )
                )
                await reconcile_usage(
                    session,
                    reservation_id=reservation.id,
                    token=token,
                    provider="openai-compatible",
                    model="model",
                    usage=NormalizedUsage(
                        input_tokens=0,
                        output_tokens=0,
                        duration_ms=1,
                        cost_units=Decimal(0),
                        quota_units=Decimal(0),
                    ),
                    provider_request_id=None,
                    outcome="provider_unavailable",
                )
                step = await session.get(Step, step_id)
                step.status = StepStatus.QUEUED
                step.lease_owner = None
                step.lease_expires_at = None
                step.available_at = func.now()

        async def retry():
            async with factory() as session:
                task = await session.get(Task, task_id)
                version = task.version
            result = await TaskControlService(factory).execute(
                task_id=task_id,
                command="retry",
                principal=Principal(1, "cli"),
                expected_task_version=version,
                step_id=step_id,
            )
            assert result.applied

        try:
            async with factory.begin() as session:
                await synchronize_profiles(
                    session, registries.profiles.items(), configuration_revision="a" * 64
                )
            first = await claim()
            assert first is not None
            await settle_and_requeue(first)
            if first_failure == "provider_unavailable":
                # A worker can stop on the provider failure before another route.
                async with factory.begin() as session:
                    step = await session.get(Step, step_id)
                    step.status = StepStatus.BLOCKED
                    step.failure_category = first_failure
                    task = await session.get(Task, task_id)
                    task.status = TaskStatus.BLOCKED
                    run = await session.get(Run, run_id)
                    run.status = RunStatus.BLOCKED
            else:
                # Automatic retry cannot get another call in the exhausted epoch.
                assert await claim() is None
            async with factory() as session:
                step = await session.get(Step, step_id)
                assert step.status is StepStatus.BLOCKED
                assert step.failure_category == first_failure
            await retry()
            second = await claim()
            assert second is not None
            await settle_and_requeue(second)
            assert await claim() is None
            await retry()
            third = await claim()
            assert third is not None
            async with factory() as session:
                rows = (
                    await session.scalars(
                        select(ProviderBudgetReservation).order_by(
                            ProviderBudgetReservation.provider_attempt,
                        )
                    )
                ).all()
                assert [(r.provider_attempt, r.budget_epoch) for r in rows] == [
                    (1, 0),
                    (2, 1),
                    (3, 2),
                ]
                assert await session.scalar(select(func.count()).select_from(RoutingDecision)) == 3
                task = await session.get(Task, task_id)
                run = await session.get(Run, run_id)
                assert task.budget_epoch == 2 and task.status is TaskStatus.RETRYING
                assert run.status is RunStatus.RUNNING
                epochs = (
                    await session.scalars(
                        select(Event).where(
                            Event.entity_id == task_id,
                            Event.event_type == "task.budget_epoch_started",
                        )
                    )
                ).all()
                assert len(epochs) == 2
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_legacy_unreserved_decisions_still_consume_epoch_zero_cap(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"provider_attempts": 1})}
        )
        _, run_id, step_id = await seed_provider_step(factory)
        try:
            async with factory.begin() as session:
                await synchronize_profiles(
                    session, registries.profiles.items(), configuration_revision="a" * 64
                )
                session.add(
                    RoutingDecision(
                        run_id=run_id,
                        step_id=step_id,
                        provider_attempt=1,
                        role="executor",
                        selected_profile_id="api",
                        alternatives=[],
                        inputs={},
                        policy_revision="b" * 64,
                    )
                )
            async with factory.begin() as session:
                assert (
                    await claim_routed_step(
                        session,
                        settings=settings,
                        registries=registries,
                        owner="legacy-test",
                        lease_seconds=60,
                        candidate_limit=20,
                    )
                    is None
                )
                step = await session.get(Step, step_id)
                assert step.failure_category == "provider_attempts_exhausted"
                assert (
                    await session.scalar(
                        select(func.count()).select_from(
                            ProviderBudgetReservation,
                        )
                    )
                    == 0
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())
