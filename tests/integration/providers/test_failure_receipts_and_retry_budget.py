"""Measured failures and retries share one hard envelope, including reservations."""

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from tests.integration.providers._test_routing_helpers import (
    bundle,
    profile,
    seed_provider_step,
)
from tests.integration.providers.test_accounting_ledger import _reserve_started
from tests.integration.storage.helpers import storage
from vuzol.config import ScopedSecretResolver
from vuzol.config.settings import HardLimits
from vuzol.providers.budgets import (
    accounting_for_profile,
    estimate_reservation,
    remaining_output_allowance,
    reserve_budget,
)
from vuzol.providers.domain import NormalizedUsage, ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.handlers import ProviderStepHandler, provider_handlers
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.registry import AdapterRegistry
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.models import ProviderBudgetReservation, Step, Task, UsageRecord
from vuzol.storage.types import BudgetReservationStatus, StepStatus
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.worker import RoutedWorkflowWorker

pytestmark = pytest.mark.postgresql


def test_failed_call_after_lease_loss_keeps_measured_receipt(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        token, reservation, step_id = await _reserve_started(factory, registries, settings)

        class LoseLeaseThenFail:
            async def execute(self, request, profile, cancellation):
                async with factory.begin() as session:
                    step = await session.get(Step, step_id)
                    step.lease_generation += 1
                    step.lease_owner = "new-owner"
                raise ProviderFailure(
                    ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                    True,
                    True,
                    usage=NormalizedUsage(input_tokens=42, output_tokens=7, duration_ms=1),
                    provider_request_id="late-failure",
                )

        handler = ProviderStepHandler(
            factory,
            registries,
            AdapterRegistry(
                registries.profiles,
                ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path),
                adapters={"api": LoseLeaseThenFail()},
            ),
        )
        try:
            async with factory() as session:
                step = await session.get(Step, step_id)
                request = StepExecutionRequest(
                    task_id=reservation.task_id,
                    run_id=reservation.run_id,
                    step_id=step_id,
                    step_type="execute_model",
                    payload=step.payload,
                    timeout_seconds=60,
                    lease=token,
                )
            await handler.execute(request, CancellationContext())
            async with factory() as session:
                usage = (await session.scalars(select(UsageRecord))).one()
                step = await session.get(Step, step_id)
                assert (usage.input_tokens, usage.output_tokens) == (42, 7)
                assert usage.late_receipt and not usage.cost_known
                assert usage.provider_request_id == "late-failure"
                assert step.lease_owner == "new-owner"
                assert step.status is StepStatus.RUNNING and step.result is None
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_measured_failure_leaves_room_for_real_retry(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        task_id, _, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )

        class RejectAnswer:
            async def execute(self, request, profile, cancellation):
                assert request.max_output_tokens == 25_000
                raise ProviderFailure(
                    ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                    retryable=True,
                    request_sent=True,
                    usage=NormalizedUsage(
                        input_tokens=42,
                        output_tokens=1219,
                        cached_tokens=12,
                        cost_units=Decimal("0.001"),
                        quota_units=Decimal("0"),
                        duration_ms=26,
                    ),
                    provider_request_id="measured-failure",
                )

        handler = ProviderStepHandler(
            factory,
            registries,
            AdapterRegistry(
                registries.profiles,
                ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path),
                adapters={"api": RejectAnswer()},
            ),
        )
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="receipt-test",
            handlers=provider_handlers(handler),
        )
        try:
            assert await worker.process_one()
            async with factory.begin() as session:
                step = await session.get(Step, step_id)
                assert step.status is StepStatus.QUEUED
                usage = (await session.scalars(select(UsageRecord))).one()
                reservation = (await session.scalars(select(ProviderBudgetReservation))).one()
                assert (usage.input_tokens, usage.output_tokens) == (42, 1219)
                assert usage.provider_request_id == "measured-failure"
                assert usage.outcome == "invalid_structured_output"
                assert reservation.status is BudgetReservationStatus.RECONCILED
                # Skip only the backoff clock in this deterministic test.
                step.available_at = func.now()
            async with factory.begin() as session:
                retry = await claim_routed_step(
                    session,
                    settings=settings,
                    registries=registries,
                    owner="retry-test",
                    lease_seconds=60,
                    candidate_limit=20,
                )
                assert retry is not None
                reservations = (
                    await session.scalars(
                        select(ProviderBudgetReservation).order_by(
                            ProviderBudgetReservation.provider_attempt
                        )
                    )
                ).all()
                assert len(reservations) == 2
                assert reservations[1].reserved_output_tokens == 25_000 - 1219
                assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1
                assert (
                    await remaining_output_allowance(
                        session,
                        task_id=task_id,
                        step_id=step_id,
                        budget_epoch=0,
                        requested=25_000,
                        limits=settings.limits,
                    )
                    == 0
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("usage_mode", ["hard", "soft"])
def test_allowance_counts_usage_reservations_and_epoch(postgres_dsn, usage_mode):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        task_id, run_id, step_id = await seed_provider_step(factory)
        configured = profile("api")
        limits = HardLimits(
            task_output_tokens=90,
            step_output_tokens=80,
            provider_call_output_tokens=100,
            usage_total_enforcement=usage_mode,
        )
        try:
            async with factory.begin() as session:
                reservation = await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=step_id,
                    profile_id=configured.id,
                    provider_attempt=1,
                    estimate=estimate_reservation(configured, input_tokens=1, output_tokens=30),
                    limits=limits,
                    accounting=accounting_for_profile(configured, purpose="coding"),
                )
                assert await remaining_output_allowance(
                    session,
                    task_id=task_id,
                    step_id=step_id,
                    budget_epoch=0,
                    requested=100,
                    limits=limits,
                ) == (50 if usage_mode == "hard" else 100)
                reservation.status = BudgetReservationStatus.RECONCILED
                session.add(
                    UsageRecord(
                        provider=configured.provider,
                        model=configured.model,
                        profile_id=configured.id,
                        task_id=task_id,
                        run_id=run_id,
                        step_id=step_id,
                        input_tokens=1,
                        output_tokens=20,
                        cost_units=Decimal("0.001"),
                        quota_units=Decimal("0"),
                        duration_ms=1,
                        reservation_id=reservation.id,
                        outcome="invalid_structured_output",
                    )
                )
                await session.flush()
                assert await remaining_output_allowance(
                    session,
                    task_id=task_id,
                    step_id=step_id,
                    budget_epoch=0,
                    requested=100,
                    limits=limits,
                ) == (60 if usage_mode == "hard" else 100)
                task = await session.get(Task, task_id)
                task.budget_epoch = 1
                assert await remaining_output_allowance(
                    session,
                    task_id=task_id,
                    step_id=step_id,
                    budget_epoch=1,
                    requested=100,
                    limits=limits,
                ) == (80 if usage_mode == "hard" else 100)
                # Lifetime history is not rewritten when an epoch changes.
                assert await session.scalar(select(func.sum(UsageRecord.output_tokens))) == 20
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_concurrent_reservations_cannot_oversubscribe_remainder(postgres_dsn):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        task_id, run_id, step_id = await seed_provider_step(factory)
        configured = profile("api")
        limits = HardLimits(task_output_tokens=100, step_output_tokens=100)

        async def reserve(attempt):
            async with factory.begin() as session:
                allowed = await remaining_output_allowance(
                    session,
                    task_id=task_id,
                    step_id=step_id,
                    budget_epoch=0,
                    requested=70,
                    limits=limits,
                )
                await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=step_id,
                    profile_id=configured.id,
                    provider_attempt=attempt,
                    estimate=estimate_reservation(
                        configured, input_tokens=1, output_tokens=allowed
                    ),
                    limits=limits,
                    accounting=accounting_for_profile(configured, purpose="coding"),
                )
                return allowed

        try:
            assert sorted(await asyncio.gather(reserve(1), reserve(2))) == [30, 70]
            async with factory() as session:
                assert (
                    await session.scalar(
                        select(func.sum(ProviderBudgetReservation.reserved_output_tokens))
                    )
                    == 100
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())
