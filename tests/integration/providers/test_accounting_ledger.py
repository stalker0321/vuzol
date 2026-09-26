"""WP01 accounting ledger: provenance, late receipts, orphan closure, breakdown."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select, update

from vuzol.providers.budgets import (
    accounting_for_profile,
    close_step_reservations,
    estimate_reservation,
    reconcile_usage,
    record_intake_usage,
    record_late_receipt,
    release_orphan_reservations,
    reserve_budget,
    usage_retry_subtotal,
    usage_totals_by_purpose,
)
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import ProviderBudgetReservation, Step, Task, UsageRecord
from vuzol.storage.types import BudgetReservationStatus, StepStatus

from ._test_routing_helpers import (
    NormalizedUsage,
    Path,
    asyncio,
    bundle,
    claim_routed_step,
    profile,
    pytest,
    seed_provider_step,
    start_step,
    storage,
    synchronize_profiles,
)

pytestmark = pytest.mark.postgresql


async def _reserve_started(
    factory: Any,
    registries: Any,
    settings: Any,
) -> tuple[Any, ProviderBudgetReservation, Any]:
    _task_id, _run_id, step_id = await seed_provider_step(factory, step_type="execute_model")
    async with factory.begin() as session:
        await synchronize_profiles(
            session, registries.profiles.items(), configuration_revision="a" * 64
        )
    async with factory.begin() as session:
        token = await claim_routed_step(
            session,
            settings=settings,
            registries=registries,
            owner="provider-worker",
            lease_seconds=60,
            candidate_limit=20,
        )
    assert token is not None
    async with factory.begin() as session:
        await start_step(session, token)
    async with factory() as session:
        reservation = await session.scalar(
            select(ProviderBudgetReservation).where(
                ProviderBudgetReservation.step_id == step_id
            )
        )
        assert reservation is not None
        step = await session.get(Step, step_id)
        assert step is not None
        assert step.status is StepStatus.RUNNING
    return token, reservation, step_id


def test_reservation_and_reconcile_carry_purpose_and_pricing(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        token, reservation, _ = await _reserve_started(factory, registries, settings)

        async with factory() as session:
            stored = await session.get(ProviderBudgetReservation, reservation.id)
            assert stored is not None
            assert stored.purpose == "coding"
            assert stored.attempt_kind == "initial"
            assert stored.currency == "USD"
            assert stored.pricing_revision is not None
            assert len(stored.pricing_revision) == 64

        async with factory.begin() as session:
            record = await reconcile_usage(
                session,
                reservation_id=reservation.id,
                token=token,
                provider="openai-compatible",
                model="model",
                usage=NormalizedUsage(input_tokens=100, output_tokens=50, duration_ms=10),
                provider_request_id="req-1",
                outcome="succeeded",
                accounting=accounting_for_profile(profile("api"), purpose="coding"),
            )
        assert record.cost_known is False  # no rates configured => unknown, not zero
        async with factory() as session:
            stored_record = await session.get(UsageRecord, record.id)
            assert stored_record is not None
            assert stored_record.purpose == "coding"
            assert stored_record.attempt_kind == "initial"
            assert stored_record.currency == "USD"
            assert stored_record.pricing_revision is not None
        await engine.dispose()

    asyncio.run(scenario())


def test_unknown_usage_is_conservative_and_never_zero(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        token, reservation, _ = await _reserve_started(factory, registries, settings)
        async with factory() as session:
            stored = await session.get(ProviderBudgetReservation, reservation.id)
            assert stored is not None
            floor = Decimal(stored.reserved_cost_units)

        async with factory.begin() as session:
            record = await reconcile_usage(
                session,
                reservation_id=reservation.id,
                token=token,
                provider="openai-compatible",
                model="model",
                usage=None,
                provider_request_id=None,
                outcome="timeout",
                conservative=True,
            )
        assert record.cost_known is False
        assert record.cost_units == floor
        assert record.cost_units is not None and record.cost_units > 0
        async with factory() as session:
            stored = await session.get(ProviderBudgetReservation, reservation.id)
            assert stored is not None
            assert stored.status is BudgetReservationStatus.CONSERVATIVE
        await engine.dispose()

    asyncio.run(scenario())


def test_late_receipt_records_money_without_business_change(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        token, reservation, step_id = await _reserve_started(factory, registries, settings)

        async with factory.begin() as session:
            step = await session.get(Step, step_id, with_for_update=True)
            assert step is not None
            # Simulate another worker taking over the lease.
            step.lease_generation += 1
            step.lease_owner = "other-worker"
            business_state = (step.status, step.attempt_count)
            step_result_before = step.result

        async with factory.begin() as session:
            with pytest.raises(LeaseLost):
                await reconcile_usage(
                    session,
                    reservation_id=reservation.id,
                    token=token,
                    provider="openai-compatible",
                    model="model",
                    usage=None,
                    provider_request_id=None,
                    outcome="succeeded",
                )

        async with factory.begin() as session:
            late = await record_late_receipt(
                session,
                reservation_id=reservation.id,
                provider="openai-compatible",
                model="model",
                usage=NormalizedUsage(input_tokens=7, output_tokens=3, duration_ms=5),
                provider_request_id="req-late",
                outcome="succeeded",
                conservative=True,
            )
        assert late is not None and late.late_receipt is True
        assert late.cost_units is not None and late.cost_units > 0

        async with factory() as session:
            step = await session.get(Step, step_id)
            assert step is not None
            # Business state is untouched by the late receipt.
            assert (step.status, step.attempt_count) == business_state
            assert step.result == step_result_before
            # Idempotent: a second late receipt returns the same row.
            again = await record_late_receipt(
                session,
                reservation_id=reservation.id,
                provider="openai-compatible",
                model="model",
                usage=None,
                provider_request_id=None,
                outcome="succeeded",
                conservative=True,
            )
            assert again is not None and again.id == late.id
        await engine.dispose()

    asyncio.run(scenario())


def test_close_step_reservations_release_and_settle(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))

        _, reservation_a, step_a = await _reserve_started(factory, registries, settings)
        async with factory.begin() as session:
            closed = await close_step_reservations(
                session, step_id=step_a, outcome="cancelled", release=True
            )
        assert closed == 1
        async with factory() as session:
            stored = await session.get(ProviderBudgetReservation, reservation_a.id)
            assert stored is not None
            assert stored.status is BudgetReservationStatus.RELEASED
            usage = await session.scalar(
                select(UsageRecord).where(UsageRecord.reservation_id == reservation_a.id)
            )
            assert usage is None

        await engine.dispose()

    asyncio.run(scenario())


def test_release_orphan_reservations_closes_terminal_leak(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        _, reservation, step_id = await _reserve_started(factory, registries, settings)

        async with factory.begin() as session:
            step = await session.get(Step, step_id, with_for_update=True)
            assert step is not None
            step.status = StepStatus.FAILED
            await session.execute(
                update(ProviderBudgetReservation)
                .where(ProviderBudgetReservation.id == reservation.id)
                .values(created_at=datetime.now(UTC) - timedelta(hours=2))
            )
        async with factory.begin() as session:
            closed = await release_orphan_reservations(
                session, older_than_seconds=900, batch_size=10
            )
        assert closed == 1
        async with factory() as session:
            stored = await session.get(ProviderBudgetReservation, reservation.id)
            assert stored is not None
            assert stored.status is BudgetReservationStatus.CONSERVATIVE
            usage = await session.scalar(
                select(UsageRecord).where(UsageRecord.reservation_id == reservation.id)
            )
            assert usage is not None and usage.late_receipt is True
        # Idempotent second sweep.
        async with factory.begin() as session:
            assert (
                await release_orphan_reservations(session, older_than_seconds=900, batch_size=10)
                == 0
            )
        await engine.dispose()

    asyncio.run(scenario())


def test_lifetime_cost_survives_budget_epoch(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        token, reservation, step_id = await _reserve_started(factory, registries, settings)
        accounting = accounting_for_profile(profile("api"), purpose="coding")

        async with factory.begin() as session:
            first = await reconcile_usage(
                session,
                reservation_id=reservation.id,
                token=token,
                provider="openai-compatible",
                model="model",
                usage=None,
                provider_request_id=None,
                outcome="succeeded",
                conservative=True,
                accounting=accounting,
            )
            repeated = await reconcile_usage(
                session,
                reservation_id=reservation.id,
                token=token,
                provider="openai-compatible",
                model="model",
                usage=None,
                provider_request_id=None,
                outcome="succeeded",
                conservative=True,
                accounting=accounting,
            )
            assert repeated.id == first.id

        # A new budget epoch resets caps but must not erase the lifetime ledger.
        async with factory.begin() as session:
            task = await session.get(Task, reservation.task_id, with_for_update=True)
            assert task is not None
            task.budget_epoch += 1
            config = profile("api")
            estimate = estimate_reservation(config, input_tokens=10, output_tokens=10)
            await reserve_budget(
                session,
                task_id=task.id,
                run_id=reservation.run_id,
                step_id=step_id,
                profile_id="api",
                provider_attempt=2,
                estimate=estimate,
                limits=settings.limits,
                accounting=accounting,
            )
        async with factory() as session:
            second = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id,
                    ProviderBudgetReservation.provider_attempt == 2,
                )
            )
            assert second is not None
        async with factory.begin() as session:
            await reconcile_usage(
                session,
                reservation_id=second.id,
                token=token,
                provider="openai-compatible",
                model="model",
                usage=None,
                provider_request_id=None,
                outcome="succeeded",
                conservative=True,
                accounting=accounting,
            )
        async with factory() as session:
            rows = await usage_totals_by_purpose(session, task_id=reservation.task_id)
            total_cost = sum(cost for _purpose, cost, _count in rows)
            total_count = sum(count for _purpose, _cost, count in rows)
            assert total_count == 2
            assert total_cost == Decimal("0.02")
        await engine.dispose()

    asyncio.run(scenario())


def test_intake_usage_and_breakdowns_use_same_rows(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        priced = profile(
            "api",
            input_cost_units_per_million=2.0,
            output_cost_units_per_million=4.0,
        )
        async with factory.begin() as session:
            known = await record_intake_usage(
                session,
                profile=priced,
                usage=NormalizedUsage(input_tokens=1_000, output_tokens=500, duration_ms=3),
                purpose="intake",
                task_id=None,
                attempt_kind="initial",
            )
            unknown = await record_intake_usage(
                session,
                profile=priced,
                usage=None,
                purpose="review",
                task_id=None,
                attempt_kind="retry",
            )
        assert known.cost_known is True and known.cost_units == Decimal("0.004")
        assert unknown.cost_known is False
        assert unknown.cost_units == Decimal("0.01")

        async with factory() as session:
            breakdown = {
                purpose: cost
                for purpose, cost, _count in await usage_totals_by_purpose(session)
            }
            assert breakdown["intake"] == Decimal("0.004")
            assert breakdown["review"] == Decimal("0.01")
            retry_cost, retry_count = await usage_retry_subtotal(session)
            assert retry_count == 1
            assert retry_cost == Decimal("0.01")
        await engine.dispose()

    asyncio.run(scenario())
