"""Soft estimates continue; an explicit money ceiling does not reset."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from tests.integration.providers._test_routing_helpers import (
    bundle,
    profile,
    seed_provider_step,
)
from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config.settings import HardLimits
from vuzol.providers.budgets import (
    EXPLICIT_MONEY_CAP_SUMMARY,
    BudgetExceeded,
    accounting_for_profile,
    estimate_reservation,
    reserve_budget,
)
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.models import Event, ProviderBudgetReservation, Run, Step, Task, UsageRecord
from vuzol.storage.types import (
    BudgetReservationStatus,
    IdempotencyClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork


def _limits(**changes: object) -> HardLimits:
    return HardLimits(**changes)  # type: ignore[arg-type]


async def _add_step(factory, run_id, ordinal: int, step_type: str):
    async with UnitOfWork(factory) as uow:
        return await uow.steps.create(
            run_id=run_id,
            ordinal=ordinal,
            step_type=step_type,
            idempotency_class=IdempotencyClass.READ_ONLY,
            status=StepStatus.QUEUED,
            max_attempts=3,
        )


@pytest.mark.postgresql
def test_soft_token_estimate_lets_review_continue(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, run_id, first = await seed_task_run_step(
            factory,
            idempotency_class=IdempotencyClass.READ_ONLY,
            step_type="execute_code",
        )
        review = await _add_step(factory, run_id, 2, "review")
        configured = profile("audit-api")
        estimate = estimate_reservation(configured, input_tokens=5_000, output_tokens=1)
        limits = _limits(task_input_tokens=1_000, usage_total_enforcement="soft")

        async def reserve(step_id, *, review_allowance: bool) -> None:
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task.id,
                    run_id=run_id,
                    step_id=step_id,
                    profile_id=configured.id,
                    provider_attempt=1,
                    estimate=estimate,
                    limits=limits,
                    review_allowance=review_allowance,
                    accounting=accounting_for_profile(
                        configured,
                        purpose="review" if review_allowance else "coding",
                    ),
                )

        await reserve(first.id, review_allowance=False)
        await reserve(review.id, review_allowance=True)
        async with factory() as session:
            stored = (
                await session.scalars(
                    select(ProviderBudgetReservation).where(
                        ProviderBudgetReservation.task_id == task.id
                    )
                )
            ).all()
            events = (
                await session.scalars(
                    select(Event).where(
                        Event.entity_id == task.id,
                        Event.event_type == "budget.soft_overrun",
                    )
                )
            ).all()
            assert len(stored) == 2
            assert [event.payload["dimension"] for event in events] == ["task_input_tokens"]
            loaded = await session.get(Task, task.id)
            assert loaded is not None
            assert loaded.status is not TaskStatus.FAILED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_explicit_money_cap_survives_epoch_child_and_raise(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        parent, parent_run, parent_step = await seed_task_run_step(factory)
        child, child_run, child_step = await seed_task_run_step(factory)
        continuation = await _add_step(factory, parent_run, 2, "review")
        async with factory.begin() as session:
            child_row = await session.get(Task, child.id)
            assert child_row is not None
            child_row.parent_task_id = parent.id
        configured = profile("audit-api")
        estimate = estimate_reservation(configured, input_tokens=1, output_tokens=1)
        tight = _limits(explicit_money_cap_units=0.015)
        opened = _limits(explicit_money_cap_units=1)

        async def reserve(task_id, run_id, step_id, limits: HardLimits):
            async with factory.begin() as session:
                row = await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=step_id,
                    profile_id=configured.id,
                    provider_attempt=1,
                    estimate=estimate,
                    limits=limits,
                    accounting=accounting_for_profile(configured, purpose="coding"),
                )
                return row.id, row.budget_epoch, row.reserved_cost_units

        first_id, first_epoch, first_cost = await reserve(
            parent.id, parent_run, parent_step.id, tight
        )
        async with factory.begin() as session:
            task = await session.get(Task, parent.id)
            assert task is not None
            task.budget_epoch = 1
        with pytest.raises(BudgetExceeded, match="explicit money cap exceeded"):
            await reserve(parent.id, parent_run, continuation.id, tight)
        with pytest.raises(BudgetExceeded, match="explicit money cap exceeded"):
            await reserve(child.id, child_run, child_step.id, tight)
        second_id, second_epoch, _second_cost = await reserve(
            parent.id, parent_run, continuation.id, opened
        )
        async with factory() as session:
            rows = {
                row.id: row
                for row in (await session.scalars(select(ProviderBudgetReservation))).all()
            }
            assert first_epoch == 0
            assert rows[first_id].budget_epoch == 0
            assert rows[first_id].reserved_cost_units == first_cost
            assert first_cost > 0
            assert second_epoch == 1
            assert rows[second_id].budget_epoch == 1
            assert len(rows) == 2
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_unknown_cost_does_not_certify_an_explicit_ceiling(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, run_id, step = await seed_task_run_step(factory)
        nxt = await _add_step(factory, run_id, 2, "review")
        configured = profile("audit-api")
        estimate = estimate_reservation(configured, input_tokens=1, output_tokens=1)
        limits = _limits(explicit_money_cap_units=1)
        async with factory.begin() as session:
            reservation = await reserve_budget(
                session,
                task_id=task.id,
                run_id=run_id,
                step_id=step.id,
                profile_id=configured.id,
                provider_attempt=1,
                estimate=estimate,
                limits=limits,
                accounting=accounting_for_profile(configured, purpose="coding"),
            )
            reservation.status = BudgetReservationStatus.RECONCILED
            session.add(
                UsageRecord(
                    provider=configured.provider,
                    profile_id=configured.id,
                    model=configured.model,
                    task_id=task.id,
                    run_id=run_id,
                    step_id=step.id,
                    input_tokens=1,
                    output_tokens=1,
                    cost_units=reservation.reserved_cost_units,
                    quota_units=Decimal("0"),
                    duration_ms=1,
                    reservation_id=reservation.id,
                    outcome="completed",
                    cost_known=False,
                )
            )
        with pytest.raises(BudgetExceeded, match="cannot be certified"):
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task.id,
                    run_id=run_id,
                    step_id=nxt.id,
                    profile_id=configured.id,
                    provider_attempt=1,
                    estimate=estimate,
                    limits=limits,
                    review_allowance=True,
                    accounting=accounting_for_profile(configured, purpose="review"),
                )
        async with factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(ProviderBudgetReservation)
                .where(ProviderBudgetReservation.task_id == task.id)
            )
            assert count == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_claim_stops_on_explicit_money_cap_without_losing_the_task(
    postgres_dsn: str, tmp_path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        settings = settings.model_copy(
            update={
                "limits": settings.limits.model_copy(update={"explicit_money_cap_units": 0.001})
            }
        )
        task_id, _run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, step_id)
            run = await session.get(Run, _run_id)
            task = await session.get(Task, task_id)
            assert token is None
            assert step is not None and step.status is StepStatus.BLOCKED
            assert step.failure_category == "money_cap_reached"
            assert step.failure_summary == EXPLICIT_MONEY_CAP_SUMMARY
            assert run is not None and run.status is RunStatus.BLOCKED
            assert task is not None
            assert task.status is TaskStatus.BLOCKED
            assert task.original_text == "answer safely"
        await engine.dispose()

    asyncio.run(scenario())


def _soft_limits(settings: object, *, enforcement: str) -> object:
    limits = settings.limits.model_copy(  # type: ignore[attr-defined]
        update={
            "usage_total_enforcement": enforcement,
            "estimated_cost_enforcement": enforcement,
            "task_cost_units": 0.001,
        }
    )
    return settings.model_copy(update={"limits": limits})  # type: ignore[attr-defined]


@pytest.mark.postgresql
def test_soft_estimated_cost_lets_claim_continue(postgres_dsn: str, tmp_path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        settings = _soft_limits(settings, enforcement="soft")
        task_id, _run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            token = await claim_routed_step(
                session,
                settings=settings,  # type: ignore[arg-type]
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, step_id)
            task = await session.get(Task, task_id)
            events = (
                await session.scalars(
                    select(Event).where(
                        Event.entity_id == task_id,
                        Event.event_type == "budget.soft_overrun",
                    )
                )
            ).all()
            assert token is not None
            assert step is not None and step.status is StepStatus.LEASED
            assert task is not None and task.status is TaskStatus.EXECUTING
            assert [event.payload["dimension"] for event in events] == ["task_cost_units"]
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_hard_estimated_cost_still_stops_claim(postgres_dsn: str, tmp_path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        settings = _soft_limits(settings, enforcement="hard")
        _task_id, _run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            token = await claim_routed_step(
                session,
                settings=settings,  # type: ignore[arg-type]
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, step_id)
            task = await session.get(Task, _task_id)
            reservations = await session.scalar(
                select(func.count()).select_from(ProviderBudgetReservation)
            )
            assert token is None
            assert step is not None and step.status is StepStatus.BLOCKED
            assert step.failure_category == "budget_exhausted"
            assert task is not None and task.status is TaskStatus.QUOTA_EXHAUSTED
            assert reservations == 0
        await engine.dispose()

    asyncio.run(scenario())


def _cli_profile(tmp_path):
    state = tmp_path / "codex-state"
    state.mkdir(parents=True)
    return profile(
        "cli-worker",
        provider="codex",
        model="gpt-test",
        api_base_url=None,
        launch_mode="cli",
        credential_required=False,
        runtime_identity="codex-a",
        state_directory=state,
        sandbox_required=True,
        capabilities=frozenset(),
        supported_task_types=frozenset({"general"}),
        roles=frozenset({"executor"}),
    )


@pytest.mark.postgresql
def test_cli_explicit_cap_needs_root_consent(postgres_dsn: str, tmp_path) -> None:
    async def scenario() -> None:
        from vuzol.providers.budgets import (
            ADMISSION_ONLY_CONSENT_SUMMARY,
            record_admission_only_consent,
        )
        from vuzol.workflows.controls import retry_blocked_step

        engine, factory = storage(postgres_dsn)
        configured = _cli_profile(tmp_path)
        settings, registries = bundle(tmp_path, configured)
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"explicit_money_cap_units": 1})}
        )
        task_id, _run_id, step_id = await seed_provider_step(factory, step_type="execute_code")
        async with factory.begin() as session:
            child = await session.get(Task, task_id)
            assert child is not None
            other = Task(
                user_id=1,
                original_text="child",
                task_type="general",
                status=TaskStatus.EXECUTING,
                parent_task_id=task_id,
            )
            session.add(other)
            await session.flush()
            session.add(
                Event(
                    entity_type="task",
                    entity_id=other.id,
                    event_type="budget.admission_only_consent",
                    actor_type="user",
                    actor_id="7",
                    payload={"scope": "task_family"},
                )
            )
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            refused = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, step_id)
            reservations = await session.scalar(
                select(func.count()).select_from(ProviderBudgetReservation)
            )
            assert refused is None
            assert step is not None and step.failure_category == "admission_only_consent_required"
            assert step.failure_summary == ADMISSION_ONLY_CONSENT_SUMMARY
            assert reservations == 0
        async with factory.begin() as session:
            await record_admission_only_consent(
                session,
                task_id=task_id,
                principal_user_id=7,
                reason="owner accepts that a running CLI session is not financially capped",
            )
            await retry_blocked_step(session, step_id, actor_id="7")
            granted = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            consent = await session.scalar(
                select(Event).where(
                    Event.entity_id == task_id,
                    Event.event_type == "budget.admission_only_consent",
                )
            )
            ceiling = await session.scalar(
                select(Event).where(
                    Event.entity_id == task_id,
                    Event.event_type == "budget.ceiling_unenforceable",
                )
            )
            assert granted is not None
            assert consent is not None and consent.payload["scope"] == "task_family"
            assert ceiling is not None
            assert ceiling.payload["dimension"] == "launch:cli-worker"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_api_explicit_cap_leases_without_cli_consent(postgres_dsn: str, tmp_path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"explicit_money_cap_units": 1})}
        )
        _task_id, _run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, step_id)
            consent = await session.scalar(
                select(Event.id).where(Event.event_type == "budget.admission_only_consent")
            )
            assert token is not None
            assert step is not None and step.status is StepStatus.LEASED
            assert consent is None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_review_ledger_refuses_cli_without_consent(postgres_dsn: str, tmp_path) -> None:
    async def scenario() -> None:
        from vuzol.config.models import ProviderRole
        from vuzol.providers.budgets import record_admission_only_consent
        from vuzol.providers.domain import ProviderRequest
        from vuzol.review.independent import DatabaseReviewAccounting, IndependentReviewError

        engine, factory = storage(postgres_dsn)
        configured = _cli_profile(tmp_path / "review-cli")
        task_id, run_id, step_id = await seed_provider_step(factory, step_type="review")
        accounting = DatabaseReviewAccounting(factory, _limits(explicit_money_cap_units=1))
        request = ProviderRequest(
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            provider_attempt=1,
            role=ProviderRole.REVIEWER,
            original_input="review the change",
            system_policy_revision="policy",
            prompt_revision="prompt",
            timeout_seconds=30,
            max_input_tokens=1_000,
            max_output_tokens=1_000,
            reserved_cost_units=Decimal("0.01"),
            reserved_quota_units=Decimal("0"),
        )
        with pytest.raises(IndependentReviewError, match="admission-only launch"):
            await accounting.reserve(request=request, profile=configured)
        async with factory() as session:
            count = await session.scalar(
                select(func.count()).select_from(ProviderBudgetReservation)
            )
            assert count == 0
        async with factory.begin() as session:
            await record_admission_only_consent(
                session,
                task_id=task_id,
                principal_user_id=7,
                reason="owner accepts the review CLI limitation",
            )
        reserved = await accounting.reserve(request=request, profile=configured)
        assert reserved.cost_units > 0
        await engine.dispose()

    asyncio.run(scenario())


async def _unknown_usage(factory, *, task_id, run_id, step_id, configured) -> object:
    async with factory.begin() as session:
        row = UsageRecord(
            provider=configured.provider,
            profile_id=configured.id,
            model=configured.model,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            input_tokens=1,
            output_tokens=1,
            cost_units=Decimal("0.01"),
            quota_units=Decimal("0"),
            duration_ms=1,
            outcome="timeout",
            purpose="research",
            cost_known=False,
        )
        session.add(row)
        await session.flush()
        return row.id


@pytest.mark.postgresql
def test_unknown_cost_continues_after_receipt(postgres_dsn: str, tmp_path) -> None:
    async def scenario() -> None:
        from vuzol.providers.budgets import (
            EXPLICIT_MONEY_CAP_UNKNOWN_SUMMARY,
            reconcile_unknown_usage,
        )
        from vuzol.workflows.controls import retry_blocked_step

        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"explicit_money_cap_units": 1})}
        )
        task_id, run_id, step_id = await seed_provider_step(factory)
        usage_id = await _unknown_usage(
            factory,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            configured=configured,
        )
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, step_id)
            assert token is None
            assert step is not None and step.failure_category == "money_cap_uncertified"
            assert step.failure_summary == EXPLICIT_MONEY_CAP_UNKNOWN_SUMMARY
        probe = await _add_step(factory, run_id, 2, "review")
        async with factory.begin() as session:
            stored = await session.get(Step, probe.id)
            assert stored is not None
            stored.status = StepStatus.PENDING
        estimate = estimate_reservation(configured, input_tokens=1, output_tokens=1)
        opened = _limits(explicit_money_cap_units=1_000_000)
        with pytest.raises(BudgetExceeded, match="cannot be certified"):
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=probe.id,
                    profile_id=configured.id,
                    provider_attempt=1,
                    estimate=estimate,
                    limits=opened,
                    accounting=accounting_for_profile(configured, purpose="research"),
                )
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.budget_epoch = 3
        with pytest.raises(BudgetExceeded, match="cannot be certified"):
            async with factory.begin() as session:
                await reserve_budget(
                    session,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=probe.id,
                    profile_id=configured.id,
                    provider_attempt=1,
                    estimate=estimate,
                    limits=opened,
                    accounting=accounting_for_profile(configured, purpose="research"),
                )
        async with factory.begin() as session:
            await reconcile_unknown_usage(
                session,
                usage_id=usage_id,  # type: ignore[arg-type]
                cost_units=Decimal("0.25"),
                principal_user_id=7,
            )
            receipt = await session.get(UsageRecord, usage_id)
            event = await session.scalar(
                select(Event).where(Event.event_type == "budget.cost_reconciled")
            )
            assert receipt is not None
            assert receipt.cost_known is True
            assert receipt.late_receipt is True
            assert receipt.cost_units == Decimal("0.25")
            assert event is not None
            assert Decimal(event.payload["previous_cost_units"]) == Decimal("0.01")
            assert Decimal(event.payload["cost_units"]) == Decimal("0.25")
            await retry_blocked_step(session, step_id, actor_id="7")
            granted = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            kept = await session.get(UsageRecord, usage_id)
            task = await session.get(Task, task_id)
            assert granted is not None
            assert kept is not None and kept.id == usage_id
            assert kept.cost_units == Decimal("0.25")
            assert task is not None and task.original_text == "answer safely"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_unknown_cost_continues_after_owner_accepts_uncertainty(
    postgres_dsn: str, tmp_path
) -> None:
    async def scenario() -> None:
        from vuzol.providers.budgets import accept_cost_uncertainty
        from vuzol.workflows.controls import retry_blocked_step
        from vuzol.workflows.transitions import transition_step

        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"explicit_money_cap_units": 1})}
        )
        task_id, run_id, step_id = await seed_provider_step(factory)
        usage_id = await _unknown_usage(
            factory,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            configured=configured,
        )
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            refused = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            assert refused is None
            await accept_cost_uncertainty(
                session,
                task_id=task_id,
                principal_user_id=7,
                reason="owner accepts this unknown receipt and keeps its floor",
            )
            kept = await session.get(UsageRecord, usage_id)
            assert kept is not None and kept.cost_known is False
            assert kept.cost_units == Decimal("0.01")
            await retry_blocked_step(session, step_id, actor_id="7")
            granted = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            assert granted is not None
            # The API profile admits one lease. Finish this call so the next
            # queued step is routed, and the new unknown receipt is what stops it.
            held = await session.get(Step, step_id)
            assert held is not None
            await transition_step(session, held, StepStatus.RUNNING, actor_type="worker")
            await transition_step(session, held, StepStatus.COMPLETED, actor_type="worker")
        later = await _add_step(factory, run_id, 2, "execute_model")
        async with factory.begin() as session:
            session.add(
                UsageRecord(
                    provider=configured.provider,
                    profile_id=configured.id,
                    model=configured.model,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=later.id,
                    cost_units=Decimal("0.01"),
                    quota_units=Decimal("0"),
                    duration_ms=1,
                    outcome="timeout",
                    purpose="research",
                    cost_known=False,
                )
            )
            await session.flush()
            blocked = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, later.id)
            fresh = await session.scalar(
                select(UsageRecord).where(
                    UsageRecord.step_id == later.id,
                    UsageRecord.cost_known.is_(False),
                )
            )
            assert blocked is None
            assert step is not None and step.failure_category == "money_cap_uncertified"
            assert fresh is not None and fresh.cost_units == Decimal("0.01")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_owner_command_records_cli_consent_and_continues(postgres_dsn: str, tmp_path) -> None:
    async def scenario() -> None:
        from vuzol.workflows.application import OwnerDecision, Principal, apply_task_command

        engine, factory = storage(postgres_dsn)
        configured = _cli_profile(tmp_path / "owner-consent")
        settings, registries = bundle(tmp_path / "owner-consent-bundle", configured)
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"explicit_money_cap_units": 1})}
        )
        task_id, _run_id, step_id = await seed_provider_step(factory, step_type="execute_code")
        async with factory.begin() as session:
            child = Task(
                user_id=1,
                original_text="child",
                task_type="general",
                status=TaskStatus.EXECUTING,
                parent_task_id=task_id,
            )
            session.add(child)
            await session.flush()
            session.add(
                Event(
                    entity_type="task",
                    entity_id=child.id,
                    event_type="budget.admission_only_consent",
                    actor_type="user",
                    actor_id="7",
                    payload={"scope": "task_family"},
                )
            )
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            refused = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            task = await session.get(Task, task_id)
            step = await session.get(Step, step_id)
            assert refused is None
            assert task is not None and step is not None
            assert step.failure_category == "admission_only_consent_required"
            version = task.version
        with pytest.raises(ValueError, match="task owner's principal"):
            async with factory.begin() as session:
                await apply_task_command(
                    session,
                    task_id=task_id,
                    command="admit_ceiling",
                    principal=Principal(user_id=7, ingress_source="cli"),
                    expected_task_version=version,
                    decision=OwnerDecision(reason="stranger accepts the limitation"),
                )
        with pytest.raises(ValueError, match="does not match"):
            async with factory.begin() as session:
                await apply_task_command(
                    session,
                    task_id=task_id,
                    command="accept_uncertainty",
                    principal=Principal(user_id=1, ingress_source="cli"),
                    expected_task_version=version,
                    decision=OwnerDecision(reason="wrong stop"),
                )
        async with factory() as session:
            root_consent = await session.scalar(
                select(Event.id).where(
                    Event.entity_id == task_id,
                    Event.event_type == "budget.admission_only_consent",
                )
            )
            uncertainty = await session.scalar(
                select(Event.id).where(Event.event_type == "budget.cost_uncertainty_accepted")
            )
            step = await session.get(Step, step_id)
            assert root_consent is None and uncertainty is None
            assert step is not None and step.status is StepStatus.BLOCKED
        async with factory.begin() as session:
            await apply_task_command(
                session,
                task_id=task_id,
                command="admit_ceiling",
                principal=Principal(user_id=1, ingress_source="cli"),
                expected_task_version=version,
                decision=OwnerDecision(
                    reason="owner accepts that a running CLI session is not financially capped"
                ),
            )
            granted = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            consent = await session.scalar(
                select(Event).where(
                    Event.entity_id == task_id,
                    Event.event_type == "budget.admission_only_consent",
                )
            )
            ceiling = await session.scalar(
                select(Event).where(
                    Event.entity_id == task_id,
                    Event.event_type == "budget.ceiling_unenforceable",
                )
            )
            assert granted is not None
            assert consent is not None and consent.payload["scope"] == "task_family"
            assert ceiling is not None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_owner_receipt_and_uncertainty_continue_without_erasing_history(
    postgres_dsn: str, tmp_path
) -> None:
    async def scenario() -> None:
        from vuzol.workflows.application import (
            OwnerDecision,
            Principal,
            apply_task_command,
            continue_blocked_receipt,
        )
        from vuzol.workflows.transitions import transition_step

        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"explicit_money_cap_units": 1})}
        )
        task_id, run_id, step_id = await seed_provider_step(factory)
        usage_id = await _unknown_usage(
            factory, task_id=task_id, run_id=run_id, step_id=step_id, configured=configured
        )
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.source_thread_id = 10
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            refused = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            task = await session.get(Task, task_id)
            assert refused is None and task is not None
            version = task.version
        with pytest.raises(ValueError, match="exactly one"):
            async with factory.begin() as session:
                await continue_blocked_receipt(
                    session,
                    chat_id=-100,
                    thread_id=10,
                    principal=Principal(user_id=7, ingress_source="telegram"),
                    cost_units=Decimal("0.25"),
                )
        async with factory() as session:
            row = await session.get(UsageRecord, usage_id)
            assert row is not None and row.cost_known is False
        async with factory.begin() as session:
            await continue_blocked_receipt(
                session,
                chat_id=-100,
                thread_id=10,
                principal=Principal(user_id=1, ingress_source="telegram"),
                cost_units=Decimal("0.25"),
            )
            granted = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            kept = await session.get(UsageRecord, usage_id)
            event = await session.scalar(
                select(Event).where(Event.event_type == "budget.cost_reconciled")
            )
            assert granted is not None
            assert kept is not None and kept.late_receipt is True
            assert kept.cost_units == Decimal("0.25")
            assert event is not None
            assert Decimal(event.payload["previous_cost_units"]) == Decimal("0.01")
            held = await session.get(Step, step_id)
            assert held is not None
            await transition_step(session, held, StepStatus.RUNNING, actor_type="worker")
            await transition_step(session, held, StepStatus.COMPLETED, actor_type="worker")
        later = await _add_step(factory, run_id, 2, "execute_model")
        fresh_id = await _unknown_usage(
            factory, task_id=task_id, run_id=run_id, step_id=later.id, configured=configured
        )
        async with factory.begin() as session:
            blocked = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, later.id)
            task = await session.get(Task, task_id)
            assert blocked is None and step is not None and task is not None
            assert step.failure_category == "money_cap_uncertified"
            version = task.version
        async with factory.begin() as session:
            await apply_task_command(
                session,
                task_id=task_id,
                command="accept_uncertainty",
                principal=Principal(user_id=1, ingress_source="cli"),
                expected_task_version=version,
                decision=OwnerDecision(
                    reason="owner accepts this unknown receipt and keeps its floor"
                ),
            )
            kept = await session.get(UsageRecord, fresh_id)
            assert kept is not None and kept.cost_known is False
            assert kept.cost_units == Decimal("0.01")
            granted = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            assert granted is not None
            held = await session.get(Step, later.id)
            assert held is not None
            await transition_step(session, held, StepStatus.RUNNING, actor_type="worker")
            await transition_step(session, held, StepStatus.COMPLETED, actor_type="worker")
        third = await _add_step(factory, run_id, 3, "execute_model")
        async with factory.begin() as session:
            session.add(
                UsageRecord(
                    provider=configured.provider,
                    profile_id=configured.id,
                    model=configured.model,
                    task_id=task_id,
                    run_id=run_id,
                    step_id=third.id,
                    cost_units=Decimal("0.01"),
                    quota_units=Decimal("0"),
                    duration_ms=1,
                    outcome="timeout",
                    purpose="research",
                    cost_known=False,
                )
            )
            await session.flush()
            blocked = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=5,
            )
            step = await session.get(Step, third.id)
            assert blocked is None
            assert step is not None and step.failure_category == "money_cap_uncertified"
        await engine.dispose()

    asyncio.run(scenario())
