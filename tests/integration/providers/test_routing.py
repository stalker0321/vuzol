import asyncio
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import (
    Capability,
    ConfigurationBundle,
    ProviderProfileConfig,
    RegistryDocument,
    ScopedSecretResolver,
    Settings,
    build_bundle,
)
from vuzol.providers.budgets import (
    BudgetExceeded,
    estimate_reservation,
    reconcile_usage,
    reserve_budget,
)
from vuzol.providers.domain import (
    EffectiveProfileState,
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.handlers import ProviderStepHandler, provider_handlers
from vuzol.providers.health import (
    effective_health,
    record_failure_observation,
    record_success_observation,
    synchronize_profiles,
)
from vuzol.providers.registry import AdapterRegistry
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.leasing import start_step
from vuzol.storage.models import (
    ProviderBudgetReservation,
    ProviderProfile,
    RoutingDecision,
    Run,
    Step,
    Task,
    UsageRecord,
)
from vuzol.storage.types import (
    BudgetReservationStatus,
    IdempotencyClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.worker import RoutedWorkflowWorker

from ..storage.helpers import storage


def profile(profile_id: str, **changes: object) -> ProviderProfileConfig:
    values: dict[str, object] = {
        "id": profile_id,
        "provider": "openai-compatible",
        "model": "model",
        "api_base_url": "https://provider.example/v1",
        "launch_mode": "api",
        "credential_required": False,
        "capabilities": frozenset(),
        "concurrency_limit": 1,
        "cost_class": "balanced",
        "roles": frozenset({"executor", "planner", "summarizer"}),
        "supported_task_types": frozenset({"general"}),
        "sandbox_required": False,
        "minimum_unknown_usage_cost": 0.01,
    }
    values.update(changes)
    return ProviderProfileConfig.model_validate(values)


def bundle(
    tmp_path: Path, *profiles: ProviderProfileConfig
) -> tuple[Settings, ConfigurationBundle]:
    settings = Settings(
        environment="test",
        repository_root=tmp_path / "repositories",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    return settings, build_bundle(
        RegistryDocument(profiles=profiles),
        settings,
        environment={},
        validate_profile_credentials=False,
    )


async def seed_provider_step(
    factory: async_sessionmaker[AsyncSession],
    *,
    step_type: str = "execute_model",
    capabilities: list[str] | None = None,
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        task = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="answer safely",
            task_type="general",
            task_draft={"task_type": "general"},
        )
        assert uow.session is not None
        stored_task = await uow.session.get(Task, task.id)
        assert stored_task is not None
        stored_task.status = TaskStatus.EXECUTING
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="simple_model",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        step = await uow.steps.create(
            run_id=run_id,
            ordinal=1,
            step_type=step_type,
            idempotency_class=IdempotencyClass.IDEMPOTENT,
            retry_class=RetryClass.TRANSIENT,
            required_capabilities=capabilities,
            status=StepStatus.QUEUED,
            max_attempts=3,
        )
    return task.id, run_id, step.id


@pytest.mark.postgresql
def test_route_reservation_and_fenced_claim_are_atomic(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        task_id, run_id, step_id = await seed_provider_step(factory)
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
        assert token is not None and token.step.id == step_id
        async with factory() as session:
            step = await session.get(Step, step_id)
            decision = await session.scalar(
                select(RoutingDecision).where(RoutingDecision.step_id == step_id)
            )
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            assert step is not None and step.executor_profile_id == "api"
            assert decision is not None and decision.selected_profile_id == "api"
            assert reservation is not None
            assert reservation.task_id == task_id and reservation.run_id == run_id
            assert reservation.status is BudgetReservationStatus.RESERVED

        async with factory.begin() as session:
            await start_step(session, token)
        async with factory.begin() as session:
            first = await reconcile_usage(
                session,
                reservation_id=reservation.id,
                token=token,
                provider="openai-compatible",
                model="model",
                usage=NormalizedUsage(
                    input_tokens=5,
                    output_tokens=3,
                    duration_ms=10,
                ),
                provider_request_id="request-1",
                outcome="succeeded",
            )
            repeated = await reconcile_usage(
                session,
                reservation_id=reservation.id,
                token=token,
                provider="openai-compatible",
                model="model",
                usage=None,
                provider_request_id=None,
                outcome="ignored",
            )
            assert first.id == repeated.id
        async with factory() as session:
            usage_count = await session.scalar(
                select(UsageRecord).where(UsageRecord.reservation_id == reservation.id)
            )
            stored = await session.get(ProviderBudgetReservation, reservation.id)
            assert usage_count is not None
            assert stored is not None and stored.status is BudgetReservationStatus.CONSERVATIVE
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_saturated_profile_does_not_block_or_starve_other_profile(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(
            tmp_path,
            profile("coder", capabilities=frozenset({Capability.CODE_EDIT})),
            profile("general", routing_priority=200),
        )
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        _task_a, _run_a, step_a = await seed_provider_step(
            factory, capabilities=[Capability.CODE_EDIT.value]
        )
        _task_b, _run_b, step_b = await seed_provider_step(factory)
        async with factory.begin() as session:
            first = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="worker-a",
                lease_seconds=60,
                candidate_limit=20,
            )
        async with factory.begin() as session:
            second = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="worker-b",
                lease_seconds=60,
                candidate_limit=20,
            )
        assert first is not None and first.step.id == step_a
        assert second is not None and second.step.id == step_b
        async with factory() as session:
            stored = await session.get(Step, step_b)
            assert stored is not None and stored.executor_profile_id == "general"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_concurrent_reservations_cannot_exceed_shared_task_budget(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, _registries = bundle(tmp_path, configured)
        settings = settings.model_copy(
            update={
                "limits": settings.limits.model_copy(
                    update={"task_cost_units": 0.015, "step_cost_units": 0.02}
                )
            }
        )
        task_id, run_id, first_step = await seed_provider_step(factory)
        async with UnitOfWork(factory) as uow:
            second = await uow.steps.create(
                run_id=run_id,
                ordinal=2,
                step_type="execute_model",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.QUEUED,
                max_attempts=3,
            )
        estimate = estimate_reservation(configured, input_tokens=1, output_tokens=1)

        async def reserve(step_id: uuid.UUID, attempt: int) -> bool:
            try:
                async with factory.begin() as session:
                    await reserve_budget(
                        session,
                        task_id=task_id,
                        run_id=run_id,
                        step_id=step_id,
                        profile_id="api",
                        provider_attempt=attempt,
                        estimate=estimate,
                        limits=settings.limits,
                    )
                return True
            except BudgetExceeded:
                return False

        outcomes = await asyncio.gather(reserve(first_step, 1), reserve(second.id, 1))
        assert sorted(outcomes) == [False, True]
        async with factory() as session:
            reservations = tuple((await session.scalars(select(ProviderBudgetReservation))).all())
            assert len(reservations) == 1
            assert reservations[0].reserved_cost_units == Decimal("0.010000")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_categorized_retry_selects_only_configured_fallback(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        primary = profile("primary", fallback_profile_ids=("fallback",))
        fallback = profile("fallback", routing_priority=999)
        settings, registries = bundle(tmp_path, primary, fallback)
        _task_id, run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            step = await session.get(Step, step_id)
            assert step is not None
            step.executor_profile_id = "primary"
            session.add(
                RoutingDecision(
                    run_id=run_id,
                    step_id=step_id,
                    provider_attempt=1,
                    decision_kind="initial",
                    role="executor",
                    selected_profile_id="primary",
                    alternatives=[{"profile_id": "fallback", "rank": 1}],
                    inputs={},
                    policy_revision="b" * 64,
                )
            )
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="fallback-worker",
                lease_seconds=60,
                candidate_limit=20,
            )
        assert token is not None
        async with factory() as session:
            step = await session.get(Step, step_id)
            decisions = tuple(
                (
                    await session.scalars(
                        select(RoutingDecision)
                        .where(RoutingDecision.step_id == step_id)
                        .order_by(RoutingDecision.provider_attempt)
                    )
                ).all()
            )
            assert step is not None and step.executor_profile_id == "fallback"
            assert [item.selected_profile_id for item in decisions] == ["primary", "fallback"]
            assert decisions[-1].decision_kind == "fallback"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_no_compatible_profile_blocks_without_losing_state(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(
            tmp_path, profile("planner-only", roles=frozenset({"planner"}))
        )
        task_id, run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="worker",
                lease_seconds=60,
                candidate_limit=20,
            )
        assert token is None
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            decision = await session.scalar(
                select(RoutingDecision).where(RoutingDecision.step_id == step_id)
            )
            assert task is not None and task.status is TaskStatus.BLOCKED
            assert run is not None and run.status is RunStatus.BLOCKED
            assert step is not None and step.status is StepStatus.BLOCKED
            assert decision is not None and decision.selected_profile_id is None
        await engine.dispose()

    asyncio.run(scenario())


class FakeAdapter:
    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        assert not cancellation.requested
        assert request.original_input == "answer safely"
        assert profile.id == "api"
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text="safe answer",
            provider_request_id="request-1",
            usage=NormalizedUsage(
                input_tokens=5,
                output_tokens=2,
                cost_units=Decimal("0.005"),
                quota_units=Decimal("1"),
                duration_ms=10,
            ),
            finish_reason="stop",
            adapter_version="fake.v1",
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


class FailingAdapter:
    def __init__(self, failure: ProviderFailure) -> None:
        self._failure = failure

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        del request, profile, cancellation
        raise self._failure

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


@pytest.mark.postgresql
def test_fake_provider_completes_model_only_workflow_step(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        resolver = ScopedSecretResolver(
            access_policy={}, secret_file_root=tmp_path / "secrets", environment={}
        )
        adapters = AdapterRegistry(
            registries.profiles,
            resolver,
            adapters={"api": FakeAdapter()},
        )
        handler = ProviderStepHandler(factory, registries, adapters)
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="provider-worker",
            handlers=provider_handlers(handler),
        )

        assert await worker.process_one()

        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            usage = await session.scalar(select(UsageRecord).where(UsageRecord.step_id == step_id))
            assert task is not None and task.status is TaskStatus.COMPLETED
            assert run is not None and run.status is RunStatus.COMPLETED
            assert step is not None and step.status is StepStatus.COMPLETED
            assert step.result is not None and step.result["text"] == "safe answer"
            assert usage is not None and usage.cost_units == Decimal("0.005000")
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("request_sent", "retryable", "expected_status", "expected_usage"),
    [
        (False, False, StepStatus.FAILED, 0),
        (True, True, StepStatus.QUEUED, 1),
    ],
)
def test_provider_failure_release_or_conservative_charge(
    postgres_dsn: str,
    tmp_path: Path,
    request_sent: bool,
    retryable: bool,
    expected_status: StepStatus,
    expected_usage: int,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        _task_id, _run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        failure = ProviderFailure(
            ProviderErrorCategory.TIMEOUT if retryable else ProviderErrorCategory.PERMANENT_REQUEST,
            retryable=retryable,
            request_sent=request_sent,
            safe_summary="safe failure",
        )
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            adapters={"api": FailingAdapter(failure)},
        )
        handler = ProviderStepHandler(factory, registries, adapters)
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="provider-worker",
            handlers=provider_handlers(handler),
        )

        assert await worker.process_one()

        async with factory() as session:
            step = await session.get(Step, step_id)
            usage_rows = tuple(
                (
                    await session.scalars(select(UsageRecord).where(UsageRecord.step_id == step_id))
                ).all()
            )
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            assert step is not None and step.status is expected_status
            assert len(usage_rows) == expected_usage
            assert reservation is not None
            expected_reservation = (
                BudgetReservationStatus.CONSERVATIVE
                if request_sent
                else BudgetReservationStatus.RELEASED
            )
            assert reservation.status is expected_reservation
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_profile_health_is_revision_scoped_and_snapshot_sync_is_idempotent(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api", credential_reference="env:PRIVATE_KEY")
        _settings, registries = bundle(tmp_path, configured)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        async with factory() as session:
            snapshot = await session.scalar(
                select(ProviderProfile).where(ProviderProfile.stable_id == "api")
            )
            assert snapshot is not None
            assert "credential_reference" not in snapshot.metadata_json
            initial = await effective_health(session, configured, configuration_revision="a" * 64)
            assert initial.healthy and initial.quota_state.value == "unknown"

        failure = ProviderFailure(
            ProviderErrorCategory.AUTHENTICATION,
            retryable=False,
            request_sent=True,
            safe_summary="authentication failed",
        )
        async with factory.begin() as session:
            await record_failure_observation(
                session,
                configured,
                configuration_revision="a" * 64,
                failure=failure,
            )
        async with factory() as session:
            unhealthy = await effective_health(session, configured, configuration_revision="a" * 64)
            unrelated_revision = await effective_health(
                session, configured, configuration_revision="c" * 64
            )
            assert not unhealthy.healthy
            assert unrelated_revision.healthy

        async with factory.begin() as session:
            await record_success_observation(session, configured, configuration_revision="a" * 64)
        async with factory() as session:
            recovered = await effective_health(session, configured, configuration_revision="a" * 64)
            assert recovered.healthy
        await engine.dispose()

    asyncio.run(scenario())
