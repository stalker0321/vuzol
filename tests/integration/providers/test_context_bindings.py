"""WP02: research output flows into synthesize through explicit input bindings."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update

from vuzol.context.resolver import BindingError, resolve_context
from vuzol.execution.artifacts import ArtifactStore
from vuzol.providers.budgets import estimate_reservation, reserve_budget
from vuzol.storage.models import Artifact, InputBinding, ProviderBudgetReservation, Step, Task
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import StepStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.domain import OutcomeKind

from ._test_routing_helpers import (
    AdapterRegistry,
    AsyncSession,
    CancellationContext,
    ConfigurationBundle,
    EffectiveProfileState,
    IdempotencyClass,
    NormalizedUsage,
    Path,
    ProviderProfileConfig,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
    ProviderStepHandler,
    QueueClass,
    RetryClass,
    ScopedSecretResolver,
    Settings,
    StepExecutionRequest,
    async_sessionmaker,
    asyncio,
    bundle,
    claim_routed_step,
    profile,
    pytest,
    seed_provider_step,
    storage,
    synchronize_profiles,
    uuid,
)

pytestmark = pytest.mark.postgresql

RESEARCH_MARKER = "RESEARCH_MARKER_42"


class RecordingAdapter:
    def __init__(self, result: ProviderResult) -> None:
        self._result = result
        self.requests: list[ProviderRequest] = []

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        del profile, cancellation
        self.requests.append(request)
        return self._result

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


def _result(text: str) -> ProviderResult:
    return ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        text=text,
        provider_request_id="request-1",
        usage=NormalizedUsage(input_tokens=5, output_tokens=2, duration_ms=1),
        finish_reason="stop",
        adapter_version="fake.v1",
    )


def _handler(
    factory: async_sessionmaker[AsyncSession],
    registries: ConfigurationBundle,
    tmp_path: Path,
    adapter: RecordingAdapter,
    settings: Settings,
) -> ProviderStepHandler:
    return ProviderStepHandler(
        factory,
        registries,
        AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(
                access_policy={}, secret_file_root=tmp_path / "secrets", environment={}
            ),
            adapters={"api": adapter},
        ),
        artifacts=ArtifactStore(
            settings.artifact_root, max_bytes=10_000_000, retention_days=14
        ),
    )


async def _seed_research_run(
    factory: async_sessionmaker[AsyncSession],
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID]:
    task_id, run_id, research_step_id = await seed_provider_step(
        factory, step_type="research_execute"
    )
    async with UnitOfWork(factory) as uow:
        synthesize = await uow.steps.create(
            run_id=run_id,
            ordinal=2,
            step_type="synthesize",
            idempotency_class=IdempotencyClass.IDEMPOTENT,
            retry_class=RetryClass.TRANSIENT,
            status=StepStatus.QUEUED,
            queue_class=QueueClass.LIGHT,
            max_attempts=3,
            dependency_metadata={"predecessor_ordinals": [1]},
        )
        assert uow.session is not None
        task = await uow.session.get(Task, task_id)
        assert task is not None
        task.project_id = "proj"
    return task_id, run_id, research_step_id, synthesize.id


def _token(step_id: uuid.UUID, run_id: uuid.UUID) -> LeaseToken:
    return LeaseToken(
        step=StepRecord(
            id=step_id,
            run_id=run_id,
            status=StepStatus.RUNNING,
            lease_generation=1,
            lease_owner="probe",
            lease_expires_at=None,
        ),
        owner="probe",
        generation=1,
    )


async def _start_with_reservation(
    factory: async_sessionmaker[AsyncSession],
    *,
    settings: Settings,
    profile_config: ProviderProfileConfig,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    context_estimate_tokens: int | None = None,
) -> None:
    async with factory.begin() as session:
        estimate = estimate_reservation(profile_config, input_tokens=1_000, output_tokens=1_000)
        reservation = await reserve_budget(
            session,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            profile_id="api",
            provider_attempt=1,
            estimate=estimate,
            limits=settings.limits,
        )
        step = await session.get(Step, step_id, with_for_update=True)
        assert step is not None
        step.status = StepStatus.RUNNING
        step.lease_owner = "probe"
        step.lease_generation = 1
        step.executor_profile_id = "api"
        payload: dict[str, object] = {
            "budget_reservation_id": str(reservation.id),
            "provider_attempt": 1,
        }
        if context_estimate_tokens is not None:
            payload["context_estimate_tokens"] = context_estimate_tokens
        step.payload = payload


def _request(
    task_id: uuid.UUID, run_id: uuid.UUID, step_id: uuid.UUID, step_type: str
) -> StepExecutionRequest:
    return StepExecutionRequest(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        step_type=step_type,
        payload={"provider_attempt": 1},
        timeout_seconds=60,
        lease=_token(step_id, run_id),
    )


def test_research_output_flows_into_synthesize(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        task_id, run_id, research_id, synth_id = await _seed_research_run(factory)

        research_adapter = RecordingAdapter(_result(RESEARCH_MARKER))
        research_handler = _handler(factory, registries, tmp_path, research_adapter, settings)
        await _start_with_reservation(
            factory,
            settings=settings,
            profile_config=profile("api"),
            task_id=task_id,
            run_id=run_id,
            step_id=research_id,
        )
        research_outcome = await research_handler.execute(
            _request(task_id, run_id, research_id, "research_execute"),
            CancellationContext(),
        )
        assert research_outcome.kind is OutcomeKind.SUCCEEDED

        async with factory() as session:
            binding = await session.scalar(
                select(InputBinding).where(InputBinding.consumer_step_id == synth_id)
            )
            assert binding is not None
            assert binding.status == "resolved"
            assert binding.producer_step_id == research_id
            assert binding.content_hash is not None
            artifact = await session.get(Artifact, binding.artifact_id)
            assert artifact is not None and artifact.content_hash == binding.content_hash
            synth_step = await session.get(Step, synth_id)
            assert synth_step is not None
            assert synth_step.payload.get("context_estimate_tokens", 0) > 0

        synthesize_adapter = RecordingAdapter(_result("final synthesis"))
        synthesize_handler = _handler(factory, registries, tmp_path, synthesize_adapter, settings)
        await _start_with_reservation(
            factory,
            settings=settings,
            profile_config=profile("api"),
            task_id=task_id,
            run_id=run_id,
            step_id=synth_id,
        )
        synth_outcome = await synthesize_handler.execute(
            _request(task_id, run_id, synth_id, "synthesize"),
            CancellationContext(),
        )
        assert synth_outcome.kind is OutcomeKind.SUCCEEDED
        assert synthesize_adapter.requests, "synthesize adapter was not called"
        packed = "".join(item.content for item in synthesize_adapter.requests[0].context)
        assert RESEARCH_MARKER in packed
        await engine.dispose()

    asyncio.run(scenario())


def test_missing_required_binding_stops_consumer(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        task_id, run_id, _research_id, synth_id = await _seed_research_run(factory)
        # A required, unresolved binding for the synthesize stage.
        async with factory.begin() as session:
            session.add(
                InputBinding(
                    consumer_step_id=synth_id,
                    slot="predecessor_result",
                    schema_name="research-result",
                    schema_version="research-result.v1",
                    scope_project_id="proj",
                    required=True,
                    status="pending",
                )
            )
        await _start_with_reservation(
            factory,
            settings=settings,
            profile_config=profile("api"),
            task_id=task_id,
            run_id=run_id,
            step_id=synth_id,
        )
        adapter = RecordingAdapter(_result("must not run"))
        handler = _handler(factory, registries, tmp_path, adapter, settings)
        outcome = await handler.execute(
            _request(task_id, run_id, synth_id, "synthesize"), CancellationContext()
        )
        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        assert outcome.category == "context_binding_binding_unresolved"
        assert adapter.requests == []
        async with factory() as session:
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == synth_id
                )
            )
            assert reservation is not None
            assert reservation.status.value == "released"
        await engine.dispose()

    asyncio.run(scenario())


def test_wrong_hash_required_binding_stops_consumer(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        task_id, run_id, _research_id, synth_id = await _seed_research_run(factory)
        store = ArtifactStore(settings.artifact_root, max_bytes=1_000_000, retention_days=14)
        async with factory.begin() as session:
            artifact = await store.persist(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=synth_id,
                artifact_type="research_result",
                content=b"bound content",
                media_type="application/json",
            )
            session.add(
                InputBinding(
                    consumer_step_id=synth_id,
                    producer_step_id=synth_id,
                    artifact_id=artifact.id,
                    slot="predecessor_result",
                    schema_name="research-result",
                    schema_version="research-result.v1",
                    content_hash="0" * 64,
                    scope_project_id="proj",
                    required=True,
                    status="resolved",
                )
            )
        await _start_with_reservation(
            factory,
            settings=settings,
            profile_config=profile("api"),
            task_id=task_id,
            run_id=run_id,
            step_id=synth_id,
        )
        adapter = RecordingAdapter(_result("must not run"))
        handler = _handler(factory, registries, tmp_path, adapter, settings)
        outcome = await handler.execute(
            _request(task_id, run_id, synth_id, "synthesize"), CancellationContext()
        )
        assert outcome.kind is OutcomeKind.PERMANENT_FAILURE
        assert outcome.category == "context_binding_hash_mismatch"
        assert adapter.requests == []
        await engine.dispose()

    asyncio.run(scenario())


def test_resolver_rejects_foreign_scope_and_expired(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, _registries = bundle(tmp_path, profile("api"))
        task_id, run_id, _research_id, synth_id = await _seed_research_run(factory)
        store = ArtifactStore(settings.artifact_root, max_bytes=1_000_000, retention_days=14)
        async with factory.begin() as session:
            artifact = await store.persist(
                session,
                task_id=task_id,
                run_id=run_id,
                step_id=synth_id,
                artifact_type="research_result",
                content=b"scope content",
                media_type="application/json",
            )
            session.add(
                InputBinding(
                    consumer_step_id=synth_id,
                    artifact_id=artifact.id,
                    slot="predecessor_result",
                    schema_name="research-result",
                    schema_version="research-result.v1",
                    content_hash=artifact.content_hash,
                    scope_project_id="other-project",
                    required=True,
                    status="resolved",
                )
            )
            await session.flush()
            artifact_id = artifact.id

        async with factory() as session:
            with pytest.raises(BindingError) as exc:
                await resolve_context(
                    session, store, consumer_step_id=synth_id, project_id="proj"
                )
            assert exc.value.category == "foreign_scope"

        # Expired freshness on a required binding fails closed as well.
        async with factory.begin() as session:
            await session.execute(
                update(InputBinding)
                .where(InputBinding.consumer_step_id == synth_id)
                .values(scope_project_id="proj", freshness_max_age_seconds=1)
            )
            await session.execute(
                update(Artifact)
                .where(Artifact.id == artifact_id)
                .values(created_at=datetime.now(UTC) - timedelta(hours=2))
            )
        async with factory() as session:
            with pytest.raises(BindingError) as expired:
                await resolve_context(
                    session, store, consumer_step_id=synth_id, project_id="proj"
                )
            assert expired.value.category == "expired"
        await engine.dispose()

    asyncio.run(scenario())


def test_estimate_includes_declared_context(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        # concurrency_limit > 1 so the second claim is not blocked by the first
        # still-leased step from the previous loop iteration.
        settings, registries = bundle(tmp_path, profile("api", concurrency_limit=5))
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )

        reserved: dict[str, int] = {}
        for label, estimate in (("plain", 0), ("context", 5_000)):
            _task_id, _run_id, step_id = await seed_provider_step(
                factory, step_type="synthesize"
            )
            async with factory.begin() as session:
                step = await session.get(Step, step_id, with_for_update=True)
                assert step is not None
                step.payload = {"context_estimate_tokens": estimate} if estimate else {}
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
            async with factory() as session:
                reservation = await session.scalar(
                    select(ProviderBudgetReservation).where(
                        ProviderBudgetReservation.step_id == step_id
                    )
                )
                assert reservation is not None
                reserved[label] = int(reservation.reserved_input_tokens)

        assert reserved["context"] - reserved["plain"] == 5_000
        await engine.dispose()

    asyncio.run(scenario())
