import asyncio
import json
import subprocess
import uuid
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import (
    Capability,
    ConfigurationBundle,
    ProjectConfig,
    ProviderProfileConfig,
    ProviderRole,
    RegistryDocument,
    ScopedSecretResolver,
    Settings,
    build_bundle,
)
from vuzol.execution.git import LocalGit
from vuzol.execution.worktrees import WorktreeService
from vuzol.experiments.domain import WorkerEditReport
from vuzol.providers.budgets import (
    BudgetExceeded,
    estimate_reservation,
    reconcile_usage,
    reserve_budget,
)
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.domain import (
    EffectiveProfileState,
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.handlers import (
    ProviderStepHandler,
    executor_provider_handlers,
    provider_handlers,
)
from vuzol.providers.health import (
    effective_health,
    record_failure_observation,
    record_success_observation,
    synchronize_profiles,
)
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.providers.registry import AdapterRegistry
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import start_step
from vuzol.storage.models import (
    ProfileHealthObservation,
    ProviderBudgetReservation,
    ProviderProfile,
    RoutingDecision,
    Run,
    Step,
    SupervisedProcess,
    Task,
    UsageRecord,
    Worktree,
)
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import (
    BudgetReservationStatus,
    IdempotencyClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
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
def test_safe_lease_recovery_can_reuse_the_same_provider(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        settings = settings.model_copy(
            update={
                "limits": settings.limits.model_copy(
                    update={"provider_call_output_tokens": 1_000}
                )
            }
        )
        _task_id, run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        async with factory.begin() as session:
            first = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="first-worker",
                lease_seconds=60,
                candidate_limit=20,
            )
            assert first is not None
            step = await session.get(Step, step_id)
            assert step is not None
            step.status = StepStatus.QUEUED
            step.lease_owner = None
            step.lease_expires_at = None
            for attempt in (2, 3):
                session.add(
                    RoutingDecision(
                        run_id=run_id,
                        step_id=step_id,
                        provider_attempt=attempt,
                        decision_kind="fallback",
                        role="executor",
                        selected_profile_id=None,
                        alternatives=[],
                        inputs={},
                        policy_revision="b" * 64,
                    )
                )

        async with factory.begin() as session:
            second = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="recovery-worker",
                lease_seconds=60,
                candidate_limit=20,
            )

        assert second is not None and second.step.id == step_id
        async with factory() as session:
            decisions = tuple(
                (
                    await session.scalars(
                        select(RoutingDecision)
                        .where(RoutingDecision.step_id == step_id)
                        .order_by(RoutingDecision.provider_attempt)
                    )
                ).all()
            )
            assert [item.selected_profile_id for item in decisions] == [
                "api",
                None,
                None,
                "api",
            ]
            assert decisions[-1].decision_kind == "initial"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_execute_code_route_requires_cli_sandbox_profile(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        api = profile(
            "api",
            sandbox_required=True,
            capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
        )
        cli = profile(
            "cli",
            provider="codex",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="cli",
            state_directory=tmp_path / "cli-state",
            sandbox_required=True,
            capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
        )
        settings, registries = bundle(tmp_path, api, cli)
        _task_id, _run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_code",
            capabilities=[Capability.CODE_EDIT.value, Capability.PROJECT_SHELL.value],
        )
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=20,
                step_types=frozenset({"execute_code"}),
            )
        assert token is not None and token.step.id == step_id
        async with factory() as session:
            step = await session.get(Step, step_id)
            assert step is not None and step.executor_profile_id == "cli"
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
            step.failure_category = "provider_unavailable"
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
def test_codex_typed_report_reaches_handler_finalization(postgres_dsn: str, tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(("git", "init", "-b", "main"), cwd=repository, check=True)
    subprocess.run(
        ("git", "config", "user.email", "test@example.invalid"), cwd=repository, check=True
    )
    subprocess.run(("git", "config", "user.name", "Test"), cwd=repository, check=True)
    (repository / "tracked.txt").write_text("base\n")
    subprocess.run(("git", "add", "tracked.txt"), cwd=repository, check=True)
    subprocess.run(("git", "commit", "-m", "base"), cwd=repository, check=True)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile(
            "codex-test",
            provider="codex",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            sandbox_required=True,
            runtime_identity="codex-test",
            state_directory=tmp_path / "provider-state",
        )
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_id = await seed_provider_step(factory, step_type="execute_code")
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            worktree = await worktrees.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=ProjectConfig(
                    id="project",
                    display_name="Project",
                    repository_path=repository,
                    default_branch="main",
                    allowed_capabilities=frozenset(),
                    sandbox_profile="default",
                ),
                owner="provider-worker",
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

        capsule = {
            "experiment_id": "codex-handler-test",
            "task_id": "typed-report",
            "worker_profile": "codex-test",
            "base_commit": worktree.base_commit,
            "target_branch": worktree.branch,
            "goal": "Edit the bounded file.",
            "classification": {
                "task_class": "bounded_feature",
                "complexity": "low",
                "risk": "low",
                "testability": "high",
                "blast_radius": "low",
                "coupling": "low",
                "novelty": "low",
                "expected_file_count": 1,
            },
            "predicted_mode": "sol_solo",
            "actual_mode": "sol_solo",
            "allowed_paths": ["tracked.txt"],
            "acceptance_criteria": ["The bounded edit is measured."],
            "required_gates": [{"name": "tests", "command_id": "make test"}],
            "maximum_execution_seconds": 30,
            "context_manifest": {"role": "worker", "entries": []},
        }
        edit_report = {
            "schema_version": "step09a-worker-edit-report.v1",
            "experiment_id": "codex-handler-test",
            "task_id": "typed-report",
            "attempt": 1,
            "claimed_complete": True,
            "implementation_summary": "Implemented the requested bounded edit.",
            "limitations": [],
            "failure_classification": None,
            "usage": None,
        }

        class Transport:
            async def run(
                self, invocation: CodexInvocation, cancellation: CancellationContext
            ) -> CodexProcessResult:
                del invocation, cancellation
                return CodexProcessResult(
                    0,
                    "\n".join(
                        (
                            json.dumps({"type": "thread.started", "thread_id": "session"}),
                            json.dumps(
                                {
                                    "type": "item.completed",
                                    "item": {
                                        "type": "agent_message",
                                        "text": json.dumps(edit_report),
                                    },
                                }
                            ),
                            json.dumps({"type": "turn.completed", "usage": {}}),
                        )
                    ),
                    "",
                    5,
                )

        provider_request = ProviderRequest(
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            provider_attempt=1,
            lease_generation=token.generation,
            role=ProviderRole.EXECUTOR,
            original_input="bounded task",
            task_draft={"step09a_capsule": capsule},
            output_schema_name="WorkerEditReport",
            output_schema_version="step09a-worker-edit-report.v1",
            output_json_schema=WorkerEditReport.model_json_schema(),
            system_policy_revision="policy",
            prompt_revision="prompt",
            timeout_seconds=30,
            max_input_tokens=1000,
            max_output_tokens=1000,
            reserved_cost_units=Decimal("1"),
            reserved_quota_units=Decimal("1"),
            sandbox_reference=f"worktree:{worktree.id}",
        )
        provider_result = await CodexCliAdapter(Transport()).execute(
            provider_request, configured, CancellationContext()
        )
        finalized = MagicMock()
        finalizer = MagicMock()
        finalizer.finalize = AsyncMock(return_value=finalized)
        handler = ProviderStepHandler(
            factory, registries, MagicMock(), worktrees=worktrees, finalizer=finalizer
        )
        step_request = StepExecutionRequest(
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            step_type="execute_code",
            payload={},
            timeout_seconds=30,
            lease=token,
        )
        result = await handler._finalize_worker_result(
            request=step_request,
            provider_request=provider_request,
            profile_id="codex-test",
            result=provider_result,
            cancellation=CancellationContext(),
            access=None,
        )
        assert result is finalized
        finalized_report = finalizer.finalize.await_args.kwargs["edit_report"]
        assert isinstance(finalized_report, WorkerEditReport)
        assert finalized_report.claimed_complete is True
        await engine.dispose()

    asyncio.run(scenario())


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
def test_pre_provider_failure_unwinds_reservation_and_worktree(
    postgres_dsn: str, tmp_path: Path
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(("git", "init", "-b", "main"), cwd=repository, check=True)
    subprocess.run(
        ("git", "config", "user.email", "test@example.invalid"),
        cwd=repository,
        check=True,
    )
    subprocess.run(("git", "config", "user.name", "Test"), cwd=repository, check=True)
    (repository / "tracked.txt").write_text("base\n")
    subprocess.run(("git", "add", "tracked.txt"), cwd=repository, check=True)
    subprocess.run(("git", "commit", "-m", "base"), cwd=repository, check=True)

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile(
            "cli",
            launch_mode="cli",
            sandbox_required=True,
            runtime_identity="test-cli",
            state_directory=tmp_path / "provider-state",
        )
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_id = await seed_provider_step(factory, step_type="execute_code")
        worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            await worktrees.prepare(
                session,
                task_id=task_id,
                run_id=run_id,
                project=ProjectConfig(
                    id="project",
                    display_name="Project",
                    repository_path=repository,
                    default_branch="main",
                    allowed_capabilities=frozenset(),
                    sandbox_profile="default",
                ),
                owner="provider-worker",
            )

        adapter = FakeAdapter()
        adapter.execute = AsyncMock(side_effect=AssertionError("provider must not be called"))  # type: ignore[method-assign]
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            adapters={"cli": adapter},
        )
        handler = ProviderStepHandler(factory, registries, adapters, worktrees=worktrees)
        handler._build_request = AsyncMock(  # type: ignore[method-assign]
            side_effect=ValueError("injected pre-provider preparation failure")
        )
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="provider-worker",
            handlers=executor_provider_handlers(handler),
        )

        assert await worker.process_one()

        async with factory() as session:
            step = await session.get(Step, step_id)
            run = await session.get(Run, run_id)
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
            process_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(SupervisedProcess)
                    .where(SupervisedProcess.run_id == run_id)
                )
                or 0
            )
            usage_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(UsageRecord)
                    .where(UsageRecord.run_id == run_id)
                )
                or 0
            )
            health_count = int(
                await session.scalar(select(func.count()).select_from(ProfileHealthObservation))
                or 0
            )
            assert step is not None and step.status is StepStatus.FAILED
            assert step.failure_category == "provider_request_invalid"
            assert step.failure_summary == "ValueError"
            assert run is not None and run.failure_category == "provider_request_invalid"
            assert reservation is not None
            assert reservation.status is BudgetReservationStatus.RELEASED
            assert worktree is not None
            assert worktree.delivery_state is WorktreeDeliveryState.WORKTREE_RETAINED
            assert process_count == usage_count == health_count == 0
        adapter.execute.assert_not_awaited()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_pre_provider_unwind_is_idempotent_and_fenced(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile("api")
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_id = await seed_provider_step(factory)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
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
        request = StepExecutionRequest(
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            step_type="execute_model",
            payload={"budget_reservation_id": "malformed", "provider_attempt": 1},
            timeout_seconds=60,
            lease=token,
        )
        handler = ProviderStepHandler(factory, registries, MagicMock())

        await handler._unwind_pre_provider(request, reservation_id=None)
        await handler._unwind_pre_provider(request, reservation_id=None)

        mismatched = StepExecutionRequest(
            task_id=request.task_id,
            run_id=request.run_id,
            step_id=request.step_id,
            step_type=request.step_type,
            payload=request.payload,
            timeout_seconds=request.timeout_seconds,
            lease=LeaseToken(
                step=token.step,
                owner=token.owner,
                generation=token.generation + 1,
            ),
        )
        with pytest.raises(LeaseLost):
            await handler._unwind_pre_provider(mismatched, reservation_id=None)

        async with factory() as session:
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            assert reservation is not None
            assert reservation.status is BudgetReservationStatus.RELEASED
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 0
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


@pytest.mark.postgresql
def test_concurrent_executor_startup_upserts_one_profile_snapshot(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("shared"))

        async def synchronize(revision: str) -> None:
            async with factory.begin() as session:
                await synchronize_profiles(
                    session,
                    registries.profiles.items(),
                    configuration_revision=revision,
                )

        await asyncio.gather(synchronize("a" * 64), synchronize("b" * 64))
        async with factory() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(ProviderProfile)
                .where(ProviderProfile.stable_id == "shared")
            )
            snapshot = await session.scalar(
                select(ProviderProfile).where(ProviderProfile.stable_id == "shared")
            )
            assert count == 1
            assert snapshot is not None
            assert snapshot.configuration_revision in {"a" * 64, "b" * 64}
        await engine.dispose()

    asyncio.run(scenario())
