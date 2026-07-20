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

__all__ = [
    "AdapterRegistry",
    "AsyncMock",
    "AsyncSession",
    "BudgetExceeded",
    "BudgetReservationStatus",
    "CancellationContext",
    "Capability",
    "CodexCliAdapter",
    "CodexInvocation",
    "CodexProcessResult",
    "ConfigurationBundle",
    "Decimal",
    "EffectiveProfileState",
    "FailingAdapter",
    "FakeAdapter",
    "IdempotencyClass",
    "LeaseLost",
    "LeaseToken",
    "LocalGit",
    "MagicMock",
    "NormalizedUsage",
    "Path",
    "ProfileHealthObservation",
    "ProjectConfig",
    "ProviderBudgetReservation",
    "ProviderErrorCategory",
    "ProviderFailure",
    "ProviderProfile",
    "ProviderProfileConfig",
    "ProviderRequest",
    "ProviderResult",
    "ProviderResultStatus",
    "ProviderRole",
    "ProviderStepHandler",
    "RegistryDocument",
    "RetryClass",
    "RoutedWorkflowWorker",
    "RoutingDecision",
    "Run",
    "RunStatus",
    "ScopedSecretResolver",
    "Settings",
    "Step",
    "StepExecutionRequest",
    "StepStatus",
    "SupervisedProcess",
    "Task",
    "TaskStatus",
    "UnitOfWork",
    "UsageRecord",
    "WorkerEditReport",
    "Worktree",
    "WorktreeDeliveryState",
    "WorktreeService",
    "async_sessionmaker",
    "asyncio",
    "build_bundle",
    "bundle",
    "claim_routed_step",
    "effective_health",
    "estimate_reservation",
    "executor_provider_handlers",
    "func",
    "json",
    "profile",
    "provider_handlers",
    "pytest",
    "reconcile_usage",
    "record_failure_observation",
    "record_success_observation",
    "reserve_budget",
    "seed_provider_step",
    "select",
    "start_step",
    "storage",
    "subprocess",
    "synchronize_profiles",
    "uuid",
]


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
