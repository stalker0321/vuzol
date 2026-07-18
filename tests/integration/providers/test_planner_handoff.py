"""PostgreSQL integration: planner validation and executor context handoff."""

from __future__ import annotations

import subprocess
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import (
    Capability,
    ProjectConfig,
    ProviderProfileConfig,
    ProviderRole,
    ScopedSecretResolver,
)
from vuzol.execution.git import LocalGit
from vuzol.execution.worktrees import WorktreeService
from vuzol.providers.domain import (
    EffectiveProfileState,
    NormalizedUsage,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.handlers import ProviderStepHandler, provider_handlers
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.registry import AdapterRegistry
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.leasing import start_step
from vuzol.storage.models import (
    ProfileHealthObservation,
    RoutingDecision,
    Step,
    Task,
    UsageRecord,
)
from vuzol.storage.types import (
    IdempotencyClass,
    RetryClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.worker import RoutedWorkflowWorker

from ._test_routing_helpers import bundle, profile, storage


class CapturingAdapter:
    """Records the ProviderRequest seen by the executor adapter."""

    def __init__(self, *, text: str = "safe answer", finish_reason: str = "stop") -> None:
        self.requests: list[ProviderRequest] = []
        self._text = text
        self._finish_reason = finish_reason

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        del profile, cancellation
        self.requests.append(request)
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text=self._text,
            provider_request_id="captured-1",
            usage=NormalizedUsage(
                input_tokens=5,
                output_tokens=2,
                cost_units=Decimal("0.005"),
                quota_units=Decimal("1"),
                duration_ms=10,
            ),
            finish_reason=self._finish_reason,
            adapter_version="capture.v1",
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


async def seed_run_with_steps(
    factory: async_sessionmaker[AsyncSession],
    *,
    steps: list[tuple[str, StepStatus, dict[str, object] | None]],
    task_draft: dict[str, object] | None = None,
    task_status: TaskStatus = TaskStatus.EXECUTING,
) -> tuple[uuid.UUID, uuid.UUID, dict[str, uuid.UUID]]:
    async with UnitOfWork(factory) as uow:
        task = await uow.tasks.create(
            user_id=1,
            chat_id=-100,
            original_text="answer safely",
            task_type="coding",
            task_draft=task_draft or {"task_type": "coding", "needs_planning": True},
        )
        assert uow.session is not None
        stored_task = await uow.session.get(Task, task.id)
        assert stored_task is not None
        stored_task.status = task_status
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.RUNNING,
        )
        ids: dict[str, uuid.UUID] = {}
        for ordinal, (step_type, status, result) in enumerate(steps):
            step = await uow.steps.create(
                run_id=run_id,
                ordinal=ordinal,
                step_type=step_type,
                idempotency_class=IdempotencyClass.IDEMPOTENT
                if step_type == "plan"
                else IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                retry_class=RetryClass.TRANSIENT if step_type == "plan" else RetryClass.POLICY,
                required_capabilities=(
                    [Capability.CODE_EDIT.value, Capability.PROJECT_SHELL.value]
                    if step_type == "execute_code"
                    else None
                ),
                status=status,
                max_attempts=3,
            )
            stored = await uow.session.get(Step, step.id)
            assert stored is not None
            if result is not None:
                stored.result = result
            ids[step_type] = step.id
    return task.id, run_id, ids


@pytest.mark.postgresql
def test_validated_plan_is_included_in_executor_context(postgres_dsn: str, tmp_path: Path) -> None:
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
            "cli",
            provider="codex",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="cli-plan",
            state_directory=tmp_path / "cli-state",
            sandbox_required=True,
            capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            supported_task_types=frozenset({"coding", "general"}),
        )
        settings, registries = bundle(tmp_path, configured)
        plan_text = "1. Inspect tracked.txt\n2. Edit carefully\n3. Validate"
        task_id, run_id, step_ids = await seed_run_with_steps(
            factory,
            steps=[
                (
                    "plan",
                    StepStatus.COMPLETED,
                    {
                        "text": plan_text,
                        "finish_reason": "stop",
                        "handoff": {"status": "ready"},
                        "profile_id": "planner",
                        "model": "nano",
                    },
                ),
                ("execute_code", StepStatus.QUEUED, None),
            ],
        )
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
                    allowed_capabilities=frozenset(
                        {Capability.CODE_EDIT, Capability.PROJECT_SHELL, Capability.GIT}
                    ),
                    sandbox_profile="default",
                ),
                owner="provider-worker",
            )
        capture = CapturingAdapter()
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            adapters={"cli": capture},
        )
        handler = ProviderStepHandler(factory, registries, adapters, worktrees=worktrees)
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="provider-worker",
                lease_seconds=60,
                candidate_limit=20,
                step_types=frozenset({"execute_code"}),
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
        async with factory() as session:
            step = await session.get(Step, step_ids["execute_code"])
            assert step is not None
            request = StepExecutionRequest(
                task_id=task_id,
                run_id=run_id,
                step_id=step.id,
                step_type="execute_code",
                payload=dict(step.payload),
                timeout_seconds=step.timeout_seconds,
                lease=token,
            )
        built, profile_id, _reservation_id, _revision = await handler._build_request(request)
        assert profile_id == "cli"
        assert len(built.context) == 1
        assert built.context[0].source == "workflow_plan_result"
        assert plan_text in built.context[0].content
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_no_plan_execute_code_builds_empty_planner_context(
    postgres_dsn: str, tmp_path: Path
) -> None:
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
            "cli",
            provider="codex",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="cli-no-plan",
            state_directory=tmp_path / "cli-state-no-plan",
            sandbox_required=True,
            capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            supported_task_types=frozenset({"coding", "general"}),
        )
        settings, registries = bundle(tmp_path, configured)
        task_id, run_id, step_ids = await seed_run_with_steps(
            factory,
            steps=[("execute_code", StepStatus.QUEUED, None)],
            task_draft={"task_type": "coding", "needs_planning": False},
        )
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
                    allowed_capabilities=frozenset(
                        {Capability.CODE_EDIT, Capability.PROJECT_SHELL, Capability.GIT}
                    ),
                    sandbox_profile="default",
                ),
                owner="provider-worker",
            )
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            adapters={"cli": CapturingAdapter()},
        )
        handler = ProviderStepHandler(factory, registries, adapters, worktrees=worktrees)
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="provider-worker",
                lease_seconds=60,
                candidate_limit=20,
                step_types=frozenset({"execute_code"}),
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
        async with factory() as session:
            step = await session.get(Step, step_ids["execute_code"])
            assert step is not None
            request = StepExecutionRequest(
                task_id=task_id,
                run_id=run_id,
                step_id=step.id,
                step_type="execute_code",
                payload=dict(step.payload),
                timeout_seconds=step.timeout_seconds,
                lease=token,
            )
        built, _profile_id, _reservation_id, _revision = await handler._build_request(request)
        assert built.context == ()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_empty_and_truncated_plan_are_not_completed(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = profile(
            "api",
            roles=frozenset({ProviderRole.PLANNER, ProviderRole.EXECUTOR}),
            supported_task_types=frozenset({"coding", "general"}),
        )
        settings, registries = bundle(tmp_path, configured)

        for finish_reason, text, expected_category in (
            ("stop", "", "planner_empty_output"),
            ("length", "partial plan only", "planner_truncated"),
        ):
            task_id, run_id, step_ids = await seed_run_with_steps(
                factory,
                steps=[
                    ("plan", StepStatus.QUEUED, None),
                    ("prepare_context", StepStatus.PENDING, None),
                ],
                task_status=TaskStatus.PLANNED,
            )
            async with factory.begin() as session:
                await synchronize_profiles(
                    session, registries.profiles.items(), configuration_revision="a" * 64
                )
            capture = CapturingAdapter(text=text, finish_reason=finish_reason)
            adapters = AdapterRegistry(
                registries.profiles,
                ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
                adapters={"api": capture},
            )
            handler = ProviderStepHandler(factory, registries, adapters)
            worker = RoutedWorkflowWorker(
                settings,
                factory,
                registries=registries,
                owner=f"plan-worker-{expected_category}",
                handlers=provider_handlers(handler),
            )
            assert await worker.process_one()
            async with factory() as session:
                step = await session.get(Step, step_ids["plan"])
                run = await session.get(Run, run_id)
                assert step is not None
                assert step.status is StepStatus.QUEUED
                assert step.failure_category == expected_category
                assert isinstance(step.result, dict)
                assert step.result.get("handoff", {}).get("status") == "rejected"
                assert run is not None and run.status is RunStatus.RUNNING
            del task_id
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_plan_retry_recovers_and_stale_result_is_fenced(postgres_dsn: str, tmp_path: Path) -> None:
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
        planner = profile(
            "planner",
            roles=frozenset({ProviderRole.PLANNER}),
            supported_task_types=frozenset({"coding", "general"}),
        )
        settings, registries = bundle(tmp_path, planner)
        _task_id, _run_id, step_ids = await seed_run_with_steps(
            factory,
            steps=[
                ("plan", StepStatus.QUEUED, None),
                ("prepare_context", StepStatus.PENDING, None),
            ],
            task_status=TaskStatus.PLANNED,
        )
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )

        class RecoveringAdapter:
            def __init__(self) -> None:
                self.calls = 0

            async def execute(
                self,
                request: ProviderRequest,
                profile: ProviderProfileConfig,
                cancellation: CancellationContext,
            ) -> ProviderResult:
                del request, profile, cancellation
                self.calls += 1
                if self.calls == 1:
                    return ProviderResult(
                        status=ProviderResultStatus.SUCCEEDED,
                        text="",
                        usage=NormalizedUsage(duration_ms=5),
                        finish_reason="stop",
                        adapter_version="recover.v1",
                    )
                return ProviderResult(
                    status=ProviderResultStatus.SUCCEEDED,
                    text="Recovered plan body",
                    usage=NormalizedUsage(duration_ms=5),
                    finish_reason="stop",
                    adapter_version="recover.v1",
                )

            async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
                del profile
                return EffectiveProfileState()

        adapter = RecoveringAdapter()
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
            adapters={"planner": adapter},
        )
        handler = ProviderStepHandler(factory, registries, adapters)
        worker = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="plan-recovery",
            handlers=provider_handlers(handler),
        )
        assert await worker.process_one()
        async with factory.begin() as session:
            step = await session.get(Step, step_ids["plan"])
            assert step is not None and step.status is StepStatus.QUEUED
            assert step.failure_category == "planner_empty_output"
            # Bypass exponential retry delay so the recovery attempt is immediately claimable.
            step.available_at = func.now()
        assert await worker.process_one()
        async with factory() as session:
            step = await session.get(Step, step_ids["plan"])
            assert step is not None and step.status is StepStatus.COMPLETED
            assert step.result is not None
            assert step.result["text"] == "Recovered plan body"
            assert step.result["handoff"]["status"] == "ready"

        # Stale fencing: a completed-but-empty result must not attach to an executor request.
        exec_profile = profile(
            "cli",
            provider="codex",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="cli-stale",
            state_directory=tmp_path / "cli-stale",
            sandbox_required=True,
            capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            supported_task_types=frozenset({"coding", "general"}),
        )
        settings2, registries2 = bundle(tmp_path, exec_profile)
        task_id, run_id, stale_ids = await seed_run_with_steps(
            factory,
            steps=[
                (
                    "plan",
                    StepStatus.COMPLETED,
                    {"text": "", "finish_reason": "stop", "handoff": {"status": "ready"}},
                ),
                ("execute_code", StepStatus.QUEUED, None),
            ],
        )
        worktrees = WorktreeService(tmp_path / "worktrees-stale", LocalGit(), retention_days=3)
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries2.profiles.items(), configuration_revision="a" * 64
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
                    allowed_capabilities=frozenset(
                        {Capability.CODE_EDIT, Capability.PROJECT_SHELL, Capability.GIT}
                    ),
                    sandbox_profile="default",
                ),
                owner="stale-worker",
            )
        handler2 = ProviderStepHandler(
            factory,
            registries2,
            AdapterRegistry(
                registries2.profiles,
                ScopedSecretResolver(access_policy={}, secret_file_root=tmp_path, environment={}),
                adapters={"cli": CapturingAdapter()},
            ),
            worktrees=worktrees,
        )
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=settings2,
                registries=registries2,
                owner="stale-worker",
                lease_seconds=60,
                candidate_limit=20,
                step_types=frozenset({"execute_code"}),
            )
        assert token is not None
        async with factory.begin() as session:
            await start_step(session, token)
        async with factory() as session:
            step = await session.get(Step, stale_ids["execute_code"])
            assert step is not None
            request = StepExecutionRequest(
                task_id=task_id,
                run_id=run_id,
                step_id=step.id,
                step_type="execute_code",
                payload=dict(step.payload),
                timeout_seconds=step.timeout_seconds,
                lease=token,
            )
        with pytest.raises(LookupError, match="fenced"):
            await handler2._build_request(request)
        await engine.dispose()

    asyncio.run(scenario())
