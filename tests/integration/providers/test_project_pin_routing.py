"""Project executor pin routing — PostgreSQL claim integration.

Cohesive suite for project-scoped /model pins: exclusive profile fence,
same-family fallback attribution, claim-time overrides, and preference CAS.
Uses shared routing helpers with explicit imports (no monolithic suite).
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import func, select

from vuzol.providers.errors import ProviderFailure
from vuzol.storage.models import ProjectExecutorPreference, RoutingDecision, Step, Task

from ._test_routing_helpers import (
    Capability,
    ProviderErrorCategory,
    ProviderRole,
    StepStatus,
    TaskStatus,
    asyncio,
    bundle,
    claim_routed_step,
    pin_project,
    profile,
    pytest,
    record_failure_observation,
    seed_provider_step,
    storage,
    synchronize_profiles,
)

@pytest.mark.postgresql
def test_project_pin_fence_blocks_cross_family_when_primary_unhealthy(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """Pinned Sol must not fall through to Grok when the Codex profile is unhealthy."""

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        (tmp_path / "codex-state").mkdir()
        (tmp_path / "grok-state").mkdir()
        codex = profile(
            "codex-subscription-prod",
            provider="codex",
            model="gpt-5.6-sol",
            model_reasoning_effort="medium",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="codex-prod",
            state_directory=tmp_path / "codex-state",
            sandbox_required=True,
            capabilities=frozenset(
                {
                    Capability.CODE_EDIT,
                    Capability.REPOSITORY_READ,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR, ProviderRole.PLANNER}),
            routing_priority=200,
            fallback_profile_ids=("grok-subscription-a",),
        )
        grok = profile(
            "grok-subscription-a",
            provider="grok",
            model="grok-build",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="grok-a",
            state_directory=tmp_path / "grok-state",
            sandbox_required=True,
            capabilities=frozenset(
                {
                    Capability.CODE_EDIT,
                    Capability.REPOSITORY_READ,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR, ProviderRole.PLANNER}),
            routing_priority=210,
            fallback_profile_ids=(),
        )
        settings, registries = bundle(tmp_path, codex, grok, projects=(pin_project(),))
        task_id, _run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_code",
            capabilities=[
                Capability.CODE_EDIT.value,
                Capability.REPOSITORY_READ.value,
                Capability.GIT.value,
                Capability.PROJECT_SHELL.value,
            ],
        )
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.project_id = "bill-buddy"
            task.task_type = "coding"
            from vuzol.storage.models import ProjectExecutorPreference

            session.add(
                ProjectExecutorPreference(
                    project_id="bill-buddy",
                    mode="pin",
                    worker_key="sol",
                    reasoning_effort="high",
                    revision=2,
                )
            )
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            await record_failure_observation(
                session,
                codex,
                configuration_revision="a" * 64,
                failure=ProviderFailure(
                    ProviderErrorCategory.AUTHENTICATION,
                    retryable=False,
                    request_sent=True,
                    safe_summary="authentication failed",
                ),
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
            step = await session.get(Step, step_id)
            task = await session.get(Task, task_id)
            decision = await session.scalar(
                select(RoutingDecision).where(RoutingDecision.step_id == step_id)
            )
            assert token is None
            assert step is not None and step.status is StepStatus.BLOCKED
            assert step.executor_profile_id is None
            assert task is not None and task.status is TaskStatus.BLOCKED
            assert decision is not None and decision.selected_profile_id is None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_pin_fence_blocks_when_primary_budget_exhausted(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        (tmp_path / "codex-state").mkdir()
        (tmp_path / "grok-state").mkdir()
        codex = profile(
            "codex-subscription-prod",
            provider="codex",
            model="gpt-5.6-sol",
            model_reasoning_effort="medium",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="codex-prod",
            state_directory=tmp_path / "codex-state",
            sandbox_required=True,
            capabilities=frozenset(
                {
                    Capability.CODE_EDIT,
                    Capability.REPOSITORY_READ,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            routing_priority=200,
            fallback_profile_ids=("grok-subscription-a",),
            minimum_unknown_usage_cost=50.0,
        )
        grok = profile(
            "grok-subscription-a",
            provider="grok",
            model="grok-build",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="grok-a",
            state_directory=tmp_path / "grok-state",
            sandbox_required=True,
            capabilities=frozenset(
                {
                    Capability.CODE_EDIT,
                    Capability.REPOSITORY_READ,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            routing_priority=210,
            fallback_profile_ids=(),
            minimum_unknown_usage_cost=0.01,
        )
        settings, registries = bundle(tmp_path, codex, grok, projects=(pin_project(),))
        settings = settings.model_copy(
            update={"limits": settings.limits.model_copy(update={"task_cost_units": 1.0})}
        )
        task_id, _run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_code",
            capabilities=[
                Capability.CODE_EDIT.value,
                Capability.REPOSITORY_READ.value,
                Capability.GIT.value,
                Capability.PROJECT_SHELL.value,
            ],
        )
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.project_id = "bill-buddy"
            task.task_type = "coding"
            from vuzol.storage.models import ProjectExecutorPreference

            session.add(
                ProjectExecutorPreference(
                    project_id="bill-buddy",
                    mode="pin",
                    worker_key="sol",
                    reasoning_effort="medium",
                    revision=2,
                )
            )
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
            step = await session.get(Step, step_id)
            assert token is None
            assert step is not None
            assert step.status is StepStatus.BLOCKED
            assert step.executor_profile_id is None
            assert step.failure_category in {"budget_exhausted", "no_compatible_profile"}
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_pin_applies_overrides_and_allows_same_family_fallback(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        (tmp_path / "grok-a").mkdir()
        (tmp_path / "grok-b").mkdir()
        grok_a = profile(
            "grok-subscription-a",
            provider="grok",
            model="grok-build",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="grok-a",
            state_directory=tmp_path / "grok-a",
            sandbox_required=True,
            capabilities=frozenset(
                {
                    Capability.CODE_EDIT,
                    Capability.REPOSITORY_READ,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            routing_priority=210,
            fallback_profile_ids=("grok-subscription-b",),
        )
        grok_b = profile(
            "grok-subscription-b",
            provider="grok",
            model="grok-build",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="grok-b",
            state_directory=tmp_path / "grok-b",
            sandbox_required=True,
            capabilities=frozenset(
                {
                    Capability.CODE_EDIT,
                    Capability.REPOSITORY_READ,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            routing_priority=220,
            fallback_profile_ids=(),
        )
        settings, registries = bundle(tmp_path, grok_a, grok_b, projects=(pin_project(),))
        task_id, _run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_code",
            capabilities=[
                Capability.CODE_EDIT.value,
                Capability.REPOSITORY_READ.value,
                Capability.GIT.value,
                Capability.PROJECT_SHELL.value,
            ],
        )
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.project_id = "bill-buddy"
            task.task_type = "coding"
            from vuzol.storage.models import ProjectExecutorPreference

            session.add(
                ProjectExecutorPreference(
                    project_id="bill-buddy",
                    mode="pin",
                    worker_key="grok",
                    reasoning_effort=None,
                    revision=3,
                )
            )
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
            await record_failure_observation(
                session,
                grok_a,
                configuration_revision="a" * 64,
                failure=ProviderFailure(
                    ProviderErrorCategory.AUTHENTICATION,
                    retryable=False,
                    request_sent=True,
                    safe_summary="authentication failed",
                ),
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
            step = await session.get(Step, step_id)
            assert token is not None and token.step.id == step_id
            assert step is not None
            assert step.executor_profile_id == "grok-subscription-b"
            assert step.payload.get("executor_preference_mode") == "pin"
            assert step.payload.get("executor_worker_key") == "grok"
            assert step.payload.get("executor_fallback_profile_id") == "grok-subscription-b"
            assert step.payload.get("executor_pin_profile_id") == "grok-subscription-a"
            assert "executor_model_override" not in step.payload
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_pin_trusted_payload_carries_codex_overrides(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        (tmp_path / "codex-state").mkdir()
        codex = profile(
            "codex-subscription-prod",
            provider="codex",
            model="gpt-5.6-sol",
            model_reasoning_effort="medium",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="codex-prod",
            state_directory=tmp_path / "codex-state",
            sandbox_required=True,
            capabilities=frozenset(
                {
                    Capability.CODE_EDIT,
                    Capability.REPOSITORY_READ,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR}),
            routing_priority=200,
            fallback_profile_ids=(),
        )
        settings, registries = bundle(tmp_path, codex, projects=(pin_project(),))
        task_id, _run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_code",
            capabilities=[
                Capability.CODE_EDIT.value,
                Capability.REPOSITORY_READ.value,
                Capability.GIT.value,
                Capability.PROJECT_SHELL.value,
            ],
        )
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.project_id = "bill-buddy"
            task.task_type = "coding"
            from vuzol.storage.models import ProjectExecutorPreference

            session.add(
                ProjectExecutorPreference(
                    project_id="bill-buddy",
                    mode="pin",
                    worker_key="terra",
                    reasoning_effort="xhigh",
                    revision=4,
                )
            )
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
            step = await session.get(Step, step_id)
            assert token is not None and token.step.id == step_id
            assert step is not None
            assert step.executor_profile_id == "codex-subscription-prod"
            assert step.payload.get("executor_model_override") == "gpt-5.6-terra"
            assert step.payload.get("executor_reasoning_effort") == "xhigh"
            assert step.payload.get("executor_worker_key") == "terra"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_concurrent_preference_first_create_and_cas(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        from vuzol.projects.executor_preference import (
            ExecutorWorkerKey,
            ensure_preference_row,
            set_auto_preference,
            set_worker_preference,
        )

        (tmp_path / "codex-state").mkdir()
        codex = profile(
            "codex-subscription-prod",
            provider="codex",
            model="gpt-5.6-sol",
            api_base_url=None,
            launch_mode="cli",
            credential_reference=None,
            credential_required=False,
            runtime_identity="codex-prod",
            state_directory=tmp_path / "codex-state",
            sandbox_required=True,
            capabilities=frozenset({Capability.CODE_EDIT}),
            supported_task_types=frozenset({"coding"}),
            roles=frozenset({ProviderRole.EXECUTOR}),
        )
        _settings, registries = bundle(tmp_path, codex)

        async def first_create(project_id: str) -> str:
            async with factory.begin() as session:
                row = await ensure_preference_row(session, project_id)
                return row.project_id

        results = await asyncio.gather(
            first_create("bill-buddy"),
            first_create("bill-buddy"),
            first_create("bill-buddy"),
        )
        assert list(results) == ["bill-buddy", "bill-buddy", "bill-buddy"]
        async with factory() as session:
            from vuzol.storage.models import ProjectExecutorPreference

            count = await session.scalar(
                select(func.count())
                .select_from(ProjectExecutorPreference)
                .where(ProjectExecutorPreference.project_id == "bill-buddy")
            )
            assert count == 1

        async with factory.begin() as session:
            view = await set_worker_preference(
                session,
                project_id="bill-buddy",
                user_id=1,
                expected_revision=1,
                worker_key=ExecutorWorkerKey.SOL,
                reasoning_effort="high",
                registries=registries,
            )
            assert view.revision == 2
        async with factory.begin() as session:
            from vuzol.projects.executor_preference import ExecutorPreferenceError

            with pytest.raises(ExecutorPreferenceError, match="stale"):
                await set_auto_preference(
                    session,
                    project_id="bill-buddy",
                    user_id=2,
                    expected_revision=1,
                )
        async with factory.begin() as session:
            view = await set_auto_preference(
                session,
                project_id="bill-buddy",
                user_id=2,
                expected_revision=2,
            )
            assert view.is_auto and view.revision == 3
        await engine.dispose()

    asyncio.run(scenario())
