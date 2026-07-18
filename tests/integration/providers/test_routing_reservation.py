"""Routing reservation tests (split for cohesion)."""

from __future__ import annotations

from ._test_routing_helpers import *


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
def test_safe_lease_recovery_can_reuse_the_same_provider(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings, registries = bundle(tmp_path, profile("api"))
        settings = settings.model_copy(
            update={
                "limits": settings.limits.model_copy(update={"provider_call_output_tokens": 1_000})
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
