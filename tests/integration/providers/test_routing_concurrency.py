"""Routing concurrency tests (split for cohesion)."""

from __future__ import annotations

from ._test_routing_helpers import *


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
