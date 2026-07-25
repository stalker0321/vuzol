"""Routed claim path: disk pressure must not burn attempts or provider budget."""

from __future__ import annotations

from pathlib import Path

from vuzol.config.settings import DiskPressureSettings, Settings

from ._test_routing_helpers import (
    ProviderBudgetReservation,
    QueueClass,
    RoutingDecision,
    Step,
    StepStatus,
    asyncio,
    bundle,
    claim_routed_step,
    profile,
    pytest,
    seed_provider_step,
    select,
    storage,
    synchronize_profiles,
)


class _Probe:
    def __init__(self, free: int) -> None:
        self.free = free

    def free_bytes(self, path: Path) -> int:
        del path
        return self.free


def _heavy_settings(tmp_path: Path, *, min_free: int) -> Settings:
    base, _ = bundle(tmp_path, profile("api"))
    return base.model_copy(
        update={
            "disk_pressure": DiskPressureSettings(min_free_bytes=min_free),
            "worktree_root": tmp_path / "worktrees",
        }
    )


@pytest.mark.postgresql
def test_routed_heavy_claim_skipped_when_disk_low_no_budget_or_attempt(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings = _heavy_settings(tmp_path, min_free=10_000)
        _, registries = bundle(tmp_path, profile("api"))
        _task_id, _run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_model",
            queue_class=QueueClass.HEAVY,
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
                owner="provider-worker",
                lease_seconds=60,
                candidate_limit=20,
                free_space_probe=_Probe(100),
            )
        assert token is None
        async with factory() as session:
            step = await session.get(Step, step_id)
            decisions = tuple(
                (
                    await session.scalars(
                        select(RoutingDecision).where(RoutingDecision.step_id == step_id)
                    )
                ).all()
            )
            reservations = tuple(
                (
                    await session.scalars(
                        select(ProviderBudgetReservation).where(
                            ProviderBudgetReservation.step_id == step_id
                        )
                    )
                ).all()
            )
            assert step is not None
            assert step.status is StepStatus.QUEUED
            assert step.attempt_count == 0
            assert step.lease_owner is None
            assert decisions == ()
            assert reservations == ()
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_routed_heavy_claim_recovers_after_disk_returns(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings = _heavy_settings(tmp_path, min_free=10_000)
        _, registries = bundle(tmp_path, profile("api"))
        _task_id, _run_id, step_id = await seed_provider_step(
            factory,
            step_type="execute_model",
            queue_class=QueueClass.HEAVY,
        )
        async with factory.begin() as session:
            await synchronize_profiles(
                session, registries.profiles.items(), configuration_revision="a" * 64
            )
        probe = _Probe(50)
        async with factory.begin() as session:
            blocked = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="provider-worker",
                lease_seconds=60,
                candidate_limit=20,
                free_space_probe=probe,
            )
        assert blocked is None
        probe.free = 100_000
        async with factory.begin() as session:
            recovered = await claim_routed_step(
                session,
                settings=settings,
                registries=registries,
                owner="provider-worker",
                lease_seconds=60,
                candidate_limit=20,
                free_space_probe=probe,
            )
        assert recovered is not None and recovered.step.id == step_id
        async with factory() as session:
            step = await session.get(Step, step_id)
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.step_id == step_id
                )
            )
            assert step is not None and step.attempt_count == 1
            assert reservation is not None
        await engine.dispose()

    asyncio.run(scenario())
