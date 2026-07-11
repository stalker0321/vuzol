import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import func, update

from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_step,
    complete_step,
    find_expired_leases,
    heartbeat_step,
)
from vuzol.storage.models import Step
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import StepStatus

from .helpers import seed_task_run_step, storage


@pytest.mark.postgresql
def test_two_workers_cannot_claim_one_step(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await seed_task_run_step(factory, capabilities=["code_edit"])

        async def claim(owner: str) -> LeaseToken | None:
            async with factory.begin() as session:
                return await claim_step(
                    session,
                    owner=owner,
                    lease_seconds=60,
                    capabilities=frozenset({"code_edit", "git"}),
                )

        claims = await asyncio.gather(claim("worker-a"), claim("worker-b"))
        assert sum(token is not None for token in claims) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_expired_lease_is_discoverable(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _, _, step = await seed_task_run_step(factory)
        async with factory.begin() as session:
            token = await claim_step(
                session, owner="worker-a", lease_seconds=60, capabilities=frozenset()
            )
        assert token is not None
        async with factory.begin() as session:
            await session.execute(
                update(Step)
                .where(Step.id == step.id)
                .values(lease_expires_at=func.now() - timedelta(seconds=1))
            )
        async with factory() as session:
            expired = await find_expired_leases(session)
            assert [record.id for record in expired] == [step.id]
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_stale_fencing_generation_cannot_heartbeat_or_complete(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _, _, step = await seed_task_run_step(factory)
        async with factory.begin() as session:
            first = await claim_step(
                session, owner="worker-a", lease_seconds=60, capabilities=frozenset()
            )
        assert first is not None
        async with factory.begin() as session:
            await session.execute(
                update(Step)
                .where(Step.id == step.id)
                .values(status=StepStatus.QUEUED, lease_owner=None, lease_expires_at=None)
            )
        async with factory.begin() as session:
            second = await claim_step(
                session, owner="worker-b", lease_seconds=60, capabilities=frozenset()
            )
        assert second is not None and second.generation == first.generation + 1
        with pytest.raises(LeaseLost):
            async with factory.begin() as session:
                await heartbeat_step(session, first, lease_seconds=60)
        with pytest.raises(LeaseLost):
            async with factory.begin() as session:
                await complete_step(session, first, result_payload={"late": True})
        async with factory.begin() as session:
            await complete_step(session, second, result_payload={"ok": True})
        await engine.dispose()

    asyncio.run(scenario())
