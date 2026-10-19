"""Step-less reserves are stored in the epoch admission checked."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy import select

from tests.integration.providers._test_routing_helpers import profile
from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config.settings import HardLimits
from vuzol.providers.budgets import (
    BudgetExceeded,
    accounting_for_profile,
    estimate_reservation,
    reserve_invocation_budget,
)
from vuzol.storage.models import ProviderBudgetReservation, Task
from vuzol.storage.types import IdempotencyClass


@pytest.mark.postgresql
def test_stepless_reserve_stores_the_epoch_it_checks(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task, run_id, _step = await seed_task_run_step(
            factory,
            idempotency_class=IdempotencyClass.READ_ONLY,
        )
        limits = HardLimits(task_input_tokens=1000)
        configured = profile("audit-api")
        estimate = estimate_reservation(configured, input_tokens=600, output_tokens=1)

        async def admit(epoch: int) -> list[int]:
            async with factory.begin() as session:
                row = await session.get(Task, task.id)
                assert row is not None
                row.budget_epoch = epoch
            accepted: list[int] = []
            for _ in range(3):
                try:
                    async with factory.begin() as session:
                        reservation = await reserve_invocation_budget(
                            session,
                            invocation_id=uuid.uuid4(),
                            profile=configured,
                            estimate=estimate,
                            limits=limits,
                            accounting=accounting_for_profile(configured, purpose="research"),
                            task_id=task.id,
                            run_id=run_id,
                        )
                    accepted.append(reservation.budget_epoch)
                except BudgetExceeded:
                    break
            return accepted

        assert await admit(0) == [0]
        assert await admit(1) == [1]
        async with factory() as session:
            rows = (
                await session.scalars(
                    select(ProviderBudgetReservation).where(
                        ProviderBudgetReservation.task_id == task.id
                    )
                )
            ).all()
            assert sorted(row.budget_epoch for row in rows) == [0, 1]
            assert {row.reserved_input_tokens for row in rows} == {600}
        async with factory.begin() as session:
            unbound = await reserve_invocation_budget(
                session,
                invocation_id=uuid.uuid4(),
                profile=configured,
                estimate=estimate_reservation(configured, input_tokens=1, output_tokens=1),
                limits=limits,
                accounting=accounting_for_profile(configured, purpose="research"),
            )
            assert unbound.task_id is None
            assert unbound.budget_epoch == 0
        await engine.dispose()

    asyncio.run(scenario())
