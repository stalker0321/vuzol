"""Watchdog: a waiting goal whose wakeup can never arrive is woken, then escalated."""

import uuid

import pytest
from sqlalchemy import func, select, update
from tests.integration.leadership.test_managed_goal_flow import (
    _enable,
    _factory,
    _session,
    _worker,
)
from tests.integration.leadership.test_plan_revisions import plan_items

from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import Event, Task, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Idle:
    async def decide(self, snapshot):  # pragma: no cover - reconcile never calls the model
        raise AssertionError("watchdog must not call the model")


async def _waiting(factory, goal_id, condition, *, age_minutes=10):
    async with factory.begin() as session:
        for item in await session.scalars(select(TransactionalOutbox)):
            item.status = DeliveryStatus.DELIVERED
        goal = await session.get(ManagedGoal, goal_id)
        goal.lifecycle = "waiting"
        goal.waiting_reason = "wait_for"
        goal.wakeup_condition = condition(goal)
    async with factory.begin() as session:
        await session.execute(
            update(ManagedGoal)
            .where(ManagedGoal.id == goal_id)
            .values(updated_at=func.now() - func.make_interval(0, 0, 0, 0, 0, age_minutes))
        )


async def _reconcile(factory):
    assert not await _worker(factory, Idle()).process_one()


def _package_wait(goal):
    return {"kind": "package", "package_id": str(goal.current_package_id)}


async def _goal_with_plan(factory):
    from tests.integration.leadership.planning_helpers import seed_plan

    goal_id = await _enable(factory, await _session(factory))
    await seed_plan(factory, goal_id, {"plan": plan_items(1)})
    return goal_id


async def test_package_wait_without_task_is_woken_then_escalated(postgres_dsn):
    """Live pilot 2026-10-30: a denied start left the goal waiting on a package with no
    task for half an hour until an operator noticed."""
    async with _factory(postgres_dsn) as factory:
        goal_id = await _goal_with_plan(factory)
        await _waiting(factory, goal_id, _package_wait)
        await _reconcile(factory)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "active" and goal.wakeup_condition is None
            wakeup = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like(f"lead:watchdog:{goal_id}:%")
                )
            )
            assert wakeup.payload["wakeup_kind"] == "watchdog"
            assert wakeup.status is DeliveryStatus.PENDING
        # The lead woke but parked on the same package state: the owner decides.
        await _waiting(factory, goal_id, _package_wait)
        await _reconcile(factory)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "waiting" and goal.waiting_reason == "goal_stalled"
            assert goal.wakeup_condition["kind"] == "owner"
            assert "без движения" in goal.wakeup_condition["question"]
            events = list(
                await session.scalars(
                    select(Event.event_type).where(
                        Event.entity_id == goal_id, Event.actor_id == "watchdog"
                    )
                )
            )
            assert events == ["managed_goal.watchdog_woke", "managed_goal.watchdog_escalated"]
        # An open owner question is a live wait: the watchdog leaves it alone.
        await _reconcile(factory)
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).waiting_reason == "goal_stalled"


async def test_live_waits_and_fresh_waits_are_left_alone(postgres_dsn):
    from tests.integration.leadership.test_plan_revisions import start

    async with _factory(postgres_dsn) as factory:
        goal_id = await _goal_with_plan(factory)
        assert (await start(factory, goal_id)).status == "applied"
        # A running task will publish its own event: no watchdog wakeup.
        await _waiting(factory, goal_id, _package_wait)
        await _reconcile(factory)
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "waiting"
            task = await session.scalar(select(Task))
            assert task.status not in {TaskStatus.COMPLETED, TaskStatus.CANCELLED}
            task.status = TaskStatus.CANCELLED
            await session.commit()
        # Too fresh to judge: a transition may still be on its way.
        await _waiting(factory, goal_id, _package_wait, age_minutes=0)
        await _reconcile(factory)
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "waiting"
        # An open owner question waits for the owner, however long it takes.
        await _waiting(factory, goal_id, lambda goal: {"kind": "owner", "question": "Как дальше?"})
        await _reconcile(factory)
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "waiting"


async def test_plan_approval_wait_without_pending_plan_is_woken(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _goal_with_plan(factory)
        await _waiting(
            factory,
            goal_id,
            lambda goal: {"kind": "plan_approval", "revision_id": str(uuid.uuid4())},
        )
        async with UnitOfWork(factory) as uow:
            from vuzol.telegram.attention import pending_owner_plan

            goal = await uow.session.get(ManagedGoal, goal_id)
            assert await pending_owner_plan(uow.session, goal) is None
        await _reconcile(factory)
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"
