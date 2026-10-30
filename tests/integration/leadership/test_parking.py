"""Owner 2026-10-30: park a goal to run another one, then return to it."""

import pytest
from sqlalchemy import select
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    PROJECT,
    _enable,
    _factory,
    _session,
)
from tests.integration.leadership.test_plan_revisions import approve_plan, plan_items, start
from tests.integration.leadership.test_task_control import command

from vuzol.discussion.domain import DomainError
from vuzol.leadership.parking import park_goal, unpark_goal
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import ProjectDiscussionSession, Task, TransactionalOutbox, WorkPackage
from vuzol.storage.types import TaskStatus, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _running_goal(factory):
    from tests.integration.leadership.planning_helpers import seed_plan

    session_id = await _session(factory)
    goal_id = await _enable(factory, session_id)
    await seed_plan(factory, goal_id, {"plan": plan_items(2)})
    await approve_plan(factory, goal_id)
    assert (await start(factory, goal_id)).status == "applied"
    return session_id, goal_id


async def _other_goal(factory, session_id):
    from vuzol.leadership.events import live_or_draft

    async with UnitOfWork(factory) as uow:
        discussion = await uow.session.get(ProjectDiscussionSession, session_id)
        other = await live_or_draft(
            uow.session,
            project_id=PROJECT,
            owner_user_id=OWNER,
            discussion=discussion,
            text="Другая задача",
            scope="Другая задача",
            criteria=[],
            constraints={},
            branch_prefix="vuzol/",
        )
        return other.id


async def test_park_frees_the_project_and_return_restores_the_goal(postgres_dsn):
    from vuzol.leadership.service import enqueue_goal_status

    async with _factory(postgres_dsn) as factory:
        session_id, goal_id = await _running_goal(factory)
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            parked = await park_goal(uow, goal, actor_id=str(OWNER))
            assert parked["interrupted_task_id"] is not None
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "parked"
            assert goal.constraints["parked"]["lifecycle"] == "active"
            package = await session.get(WorkPackage, goal.current_package_id)
            assert package.status is WorkPackageStatus.STOPPED
            task = await session.scalar(select(Task))
            assert task.status is TaskStatus.CANCELLED
        # The project is free: a new conversation becomes its own goal.
        other_id = await _other_goal(factory, session_id)
        assert other_id != goal_id
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            with pytest.raises(DomainError, match="goal_live_blocks_return"):
                await unpark_goal(uow, goal)
        # Closing the other goal reminds the owner of the parked one.
        async with UnitOfWork(factory) as uow:
            other = await uow.session.get(ManagedGoal, other_id, with_for_update=True)
            other.lifecycle = "cancelled"
            other.version += 1
            await enqueue_goal_status(uow.session, other)
        async with factory() as session:
            reminder = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key
                    == f"lead:parked-reminder:{other_id}:{goal_id}"
                )
            )
            assert "отложенная цель" in reminder.payload["html"]
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            await unpark_goal(uow, goal)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "active" and "parked" not in goal.constraints
            package = await session.get(WorkPackage, goal.current_package_id)
            # The interrupted step failed: the lead retries it by replanning or skips.
            assert package.status is WorkPackageStatus.PAUSED
            wakeup = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like(f"lead:unparked:{goal_id}:%")
                )
            )
            assert wakeup.payload["wakeup_kind"] == "unparked"


async def test_lead_parks_only_on_request_and_a_parked_goal_refuses_work(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        _, goal_id = await _running_goal(factory)
        refused = await command(factory, goal_id, "park_goal", {}, reason="later")
        assert refused.status == "denied" and refused.reason == "owner_request_required"
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            await park_goal(uow, goal, actor_id=str(OWNER))
        late = await command(factory, goal_id, "ask_owner", {"question": "Что дальше?"})
        assert late.status == "denied" and late.reason == "goal_parked"
