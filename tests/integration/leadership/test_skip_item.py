"""Owner 2026-10-30: skip a stuck plan step and let the lead go on."""

import pytest
from sqlalchemy import select
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    _enable,
    _factory,
    _session,
)
from tests.integration.leadership.test_plan_revisions import (
    approve_plan,
    plan_items,
    start,
)
from tests.integration.leadership.test_task_control import command

from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    PackageControlIngress,
    PackageControlResultCode,
    PackageControlSource,
)
from vuzol.discussion.domain import PackageControlAction
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.leadership.persistence import ManagedGoal
from vuzol.leadership.tasks import task_context
from vuzol.storage.models import Event, PlanRevision, Task, TransactionalOutbox, WorkPackage
from vuzol.storage.types import TaskStatus, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.transitions import transition_task

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _first_item_failed(factory):
    from tests.integration.leadership.planning_helpers import seed_plan

    goal_id = await _enable(factory, await _session(factory))
    await seed_plan(factory, goal_id, {"plan": plan_items(2)})
    await approve_plan(factory, goal_id)
    assert (await start(factory, goal_id)).status == "applied"
    async with UnitOfWork(factory) as uow:
        task = await uow.session.scalar(select(Task))
        await transition_task(uow.session, task, TaskStatus.FAILED, actor_type="test")
        await WorkPackageSequencer(uow).observe_terminal(task_id=task.id)
    async with factory() as session:
        goal = await session.get(ManagedGoal, goal_id)
        package = await session.get(WorkPackage, goal.current_package_id)
        assert package.status is WorkPackageStatus.PAUSED and package.cursor_ordinal == 1
    return goal_id


async def _skip_button(factory, goal_id, key):
    async with factory() as session:
        goal = await session.get(ManagedGoal, goal_id)
        package = await session.get(WorkPackage, goal.current_package_id)
        revision = await session.get(PlanRevision, package.head_revision_id)
        skip = AuthoritativeControlCommand(
            action=PackageControlAction.SKIP_ITEM,
            package_id=package.id,
            plan_revision_number=revision.revision_number,
            h8=revision.content_hash[:8],
            expected_status_generation=package.version,
            user_id=OWNER,
            source=PackageControlSource.TELEGRAM_CALLBACK,
            external_idempotency_key=key,
        )
    return await PackageControlIngress(
        factory, enabled=True, authorized_user_ids=frozenset({OWNER})
    ).apply(skip)


async def test_skip_button_moves_past_a_failed_step_and_wakes_the_lead(postgres_dsn):
    from vuzol.telegram.work_package_projections import build_work_package_action_card

    async with _factory(postgres_dsn) as factory:
        goal_id = await _first_item_failed(factory)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            card = await build_work_package_action_card(session, goal.current_package_id)
        labels = {label for row in card.callback_buttons for label, _data in row}
        assert "⏭ Пропустить шаг" in labels
        assert (await _skip_button(factory, goal_id, "skip:1")).code is (
            PackageControlResultCode.APPLIED
        )
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package = await session.get(WorkPackage, goal.current_package_id)
            assert package.status is WorkPackageStatus.RUNNING and package.cursor_ordinal == 2
            assert (await task_context(session, goal))["start_spec"]["ordinal"] == 2
            skipped = await session.scalar(
                select(Event).where(Event.event_type == "work_package.item_skipped")
            )
            assert skipped.payload["ordinal"] == 1
            wakeup = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like("lead:item-skipped:%")
                )
            )
            assert wakeup.payload["wakeup_kind"] == "item_skipped"
        # Item 2 depends on the skipped item 1: the owner's skip lets it start.
        assert (await start(factory, goal_id)).status == "applied"


async def test_lead_skips_only_on_request_and_the_last_skip_finishes_the_plan(postgres_dsn):
    from vuzol.leadership.skip import skip_goal_item

    async with _factory(postgres_dsn) as factory:
        goal_id = await _first_item_failed(factory)
        # No owner message behind this turn: the lead may not leave work undone.
        refused = await command(factory, goal_id, "skip_item", {}, reason="stuck")
        assert refused.status == "denied" and refused.reason == "owner_request_required"
        assert (await _skip_button(factory, goal_id, "skip:2")).code is (
            PackageControlResultCode.APPLIED
        )
        assert (await start(factory, goal_id)).status == "applied"
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            # The last step is still running: skipping cancels it and ends the plan.
            result = await skip_goal_item(uow, goal, actor_type="human", actor_id=str(OWNER))
            assert result["package_completed"] is True
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package = await session.get(WorkPackage, goal.current_package_id)
            assert package.status is WorkPackageStatus.COMPLETED
            statuses = set(await session.scalars(select(Task.status)))
            assert statuses == {TaskStatus.FAILED, TaskStatus.CANCELLED}
            assert (await task_context(session, goal))["start_spec"] is None
