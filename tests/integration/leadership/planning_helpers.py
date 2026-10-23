"""Canned strong-planner transport for lifecycle tests that do not inspect planning.

Each helper runs the actual queued planner turn and leaves plan_ready for the lead.
The dedicated planning tests assert the separate turns and provider boundary directly.
"""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import select

from vuzol.config.models import ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import content_revision
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import ProjectExecutorPreference, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus


def fixture_plan():
    from tests.integration.leadership.test_managed_goal_flow import _plan

    return _plan("Package A", "Write the first marker", ["marker-a"], "MARKER.txt")


def planner_profile():
    from tests.integration.leadership.test_lead_surface import _profile

    return _profile().model_copy(
        update={
            "id": "planner-fixture",
            "roles": frozenset({ProviderRole.PLANNER}),
            "context_limit": 128000,
        }
    )


async def pin_planner(factory):
    from tests.integration.leadership.test_managed_goal_flow import PROJECT

    profile = planner_profile()
    async with factory.begin() as session:
        row = await session.get(ProjectExecutorPreference, PROJECT)
        if row is None:
            row = ProjectExecutorPreference(project_id=PROJECT, mode="auto")
            session.add(row)
        row.model_assignments = {
            **(row.model_assignments or {}),
            "planner_strong": {
                "profile_id": profile.id,
                "model": profile.model,
                "reasoning_effort": profile.model_reasoning_effort,
                "content_revision": content_revision(profile),
            },
        }


async def run_planner(factory, plan=None):
    from tests.integration.leadership.test_lead_surface import _Transport
    from tests.integration.leadership.test_managed_goal_flow import PROJECT, TEST_REPOSITORY_ROOT

    from vuzol.config.settings import ProjectLeadSettings, Settings

    async with factory() as session:
        pending = await session.scalar(
            select(TransactionalOutbox).where(
                TransactionalOutbox.destination == "project_lead",
                TransactionalOutbox.status == DeliveryStatus.PENDING,
                TransactionalOutbox.payload["wakeup_kind"].as_string() == "plan_requested",
            )
        )
        if pending is not None:
            goal = await session.get(ManagedGoal, pending.linked_entity_id)
            if (
                goal.lease_owner not in {None, "fixture-planner"}
                and goal.lease_until
                and goal.lease_until > datetime.now(UTC)
            ):
                return False
    if pending is None:
        return False
    await pin_planner(factory)
    worker = build_project_lead_worker(
        factory,
        settings=Settings(environment="test", project_lead=ProjectLeadSettings(enabled=True)),
        profiles=ProfileRegistry((planner_profile(),)),
        owner="fixture-planner",
        planner_adapter=_Transport({"plan": plan or fixture_plan()}),
        repository_paths={PROJECT: TEST_REPOSITORY_ROOT},
    )
    # Lifecycle fixtures select the queued planner turn before asserting task behavior.
    async with factory.begin() as session:
        row = await session.get(TransactionalOutbox, pending.id)
        row.available_at = datetime.now(UTC) - timedelta(days=1)
    assert await worker.process_one()
    return True


async def seed_plan(factory, goal_id, payload):
    """PR5 state fixture: bypass lead generation, exercise the authoritative revision writer."""
    from tests.integration.leadership.test_managed_goal_flow import TEST_REPOSITORY_ROOT

    from vuzol.discussion.domain import DomainError
    from vuzol.leadership.batches import propose_plan
    from vuzol.leadership.service import plan_draft_from_payload
    from vuzol.storage.unit_of_work import UnitOfWork

    async with UnitOfWork(factory) as uow:
        goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
        try:
            async with uow.session.begin_nested():
                result = await propose_plan(
                    uow,
                    goal,
                    plan_draft_from_payload(payload),
                    repository_path=TEST_REPOSITORY_ROOT,
                )
        except DomainError as error:
            return SimpleNamespace(status="denied", reason=error.code, result={})
        goal.version += 1
        goal.fence_epoch += 1
        return SimpleNamespace(status="applied", reason=None, result=result, version=goal.version)
