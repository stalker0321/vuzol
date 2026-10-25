"""PR5: revision-bound approval, preserved work, and legacy data conversion."""

import uuid
from copy import deepcopy

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.planning_helpers import seed_plan
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    TEST_REPOSITORY_ROOT,
    _enable,
    _factory,
    _session,
    _worker,
)
from tests.integration.leadership.test_task_control import command
from tests.unit.leadership.test_plan_contract import item

from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    PackageControlIngress,
    PackageControlSource,
)
from vuzol.discussion.domain import DomainError, PackageControlAction, canonical_plan_hash
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.discussion.service import WorkPackageService
from vuzol.leadership.legacy_plans import convert_legacy_plan
from vuzol.leadership.model import LeadCommandDraft, LeadDecision
from vuzol.leadership.persistence import ManagedGoal
from vuzol.leadership.tasks import task_context
from vuzol.storage.models import (
    MaterializationLink,
    PlanRevision,
    Task,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import DeliveryStatus, TaskStatus, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.work_package_projections import build_work_package_plan_card

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def plan_items(count=2, **changes):
    return {
        "title": "Import",
        "items": [
            item(
                f"item-{i}",
                suggested_risk="low",
                estimated_complexity="small",
                needs_approval=False,
                dependencies=[] if i == 1 else [f"item-{i - 1}"],
                **changes,
            )
            for i in range(1, count + 1)
        ],
    }


async def approval_command(factory, goal_id):
    async with factory() as session:
        goal = await session.get(ManagedGoal, goal_id)
        package = await session.get(WorkPackage, goal.current_package_id)
        revision = await session.get(PlanRevision, package.head_revision_id)
        return AuthoritativeControlCommand(
            action=PackageControlAction.APPROVE,
            package_id=package.id,
            plan_revision_number=revision.revision_number,
            h8=revision.content_hash[:8],
            content_hash=revision.content_hash,
            scope_revision=goal.scope_revision,
            expected_status_generation=package.version,
            user_id=OWNER,
            source=PackageControlSource.TELEGRAM_CALLBACK,
            external_idempotency_key=f"approve:{revision.id}",
        )


async def approve_plan(factory, goal_id):
    return await PackageControlIngress(
        factory, enabled=True, authorized_user_ids=frozenset({OWNER})
    ).apply(await approval_command(factory, goal_id))


async def start(factory, goal_id):
    async with factory() as session:
        goal = await session.get(ManagedGoal, goal_id)
        spec = (await task_context(session, goal))["start_spec"]
    return await command(factory, goal_id, "start_task", {"spec": spec})


@pytest.mark.parametrize("kind", ["multi", "high", "large"])
async def test_owner_gate_cannot_be_bypassed_and_approval_wakes_once(postgres_dsn, kind):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        plan = plan_items(2 if kind == "multi" else 1)
        if kind == "high":
            plan["items"][0]["suggested_risk"] = "high"
        if kind == "large":
            plan["items"][0]["estimated_complexity"] = "large"
        proposed = await seed_plan(factory, goal_id, {"plan": plan})
        assert proposed.status == "applied", proposed
        assert proposed.result["owner_approval_required"]
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id)
            package = await uow.session.get(WorkPackage, goal.current_package_id)
            revision = await uow.session.get(PlanRevision, package.head_revision_id)
            assert goal.proposed_plan is None
            assert canonical_plan_hash(revision.immutable_body) == revision.content_hash
            assert len(revision.immutable_body["source_repo_commit"]) == 40
            with pytest.raises(DomainError, match="owner_plan_approval_required"):
                await WorkPackageService(uow).bind_delegated_approval(
                    package_id=package.id,
                    revision_number=revision.revision_number,
                    h8=revision.content_hash[:8],
                    expected_status_generation=package.version,
                    delegation_id=goal.delegation_id,
                )
            card = await build_work_package_plan_card(uow.session, package.id)
            assert "src/importer.py" in card.html
            assert plan["items"][0]["summary"] in card.html
        assert (await start(factory, goal_id)).status == "denied"
        ingress = PackageControlIngress(
            factory, enabled=True, authorized_user_ids=frozenset({OWNER})
        )
        approval = await approval_command(factory, goal_id)
        await ingress.apply(approval)
        assert (await ingress.apply(approval)).duplicate
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            wakeups = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["wakeup_kind"].as_string() == "plan_approved",
                    )
                )
            )
            assert len(wakeups) == 1

        class Start:
            async def decide(self, snapshot):
                return LeadDecision(
                    commands=(
                        LeadCommandDraft(
                            name="start_task",
                            payload={"spec": snapshot.result_context["task_control"]["start_spec"]},
                        ),
                    )
                )

        # Consume the older enable event too; each item is still dispatched once.
        worker = _worker(factory, Start())
        await worker.process_one()
        await worker.process_one()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            wakeup = await session.get(TransactionalOutbox, wakeups[0].id)
            assert wakeup.status is DeliveryStatus.DELIVERED


@pytest.mark.parametrize(
    "field,value",
    [("content_hash", "a" * 64), ("plan_revision_number", 999), ("scope_revision", 2)],
)
async def test_stale_approval_fails_closed(postgres_dsn, field, value):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        assert (await seed_plan(factory, goal_id, {"plan": plan_items()})).status == "applied"
        approval = (await approval_command(factory, goal_id)).model_copy(update={field: value})
        with pytest.raises(DomainError, match="stale_revision"):
            await PackageControlIngress(
                factory, enabled=True, authorized_user_ids=frozenset({OWNER})
            ).apply(approval)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0


async def test_future_revision_keeps_materialized_contracts_and_accumulated_base(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        plan = plan_items(3)
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        old_approval = await approval_command(factory, goal_id)
        await approve_plan(factory, goal_id)
        assert (await start(factory, goal_id)).status == "applied"
        async with UnitOfWork(factory) as uow:
            task = await uow.session.scalar(select(Task))
            task.status = TaskStatus.COMPLETED
            await WorkPackageSequencer(uow).observe_terminal(task_id=task.id)
            original_task_id = task.id
        assert (await start(factory, goal_id)).status == "applied"
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package = await session.get(WorkPackage, goal.current_package_id)
            current = await session.scalar(
                select(Task)
                .join(MaterializationLink, MaterializationLink.task_id == Task.id)
                .where(MaterializationLink.ordinal == 2)
            )
            current.status = TaskStatus.FAILED
            package.status = WorkPackageStatus.PAUSED
            package.integration_branch = goal.integration_branch
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "b" * 40
        for ordinal in (0, 1):
            changed = deepcopy(plan)
            changed["items"][ordinal]["allowed_scope"] = "other.py"
            refused = await seed_plan(factory, goal_id, {"plan": changed})
            assert refused.reason == "revision_conflict"
        plan["items"][2]["goal"] = "Finish the future import"
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        with pytest.raises(DomainError, match=r"stale_generation|stale_revision"):
            await PackageControlIngress(
                factory, enabled=True, authorized_user_ids=frozenset({OWNER})
            ).apply(old_approval.model_copy(update={"external_idempotency_key": "old-card"}))
        await approve_plan(factory, goal_id)
        result = await start(factory, goal_id)
        assert result.status == "applied", result
        async with factory() as session:
            package = await session.get(WorkPackage, uuid.UUID(result.result["package_id"]))
            assert package.cursor_ordinal == 2
            assert package.integration_head_commit == "b" * 40
            assert package.integration_base_commit == "a" * 40
            assert (await session.get(Task, original_task_id)).status is TaskStatus.COMPLETED
            assert await session.scalar(select(func.count()).select_from(Task)) == 3


@pytest.mark.parametrize("lifecycle", ["paused", "completed"])
async def test_legacy_conversion_is_idempotent_and_preserves_lifecycle(postgres_dsn, lifecycle):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        raw = plan_items(1)
        raw["items"][0]["local_id"] = None
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            goal.proposed_plan = deepcopy(raw)
            goal.lifecycle = lifecycle
            await convert_legacy_plan(uow, goal, TEST_REPOSITORY_ROOT)
            await convert_legacy_plan(uow, goal, TEST_REPOSITORY_ROOT)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == lifecycle
            assert goal.proposed_plan == raw
            count = await session.scalar(select(func.count()).select_from(PlanRevision))
            assert count == (1 if lifecycle == "paused" else 0)
            if lifecycle == "paused":
                package = await session.get(WorkPackage, goal.current_package_id)
                revision = await session.get(PlanRevision, package.head_revision_id)
                assert revision.immutable_body["items"][0]["local_id"] == "legacy-1"
                assert package.approved_revision_id is None


@pytest.mark.parametrize(
    "field,value",
    [("plan_hash", "a" * 64), ("plan_revision_id", str(uuid.uuid4())), ("scope_revision", 2)],
)
async def test_start_spec_is_bound_to_the_persisted_revision(postgres_dsn, field, value):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        assert (await seed_plan(factory, goal_id, {"plan": plan_items(1)})).status == "applied"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            context = await task_context(session, goal)
            spec = context["start_spec"]
            assert context["next_item"]["local_id"] == "item-1"
            assert (
                context["next_item"]["allowed_scope"] == "src/importer.py, tests/test_importer.py"
            )
        refused = await command(factory, goal_id, "start_task", {"spec": {**spec, field: value}})
        assert refused.reason == "task_spec_stale"
        assert (await start(factory, goal_id)).status == "applied"
        async with factory() as session:
            revision = await session.get(PlanRevision, uuid.UUID(spec["plan_revision_id"]))
            assert revision.approved_by_user_id is None
            assert revision.approved_by_delegation_id is not None


async def test_replanning_before_approval_invalidates_the_old_card(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        plan = plan_items()
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        old = await approval_command(factory, goal_id)
        plan["items"][1]["allowed_scope"] = "src/new_importer.py"
        revised = await seed_plan(factory, goal_id, {"plan": plan})
        assert revised.status == "applied", revised
        ingress = PackageControlIngress(
            factory, enabled=True, authorized_user_ids=frozenset({OWNER})
        )
        with pytest.raises(DomainError, match=r"stale_generation|stale_revision"):
            await ingress.apply(old)
        current = await approval_command(factory, goal_id)
        await ingress.apply(current)
        # A different callback id for the same approved revision also emits no second wakeup.
        async with factory() as session:
            package = await session.get(WorkPackage, current.package_id)
            generation = package.version
        await ingress.apply(
            current.model_copy(
                update={
                    "expected_status_generation": generation,
                    "external_idempotency_key": "second-click",
                }
            )
        )
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(TransactionalOutbox)
                    .where(
                        TransactionalOutbox.payload["wakeup_kind"].as_string() == "plan_approved"
                    )
                )
                == 1
            )


@pytest.mark.parametrize("new_proposal", [False, True])
async def test_legacy_conversion_preserves_materialized_history(postgres_dsn, new_proposal):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        raw = plan_items(1)
        assert (await seed_plan(factory, goal_id, {"plan": raw})).status == "applied"
        assert (await start(factory, goal_id)).status == "applied"
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            package = await uow.session.get(WorkPackage, goal.current_package_id)
            revision = await uow.session.get(PlanRevision, package.head_revision_id)
            original_body = deepcopy(revision.immutable_body)
            original_id = revision.id
            task = await uow.session.scalar(select(Task))
            task.status = TaskStatus.COMPLETED
            await WorkPackageSequencer(uow).observe_terminal(task_id=task.id)
            goal.lifecycle = "paused"
            goal.constraints = {
                k: v for k, v in goal.constraints.items() if k != "legacy_plan_revision_v1"
            }
            if new_proposal:
                raw["title"] = "Future import"
                raw["items"][0]["summary"] = "Create the next marker"
            goal.proposed_plan = raw
            await convert_legacy_plan(uow, goal, TEST_REPOSITORY_ROOT)
            await convert_legacy_plan(uow, goal, TEST_REPOSITORY_ROOT)
        async with factory() as session:
            assert (await session.get(PlanRevision, original_id)).immutable_body == original_body
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert await session.scalar(select(func.count()).select_from(PlanRevision)) == (
                2 if new_proposal else 1
            )
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "paused"


async def test_paused_goal_can_revise_and_approve_without_resuming(postgres_dsn):
    from tests.integration.leadership.test_lead_review_regressions import control

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        plan = plan_items()
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        assert (await control(factory, goal_id, "pause_goal")).status == "applied"
        plan["items"][1]["goal"] = "Changed future goal"
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        await approve_plan(factory, goal_id)
        assert (await start(factory, goal_id)).reason == "goal_paused"
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "paused"
            assert await session.scalar(select(func.count()).select_from(Task)) == 0


async def test_next_item_of_an_approved_plan_does_not_ask_for_approval(postgres_dsn):
    """Live pilot 2026-10-24: the lead read the planner's needs_approval on the next item of an
    approved plan as "wait for the owner's button" and stopped."""
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        plan = plan_items(1)
        plan["items"][0]["needs_approval"] = True
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package = await session.get(WorkPackage, goal.current_package_id)
            package.approved_revision_id = package.head_revision_id
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            context = await task_context(session, goal)
            assert context["start_spec"] is not None
            assert "needs_approval" not in context["next_item"]
            assert context["next_item"]["plan_approved"] is True


def pilot_items(count):
    plan = plan_items(count)
    for entry in plan["items"]:
        entry.update(suggested_risk="medium", needs_approval=True)
    return plan


async def test_follow_up_items_at_the_approved_risk_need_no_new_approval(postgres_dsn):
    """Live pilot 2026-10-25: the owner approved a one-item README plan, then had to approve
    again for each review-driven follow-up item (revisions 2 and 3), all of the same risk."""
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        plan = pilot_items(1)
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        await approve_plan(factory, goal_id)
        assert (await start(factory, goal_id)).status == "applied"
        async with UnitOfWork(factory) as uow:
            task = await uow.session.scalar(select(Task))
            task.status = TaskStatus.COMPLETED
            await WorkPackageSequencer(uow).observe_terminal(task_id=task.id)

        grown = pilot_items(2)
        grown["items"][0] = plan["items"][0]
        proposed = await seed_plan(factory, goal_id, {"plan": grown})
        assert proposed.status == "applied", proposed
        assert proposed.result["owner_approval_required"] is False
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.waiting_reason != "plan_approval"
            outbox = await session.scalars(
                select(TransactionalOutbox.operation_type).where(
                    TransactionalOutbox.destination == "work_package_projection",
                    TransactionalOutbox.idempotency_key.like("wp:projection:proposal:%"),
                )
            )
            assert "repost_plan" in outbox.all()
        riskier = deepcopy(grown)
        riskier["items"].append(
            item("item-3", suggested_risk="high", dependencies=["item-2"], needs_approval=True)
        )
        proposed = await seed_plan(factory, goal_id, {"plan": riskier})
        assert proposed.result.get("owner_approval_required") is True, proposed
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).waiting_reason == "plan_approval"
        back = deepcopy(grown)
        back["items"][1]["goal"] = "Finish the import"
        proposed = await seed_plan(factory, goal_id, {"plan": back})
        assert proposed.result["owner_approval_required"] is False
        result = await start(factory, goal_id)
        assert result.status == "applied", result
