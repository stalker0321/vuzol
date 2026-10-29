"""Owner-confirmed handoffs retain the request and cancel atomically."""

import uuid
from dataclasses import replace

import pytest
from sqlalchemy import select
from tests.integration.leadership import test_managed_goal_flow as flow
from tests.integration.leadership.test_lead_review_regressions import StartModel
from tests.integration.leadership.test_lead_surface import owner_message
from tests.integration.leadership.test_task_control import command

from vuzol.discussion.domain import DomainError
from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.model import LeadCommandDraft, LeadDecision
from vuzol.leadership.persistence import LeadershipCommandReceipt, ManagedGoal
from vuzol.storage.models import Task, TransactionalOutbox, WorkPackage
from vuzol.storage.types import DeliveryStatus, TaskStatus, WorkPackageStatus
from vuzol.workflows.application import Principal

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
CHOICE = "Новая задача, текущую закрыть"
OPTIONS = ["Дополнение к текущей", CHOICE, "Новая задача после текущей"]
REQUEST = "Показывай недели в длительности"


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def confirm(factory, goal_id, answer=CHOICE):
    asked = await command(
        factory, goal_id, "ask_owner", {"question": "Закрыть текущую цель?", "options": OPTIONS}
    )
    return await flow._lead_service(factory).apply_human(
        Principal(flow.OWNER, "telegram"),
        LeadCommand(
            name="answer_permission",
            project_id=flow.PROJECT,
            goal_id=goal_id,
            expected_version=asked.version,
            idempotency_key=str(uuid.uuid4()),
            payload={"answer": answer, "question_version": asked.version},
        ),
    )


async def state(factory):
    async with factory() as session:
        return (
            list(await session.scalars(select(ManagedGoal))),
            list(await session.scalars(select(TransactionalOutbox))),
        )


class RoutingLead:
    def __init__(self):
        self.snapshots = []

    async def decide(self, snapshot):
        self.snapshots.append(snapshot)
        if snapshot.owner_answer == CHOICE:
            source = snapshot.result_context["owner_answer"]["restart_from_event_id"]
            return LeadDecision(
                commands=(LeadCommandDraft("cancel_goal", {"restart_from_event_id": source}),)
            )
        if snapshot.owner_event and snapshot.owner_event["text"] == REQUEST:
            return LeadDecision(
                commands=(
                    LeadCommandDraft(
                        "ask_owner",
                        {"question": "Это новая задача? Закрыть текущую?", "options": OPTIONS},
                    ),
                )
            )
        return LeadDecision()


@pytest.mark.parametrize("worked", [False, True])
@pytest.mark.parametrize("paused", [False, True])
async def test_handoff_replays_once_and_keeps_cancellation_semantics(
    postgres_dsn, tmp_path, worked, paused
):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        if worked:
            await flow._worker(factory, StartModel()).process_one()
            await flow._worker(factory, StartModel()).process_one()
        if paused:
            await command(factory, goal_id, "pause_goal", {})
        event = await owner_message(factory, tmp_path, text=REQUEST)
        async with factory.begin() as session:
            row = await session.get(TransactionalOutbox, event.id)
            row.payload = {**row.payload, "attachments": [{"file_id": "saved-document"}]}
            # Process only the owner input, not older planning/task wakeups.
            for item in await session.scalars(select(TransactionalOutbox)):
                if item.destination == "project_lead" and item.id != event.id:
                    item.status = DeliveryStatus.DELIVERED
        model = RoutingLead()
        worker = flow._worker(factory, model)
        assert await worker.process_one()
        async with factory() as session:
            old = await session.get(ManagedGoal, goal_id)
            assert old.scope_text == "marker files only"
            version = old.wakeup_condition["asked_version"]
        result = await flow._lead_service(factory).apply_human(
            Principal(flow.OWNER, "telegram"),
            LeadCommand(
                name="answer_permission",
                project_id=flow.PROJECT,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key="owner-confirms",
                payload={"answer": CHOICE, "question_version": version},
            ),
        )
        assert result.status == "applied"
        assert await worker.process_one()
        goals, outbox = await state(factory)
        old = next(g for g in goals if g.id == goal_id)
        new = next(g for g in goals if g.id != goal_id)
        assert old.lifecycle == "cancelled" and old.waiting_reason == "closed_for_new_task"
        assert new.lifecycle == "draft" and new.original_request == REQUEST
        assert new.scope_text == REQUEST and new.constraints["mandate_required"] is True
        assert new.current_package_id is None and new.proposed_plan is None
        wakeups = [
            o for o in outbox if o.destination == "project_lead" and o.linked_entity_id == new.id
        ]
        assert len(wakeups) == 1
        assert wakeups[0].payload["text"] == REQUEST
        assert wakeups[0].payload["attachments"] == [{"file_id": "saved-document"}]
        assert len([o for o in outbox if o.destination == "goal_retrospective"]) == int(worked)
        async with factory() as session:
            if worked:
                assert (await session.scalar(select(Task))).status == TaskStatus.CANCELLED
                assert (
                    await session.scalar(select(WorkPackage))
                ).status == WorkPackageStatus.STOPPED
            receipt = await session.scalar(
                select(LeadershipCommandReceipt).where(
                    LeadershipCommandReceipt.command_name == "cancel_goal"
                )
            )
        replay = LeadCommand(
            name="cancel_goal",
            project_id=flow.PROJECT,
            goal_id=goal_id,
            expected_version=receipt.expected_version,
            idempotency_key=receipt.idempotency_key,
            payload=receipt.payload,
            reason="",
        )
        service = flow._lead_service(factory)
        assert (
            await service.apply_agent(server_agent_principal(flow.PROJECT), "worker", replay)
        ).status == "duplicate"
        assert (
            await service.apply_agent(
                server_agent_principal(flow.PROJECT),
                "worker",
                replace(replay, idempotency_key="another-attempt", expected_version=old.version),
            )
        ).status == "duplicate"
        # Even replaying the consumed source after the successor ends cannot route it again.
        async with factory.begin() as session:
            (await session.get(ManagedGoal, new.id)).lifecycle = "cancelled"
            (await session.get(TransactionalOutbox, event.id)).status = DeliveryStatus.PENDING
            (
                await session.get(TransactionalOutbox, wakeups[0].id)
            ).status = DeliveryStatus.DELIVERED
        assert await worker.process_one()
        goals, outbox = await state(factory)
        assert len(goals) == 2
        assert (
            len(
                [
                    o
                    for o in outbox
                    if o.destination == "project_lead" and o.linked_entity_id == new.id
                ]
            )
            == 1
        )


@pytest.mark.parametrize(
    "fault",
    [
        "unknown",
        "goal",
        "owner",
        "project",
        "session",
        "missing",
        "addition",
        "later",
        "old_answer",
    ],
)
async def test_handoff_denials_leave_goal_and_children_untouched(postgres_dsn, tmp_path, fault):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        if fault == "old_answer":
            await owner_message(factory, tmp_path, message_id=899, text="Earlier request")
            await confirm(factory, goal_id)
        event = await owner_message(factory, tmp_path, text=REQUEST)
        if fault not in {"missing", "old_answer"}:
            await confirm(
                factory, goal_id, {"addition": OPTIONS[0], "later": OPTIONS[2]}.get(fault, CHOICE)
            )
        source_id = event.id
        async with factory.begin() as session:
            source = await session.get(TransactionalOutbox, source_id)
            if fault == "unknown":
                source_id = uuid.uuid4()
            elif fault == "goal":
                source.linked_entity_id = uuid.uuid4()
            elif fault in {"owner", "session"}:
                key, value = (
                    ("owner_user_id", 99) if fault == "owner" else ("session_id", str(uuid.uuid4()))
                )
                source.payload = {**source.payload, key: value}
            elif fault == "project":
                source.payload = {**source.payload, "project_id": "foreign"}
            old = await session.get(ManagedGoal, goal_id)
            before = old.lifecycle, old.version, old.fence_epoch
        result = await command(
            factory, goal_id, "cancel_goal", {"restart_from_event_id": str(source_id)}
        )
        assert result.status == "denied" and result.reason.startswith("restart_")
        goals, outbox = await state(factory)
        assert len(goals) == 1
        assert (goals[0].lifecycle, goals[0].version, goals[0].fence_epoch) == before
        assert not any(o.idempotency_key.startswith("lead:restart:") for o in outbox)


async def test_draft_failure_rolls_back_children_and_retrospective(
    postgres_dsn, tmp_path, monkeypatch
):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await flow._worker(factory, StartModel()).process_one()
        await flow._worker(factory, StartModel()).process_one()
        event = await owner_message(factory, tmp_path, text=REQUEST)
        await confirm(factory, goal_id)

        async def unavailable(*args, **kwargs):
            raise DomainError("restart_draft_unavailable")

        monkeypatch.setattr("vuzol.leadership.events.live_or_draft", unavailable)
        result = await command(
            factory, goal_id, "cancel_goal", {"restart_from_event_id": str(event.id)}
        )
        assert result.status == "denied" and result.reason == "restart_draft_unavailable"
        goals, outbox = await state(factory)
        assert len(goals) == 1 and goals[0].lifecycle == "active"
        assert not any(o.destination == "goal_retrospective" for o in outbox)
        async with factory() as session:
            assert (await session.scalar(select(Task))).status != TaskStatus.CANCELLED
            assert (await session.scalar(select(WorkPackage))).status != WorkPackageStatus.STOPPED
