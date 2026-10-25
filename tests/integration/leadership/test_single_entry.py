"""PR9: durable text admission, owner mandate, frozen modes and terminal races."""
# ruff: noqa: RUF001

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_lead_surface import _drain, _message, _profile, _Transport
from tests.integration.leadership.test_managed_goal_flow import (
    CHAT,
    OWNER,
    PROJECT,
    THREAD,
    _enable,
    _factory,
    _runtime,
    _session,
    _worker,
)
from tests.integration.leadership.test_plan_revisions import start
from tests.integration.leadership.test_task_control import command

from vuzol.config.registries import ProfileRegistry
from vuzol.leadership.access import ProjectAccess, ProjectApplication, WorkRequest
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.model import LeadCommandDraft, LeadDecision
from vuzol.leadership.persistence import (
    GoalDelegation,
    LeadershipCommandReceipt,
    LeadershipTurn,
    ManagedGoal,
    current_request,
    has_owner_mandate,
)
from vuzol.leadership.provenance import RequestOrigin
from vuzol.storage.models import (
    ConversationTurn,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.domain import IngressStatus, WorkPackageControlUpdate
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.work_package_projections import build_work_package_plan_card

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def runtime(tmp_path):
    repo = tmp_path / "repositories" / PROJECT
    repo.mkdir(parents=True)
    result = _runtime(tmp_path, repo, lead=True, discussion=False)
    assert result.settings.chat.enabled
    return result


class Answer:
    def __init__(self):
        self.seen = []

    async def decide(self, snapshot):
        self.seen.append(snapshot)
        return LeadDecision(reply="Да, это можно обсудить здесь.", profile_id="fixture")


async def finish_goal(factory, goal_id):
    async with factory.begin() as session:
        goal = await session.get(ManagedGoal, goal_id, with_for_update=True)
        goal.lifecycle = "completed"
        goal.version += 1
        goal.fence_epoch += 1
        goal.lease_owner = goal.lease_until = None


@pytest.mark.parametrize("prefix", ["", "/lead "])
async def test_question_is_a_lead_turn_without_task_or_jev(postgres_dsn, runtime, prefix):
    async with _factory(postgres_dsn) as factory:
        ingress = TelegramIngressService(runtime, factory)
        update = _message(901, prefix + "Можно обсудить устройство проекта?")
        accepted = await ingress.accept_message(update)
        assert accepted.status is IngressStatus.HANDLED
        assert (await ingress.accept_message(update)).status is IngressStatus.DUPLICATE
        assert (
            await ingress.accept_message(update.model_copy(update={"update_id": 902}))
        ).status is IngressStatus.DUPLICATE
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal.lifecycle == "draft" and not has_owner_mandate(goal)
            intake = await session.get(TelegramIntakeMessage, accepted.intake_id)
            assert intake.task_id is None
            assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 1
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.destination.in_(
                        ("discussion_classify", "workflow_dispatch")
                    )
                )
            )
        transport = _Transport({"reply": "Да, это можно обсудить здесь.", "commands": []})
        settings = runtime.settings.model_copy(
            update={
                "project_lead": runtime.settings.project_lead.model_copy(
                    update={"profile_id": "lead-local"}
                )
            }
        )
        worker = build_project_lead_worker(
            factory,
            settings=settings,
            profiles=ProfileRegistry((_profile(),)),
            owner="single-entry",
            adapter=transport,
        )
        assert await worker.process_one()
        assert not await worker.process_one()
        assert len(transport.requests) == 1
        assert "Да, это можно обсудить здесь." in await _drain(factory)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert await session.scalar(select(func.count()).select_from(WorkPackage)) == 0
            assert await session.scalar(select(func.count()).select_from(LeadershipTurn)) == 1
            assert (await session.get(ManagedGoal, goal.id)).lifecycle == "draft"


async def test_concurrent_telegram_mcp_and_uuid_replay(postgres_dsn, runtime):
    async with _factory(postgres_dsn) as factory:
        discussion = await _session(factory)
        access = ProjectAccess(user_id=OWNER, projects={PROJECT: discussion}, allow_submit=True)
        app = ProjectApplication(runtime, factory)
        work = WorkRequest(project_id=PROJECT, request_id=uuid.uuid4(), request="Сделай страницу")
        origin = RequestOrigin(
            channel="mcp", display_name="ChatGPT", request_id=str(work.request_id)
        )
        telegram, receipt, repeated = await asyncio.gather(
            TelegramIngressService(runtime, factory).accept_message(
                _message(901, "Уточню оформление")
            ),
            app.submit(access, work, origin=origin),
            app.submit(access, work, origin=origin),
        )
        assert telegram.status is IngressStatus.HANDLED
        assert {receipt["status"], repeated["status"]} == {"accepted", "duplicate"}
        assert receipt["event_id"] == repeated["event_id"]
        assert receipt["receipt_id"] == repeated["receipt_id"]
        for changed in ({"request": "Другой текст"}, {"scope": "Другой объём"}):
            conflict = await app.submit(access, work.model_copy(update=changed))
            assert conflict["reason"] == "idempotency_conflict"
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1
            assert await session.scalar(select(func.count()).select_from(GoalDelegation)) == 1
            assert (
                await session.scalar(select(func.count()).select_from(LeadershipCommandReceipt))
                == 2
            )
            assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 2
            event = await session.get(TransactionalOutbox, receipt["event_id"])
            assert event.payload["request_origin"]["request_id"] == str(work.request_id)
            assert event.payload["request_origin"]["authorized_user_id"] == OWNER
        model = Answer()
        worker = _worker(factory, model)
        for _ in range(4):
            assert await worker.process_one()
            if len(model.seen) == 2:
                break
            async with factory.begin() as session:
                for event in await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "project_lead",
                        TransactionalOutbox.status == DeliveryStatus.PENDING,
                    )
                ):
                    event.available_at = datetime.now(UTC) - timedelta(seconds=1)
        assert len(model.seen) == 2
        assert len({s.goal_id for s in model.seen}) == 1
        assert model.seen[1].result_context["conversation"]
        await finish_goal(factory, receipt["goal_id"])
        retried = await app.submit(access, work, origin=origin)
        assert retried["event_id"] == receipt["event_id"]
        assert retried["status"] == "duplicate"
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1


async def test_plain_work_gets_plan_then_owner_button_allows_lead_start(postgres_dsn, runtime):
    class PlanAndStart:
        async def decide(self, snapshot):
            if snapshot.owner_event:
                return LeadDecision(
                    reply="Подготовлю план и пришлю его на одобрение.",
                    commands=(LeadCommandDraft("request_plan", {"goal": snapshot.request}),),
                )
            if snapshot.waiting_reason == "plan_approval":
                return LeadDecision()
            return LeadDecision(
                commands=(
                    LeadCommandDraft(
                        "start_task",
                        {"spec": snapshot.result_context["task_control"]["start_spec"]},
                    ),
                )
            )

    async with _factory(postgres_dsn) as factory:
        assert (
            await TelegramIngressService(runtime, factory).accept_message(
                _message(901, "Сделай страницу с описанием проекта")
            )
        ).status is IngressStatus.HANDLED
        worker = _worker(factory, PlanAndStart())
        assert await worker.process_one()  # lead + durable strong planner turn
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal.lifecycle == "waiting" and goal.waiting_reason == "plan_approval"
            assert not has_owner_mandate(goal)
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            card = await build_work_package_plan_card(session, goal.current_package_id)
        assert (await start(factory, goal.id)).reason == "owner_mandate_required"
        text = await _drain(factory)
        assert "Подготовлю план" in text
        assert card.html in text
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.work_package_id == card.package_id,
                    TelegramMessageLink.message_role == "work_package_plan",
                )
            )
        callbacks = {label: data for row in card.callback_buttons for label, data in row}
        result = await TelegramControlService(runtime, factory).accept(
            WorkPackageControlUpdate(
                bot_id="bot",
                update_id=902,
                callback_query_id="approve-single-entry",
                callback_data=callbacks["✅ Запускай"],
                chat_id=CHAT,
                message_thread_id=THREAD,
                message_id=link.message_id,
                user_id=OWNER,
            )
        )
        assert result.status is IngressStatus.HANDLED
        async with factory() as session:
            approved = await session.get(ManagedGoal, goal.id)
            assert has_owner_mandate(approved) and approved.lifecycle == "active"
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
        assert await worker.process_one()  # stale plan_ready
        assert await worker.process_one()  # approval event -> lead start_task
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
        # A new scope invalidates the mandate and still uses current_request.
        async with factory.begin() as session:
            task = await session.scalar(select(Task))
            from vuzol.storage.types import TaskStatus

            task.status = TaskStatus.CANCELLED
        await TelegramIngressService(runtime, factory).accept_message(
            _message(903, "Сделай справку")
        )
        async with factory() as session:
            event = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:903"
                )
            )
        amended = await command(
            factory,
            goal.id,
            "amend_goal",
            {
                "scope": "Сделай справку",
                "criteria": [{"id": "help", "description": "Есть справка"}],
                "source_event_id": str(event.id),
                "inflight_policy": "finish_current",
            },
            reason="Владелец изменил запрос",
        )
        assert amended.status == "applied"
        assert await _worker(factory, Answer()).process_one()
        async with factory() as session:
            updated = await session.get(ManagedGoal, goal.id)
            assert current_request(updated) == "Сделай справку"
            assert not has_owner_mandate(updated)
        assert (await start(factory, goal.id)).reason == "owner_mandate_required"


async def test_think_message_never_reaches_the_lead(postgres_dsn, runtime):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            for item in await session.scalars(select(TransactionalOutbox)):
                item.status = DeliveryStatus.DELIVERED
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(_message(900, "/think"))
        await ingress.accept_message(_message(901, "Сделай страницу"))
        model = Answer()
        assert not await _worker(factory, model).process_one()
        assert not model.seen
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "active" and goal.scope_revision == 1
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:901"
                )
            )
            # /think goes to its own discussion model through the interpreter.
            assert await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.destination == "discussion_classify"
                )
            )
            assert await session.scalar(select(func.count()).select_from(WorkPackage)) == 0


@pytest.mark.parametrize("when", ["before_admission", "queued", "inference", "provider_fenced"])
async def test_late_owner_event_survives_completed_goal(postgres_dsn, runtime, when):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            for item in await session.scalars(select(TransactionalOutbox)):
                item.status = DeliveryStatus.DELIVERED
        if when == "before_admission":
            await finish_goal(factory, goal_id)
        await TelegramIngressService(runtime, factory).accept_message(_message(901, "Новый вопрос"))
        if when == "queued":
            await finish_goal(factory, goal_id)
        model = Answer()
        worker = _worker(factory, model)
        if when in {"inference", "provider_fenced"}:

            class RacingAnswer:
                async def decide(self, snapshot):
                    await finish_goal(factory, goal_id)
                    if when == "provider_fenced":
                        from vuzol.leadership.model import LeadModelUnavailable

                        raise LeadModelUnavailable("lead_model_unavailable")
                    return LeadDecision(reply="Этот ответ уже устарел.")

            assert await _worker(factory, RacingAnswer()).process_one()
            async with factory.begin() as session:
                event = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:901"
                    )
                )
                assert event.status is DeliveryStatus.PENDING
                event.available_at = datetime.now(UTC) - timedelta(seconds=1)
        assert await worker.process_one()
        assert len(model.seen) == 1 and model.seen[0].goal_id != goal_id
        assert model.seen[0].owner_event["text"] == "Новый вопрос"
        assert "Да, это можно обсудить здесь." in await _drain(factory)
        assert not await worker.process_one()
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "completed"
            assert (await session.get(ManagedGoal, model.seen[0].goal_id)).lifecycle == "draft"
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 2
            assert await session.scalar(select(func.count()).select_from(Task)) == 0


async def test_late_task_result_never_opens_new_goal(postgres_dsn, runtime):
    from tests.integration.leadership.planning_helpers import fixture_plan, seed_plan

    from vuzol.storage.types import TaskStatus
    from vuzol.workflows.lead_events import publish_task_event

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": fixture_plan()})
        assert (await start(factory, goal_id)).status == "applied"
        await finish_goal(factory, goal_id)
        async with factory.begin() as session:
            for event in await session.scalars(select(TransactionalOutbox)):
                event.status = DeliveryStatus.DELIVERED
            task = await session.scalar(select(Task))
            task.status = TaskStatus.COMPLETED
            await publish_task_event(session, task)
        model = Answer()
        worker = _worker(factory, model)
        assert await worker.process_one()
        assert not await worker.process_one()
        assert not model.seen
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "completed"
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1


@pytest.mark.parametrize("wakeup", ["owner_message", "plan_ready"])
async def test_goal_moving_on_mid_call_is_not_a_model_failure(postgres_dsn, runtime, wakeup):
    """Live pilot 2026-10-24: the owner approved the plan while the lead was thinking about
    "plan ready"; the guard cancelled the stale call and the goal was parked as a model error."""
    from vuzol.leadership.model import LeadModelUnavailable
    from vuzol.leadership.wakeups import enqueue_goal_wakeup
    from vuzol.storage.models import Event

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            for item in await session.scalars(select(TransactionalOutbox)):
                item.status = DeliveryStatus.DELIVERED
        if wakeup == "owner_message":
            await TelegramIngressService(runtime, factory).accept_message(
                _message(901, "Как дела?")
            )
        else:
            async with factory.begin() as session:
                await enqueue_goal_wakeup(
                    session,
                    goal_id=goal_id,
                    idempotency_key="plan-ready-test",
                    package_id=None,
                    kind="plan_ready",
                )

        class Approved:
            async def decide(self, snapshot):
                async with factory.begin() as session:
                    goal = await session.get(ManagedGoal, goal_id, with_for_update=True)
                    goal.version += 1
                    goal.fence_epoch += 1
                raise LeadModelUnavailable("lead_model_unavailable")

        assert await _worker(factory, Approved()).process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "active" and goal.waiting_reason is None
            assert goal.lease_owner is None
            assert not await session.scalar(
                select(Event.id).where(Event.event_type == "managed_goal.lead_parked")
            )
            event = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "project_lead",
                    TransactionalOutbox.status != DeliveryStatus.DELIVERED,
                )
            )
        # The newer event (here the plan approval) drives the next turn; this one is done.
        assert event is None


@pytest.mark.parametrize(
    "reason", ["lead_response_invalid", "lead_model_unavailable", "goal_review_invalid"]
)
@pytest.mark.parametrize("second", ["answers", "malformed_again"])
async def test_one_malformed_lead_answer_is_retried_once(postgres_dsn, runtime, second, reason):
    """Live pilot 2026-10-24: one malformed JSON answer, and later one dropped provider call,
    parked the goal with a red error."""
    from vuzol.leadership.model import LeadModelUnavailable
    from vuzol.storage.models import Event

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            for item in await session.scalars(select(TransactionalOutbox)):
                item.status = DeliveryStatus.DELIVERED
        await TelegramIngressService(runtime, factory).accept_message(_message(902, "Запускай"))

        class Flaky:
            def __init__(self):
                self.seen = []

            async def decide(self, snapshot):
                self.seen.append(snapshot)
                if len(self.seen) == 1 or second == "malformed_again":
                    raise LeadModelUnavailable(reason)
                return LeadDecision(reply="Запускаю.", profile_id="fixture")

        model = Flaky()
        worker = _worker(factory, model)
        assert await worker.process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.waiting_reason is None
        assert await worker.process_one()
        assert len(model.seen) == 2
        # The retry carries the same owner message.
        assert model.seen[1].owner_event["text"] == "Запускай"
        assert not await worker.process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            parked = await session.scalar(
                select(Event.id).where(Event.event_type == "managed_goal.lead_parked")
            )
            if second == "answers":
                assert goal.waiting_reason is None and parked is None
            else:
                assert goal.waiting_reason == reason and parked is not None
