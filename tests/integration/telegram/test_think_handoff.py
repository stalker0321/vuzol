"""Think handoff with the pilot flags, persisted replies and real admission/delivery."""

import asyncio
import uuid

import pytest
from sqlalchemy import select
from telegram import Update

from tests.integration.interpretation.test_conversation_modes import modes_runtime, turn
from tests.integration.storage.helpers import storage
from vuzol.discussion.agent import DeliverDiscussionReplyHandler, schedule_discussion_agent
from vuzol.discussion.conversation import select_conversation_route
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.leadership.persistence import LeadershipCommandReceipt, ManagedGoal, has_owner_mandate
from vuzol.storage.models import (
    ConversationTurn,
    ProjectDiscussionSession,
    Run,
    Step,
    Task,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    DiscussionSessionStatus,
    InteractionMode,
    StepStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.adapter import _callback_answer, control_update
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.domain import IngressStatus
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
BRIEF = {
    "request": "Добавь поиск по названию",
    "scope": "Только список проектов, без изменения карточек.",
    "criteria": ["Поиск находит проект по части названия", "Пустой запрос показывает все проекты"],
}


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def env(postgres_dsn, tmp_path):
    runtime = modes_runtime(tmp_path)
    assert runtime.settings.project_lead.enabled
    assert runtime.settings.chat.enabled
    assert not runtime.settings.project_discussion_enabled
    engine, factory = storage(postgres_dsn)
    client = FakeTelegramClient(next_message_id=2000)
    try:
        yield runtime, factory, client
    finally:
        await engine.dispose()


def delivery(env, *, lead=True, rich=False):
    _, factory, client = env
    return TelegramDeliveryService(
        factory,
        client,
        owner="handoff-test",
        lease_seconds=30,
        max_attempts=3,
        retry_min_seconds=1,
        retry_max_seconds=10,
        project_lead_enabled=lead,
        rich_messages=rich,
    )


async def drain(env, **kwargs):
    service = delivery(env, **kwargs)
    for _ in range(50):
        if not await service.deliver_one():
            return
    pytest.fail("delivery did not drain")


async def reply(env, *, handoff=BRIEF, mode="think", text="Согласовали поиск.", thinking=False):
    runtime, factory, _ = env
    await TelegramIngressService(runtime, factory).accept_message(turn(f"/{mode}"))
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.active_session_id(chat_id=-100, message_thread_id=10)
        session_id = session_id or await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
        memory = DiscussionMemoryService(uow)
        source_id, _ = await memory.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content="Да, добавь такой поиск.",
            classifier_mode=InteractionMode.DISCUSSION,
        )
        task_id = await schedule_discussion_agent(
            uow.session,
            runtime=runtime,
            session_id=session_id,
            source_turn_id=source_id,
            project_id="vuzol",
            chat_id=-100,
            thread_id=10,
            user_id=42,
            interaction_mode=InteractionMode.DISCUSSION,
            classifier_confidence=1,
            memory_pack=await memory.load_context(session_id=session_id),
            conversation_route=await select_conversation_route(
                uow.session, runtime, project_id="vuzol", mode=mode
            ),
        )
        task = await uow.session.get(Task, task_id)
        enabled = mode == "think" and runtime.settings.project_lead.enabled
        assert ('"handoff"' in task.original_text) == enabled
        assert task.task_draft["handoff_enabled"] == enabled
        steps = (
            await uow.session.scalars(
                select(Step).join(Run, Run.id == Step.run_id).where(Run.task_id == task_id)
            )
        ).all()
        execute = next(s for s in steps if s.step_type == "execute_agent")
        deliver = next(s for s in steps if s.step_type == "deliver_discussion_reply")
        execute.status = StepStatus.COMPLETED
        execute.result = {"structured_output": {"reply": text, "handoff": handoff}}
        if thinking:
            uow.session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=1999,
                    message_role=f"thinking:{source_id.hex}",
                )
            )
    request = StepExecutionRequest(
        task_id=task_id,
        run_id=deliver.run_id,
        step_id=deliver.id,
        step_type=deliver.step_type,
        payload={},
        timeout_seconds=60,
        lease=LeaseToken(
            step=StepRecord(
                id=deliver.id,
                run_id=deliver.run_id,
                status=StepStatus.RUNNING,
                lease_generation=1,
                lease_owner="test",
                lease_expires_at=None,
            ),
            owner="test",
            generation=1,
        ),
    )
    result = await DeliverDiscussionReplyHandler(factory).execute(request, CancellationContext())
    assistant_id = uuid.UUID(result.result["assistant_turn_id"])
    # Delivery-step retries retain the same answer/brief.
    again = await DeliverDiscussionReplyHandler(factory).execute(request, CancellationContext())
    assert again.result == result.result
    return assistant_id


async def button(env, assistant_id, *, number=9000):
    _, factory, _ = env
    async with factory() as session:
        item = await session.scalar(
            select(TransactionalOutbox).where(
                TransactionalOutbox.idempotency_key == f"discussion_reply:{assistant_id}"
            )
        )
        link = await session.scalar(
            select(TelegramMessageLink)
            .where(
                TelegramMessageLink.task_id == uuid.UUID(item.payload["worker_task_id"]),
                TelegramMessageLink.message_role == "discussion_reply",
            )
            .order_by(TelegramMessageLink.message_id.desc())
            .limit(1)
        )
    callback = f"v2:th:{assistant_id.hex}"
    assert len(callback.encode()) <= 64
    parsed = control_update(
        Update.de_json(
            {
                "update_id": number,
                "callback_query": {
                    "id": str(number),
                    "from": {"id": 42, "is_bot": False, "first_name": "Owner"},
                    "chat_instance": "test",
                    "data": callback,
                    "message": {
                        "message_id": link.message_id,
                        "date": 0,
                        "message_thread_id": 10,
                        "chat": {"id": -100, "type": "supergroup"},
                    },
                },
            },
            None,
        ),
        "main",
    )
    assert parsed is not None and parsed.assistant_turn_id == assistant_id
    return parsed


@pytest.mark.parametrize("thinking,long", [(False, False), (True, False), (False, True)])
async def test_handoff_delivered_and_pressed_once(env, thinking, long):
    runtime, factory, client = env
    assistant_id = await reply(env, thinking=thinking, text="Согласовали. " * (800 if long else 1))
    await drain(env, rich=long)
    messages = [m[2] for m in client.sent + client.edited]
    assert "Задача для лида:" in "".join(messages)
    assert BRIEF["request"] in "".join(messages)
    assert BRIEF["scope"] in "".join(messages)
    assert BRIEF["criteria"][0] in "".join(messages)
    keyboards = [k for k in client.sent_keyboards + client.edited_keyboards if k]
    assert keyboards == [((("➡️ Передать лиду", f"v2:th:{assistant_id.hex}"),),)]
    assert not client.rich_sent
    update = await button(env, assistant_id)
    controls = TelegramControlService(runtime, factory)
    result = await controls.accept(update)
    assert result.status is IngressStatus.HANDLED
    assert _callback_answer(result) == ("Передано лиду", False)
    for number in (update.update_id, update.update_id + 1):
        duplicate = await controls.accept(
            update.model_copy(update={"update_id": number, "callback_query_id": str(number)})
        )
        assert duplicate.status is IngressStatus.DUPLICATE
        assert _callback_answer(duplicate) == ("Уже передано", False)
    async with factory() as session:
        goal = (await session.scalars(select(ManagedGoal))).one()
        assert goal.lifecycle == "draft" and not has_owner_mandate(goal)
        assert goal.original_request == BRIEF["request"] and goal.scope_text == BRIEF["scope"]
        assert goal.criteria == [
            {"id": f"c{i}", "description": c} for i, c in enumerate(BRIEF["criteria"], 1)
        ]
        receipt = (await session.scalars(select(LeadershipCommandReceipt))).one()
        assert receipt.idempotency_key == f"think-handoff:{assistant_id}"
        assert receipt.payload["request_origin"]["channel"] == "think"
        events = (
            await session.scalars(
                select(TransactionalOutbox).where(TransactionalOutbox.destination == "project_lead")
            )
        ).all()
        assert len(events) == 1 and events[0].operation_type == "lead_wakeup"
        assert (await session.scalar(select(TopicMapping))).conversation_mode == "lead"
    await drain(env)
    assert client.edited_keyboards[-1] == ((("✅ Передано лиду", f"v2:th:{assistant_id.hex}"),),)
    assert any("Задача из обсуждения:" in m[2] for m in client.sent)
    # A subsequent owner message enters the same lead conversation.
    assert (
        await TelegramIngressService(runtime, factory).accept_message(
            turn("делай как обсудили", 10001)
        )
    ).status is IngressStatus.HANDLED


@pytest.mark.parametrize("case", ["absent", "malformed", "disabled", "lead", "disabled_delivery"])
async def test_plain_reply_without_handoff_button(env, case):
    runtime, factory, client = env
    if case == "disabled":
        runtime = runtime.model_copy(
            update={
                "settings": runtime.settings.model_copy(
                    update={
                        "project_lead": runtime.settings.project_lead.model_copy(
                            update={"enabled": False}
                        )
                    }
                )
            }
        )
        env = runtime, factory, client
    brief = None if case == "absent" else {"request": 17} if case == "malformed" else BRIEF
    assistant_id = await reply(env, handoff=brief, mode="lead" if case == "lead" else "think")
    await drain(env, lead=case not in {"disabled", "disabled_delivery"})
    assert any("Согласовали поиск." in m[2] for m in client.sent)
    assert not any(client.sent_keyboards)
    assert not any("Задача для лида:" in m[2] for m in client.sent)
    async with factory() as session:
        assert not (await session.scalars(select(ManagedGoal))).all()
        item = await session.scalar(
            select(TransactionalOutbox).where(
                TransactionalOutbox.idempotency_key == f"discussion_reply:{assistant_id}"
            )
        )
        if case != "disabled_delivery":
            assert item.payload["handoff"] is None


@pytest.mark.parametrize(
    "case", ["user", "allowed_other_owner", "thread", "chat", "inactive", "disabled", "message"]
)
async def test_unauthorized_or_unavailable_press(env, case):
    runtime, factory, _ = env
    assistant_id = await reply(env)
    await drain(env)
    update = await button(env, assistant_id)
    if case in {"user", "allowed_other_owner"}:
        update = update.model_copy(update={"user_id": 99})
    if case == "allowed_other_owner":
        runtime = runtime.model_copy(
            update={"settings": runtime.settings.model_copy(update={"allowed_user_ids": (42, 99)})}
        )
    if case in {"thread", "chat", "message"}:
        key = {"thread": "message_thread_id", "chat": "chat_id", "message": "message_id"}[case]
        update = update.model_copy(update={key: 20})
    if case == "inactive":
        async with factory.begin() as session:
            discussion = await session.scalar(select(ProjectDiscussionSession))
            discussion.status = DiscussionSessionStatus.ARCHIVED
    if case == "disabled":
        runtime = runtime.model_copy(
            update={
                "settings": runtime.settings.model_copy(
                    update={
                        "project_lead": runtime.settings.project_lead.model_copy(
                            update={"enabled": False}
                        )
                    }
                )
            }
        )
    assert (
        await TelegramControlService(runtime, factory).accept(update)
    ).status is IngressStatus.REJECTED
    async with factory() as session:
        assert not (await session.scalars(select(LeadershipCommandReceipt))).all()
        assert not (await session.scalars(select(ManagedGoal))).all()
        assert (await session.scalar(select(TopicMapping))).conversation_mode == "think"


async def test_newer_handoff_supersedes_old_and_duplicate_does_not_switch_mode(env):
    runtime, factory, _ = env
    older = await reply(env)
    newer = await reply(env)
    await drain(env)
    controls = TelegramControlService(runtime, factory)
    assert (await controls.accept(await button(env, newer))).status is IngressStatus.HANDLED
    await TelegramIngressService(runtime, factory).accept_message(turn("/think", 15000))
    for assistant_id in (older, newer):
        assert (
            await controls.accept(await button(env, assistant_id, number=9001))
        ).status is IngressStatus.DUPLICATE
    async with factory() as session:
        assert len((await session.scalars(select(LeadershipCommandReceipt))).all()) == 1
        assert (await session.scalar(select(TopicMapping))).conversation_mode == "think"


async def test_concurrent_presses_create_one_event(env):
    runtime, factory, _ = env
    assistant_id = await reply(env)
    await drain(env)
    update = await button(env, assistant_id)
    controls = TelegramControlService(runtime, factory)
    results = await asyncio.gather(
        controls.accept(update),
        controls.accept(update.model_copy(update={"update_id": 9001, "callback_query_id": "9001"})),
    )
    assert sorted(r.status for r in results) == sorted(
        [IngressStatus.HANDLED, IngressStatus.DUPLICATE]
    )
    async with factory() as session:
        assert len((await session.scalars(select(LeadershipCommandReceipt))).all()) == 1
        turns = (
            await session.scalars(
                select(ConversationTurn).where(
                    ConversationTurn.source == ConversationTurnSource.CONTROL
                )
            )
        ).all()
        assert len(turns) == 1


async def test_busy_lead_receives_new_brief_on_its_live_goal(env):
    runtime, factory, _ = env
    first = await reply(env)
    await drain(env)
    controls = TelegramControlService(runtime, factory)
    assert (await controls.accept(await button(env, first))).status is IngressStatus.HANDLED
    async with factory.begin() as session:
        goal = await session.scalar(select(ManagedGoal))
        goal.lifecycle = "active"
        goal_id = goal.id
    await TelegramIngressService(runtime, factory).accept_message(turn("/think", 15000))
    second_brief = {"request": "Добавь фильтр по статусу", "scope": None, "criteria": []}
    second = await reply(env, handoff=second_brief)
    await drain(env)
    assert (
        await controls.accept(await button(env, second, number=9001))
    ).status is IngressStatus.HANDLED
    async with factory() as session:
        goals = (await session.scalars(select(ManagedGoal))).all()
        assert len(goals) == 1 and goals[0].id == goal_id and goals[0].lifecycle == "active"
        receipts = (await session.scalars(select(LeadershipCommandReceipt))).all()
        assert len(receipts) == 2 and all(r.goal_id == goal_id for r in receipts)
        assert (await session.scalar(select(TopicMapping))).conversation_mode == "lead"


async def test_submission_failure_rolls_back_event_mode_and_confirmation(env, monkeypatch):
    from vuzol.leadership.events import ProjectEventService

    runtime, factory, _ = env
    assistant_id = await reply(env)
    await drain(env)
    update = await button(env, assistant_id)
    controls = TelegramControlService(runtime, factory)
    original = ProjectEventService.submit_event

    async def fail_after_admission(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("simulated crash before transaction commit")

    with monkeypatch.context() as patch:
        patch.setattr(ProjectEventService, "submit_event", fail_after_admission)
        with pytest.raises(RuntimeError, match="simulated crash"):
            await controls.accept(update)
    async with factory() as session:
        assert not (await session.scalars(select(ManagedGoal))).all()
        assert not (await session.scalars(select(LeadershipCommandReceipt))).all()
        assert (await session.scalar(select(TopicMapping))).conversation_mode == "think"
        assert not (
            await session.scalars(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.operation_type.in_(("lead_wakeup", "edit_reply_markup"))
                )
            )
        ).all()
    assert (await controls.accept(update)).status is IngressStatus.HANDLED
