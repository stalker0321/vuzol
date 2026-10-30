"""Owner decisions through real Telegram delivery bindings and command fences."""
# ruff: noqa: RUF001

import re
import uuid
from dataclasses import replace

import pytest
from sqlalchemy import select
from tests.integration.leadership.test_lead_review_regressions import control
from tests.integration.leadership.test_lead_surface import _message
from tests.integration.leadership.test_managed_goal_flow import (
    CHAT,
    OWNER,
    PROJECT,
    THREAD,
    _enable,
    _factory,
    _runtime,
    _session,
)
from tests.integration.leadership.test_plan_revisions import plan_items
from tests.integration.leadership.test_readable_cards import drain
from tests.integration.leadership.test_task_control import command

from vuzol.config.settings import get_settings
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import TelegramMessageLink, TransactionalOutbox, WorkPackage
from vuzol.storage.types import WorkPackageStatus
from vuzol.telegram.adapter import _callback_answer
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, IngressStatus, WorkPackageControlUpdate
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient, enqueue_chat_notice
from vuzol.telegram.work_package_projections import build_work_package_plan_card
from vuzol.telegram.work_packages import encode_work_package_callback, parse_work_package_callback

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    get_settings.cache_clear()
    repo = tmp_path / "repositories" / PROJECT
    repo.mkdir(parents=True)
    base = _runtime(tmp_path, repo, lead=True)
    yield base.model_copy(
        update={
            "settings": base.settings.model_copy(
                # Production runs the lead with the legacy discussion flag off; the
                # lead's plan buttons must not depend on it.
                update={"allowed_user_ids": (OWNER, 99), "project_discussion_enabled": False}
            )
        }
    )
    get_settings.cache_clear()


async def question_link(factory, goal_id, version):
    async with factory() as session:
        return await session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.message_role == f"lq:{goal_id.hex}:{version}"
            )
        )


@pytest.mark.parametrize("via", ["reply", "button"])
async def test_question_answer_uses_delivered_version_and_does_not_resume_pause(
    postgres_dsn, runtime, via
):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        asked = await command(
            factory, goal_id, "ask_owner", {"question": "Нужен ли отдельный вход?"}
        )
        client = FakeTelegramClient()
        await drain(factory, client)
        link = await question_link(factory, goal_id, asked.version)
        assert link is not None
        assert sum("Нужен ли отдельный вход?" in body for _, _, body in client.sent) == 1
        keyboard = next(keys for keys in client.sent_keyboards if keys)
        assert [label for row in keyboard for label, _ in row] == ["Да", "Нет"]
        assert all(len(data.encode()) <= 64 for row in keyboard for _, data in row)
        await control(factory, goal_id, "pause_goal")
        if via == "reply":
            update = _message(900, "Да").model_copy(update={"reply_to_message_id": link.message_id})
            handler = TelegramIngressService(runtime, factory).accept_message
        else:
            update = ControlUpdate(
                bot_id="test",
                update_id=900,
                callback_query_id="answer-choice",
                chat_id=CHAT,
                message_thread_id=THREAD,
                message_id=link.message_id,
                user_id=OWNER,
                action_kind="lead_answer",
                question_version=asked.version,
                answer_option=0,
            )
            handler = TelegramControlService(runtime, factory).accept
        refused = await handler(update.model_copy(update={"user_id": 99}))
        assert refused.status is IngressStatus.REJECTED
        assert (await handler(update)).status is IngressStatus.HANDLED
        assert (await handler(update)).reason == "duplicate"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "paused"
            assert goal.constraints["owner_answer"]["question_version"] == asked.version
            assert goal.constraints["owner_answer"]["answer"] == "Да"
            assert not await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:900"
                )
            )


async def test_old_reply_and_old_button_do_not_answer_replacement_question(postgres_dsn, runtime):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        first = await command(
            factory, goal_id, "ask_owner", {"question": "Нужен ли отдельный вход?"}
        )
        client = FakeTelegramClient()
        await drain(factory, client)
        link = await question_link(factory, goal_id, first.version)
        second = await command(
            factory,
            goal_id,
            "ask_owner",
            {"question": "Какие разделы оставить?", "options": ["Все", "Только главную"]},
        )
        await drain(factory, client)
        reply = _message(901, "Да").model_copy(update={"reply_to_message_id": link.message_id})
        assert (
            await TelegramIngressService(runtime, factory).accept_message(reply)
        ).status is IngressStatus.REJECTED
        clicked = await TelegramControlService(runtime, factory).accept(
            ControlUpdate(
                bot_id="test",
                update_id=902,
                callback_query_id="old-choice",
                chat_id=CHAT,
                message_thread_id=THREAD,
                message_id=link.message_id,
                user_id=OWNER,
                action_kind="lead_answer",
                question_version=first.version,
                answer_option=0,
            )
        )
        assert clicked.status is IngressStatus.REJECTED
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.wakeup_condition["asked_version"] == second.version
            assert "owner_answer" not in goal.constraints


@pytest.mark.parametrize("decision", ["✅ Запускай", "❌ Отменить"])
async def test_plan_is_the_only_approval_decision_and_change_reply_keeps_context(
    postgres_dsn, runtime, decision
):
    from tests.integration.leadership.planning_helpers import seed_plan

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        plan = plan_items(1)
        summary = (
            "Добавить проверку входных данных и понятное сообщение об ошибке; "
            "функция пока является заглушкой."
        )
        plan["title"] = "Проверка входных данных"
        plan["items"][0].update(
            summary=summary,
            allowed_scope="src/importer.py",
            completion_criteria=["Тесты проверяют корректный ввод и ошибку."],
            suggested_risk="high",
        )
        await seed_plan(factory, goal_id, {"plan": plan})
        asked = await command(
            factory,
            goal_id,
            "ask_owner",
            {"question": f"Утвердить план {goal_id}, hash {'a' * 64} и запуск?"},
        )
        assert asked.status == "waiting"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.waiting_reason == "plan_approval"
            assert goal.wakeup_condition["kind"] == "plan_approval"
            assert not await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like("lead:question:%")
                )
            )
            card = await build_work_package_plan_card(session, goal.current_package_id)
            assert summary in card.html and "Важно:" in card.html
            assert not re.search(
                r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}|[0-9a-f]{40,64}", card.html
            )
            assert all(
                word not in card.html
                for word in ("Plan approval", "Ревизия", "scope", "версия", "high")
            )
            assert [label for row in card.callback_buttons for label, _ in row] == [
                "✅ Запускай",
                "✏️ Изменить",
                "❌ Отменить",
            ]
        client = FakeTelegramClient()
        await drain(factory, client)
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.work_package_id == card.package_id,
                    TelegramMessageLink.message_role == "work_package_plan",
                )
            )
        callbacks = {label: data for row in card.callback_buttons for label, data in row}
        update = WorkPackageControlUpdate(
            bot_id="test",
            update_id=910,
            callback_query_id="change",
            callback_data=callbacks["✏️ Изменить"],
            chat_id=CHAT,
            message_thread_id=THREAD,
            message_id=link.message_id,
            user_id=OWNER,
        )
        controls = TelegramControlService(runtime, factory)
        assert (
            await controls.accept(update.model_copy(update={"user_id": 99}))
        ).status is IngressStatus.REJECTED
        changed = await controls.accept(update)
        assert changed.reason == "change_plan"
        assert "Ответь на карточку плана" in _callback_answer(changed)[0]
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(
            _message(911, "Добавь тест пустого ввода").model_copy(
                update={"reply_to_message_id": link.message_id}
            )
        )
        await ingress.accept_message(_message(912, "Да, запускай"))
        async with factory() as session:
            event = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:911"
                )
            )
            assert event.payload["reply_context"]["message_id"] == link.message_id
            assert event.payload["reply_context"]["plan_revision_id"] == str(card.revision_id)
            assert event.payload["reply_context"]["plan"]["items"][0]["summary"] == summary
            assert (
                await session.get(WorkPackage, card.package_id)
            ).status is WorkPackageStatus.DRAFT
        for label in ("✅ Запускай", "✏️ Изменить", "❌ Отменить"):
            original = parse_work_package_callback(callbacks[label])
            stale = encode_work_package_callback(
                replace(original, revision_number=original.revision_number + 1)
            )
            assert (
                await controls.accept(
                    update.model_copy(update={"callback_data": stale, "callback_query_id": label})
                )
            ).status is IngressStatus.REJECTED
        approved = await controls.accept(
            update.model_copy(
                update={"callback_data": callbacks[decision], "callback_query_id": "decision"}
            )
        )
        assert approved.status is IngressStatus.HANDLED
        async with factory() as session:
            expected = (
                WorkPackageStatus.APPROVED
                if decision == "✅ Запускай"
                else WorkPackageStatus.DISCARDED
            )
            assert (await session.get(WorkPackage, card.package_id)).status is expected


async def test_status_edits_one_message_and_pause_button_is_fenced(postgres_dsn, runtime):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        client = FakeTelegramClient()
        await drain(factory, client)
        assert len(client.sent) == 1
        await control(factory, goal_id, "pause_goal")
        await drain(factory, client)
        assert len(client.sent) == 1 and len(client.edited) == 1
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == f"ls:{goal_id.hex}"
                )
            )
            version = goal.version
        assert client.edited[0][1] == link.message_id
        button = ControlUpdate(
            bot_id="test",
            update_id=930,
            callback_query_id="resume",
            chat_id=CHAT,
            message_thread_id=THREAD,
            message_id=link.message_id,
            user_id=OWNER,
            action_kind="lead_resume",
            goal_version=version,
        )
        controls = TelegramControlService(runtime, factory)
        assert (
            await controls.accept(button.model_copy(update={"user_id": 99}))
        ).status is IngressStatus.REJECTED
        assert (await controls.accept(button)).status is IngressStatus.HANDLED
        assert (
            await controls.accept(button.model_copy(update={"callback_query_id": "stale-resume"}))
        ).status is IngressStatus.REJECTED
        await drain(factory, client)
        assert len(client.sent) == 1
        assert {message_id for _, message_id, _ in client.edited} == {link.message_id}
        await command(factory, goal_id, "ask_owner", {"question": "Как назвать раздел?"})
        await drain(factory, client)
        assert len(client.sent) == 2
        await control(factory, goal_id, "cancel_goal")
        await drain(factory, client)
        assert len(client.sent) == 3 and "ОТМЕНЕНО" in client.sent[-1][2]


async def test_notice_identifier_guard_at_outbox_boundary(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            await enqueue_chat_notice(
                session,
                chat_id=CHAT,
                thread_id=THREAD,
                key="lead:test-notice",
                text=f"Готово <b> & {'a' * 40} {'B' * 64} 12345678-1234-1234-1234-123456789012",
            )
        client = FakeTelegramClient()
        await drain(factory, client)
        assert client.sent[0][2] == "Готово &lt;b&gt; &amp;   "


async def test_lead_reply_guard_and_question_do_not_duplicate_owner_decisions(
    postgres_dsn, runtime, tmp_path
):
    from tests.integration.leadership.test_lead_surface import owner_message
    from tests.integration.leadership.test_managed_goal_flow import _worker

    from vuzol.leadership.model import LeadCommandDraft, LeadDecision

    class Reply:
        async def decide(self, snapshot):
            if snapshot.owner_event:
                return LeadDecision(
                    reply=f"Проверю <код> {snapshot.goal_id} {'a' * 40} {'B' * 64}."
                )
            return LeadDecision()

    class Ask:
        async def decide(self, snapshot):
            return LeadDecision(
                reply="Нужен ли отдельный вход?",
                commands=(
                    LeadCommandDraft(
                        name="ask_owner", payload={"question": "Нужен ли отдельный вход?"}
                    ),
                ),
            )

    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await _worker(factory, Reply()).process_one()
        client = FakeTelegramClient()
        await drain(factory, client)
        await owner_message(factory, tmp_path, 950, "Проверь код")
        await _worker(factory, Reply()).process_one()
        await drain(factory, client)
        reply = next(body for _, _, body in client.sent if "Проверю" in body)
        assert "&lt;код&gt;" in reply and "<код>" not in reply
        assert not re.search(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}|[0-9a-f]{40,64}", reply)
        await owner_message(factory, tmp_path, 951, "Уточни следующий шаг")
        await _worker(factory, Ask()).process_one()
        await drain(factory, client)
        assert sum("Нужен ли отдельный вход?" in body for _, _, body in client.sent) == 1


async def test_live_status_serializes_delivery_and_quarantines_unknown_send(postgres_dsn):
    from telegram.error import RetryAfter, TimedOut
    from tests.integration.telegram.test_delivery import service as delivery_service

    from vuzol.storage.types import DeliveryStatus
    from vuzol.telegram.delivery import prepare_delivery

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await control(factory, goal_id, "pause_goal")
        async with factory() as session:
            rows = list(
                await session.scalars(
                    select(TransactionalOutbox)
                    .where(TransactionalOutbox.idempotency_key.like("lead:state:%"))
                    .order_by(TransactionalOutbox.payload["revision"].as_integer())
                )
            )
            with pytest.raises(RetryAfter):
                await prepare_delivery(session, rows[1], None)
        client = FakeTelegramClient(fail=TimedOut("lost response"))
        worker = delivery_service(factory, client)
        assert await worker.deliver_one()
        await drain(factory, FakeTelegramClient())
        async with factory() as session:
            first, second = (
                await session.get(TransactionalOutbox, rows[0].id),
                await session.get(TransactionalOutbox, rows[1].id),
            )
            assert first.status is DeliveryStatus.AMBIGUOUS
            assert second.status is DeliveryStatus.DEAD_LETTER
            assert second.last_error_category == "lead_projection_ambiguous"


async def test_waiting_lead_wait_keeps_plan_approval_and_approval_always_resumes(
    postgres_dsn, runtime
):
    """Live pilot 2026-10-24: a lead "wait" on a goal already waiting for plan approval was
    denied, turned into an owner question and replaced the reason; the owner's later approval
    then left the goal waiting forever."""
    from tests.integration.leadership.planning_helpers import seed_plan

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": plan_items(1)})
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package_id = goal.current_package_id
            # As the worker leaves an idle lead with a plan on the owner's card.
            goal.lifecycle = "waiting"
            goal.waiting_reason = "plan_approval"
            goal.wakeup_condition = {"kind": "plan_approval"}
        waited = await command(
            factory,
            goal_id,
            "wait_for",
            {"condition": {"kind": "package", "package_id": str(package_id)}},
        )
        assert waited.status == "waiting"
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.waiting_reason == "plan_approval"
            assert not await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like("lead:question:%")
                )
            )
            # A goal already parked under another reason by the old behavior.
            goal.waiting_reason = "goal_waiting"
            goal.wakeup_condition = None
            goal.constraints = {**goal.constraints, "owner_answer": {"answer": "убери шаг"}}
            card = await build_work_package_plan_card(session, package_id)
        client = FakeTelegramClient()
        await drain(factory, client)
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.work_package_id == package_id,
                    TelegramMessageLink.message_role == "work_package_plan",
                )
            )
        callbacks = {label: data for row in card.callback_buttons for label, data in row}
        approved = await TelegramControlService(runtime, factory).accept(
            WorkPackageControlUpdate(
                bot_id="test",
                update_id=940,
                callback_query_id="approve",
                callback_data=callbacks["✅ Запускай"],
                chat_id=CHAT,
                message_thread_id=THREAD,
                message_id=link.message_id,
                user_id=OWNER,
            )
        )
        assert approved.status is IngressStatus.HANDLED
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "active" and goal.waiting_reason is None
            # Live 2026-10-30: an old answer re-planned after every approval.
            assert goal.constraints["owner_answer"]["superseded"] is True


async def test_delegated_item_results_send_the_owner_no_card(postgres_dsn, runtime):
    """Live pilot 2026-10-25: the owner had to accept each item; the lead accepts items
    and the owner the goal."""
    import uuid

    from tests.integration.leadership.planning_helpers import seed_plan
    from tests.integration.leadership.test_plan_revisions import start

    from vuzol.storage.models import Task
    from vuzol.storage.types import TaskStatus

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": plan_items(1)})
        started = await start(factory, goal_id)
        task_id = uuid.UUID(started.result["task_id"])
        client = FakeTelegramClient()
        await drain(factory, client)
        async with factory.begin() as session:
            task = await session.get(Task, task_id, with_for_update=True)
            task.status = TaskStatus.WAITING_APPROVAL
            task.version += 1
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="task",
                    linked_entity_id=task_id,
                    idempotency_key=f"card:{uuid.uuid4()}",
                    payload={"role": "intake_ack", "task_id": str(task_id)},
                )
            )
        await drain(factory, client)
        async with factory() as session:
            assert not await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.task_id == task_id,
                    TelegramMessageLink.message_role == "task_status",
                )
            )


async def test_shown_lead_task_card_follows_the_task_after_the_decision(postgres_dsn, runtime):
    """Live pilot 2026-10-24: a lead task's result card kept its "Принять" buttons after the
    lead accepted the result, and pressing it only said the action was unavailable.
    Here the owner kept item acceptance, so the card asks them."""
    import uuid

    from tests.integration.leadership.planning_helpers import seed_plan
    from tests.integration.leadership.test_plan_revisions import start

    from vuzol.leadership.persistence import GoalDelegation
    from vuzol.storage.models import Task
    from vuzol.storage.types import TaskStatus

    async def card(task_id, status):
        async with factory.begin() as session:
            task = await session.get(Task, task_id, with_for_update=True)
            task.status = status
            task.version += 1
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="task",
                    linked_entity_id=task_id,
                    idempotency_key=f"card:{uuid.uuid4()}",
                    payload={"role": "intake_ack", "task_id": str(task_id)},
                )
            )

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": plan_items(1)})
        started = await start(factory, goal_id)
        assert started.status == "applied"
        task_id = uuid.UUID(started.result["task_id"])
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            delegation = await session.get(GoalDelegation, goal.delegation_id)
            delegation.allowed_actions = [
                action
                for action in delegation.allowed_actions
                if action not in {"accept_result", "apply_integration"}
            ]
        client = FakeTelegramClient()
        await drain(factory, client)
        await card(task_id, TaskStatus.EXECUTING)
        await drain(factory, client)
        async with factory() as session:
            assert not await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.task_id == task_id,
                    TelegramMessageLink.message_role == "task_status",
                )
            )
        await card(task_id, TaskStatus.WAITING_APPROVAL)
        await drain(factory, client)
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.task_id == task_id,
                    TelegramMessageLink.message_role == "task_status",
                )
            )
        assert link is not None
        edits = len(client.edited)
        await card(task_id, TaskStatus.COMPLETED)
        await drain(factory, client)
        assert len(client.edited) > edits
        assert client.edited[-1][1] == link.message_id
        # The owner sees the step of the goal, not ids, accounts, tokens or delivery states.
        html = client.edited[-1][2]
        assert "Шаг 1 из 1" in html
        for noise in ("Задача №", "Проект:", "Исполнитель", "Токены", "Доставка", "Работа моделей"):
            assert noise not in html


async def test_owner_retry_of_a_blocked_lead_item_resumes_the_waiting_goal(postgres_dsn, runtime):
    """Live pilot 2026-10-25: a deploy blocked a lead task's review; the owner's retry ran it
    again but the goal stayed waiting, so the lead never heard how the item ended."""
    import uuid

    from tests.integration.leadership.planning_helpers import seed_plan
    from tests.integration.leadership.test_plan_revisions import start

    from vuzol.discussion.sequencer import WorkPackageSequencer
    from vuzol.discussion.service import WorkPackageService
    from vuzol.leadership.service import request_owner_recovery
    from vuzol.storage.models import PlanRevision, Run, Step, Task, TransactionalOutbox
    from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
    from vuzol.storage.unit_of_work import UnitOfWork

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": plan_items(1)})
        started = await start(factory, goal_id)
        assert started.status == "applied"
        task_id = uuid.UUID(started.result["task_id"])
        from vuzol.workflows.dispatch import WorkflowDispatcher

        dispatcher = WorkflowDispatcher(runtime, factory, owner="retry-dispatch")
        for _ in range(5):
            if not await dispatcher.process_one():
                break
        async with factory.begin() as session:
            run = await session.scalar(select(Run).where(Run.task_id == task_id))
            step = await session.scalar(
                select(Step).where(Step.run_id == run.id).order_by(Step.ordinal.desc()).limit(1)
            )
            step.status = StepStatus.BLOCKED
            step.failure_category = "provider_unavailable"
            run.status = RunStatus.BLOCKED
            (await session.get(Task, task_id)).status = TaskStatus.BLOCKED
        async with UnitOfWork(factory) as uow:
            paused = await WorkPackageSequencer(uow).observe_terminal(task_id=task_id)
        assert paused is not None
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id, with_for_update=True)
            goal.lifecycle = "waiting"
            goal.waiting_reason = "project_task_running"
            await request_owner_recovery(session, goal, "project_task_running")
            # The owner sees which item stopped, why, and the card that continues it.
            question = goal.wakeup_condition["question"]
            assert question.startswith("Пункт 1 из 1 «"), question
            assert "остановился: модель была недоступна во время" in question
            assert "«Повторить» запустит пункт заново" in question
            reposts = await session.scalars(
                select(TransactionalOutbox.idempotency_key).where(
                    TransactionalOutbox.operation_type == "repost_plan"
                )
            )
            keys = reposts.all()
            assert any(key.startswith("wp:repost:recovery:") for key in keys)
            assert any(key.endswith(":terminal_pause") for key in keys)
            package = await session.get(WorkPackage, goal.current_package_id)
            revision = await session.get(PlanRevision, package.head_revision_id)
            package_id, version = package.id, package.version
            number, h8 = revision.revision_number, revision.content_hash[:8]
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).retry_item(
                package_id=package_id,
                revision_number=number,
                h8=h8,
                expected_status_generation=version,
                user_id=OWNER,
            )
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "active" and goal.waiting_reason is None
            assert goal.wakeup_condition is None


async def test_plain_topic_message_answers_open_question_and_supersedes_old_answer(
    postgres_dsn, runtime
):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        first = await command(factory, goal_id, "ask_owner", {"question": "Нужна ли проверка?"})
        client = FakeTelegramClient()
        await drain(factory, client)
        link = await question_link(factory, goal_id, first.version)
        ingress = TelegramIngressService(runtime, factory)
        reply = _message(909, "Убери проверку").model_copy(
            update={"reply_to_message_id": link.message_id}
        )
        assert (await ingress.accept_message(reply)).status is IngressStatus.HANDLED
        await command(factory, goal_id, "ask_owner", {"question": "Как продолжить?"})
        await drain(factory, client)
        answered = await ingress.accept_message(_message(910, "Запусти проверку"))
        assert answered.status is IngressStatus.HANDLED
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            # Not a reply, yet it answers the open question: the goal can act again.
            assert goal.lifecycle == "active" and goal.wakeup_condition is None
            # The day-old answer no longer steers the lead.
            assert goal.constraints["owner_answer"]["superseded"] is True
            event = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == f"lead:message:{CHAT}:910"
                )
            )
            assert event.payload["text"] == "Запусти проверку"
            assert event.payload["goal_version"] == goal.version


async def test_topic_message_releases_a_goal_waiting_on_its_package(postgres_dsn, runtime):
    """Live 2026-10-30: a denied start left the goal waiting on a package that never
    started; the owner's «продолжай» did not wake it and the next start was denied."""
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle = "waiting"
            goal.waiting_reason = "wait_for"
            goal.wakeup_condition = {"kind": "package", "package_id": str(uuid.uuid4())}
        ingress = TelegramIngressService(runtime, factory)
        assert (
            await ingress.accept_message(_message(911, "продолжай"))
        ).status is IngressStatus.HANDLED
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "active" and goal.wakeup_condition is None


async def test_denied_lead_command_keeps_plan_approval_and_reposts_plan(postgres_dsn, runtime):
    """Live 2026-10-30: a lead command denied on a goal waiting for plan approval
    became "уточни следующий шаг" and the plan button was out of sight."""
    from tests.integration.leadership.planning_helpers import seed_plan
    from tests.integration.leadership.test_managed_goal_flow import _worker

    from vuzol.leadership.model import LeadCommandDraft, LeadDecision
    from vuzol.storage.types import DeliveryStatus

    class Start:
        async def decide(self, snapshot):
            spec = snapshot.result_context.get("task_control", {}).get("start_spec")
            return LeadDecision(
                commands=(LeadCommandDraft(name="start_task", payload={"spec": spec}),)
            )

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": plan_items(2)})
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package_id = goal.current_package_id
            goal.lifecycle = "waiting"
            goal.waiting_reason = "plan_approval"
            goal.wakeup_condition = {"kind": "plan_approval"}
            session.add(
                TransactionalOutbox(
                    destination="project_lead",
                    operation_type="lead_wakeup",
                    linked_entity_type="managed_goal",
                    linked_entity_id=goal_id,
                    idempotency_key="test:plan-ready",
                    payload={
                        "goal_id": str(goal_id),
                        "package_id": str(package_id),
                        "wakeup_kind": "plan_ready",
                        "goal_version": goal.version,
                    },
                    status=DeliveryStatus.PENDING,
                )
            )
        worker = _worker(factory, Start())
        for _ in range(3):
            await worker.process_one()
        async with factory() as session:
            from vuzol.leadership.persistence import LeadershipCommandReceipt

            denied = await session.scalar(
                select(LeadershipCommandReceipt).where(
                    LeadershipCommandReceipt.command_name == "start_task"
                )
            )
            assert denied.status == "denied" and denied.reason == "goal_waiting"
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.waiting_reason == "plan_approval"
            assert goal.wakeup_condition["kind"] == "plan_approval"
            assert not await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like("lead:question:%")
                )
            )
            assert await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like("wp:repost:pending:%")
                )
            )


async def test_finished_package_offers_no_next_item(postgres_dsn, runtime):
    """Live 2026-10-30: a completed package still offered its final check as next."""
    from tests.integration.leadership.planning_helpers import seed_plan

    from vuzol.leadership.tasks import task_context

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await seed_plan(factory, goal_id, {"plan": plan_items(2)})
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert (await task_context(session, goal))["start_spec"] is not None
            package = await session.get(WorkPackage, goal.current_package_id)
            package.status = WorkPackageStatus.COMPLETED
            context = await task_context(session, goal)
            assert context["start_spec"] is None and "next_item" not in context


async def test_planner_quota_switch_button_requeues_the_plan_on_the_fallback(postgres_dsn, runtime):
    """Owner 2026-10-30: one button switches a planner limit wait to the fallback."""
    from vuzol.leadership.quota_wait import park_planner_for_quota
    from vuzol.storage.types import DeliveryStatus

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id, with_for_update=True)
            request = TransactionalOutbox(
                destination="project_lead",
                operation_type="lead_wakeup",
                linked_entity_type="managed_goal",
                linked_entity_id=goal_id,
                idempotency_key="test:plan-request",
                payload={
                    "goal_id": str(goal_id),
                    "wakeup_kind": "plan_requested",
                    "command_name": "request_plan",
                    "request": {"goal": "README", "constraints": []},
                },
                status=DeliveryStatus.DELIVERED,
            )
            session.add(request)
            await session.flush()
            await park_planner_for_quota(
                session,
                goal,
                request=request,
                outcome={
                    "reason": "planner_quota_exhausted",
                    "profile_id": "codex-a",
                    "fallback_profile_id": "deepseek",
                    "fallback_label": "DeepSeek",
                },
            )
        client = FakeTelegramClient()
        await drain(factory, client)
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == f"lw:{goal_id.hex}"
                )
            )
            assert link is not None
        button = ControlUpdate(
            bot_id="test",
            update_id=931,
            callback_query_id="switch",
            chat_id=CHAT,
            message_thread_id=THREAD,
            message_id=link.message_id,
            user_id=OWNER,
            action_kind="lead_fallback",
            goal_version=1,
        )
        controls = TelegramControlService(runtime, factory)
        assert (await controls.accept(button)).status is IngressStatus.HANDLED
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle != "waiting" and goal.wakeup_condition is None
            retried = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key.like("lead:planner-fallback:%")
                )
            )
            assert retried.payload["wakeup_kind"] == "plan_requested"
            assert retried.payload["planner_profile_id"] == "deepseek"
            assert retried.payload["request"] == {"goal": "README", "constraints": []}
        again = await controls.accept(button.model_copy(update={"callback_query_id": "again"}))
        assert again.status is IngressStatus.REJECTED and again.reason == "quota_wait_over"
