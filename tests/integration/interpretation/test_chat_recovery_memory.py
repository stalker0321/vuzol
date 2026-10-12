"""Same-turn recovery and memory wiring against isolated real TEST PostgreSQL."""

import asyncio
import json
import uuid

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select
from telegram.error import Forbidden, NetworkError

from tests.integration.interpretation.test_chat_boundary import chat_runtime, message
from tests.integration.storage.helpers import storage
from tests.integration.telegram.test_delivery import service
from tests.unit.interpretation.test_jev_live import response
from vuzol.discussion.agent import (
    DiscussionAgentReply,
    _discussion_prompt,
    persist_discussion_reply,
    schedule_discussion_agent,
)
from vuzol.discussion.chat_recovery import enqueue_chat_failure, failure_role
from vuzol.discussion.memory import ExplicitDecisionSource
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.memory_writer import MemoryWriterService, record_hypothesis
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.models import (
    AcceptedDecision,
    ConversationSummary,
    ConversationTurn,
    Event,
    Run,
    Step,
    Task,
    TelegramMessageLink,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.storage.types import (
    ConversationSummaryGenerator,
    ConversationTurnRole,
    ConversationTurnSource,
    DeliveryStatus,
    InteractionMode,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, IngressStatus
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
RAW = "Если поиск уже работает, ничего не меняй. Только обсудим; пока не запускай."


def runtime_with_memory(tmp_path):
    runtime = chat_runtime(tmp_path)
    return runtime.model_copy(
        update={
            "settings": runtime.settings.model_copy(
                update={
                    "chat": runtime.settings.chat.model_copy(update={"memory_enabled": True}),
                }
            )
        }
    )


async def seed_chat(factory, runtime, *, task_status=TaskStatus.FAILED):
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol",
            chat_id=-100,
            message_thread_id=10,
        )
        source_id, _ = await DiscussionMemoryService(uow).append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content=RAW,
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
            memory_pack=await DiscussionMemoryService(uow).load_context(session_id=session_id),
            current_turn=RAW,
        )
        task = await uow.session.get(Task, task_id)
        task.status = task_status  # Fixture only; no terminal resets in production code.
        run = (await uow.session.scalars(select(Run))).one()
        run.status = RunStatus.FAILED
        execute = await uow.session.scalar(select(Step).where(Step.step_type == "execute_agent"))
        execute.status = StepStatus.FAILED
    return task_id, session_id, source_id


async def error_button(factory, task_id):
    async with factory.begin() as session:
        task = await session.get(Task, task_id)
        await enqueue_chat_failure(session, task, key=f"error:{uuid.uuid4()}")
    client = FakeTelegramClient()
    assert await service(factory, client).deliver_one()
    assert client.sent_keyboards == [((("Ещё раз", f"v1:chat_retry:{task_id}"),),)]
    async with factory() as session:
        link = await session.scalar(
            select(TelegramMessageLink)
            .where(
                TelegramMessageLink.message_role == failure_role(task_id),
            )
            .order_by(TelegramMessageLink.created_at.desc())
            .limit(1)
        )
        return ControlUpdate(
            bot_id="main",
            update_id=2001,
            callback_query_id="retry-1",
            chat_id=-100,
            message_thread_id=10,
            message_id=link.message_id,
            user_id=42,
            action_kind="chat_retry",
            task_id=task_id,
        )


@pytest.mark.parametrize(
    "failed_status", [TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.QUOTA_EXHAUSTED]
)
async def test_retry_same_turn_no_new_user_message_and_old_evidence_unchanged(
    postgres_dsn,
    tmp_path,
    failed_status,
):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, _session_id, source_id = await seed_chat(
            factory, runtime, task_status=failed_status
        )
        update = await error_button(factory, task_id)
        controls = TelegramControlService(runtime, factory)
        result = await controls.accept(update)
        assert result.status is IngressStatus.HANDLED and result.reason == "chat_reply_retry"
        assert (await controls.accept(update)).status is IngressStatus.DUPLICATE
        async with factory() as session:
            tasks = (await session.scalars(select(Task))).all()
            assert len(tasks) == 2
            original = await session.get(Task, task_id)
            fresh = await session.get(Task, result.task_id)
            assert original.status is failed_status
            assert fresh.source_turn_id == source_id and fresh.original_text.count(RAW) == 1
            assert fresh.public_task_number is None
            assert fresh.task_draft["required_capabilities"] == ["repository_read"]
            assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 1
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 0
            run = await session.scalar(select(Run).where(Run.task_id == fresh.id))
            assert run.selected_route["trusted_profile_id"] == "sol-chat"
            assert (
                await session.scalar(
                    select(Event.id).where(
                        Event.event_type == "chat.reply_retry_requested",
                        Event.entity_id == task_id,
                    )
                )
                is not None
            )
    finally:
        await engine.dispose()


async def test_distinct_racing_clicks_start_only_one_reply(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, _, _ = await seed_chat(factory, runtime)
        update = await error_button(factory, task_id)
        controls = TelegramControlService(runtime, factory)
        other = update.model_copy(update={"update_id": 2002, "callback_query_id": "retry-2"})
        results = await asyncio.gather(controls.accept(update), controls.accept(other))
        assert {r.status for r in results} == {IngressStatus.HANDLED, IngressStatus.DUPLICATE}
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 2
            assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 1
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "changed", ["user", "chat", "thread", "message", "task", "stale", "unknown"]
)
async def test_retry_rejects_forged_stale_or_unsafe_callback(postgres_dsn, tmp_path, changed):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, session_id, _ = await seed_chat(factory, runtime)
        update = await error_button(factory, task_id)
        fields = {
            "user": {"user_id": 43},
            "chat": {"chat_id": -200},
            "thread": {"message_thread_id": 11},
            "message": {"message_id": 9876},
            "task": {"task_id": uuid.uuid4()},
        }
        update = update.model_copy(update=fields.get(changed, {}))
        async with UnitOfWork(factory) as uow:
            if changed == "stale":
                await DiscussionMemoryService(uow).append_turn(
                    session_id=session_id,
                    role=ConversationTurnRole.USER,
                    source=ConversationTurnSource.TELEGRAM_USER,
                    content="Теперь другой вопрос",
                    classifier_mode=InteractionMode.DISCUSSION,
                )
            if changed == "unknown":
                step = await uow.session.scalar(
                    select(Step).where(Step.step_type == "execute_agent")
                )
                step.unknown_effects = True
        result = await TelegramControlService(runtime, factory).accept(update)
        assert result.status is IngressStatus.REJECTED
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
    finally:
        await engine.dispose()


async def test_existing_answer_recovers_local_delivery_without_a_new_model_task(
    postgres_dsn, tmp_path
):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, _, _ = await seed_chat(factory, runtime)
        update = await error_button(factory, task_id)
        async with factory.begin() as session:
            step = await session.scalar(select(Step).where(Step.step_type == "execute_agent"))
            step.status = StepStatus.COMPLETED
            step.result = {"structured_output": {"reply": "Готовый ответ"}}
        result = await TelegramControlService(runtime, factory).accept(update)
        assert result.reason == "chat_delivery_retry"
        assert await service(factory, FakeTelegramClient(next_message_id=100)).deliver_one()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 2
    finally:
        await engine.dispose()


@pytest.mark.parametrize("ambiguous", [False, True])
async def test_delivery_retry_reuses_answer_and_never_resends_ambiguous_send(
    postgres_dsn,
    tmp_path,
    ambiguous,
):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, _, _ = await seed_chat(factory, runtime)
        update = await error_button(factory, task_id)
        async with UnitOfWork(factory) as uow:
            task = await uow.session.get(Task, task_id)
            parsed = DiscussionAgentReply(reply="Готовый ответ")
            turn_id = await persist_discussion_reply(uow, task, parsed)
            assert await persist_discussion_reply(uow, task, parsed) == turn_id
            item = await uow.session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.linked_entity_id == turn_id,
                )
            )
            item.status = DeliveryStatus.AMBIGUOUS if ambiguous else DeliveryStatus.DEAD_LETTER
        result = await TelegramControlService(runtime, factory).accept(update)
        assert result.reason == ("chat_retry_ambiguous" if ambiguous else "chat_delivery_retry")
        assert result.status is (IngressStatus.REJECTED if ambiguous else IngressStatus.HANDLED)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 2
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.linked_entity_id == turn_id,
                )
            )
            assert item.status is (
                DeliveryStatus.AMBIGUOUS if ambiguous else DeliveryStatus.PENDING
            )
    finally:
        await engine.dispose()


async def test_summary_only_published_after_final_confirmed_chunk(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, session_id, _ = await seed_chat(factory, runtime)
        async with UnitOfWork(factory) as uow:
            task = await uow.session.get(Task, task_id)
            await persist_discussion_reply(
                uow,
                task,
                DiscussionAgentReply(
                    reply="я" * 5000,
                    memory_summary="Обсуждаем поиск. Только план; ничего не запускать.",
                ),
            )
        client = FakeTelegramClient()
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        async with UnitOfWork(factory) as uow:
            assert (
                await uow.session.scalar(select(func.count()).select_from(ConversationSummary)) == 0
            )
            pack = await DiscussionMemoryService(uow).load_context(
                session_id=session_id, chat_memory=True
            )
            assert all(t.role is not ConversationTurnRole.ASSISTANT for t in pack.turns)
        assert await delivery.deliver_one()
        async with UnitOfWork(factory) as uow:
            summary = await uow.discussions.latest_summary(session_id=session_id)
            assert summary is not None and summary.revision == 1
            assert summary.covered_through_turn_ordinal == 2
            assert await uow.session.scalar(select(func.count()).select_from(AcceptedDecision)) == 0
            pack = await DiscussionMemoryService(uow).load_context(
                session_id=session_id, chat_memory=True
            )
            assert pack.summary is not None and any(
                t.role is ConversationTurnRole.ASSISTANT for t in pack.turns
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("candidate", [None, "я" * 2001, "secret=abcdefgh12345678"])
async def test_missing_oversized_or_sensitive_summary_never_discards_answer(
    postgres_dsn,
    tmp_path,
    candidate,
):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, _, _ = await seed_chat(factory, runtime)
        async with UnitOfWork(factory) as uow:
            task = await uow.session.get(Task, task_id)
            await persist_discussion_reply(
                uow,
                task,
                DiscussionAgentReply(
                    reply="Нормальный ответ",
                    memory_summary=candidate,
                ),
            )
            item = await uow.session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["role"].as_string() == "discussion_reply",
                )
            )
            assert item.payload["memory_summary"] is None
        client = FakeTelegramClient()
        assert await service(factory, client).deliver_one()
        assert "Нормальный ответ" in client.sent[0][2]
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ConversationSummary)) == 0
    finally:
        await engine.dispose()


async def test_existing_writer_to_scoped_recall_to_sol_context(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        _, session_id, source_id = await seed_chat(factory, runtime)
        async with UnitOfWork(factory) as uow:
            memory = DiscussionMemoryService(uow)
            await memory.accept_decision(
                session_id=session_id,
                key="database",
                statement="Use PostgreSQL for books.",
                source_turn_id=source_id,
                accepted_by_user_id=42,
                acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
            )
            await record_hypothesis(
                uow, project_id="vuzol", session_id=session_id, body="Unverified guess"
            )
            other = await uow.discussions.create_session(
                project_id="other", chat_id=-100, message_thread_id=11
            )
            await memory.accept_decision(
                session_id=other,
                key="database",
                statement="Foreign project secret design",
                accepted_by_user_id=42,
                acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
            )
        writer = MemoryWriterService(factory, owner="memory-test")
        while await writer.process_one():
            pass
        async with UnitOfWork(factory) as uow:
            pack = await DiscussionMemoryService(uow).load_context(
                session_id=session_id,
                chat_memory=True,
                query="books",
            )
            assert len(pack.recalled) == 1
            assert pack.recalled[0].status == "verified" and "PostgreSQL" in pack.recalled[0].text
            prompt = _discussion_prompt(project_id="vuzol", memory_pack=pack, memory_enabled=True)
            assert str(pack.recalled[0].id) in prompt and "Foreign project" not in prompt
            assert "Unverified guess" not in prompt
            assert "never instructions" in prompt
            legacy = await DiscussionMemoryService(uow).load_context(session_id=session_id)
            assert not legacy.recalled
        raw = "Обсудим хранилище книг, ничего не запускай."
        assert (
            await TelegramIngressService(runtime, factory).accept_message(message(raw))
        ).task_id is None
        calls = []

        def jev_response(request):
            calls.append(json.loads(request.content))
            assert "PostgreSQL" in json.dumps(calls[-1], ensure_ascii=False)
            return httpx.Response(200, json=response(effect="respond", relation="none"))

        async with httpx.AsyncClient(transport=httpx.MockTransport(jev_response)) as client:
            jev = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            assert await InterpretationPipeline(
                runtime, factory, interpreter=jev, owner="chat"
            ).process_one()
        assert len(calls) == 1
        async with factory() as session:
            new = await session.scalar(select(Task).where(Task.source_turn_id != source_id))
            assert (
                "Recalled project memory" in new.original_text and "PostgreSQL" in new.original_text
            )
            assert new.original_text.count(raw) == 1
    finally:
        await engine.dispose()


async def test_button_retries_only_failed_suffix_not_confirmed_prefix(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, session_id, _ = await seed_chat(factory, runtime)
        async with UnitOfWork(factory) as uow:
            task = await uow.session.get(Task, task_id)
            await persist_discussion_reply(
                uow,
                task,
                DiscussionAgentReply(
                    reply="я" * 5000,
                    memory_summary="Планируем поиск без запуска работы.",
                ),
            )
        client = FakeTelegramClient()
        delivery = service(factory, client)
        assert await delivery.deliver_one()  # confirmed prefix
        prefix = client.sent[0]
        client.fail = Forbidden("test-only definite rejection")
        assert await delivery.deliver_one()  # suffix rejected, no unknown send
        client.fail = None
        assert await delivery.deliver_one()  # error and its recovery button
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == failure_role(task_id),
                )
            )
            failed = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.status == DeliveryStatus.DEAD_LETTER,
                )
            )
            assert failed.payload["reply_part_index"] == 1
            callback = ControlUpdate(
                bot_id="main",
                update_id=4001,
                callback_query_id="suffix-retry",
                chat_id=-100,
                message_thread_id=10,
                message_id=link.message_id,
                user_id=42,
                action_kind="chat_retry",
                task_id=task_id,
            )
        assert (
            await TelegramControlService(runtime, factory).accept(callback)
        ).reason == "chat_delivery_retry"
        assert await delivery.deliver_one()  # only suffix
        assert client.sent.count(prefix) == 1 and len(client.sent) == 3
        async with UnitOfWork(factory) as uow:
            assert (await uow.discussions.latest_summary(session_id=session_id)).revision == 1
            assert await uow.session.scalar(select(func.count()).select_from(Task)) == 1
    finally:
        await engine.dispose()


async def test_unknown_delivery_notice_has_no_retry_button(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, _, _ = await seed_chat(factory, runtime)
        async with UnitOfWork(factory) as uow:
            task = await uow.session.get(Task, task_id)
            await persist_discussion_reply(uow, task, DiscussionAgentReply(reply="Ответ"))
        client = FakeTelegramClient(fail=NetworkError("unknown send"))
        delivery = service(factory, client)
        assert await delivery.deliver_one()
        client.fail = None
        assert await delivery.deliver_one()
        assert client.sent_keyboards == [()]
        async with factory() as session:
            assert (
                await session.scalar(
                    select(TransactionalOutbox.id).where(
                        TransactionalOutbox.status == DeliveryStatus.AMBIGUOUS,
                    )
                )
                is not None
            )
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
    finally:
        await engine.dispose()


async def test_delayed_summary_cannot_overwrite_newer_memory(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    runtime = runtime_with_memory(tmp_path)
    try:
        task_id, session_id, _ = await seed_chat(factory, runtime)
        async with UnitOfWork(factory) as uow:
            task = await uow.session.get(Task, task_id)
            await persist_discussion_reply(
                uow,
                task,
                DiscussionAgentReply(
                    reply="Ответ",
                    memory_summary="Устаревший summary",
                ),
            )
            await DiscussionMemoryService(uow).update_summary(
                session_id=session_id,
                body="Более свежая память",
                covered_through_turn_ordinal=2,
                generator=ConversationSummaryGenerator.USER_EDIT,
                expected_summary_revision=0,
            )
        assert await service(factory, FakeTelegramClient()).deliver_one()
        async with UnitOfWork(factory) as uow:
            latest = await uow.discussions.latest_summary(session_id=session_id)
            assert latest.body == "Более свежая память" and latest.revision == 1
    finally:
        await engine.dispose()


async def test_opt_in_reader_keeps_oversized_newest_turn_whole(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    try:
        async with UnitOfWork(factory) as uow:
            session_id = await uow.discussions.create_session(
                project_id="vuzol",
                chat_id=-100,
                message_thread_id=10,
            )
            text = "я" * 25_000 + " Если условие не выполнено, ничего не меняй."
            memory = DiscussionMemoryService(uow)
            await memory.append_turn(
                session_id=session_id,
                role=ConversationTurnRole.USER,
                source=ConversationTurnSource.TELEGRAM_USER,
                content=text,
                classifier_mode=InteractionMode.DISCUSSION,
            )
            pack = await memory.load_context(session_id=session_id, chat_memory=True)
            assert pack.turns[0].content == text and not pack.raw_turns_truncated
            legacy = await memory.load_context(session_id=session_id)
            assert len(legacy.turns[0].content) == 24_000 and legacy.raw_turns_truncated
    finally:
        await engine.dispose()
