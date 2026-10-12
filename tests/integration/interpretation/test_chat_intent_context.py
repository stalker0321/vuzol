"""Conversation references, full unsummarized history and document partitions: TEST PG."""

import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from tests.integration.interpretation.test_chat_boundary import message
from tests.integration.interpretation.test_chat_documents import (
    Downloader,
    attachment,
    document_message,
)
from tests.integration.interpretation.test_chat_recovery_memory import runtime_with_memory
from tests.integration.storage.helpers import storage
from tests.integration.telegram.test_delivery import service
from tests.unit.interpretation.test_jev_live import response
from vuzol.discussion.agent import (
    DiscussionAgentReply,
    persist_discussion_reply,
    schedule_discussion_agent,
)
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.models import ConversationTurn, Run, Task, TransactionalOutbox
from vuzol.storage.types import (
    ConversationSummaryGenerator,
    ConversationTurnRole,
    ConversationTurnSource,
    InteractionMode,
    RunStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient

pytestmark = pytest.mark.postgresql


async def finish_reply(factory, task_id, reply, *, client, summary=None):
    async with UnitOfWork(factory) as uow:
        task = await uow.session.get(Task, task_id)
        await persist_discussion_reply(
            uow,
            task,
            DiscussionAgentReply(
                reply=reply,
                memory_summary=summary,
            ),
        )
        task.status = TaskStatus.COMPLETED  # Test fixture, not a production transition.
        run = await uow.session.scalar(select(Run).where(Run.task_id == task_id))
        run.status = RunStatus.COMPLETED
    delivery = service(factory, client)
    assert await delivery.deliver_one()
    while await delivery.deliver_one():
        pass


def test_proposal_critique_references_and_followups_all_reach_read_only_sol(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = runtime_with_memory(tmp_path)
        ingress = TelegramIngressService(runtime, factory)
        critique = "Пересмотри шкалы. Архитектуру не меняй; пока не запускай. " + "к" * 3800
        references = ("FIRST REF " + "я" * 12000, "SECOND REF " + "ю" * 12000)
        requests = []
        telegram = FakeTelegramClient()
        choices = [
            ("plan", "new", "no_target"),
            ("respond", "review_result", "unresolved"),
            ("respond", "amend", "conversation_context"),
            ("respond", "continue", "no_target"),
            ("respond", "continue", "conversation_context"),
        ]

        def handler(request):
            payload = json.loads(request.content)
            requests.append(payload)
            return httpx.Response(200, json=response(*choices[len(requests) - 1]))

        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                jev = JevDecisionsInterpreter(
                    credential=SecretStr("test-key"),
                    profile_id="jev-chat",
                    admitted=lambda _: True,
                    client=client,
                )
                pipeline = InterpretationPipeline(
                    runtime,
                    factory,
                    owner="context",
                    interpreter=jev,
                    downloader=Downloader(
                        {"one": references[0].encode(), "two": references[1].encode()}
                    ),
                )
                turns = [
                    message("Предложи шкалы оценки книг, это обсуждение."),
                    message(critique),
                    document_message(
                        caption="Вот два референса. Только обсуди.",
                        attachments=(
                            attachment(),
                            attachment("second.md", "text/markdown", "two"),
                        ),
                    ),
                    message("Мы продолжаем обсуждение, задачи ещё нет."),
                    message("Я жду твоего мнения; если не согласен, объясни почему."),
                ]
                for index, turn in enumerate(turns):
                    turn = turn.model_copy(
                        update={"message_id": 1101 + index, "update_id": 1101 + index}
                    )
                    assert (await ingress.accept_message(turn)).task_id is None
                    assert await pipeline.process_one()
                    async with factory() as session:
                        hidden = await session.scalar(
                            select(Task).order_by(Task.created_at.desc()).limit(1)
                        )
                        assert hidden is not None and hidden.source_turn_id is not None
                        assert hidden.public_task_number is None
                        assert hidden.task_draft["required_capabilities"] == ["repository_read"]
                        assert hidden.original_text.count(turn.text) == 1
                        if index >= 1:
                            assert critique in hidden.original_text
                        if index >= 2:
                            assert all(hidden.original_text.count(ref) == 1 for ref in references)
                            context = json.loads(
                                requests[-1]["state"]["context"]["discussion_context"]
                            )
                            assert any(critique in t["content"] for t in context["turns"])
                        task_id = hidden.id
                    await finish_reply(
                        factory,
                        task_id,
                        "Исходное предложение. " + "п" * 6990
                        if index == 0
                        else "Обсуждаем дальше.",
                        client=telegram,
                        summary="Обсуждаем шкалы оценки книг; реализации ещё нет."
                        if index == 0
                        else None,
                    )
                assert len(requests) == 5
                async with factory() as session:
                    assert await session.scalar(select(func.count()).select_from(Task)) == 5
                    assert (
                        await session.scalar(select(func.count()).select_from(ConversationTurn))
                        == 10
                    )
                    notices = await session.scalar(
                        select(func.count())
                        .select_from(TransactionalOutbox)
                        .where(
                            TransactionalOutbox.payload["role"].as_string() == "chat_notice",
                        )
                    )
                    assert notices == 0
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_chat_reader_retains_more_than_twenty_uncovered_turns(postgres_dsn):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        try:
            async with UnitOfWork(factory) as uow:
                session_id = await uow.discussions.create_session(
                    project_id="vuzol",
                    chat_id=-100,
                    message_thread_id=10,
                )
                memory = DiscussionMemoryService(uow)
                for index in range(30):
                    await memory.append_turn(
                        session_id=session_id,
                        role=ConversationTurnRole.USER,
                        source=ConversationTurnSource.TELEGRAM_USER,
                        content=f"Short unsummarized correction {index}; do not start.",
                        classifier_mode=InteractionMode.DISCUSSION,
                    )
                packed = await memory.load_context(session_id=session_id, chat_memory=True)
                assert len(packed.turns) == 30 and not packed.raw_turns_truncated
                assert "correction 0;" in packed.turns[0].content
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_compiled_prompt_compacts_only_covered_overlap_and_can_publish_summary(
    postgres_dsn, tmp_path
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = runtime_with_memory(tmp_path)
        old = "Covered proposal " + "x" * 38_000
        current = "Do not change the architecture; only discuss. " + "y" * 700
        try:
            async with UnitOfWork(factory) as uow:
                session_id = await uow.discussions.create_session(
                    project_id="vuzol",
                    chat_id=-100,
                    message_thread_id=10,
                )
                memory = DiscussionMemoryService(uow)
                await memory.append_turn(
                    session_id=session_id,
                    role=ConversationTurnRole.USER,
                    source=ConversationTurnSource.TELEGRAM_USER,
                    content=old,
                    classifier_mode=InteractionMode.DISCUSSION,
                )
                await memory.update_summary(
                    session_id=session_id,
                    body="Initial proposal summarized.",
                    covered_through_turn_ordinal=2,
                    generator=ConversationSummaryGenerator.MODEL,
                    expected_summary_revision=0,
                )
                source_id, _ = await memory.append_turn(
                    session_id=session_id,
                    role=ConversationTurnRole.USER,
                    source=ConversationTurnSource.TELEGRAM_USER,
                    content=current,
                    classifier_mode=InteractionMode.DISCUSSION,
                )
                pack = await memory.load_context(session_id=session_id, chat_memory=True)
                assert len(pack.turns) == 2 and not pack.raw_turns_truncated
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
                    memory_pack=pack,
                    current_turn=current,
                )
                task = await uow.session.get(Task, task_id)
                assert old not in task.original_text
                assert task.original_text.count(current) == 1
                assert "Initial proposal summarized." in task.original_text
                assert task.task_draft["memory_summary_eligible"]
                assert len(task.original_text) < 40_000
            await finish_reply(
                factory,
                task_id,
                "Still discussing.",
                client=FakeTelegramClient(),
                summary="Discuss the proposal; do not change architecture or implement it.",
            )
            async with UnitOfWork(factory) as uow:
                summary = await uow.discussions.latest_summary(session_id=session_id)
                assert summary.revision == 2 and summary.covered_through_turn_ordinal == 3
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_true_context_overflow_is_explicit_without_partial_history_or_hidden_task(
    postgres_dsn, tmp_path
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = runtime_with_memory(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            async with UnitOfWork(factory) as uow:
                session_id = await uow.discussions.create_session(
                    project_id="vuzol",
                    chat_id=-100,
                    message_thread_id=10,
                )
                memory = DiscussionMemoryService(uow)
                for index in range(3):
                    await memory.append_turn(
                        session_id=session_id,
                        role=ConversationTurnRole.USER,
                        source=ConversationTurnSource.TELEGRAM_USER,
                        content=f"Correction {index}: " + "я" * 15_000,
                        classifier_mode=InteractionMode.DISCUSSION,
                    )
            await ingress.accept_message(message("Учитывай все мои замечания; пока не запускай."))
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: pytest.fail("Incomplete context must not reach Jev")
                )
            ) as client:
                pipeline = InterpretationPipeline(
                    runtime,
                    factory,
                    owner="overflow",
                    interpreter=JevDecisionsInterpreter(
                        credential=SecretStr("test-key"),
                        profile_id="jev-chat",
                        admitted=lambda _: True,
                        client=client,
                    ),
                )
                assert await pipeline.process_one()
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 3
                notice = (
                    await session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "telegram",
                        )
                    )
                ).one()
                assert "не помещается" in notice.payload["html"]
                assert "История сохранена" in notice.payload["html"]
                assert "40 000" in notice.payload["html"] and "50 000" in notice.payload["html"]
        finally:
            await engine.dispose()

    asyncio.run(scenario())
