"""Frozen work evidence, source fences and admission: isolated TEST PostgreSQL."""

import asyncio
from dataclasses import replace

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from tests.integration.interpretation.test_chat_boundary import chat_runtime, message
from tests.integration.interpretation.test_chat_documents import Downloader, document_message
from tests.integration.providers.test_handler_request_guards import _running_step
from tests.integration.storage.helpers import storage
from tests.unit.interpretation.test_jev_live import response
from vuzol.discussion.context_budget import ChatContextError
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.work_context import freeze_work_context, load_work_context
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.attempts import snapshot_task_spec
from vuzol.storage.models import ConversationTurn, Task, TaskSpecRevision, TransactionalOutbox
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    InteractionMode,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.ingress import TelegramIngressService

pytestmark = pytest.mark.postgresql


async def append(memory, session_id, content, *, assistant=False):
    return (
        await memory.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.ASSISTANT if assistant else ConversationTurnRole.USER,
            source=ConversationTurnSource.MODEL
            if assistant
            else ConversationTurnSource.TELEGRAM_USER,
            content=content,
            classifier_mode=InteractionMode.DISCUSSION,
        )
    )[0]


@pytest.mark.parametrize("case", ["later", "current", "history", "scope", "spec", "summary"])
def test_work_snapshot_ignores_new_history_and_rejects_source_or_scope_drift(postgres_dsn, case):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        task_id, *_ = await _running_step(factory)
        try:
            async with UnitOfWork(factory) as uow:
                session = uow.session
                discussion_id = await uow.discussions.create_session(
                    project_id="proj", chat_id=-100, message_thread_id=10
                )
                memory = DiscussionMemoryService(uow)
                history_id = await append(
                    memory,
                    discussion_id,
                    "Если поиск сломан, исправь только его. Без деплоя.",  # noqa: RUF001
                )
                await append(
                    memory, discussion_id, "UNDELIVERED suggestion: deploy now.", assistant=True
                )
                # Do not import unpublished suggestions even with memory disabled.
                prior = await memory.load_context(
                    session_id=discussion_id, chat_context=True, delivered_only=True
                )
                assert len(prior.turns) == 1
                source_id = await append(memory, discussion_id, "Окей, реализуй по обсуждению.")
                task = await session.get(Task, task_id)
                task.original_text = "Окей, реализуй по обсуждению."
                task.ingress_source = "chat_work"
                task.source_turn_id = source_id
                task.source_thread_id = 10
                snapshot = await freeze_work_context(
                    session, prior=prior, source_turn_id=source_id, project_id="proj"
                )
                await snapshot_task_spec(
                    session, task, chat_work_context=snapshot.model_dump(mode="json")
                )
                pinned = task.spec_revision
                before = await load_work_context(session, task, expected_revision=pinned)
                assert "UNDELIVERED" not in "".join(i.content for i in before)
                if case == "later":
                    await append(memory, discussion_id, "Later change: deploy a different site.")
                    assert (
                        await load_work_context(session, task, expected_revision=pinned) == before
                    )
                    return
                if case == "current":
                    task.original_text = "Implement and deploy now."
                elif case == "history":
                    old = await session.get(ConversationTurn, history_id)
                    old.content = "Restrictions removed."
                elif case == "scope":
                    task.source_thread_id = 99
                elif case == "spec":
                    spec = await session.scalar(
                        select(TaskSpecRevision).where(
                            TaskSpecRevision.task_id == task.id,
                            TaskSpecRevision.spec_revision == pinned,
                        )
                    )
                    spec.spec = {
                        **spec.spec,
                        "chat_work_context": {
                            **spec.spec["chat_work_context"],
                            "content": "forged",
                        },
                    }
                elif case == "summary":
                    from vuzol.storage.types import ConversationSummaryGenerator

                    await memory.update_summary(
                        session_id=discussion_id,
                        body="Original summary.",
                        covered_through_turn_ordinal=2,
                        generator=ConversationSummaryGenerator.MODEL,
                        expected_summary_revision=0,
                    )
                    prior = await memory.load_context(
                        session_id=discussion_id, chat_context=True, delivered_only=True
                    )
                    prior = replace(prior, turns=tuple(t for t in prior.turns if t.id != source_id))
                    snapshot = await freeze_work_context(
                        session, prior=prior, source_turn_id=source_id, project_id="proj"
                    )
                    await snapshot_task_spec(
                        session, task, chat_work_context=snapshot.model_dump(mode="json")
                    )
                    from vuzol.storage.models import ConversationSummary

                    summary = await session.scalar(select(ConversationSummary))
                    summary.body = "Forged summary."
                    pinned = task.spec_revision
                await session.flush()
                with pytest.raises(ValueError, match="mismatch"):
                    await load_work_context(session, task, expected_revision=pinned)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_explicit_work_from_current_document_does_not_require_prior_discussion(
    postgres_dsn, tmp_path
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        text = "Specification: only a local test website, no accounts. " + "x" * 1000
        caption = "Реализуй по этому файлу. Если уже готово, не меняй. Без деплоя."
        try:
            await TelegramIngressService(runtime, factory).accept_message(
                document_message(caption=caption)
            )
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200, json=response("execute_request", "new", "conversation_context")
                    )
                )
            ) as client:
                jev = JevDecisionsInterpreter(
                    credential=SecretStr("test-key"),
                    profile_id="jev-chat",
                    admitted=lambda _: True,
                    client=client,
                )
                assert await InterpretationPipeline(
                    runtime,
                    factory,
                    owner="current-doc",
                    interpreter=jev,
                    downloader=Downloader({"one": text.encode()}),
                ).process_one()
            async with factory() as session:
                task = await session.scalar(select(Task))
                assert task is not None and task.ingress_source == "chat_work"
                spec = await session.scalar(
                    select(TaskSpecRevision).where(
                        TaskSpecRevision.task_id == task.id,
                        TaskSpecRevision.spec_revision == task.spec_revision,
                    )
                )
                snapshot = spec.spec["chat_work_context"]
                assert snapshot["current_document_text_chars"] == len(text)
                assert snapshot["sources"] == []
                content = "".join(c.content for c in await load_work_context(session, task))
                assert caption in content and text in content
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["missing", "text", "documents", "incomplete"])
def test_work_context_cannot_drop_missing_or_oversized_evidence(postgres_dsn, mode):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        try:
            async with UnitOfWork(factory) as uow:
                discussion_id = await uow.discussions.create_session(
                    project_id="proj", chat_id=-100, message_thread_id=10
                )
                memory = DiscussionMemoryService(uow)
                if mode != "missing":
                    await append(
                        memory,
                        discussion_id,
                        "Context " + ("x" * 39_000 if mode == "text" else "x"),
                    )
                prior = await memory.load_context(
                    session_id=discussion_id, chat_context=True, delivered_only=True
                )
                source_id = await append(
                    memory, discussion_id, "Implement " + ("y" * 2000 if mode == "text" else "y")
                )
                if mode == "incomplete":
                    prior = replace(prior, uncovered_history_complete=False)
                elif mode == "documents":
                    turn = replace(
                        prior.turns[0],
                        document_context="z" * 50_001,
                        document_text_chars=50_001,
                        caption="Only chosen reference.",
                    )
                    prior = replace(prior, turns=(turn,))
                with pytest.raises(ChatContextError):
                    await freeze_work_context(
                        uow.session, prior=prior, source_turn_id=source_id, project_id="proj"
                    )
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("memory_enabled", [True, False])
def test_no_discussion_cannot_create_a_bare_begin_task(postgres_dsn, tmp_path, memory_enabled):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        runtime = runtime.model_copy(
            update={
                "settings": runtime.settings.model_copy(
                    update={
                        "chat": runtime.settings.chat.model_copy(
                            update={"memory_enabled": memory_enabled}
                        )
                    }
                )
            }
        )
        try:
            await TelegramIngressService(runtime, factory).accept_message(message("Окей, начинай."))
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200, json=response("execute_request", "continue", "conversation_context")
                    )
                )
            ) as client:
                jev = JevDecisionsInterpreter(
                    credential=SecretStr("test-key"),
                    profile_id="jev-chat",
                    admitted=lambda _: True,
                    client=client,
                )
                assert await InterpretationPipeline(
                    runtime, factory, owner="no-context", interpreter=jev
                ).process_one()
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                source = await session.scalar(select(ConversationTurn))
                assert not source.should_create_task
                notice = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["role"].as_string() == "chat_notice"
                    )
                )
                assert "Уточни" in notice.payload["html"]
        finally:
            await engine.dispose()

    asyncio.run(scenario())
