"""Actual durable chat intake boundary; fake Jev/CLI, real TEST PostgreSQL."""

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from tests.integration.providers._test_routing_helpers import profile
from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from tests.unit.interpretation.test_jev_live import response
from vuzol.config import Capability, RegistryDocument, RuntimeConfiguration, build_bundle
from vuzol.config.settings import ChatSettings
from vuzol.discussion.agent import DeliverDiscussionReplyHandler
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.models import (
    ConversationTurn,
    ProjectExecutorPreference,
    Run,
    Step,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicTaskCounter,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    IntakeStatus,
    InteractionMode,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import prepare_delivery
from vuzol.telegram.domain import MessageUpdate
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import (
    build_project_status_dashboard,
    enqueue_task_status_projection,
    enqueue_terminal_task_projections,
)
from vuzol.workflows.dispatch import WorkflowDispatcher
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

pytestmark = pytest.mark.postgresql


def chat_runtime(tmp_path: Path) -> RuntimeConfiguration:
    original = telegram_runtime(tmp_path)
    configured = original.settings.model_copy(
        update={
            "chat": ChatSettings(enabled=True, profile_id="sol-chat"),
        }
    )
    profiles = (
        profile(
            "jev-chat",
            provider="jev",
            roles=frozenset({"interpreter"}),
            model="typesafe/jev-1.13",
            input_cost_units_per_million=0.042,
            output_cost_units_per_million=0,
        ),
        profile(
            "sol-chat",
            provider="codex",
            runtime_identity="chat-test",
            state_directory=tmp_path / "codex-state",
            fallback_profile_ids=("other-worker",),
            model="gpt-6.1-sol",
            launch_mode="cli",
            sandbox_required=True,
            supported_task_types=frozenset({"architecture"}),
            capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
        profile(
            "other-worker",
            provider="pi",
            runtime_identity="other-test",
            state_directory=tmp_path / "pi-state",
            launch_mode="cli",
            sandbox_required=True,
            supported_task_types=frozenset({"architecture"}),
            capabilities=frozenset({Capability.REPOSITORY_READ}),
        ),
    )
    document = RegistryDocument(
        projects=original.registries.projects.items(),
        topics=original.registries.topics.items(),
        sandboxes=original.registries.sandboxes.items(),
        profiles=profiles,
    )
    return RuntimeConfiguration(settings=configured, registries=build_bundle(document, configured))


def message(text: str, *, reply: int | None = None) -> MessageUpdate:
    return MessageUpdate(
        bot_id="main",
        update_id=1101,
        chat_id=-100,
        message_thread_id=10,
        message_id=1101,
        user_id=42,
        text=text,
        reply_to_message_id=reply,
    )


@pytest.mark.parametrize("effect", ["respond", "plan", "status", "capture"])
def test_chat_answer_has_no_public_task_start_or_catalogue(
    postgres_dsn: str,
    tmp_path: Path,
    effect: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        raw = "Обсуди реализацию, только предложи план; " + "контекст " * 550 + "пока не запускай."
        accepted = await TelegramIngressService(runtime, factory).accept_message(message(raw))
        assert accepted.task_id is None
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(TransactionalOutbox)
                    .where(TransactionalOutbox.destination == "telegram")
                )
                == 0
            )
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(json.loads(request.content))
            assert calls[-1]["state"]["current_turn"] == raw
            return httpx.Response(200, json=response(effect=effect, relation="none"))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            jev = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            pipeline = InterpretationPipeline(runtime, factory, interpreter=jev, owner="chat")
            assert await pipeline.process_one()
            assert not await pipeline.process_one()
        assert len(calls) == 1
        async with factory.begin() as session:
            task = (await session.scalars(select(Task))).one()
            run = (await session.scalars(select(Run))).one()
            assert task.task_type == "discussion_agent_internal"
            assert task.public_task_number is None and task.topic_task_number is None
            assert task.original_text.endswith(raw)
            assert run.status is RunStatus.RUNNING
            assert run.selected_route["trusted_profile_id"] == "sol-chat"
            steps = (await session.scalars(select(Step).where(Step.run_id == run.id))).all()
            execution = next(s for s in steps if s.step_type == "execute_agent")
            assert execution.required_capabilities == ["repository_read"]
            assert await session.scalar(select(func.count()).select_from(TopicTaskCounter)) == 0
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1
            dashboard = await build_project_status_dashboard(session, -100)
            assert "Задача" not in dashboard.html
            await enqueue_task_status_projection(session, task, run)
            task.status = TaskStatus.COMPLETED
            await enqueue_terminal_task_projections(session, task, run)
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(TransactionalOutbox)
                    .where(TransactionalOutbox.destination == "telegram")
                )
                == 0
            )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("effect", ["mixed", "control_request", "abstain"])
def test_chat_clarifies_without_partial_task_or_hidden_execution(
    postgres_dsn: str,
    tmp_path: Path,
    effect: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        await TelegramIngressService(runtime, factory).accept_message(
            message("Останови прогон и переделай; новый пока не запускай.")
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, json=response(effect, "null" if effect == "mixed" else "none")
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
                runtime, factory, interpreter=jev, owner="chat"
            ).process_one()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert await session.scalar(select(func.count()).select_from(Run)) == 0
            notice = (
                await session.scalars(
                    select(TransactionalOutbox).where(TransactionalOutbox.destination == "telegram")
                )
            ).one()
            prepared = await prepare_delivery(session, notice)
            assert prepared.message_role == "chat_notice"
            assert not prepared.callback_buttons and prepared.task_id is None
            assert any(
                word in prepared.html for word in ("Ничего не запущено", "карточки", "Уточни")
            )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("legacy_auto", [False, True])
def test_only_execution_is_promoted_once_and_waits_for_start(
    postgres_dsn: str,
    tmp_path: Path,
    legacy_auto: bool,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        runtime = runtime.model_copy(
            update={
                "settings": runtime.settings.model_copy(
                    update={
                        "interpretation": runtime.settings.interpretation.model_copy(
                            update={
                                "automatic_execution_enabled": legacy_auto,
                            }
                        ),
                    }
                )
            }
        )
        raw = "Добавь поиск: " + "контекст " * 550 + "если сломан; только поиск."
        accepted = await TelegramIngressService(runtime, factory).accept_message(message(raw))
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json=response())

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            jev = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            assert await InterpretationPipeline(
                runtime, factory, interpreter=jev, owner="chat"
            ).process_one()
        dispatcher = WorkflowDispatcher(runtime, factory, owner="dispatch")
        assert await dispatcher.process_one()
        assert not await dispatcher.process_one()
        async with factory() as session:
            task = (await session.scalars(select(Task))).one()
            run = (await session.scalars(select(Run))).one()
            intake = await session.get(TelegramIntakeMessage, accepted.intake_id)
            assert intake is not None and intake.task_id == task.id
            assert task.original_text == raw and task.public_task_number == 100001
            assert task.ingress_source == "chat_work" and task.source_turn_id is not None
            assert run.status is RunStatus.CREATED
            assert task.status is TaskStatus.INTERPRETED
            ack = (
                await session.scalars(
                    select(TransactionalOutbox).where(TransactionalOutbox.destination == "telegram")
                )
            ).one()
            prepared = await prepare_delivery(session, ack)
            assert "start" in prepared.buttons
        assert len(calls) == 1
        await engine.dispose()

    asyncio.run(scenario())


def test_conversation_memory_and_plain_reply_delivery(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        async with UnitOfWork(factory) as uow:
            session_id = await uow.discussions.create_session(
                project_id="vuzol", chat_id=-100, message_thread_id=10
            )
            await uow.discussions.append_turn(
                session_id=session_id,
                role=ConversationTurnRole.ASSISTANT,
                source=ConversationTurnSource.MODEL,
                content="Предлагаю обсудить сайт книг, без запуска реализации.",
                classifier_mode=InteractionMode.DISCUSSION,
            )
        await TelegramIngressService(runtime, factory).accept_message(message("Да, обсудим."))

        def handler(request: httpx.Request) -> httpx.Response:
            context = json.loads(request.content)["state"]["context"]
            memory = json.loads(context["discussion_context"])
            assert "сайт книг" in memory["turns"][-1]["content"]
            return httpx.Response(200, json=response("plan", "none"))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            jev = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            await InterpretationPipeline(
                runtime, factory, interpreter=jev, owner="chat"
            ).process_one()
        async with factory.begin() as session:
            task = (await session.scalars(select(Task))).one()
            steps = (await session.scalars(select(Step))).all()
            execute = next(s for s in steps if s.step_type == "execute_agent")
            deliver = next(s for s in steps if s.step_type == "deliver_discussion_reply")
            execute.status = StepStatus.COMPLETED
            execute.result = {
                "structured_output": {"reply": "Начнём обсуждение каталога. Ничего не запускаю."}
            }
        lease = LeaseToken(
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
        )
        outcome = await DeliverDiscussionReplyHandler(factory).execute(
            StepExecutionRequest(
                task_id=task.id,
                run_id=deliver.run_id,
                step_id=deliver.id,
                step_type="deliver_discussion_reply",
                payload={},
                timeout_seconds=60,
                lease=lease,
            ),
            CancellationContext(),
        )
        assert outcome.result["assistant_turn_id"]
        async with factory() as session:
            reply = (
                await session.scalars(
                    select(TransactionalOutbox).where(TransactionalOutbox.destination == "telegram")
                )
            ).one()
            prepared = await prepare_delivery(session, reply)
            assert "Начнём" in prepared.html
            assert not prepared.buttons and not prepared.callback_buttons
            assert prepared.task_id is None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("unavailable", ["ready", "disabled", "model_changed", "missing"])
def test_responder_is_exact_independent_of_project_worker_and_never_falls_back(
    postgres_dsn: str,
    tmp_path: Path,
    unavailable: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        await TelegramIngressService(runtime, factory).accept_message(message("Что думаешь?"))
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=response("respond", "none"))
            )
        ) as client:
            jev = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            await InterpretationPipeline(
                runtime, factory, interpreter=jev, owner="chat"
            ).process_one()
        async with factory.begin() as session:
            await synchronize_profiles(
                session,
                runtime.registries.profiles.items(),
                configuration_revision=runtime.registries.revision,
            )
            session.add(
                ProjectExecutorPreference(
                    project_id="vuzol", mode="pin", worker_key="kimi", revision=1
                )
            )
            steps = (await session.scalars(select(Step))).all()
            for step in steps:
                if step.step_type == "prepare_worktree":
                    step.status = StepStatus.COMPLETED
                elif step.step_type == "execute_agent":
                    step.status = StepStatus.QUEUED
        registries = runtime.registries
        if unavailable != "ready":
            original = registries.profiles.items()
            document = RegistryDocument(
                projects=registries.projects.items(),
                topics=registries.topics.items(),
                sandboxes=registries.sandboxes.items(),
                profiles=tuple(
                    p.model_copy(
                        update={"enabled": False}
                        if unavailable == "disabled"
                        else {"model": "another-model"}
                    )
                    if p.id == "sol-chat"
                    else p
                    for p in original
                    if unavailable != "missing" or p.id != "sol-chat"
                ),
            )
            # Runtime changed after materialization: pin must not silently degrade.
            registries = build_bundle(
                document, runtime.settings.model_copy(update={"chat": ChatSettings()})
            )
        async with factory.begin() as session:
            token = await claim_routed_step(
                session,
                settings=runtime.settings,
                registries=registries,
                owner="executor",
                lease_seconds=60,
                candidate_limit=20,
                step_types=frozenset({"execute_agent"}),
            )
            if unavailable != "ready":
                assert token is None
                task = (await session.scalars(select(Task))).one()
                assert task.status in {TaskStatus.BLOCKED, TaskStatus.QUOTA_EXHAUSTED}
                notice = (
                    await session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "telegram"
                        )
                    )
                ).one()
                assert notice.payload["role"] == "chat_notice"
            else:
                assert token is not None
                leased = await session.get(Step, token.step.id)
                assert leased is not None and leased.executor_profile_id == "sol-chat"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("effect", ["status", "execute_request"])
def test_reply_queries_result_or_clarifies_without_resuming_or_cataloguing(
    postgres_dsn: str,
    tmp_path: Path,
    effect: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        async with UnitOfWork(factory) as uow:
            existing = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="Существующая задача",
                task_type="coding",
            )
            assert uow.session is not None
            stored = await uow.session.get(Task, existing.id)
            assert stored is not None
            stored.status = TaskStatus.PAUSED
            uow.session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=801,
                    task_id=existing.id,
                    message_role="task_status",
                )
            )
        accepted = await TelegramIngressService(runtime, factory).accept_message(
            message("Объясни результат, не продолжай.", reply=801)
        )
        assert accepted.task_id is None

        def handler(request: httpx.Request) -> httpx.Response:
            context = json.loads(request.content)["state"]["context"]
            assert context["reply_linked_task"]["task_id"] == str(existing.id)
            return httpx.Response(
                200,
                json=response(
                    effect,
                    "review_result" if effect == "status" else "continue",
                    str(existing.id),
                ),
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            jev = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            await InterpretationPipeline(
                runtime, factory, interpreter=jev, owner="chat"
            ).process_one()
        async with factory() as session:
            stored = await session.get(Task, existing.id)
            assert stored is not None and stored.status is TaskStatus.PAUSED
            user_turn = (
                await session.scalars(
                    select(ConversationTurn).where(
                        ConversationTurn.role == ConversationTurnRole.USER
                    )
                )
            ).one()
            assert not user_turn.should_create_task
            if effect == "status":
                hidden = (await session.scalars(select(Task).where(Task.id != existing.id))).one()
                assert hidden.public_task_number is None
                assert str(existing.id) in hidden.original_text
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(TransactionalOutbox)
                        .where(TransactionalOutbox.destination == "telegram")
                    )
                    == 0
                )
            else:
                assert await session.scalar(select(func.count()).select_from(Task)) == 1
                assert await session.scalar(select(func.count()).select_from(Run)) == 0
                notice = (
                    await session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "telegram"
                        )
                    )
                ).one()
                prepared = await prepare_delivery(session, notice)
                assert prepared.message_role == "chat_notice"
                assert not prepared.callback_buttons and prepared.task_id is None
        await engine.dispose()

    asyncio.run(scenario())


def test_failed_jev_turn_notifies_without_creating_any_task(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        base = chat_runtime(tmp_path)
        runtime = base.model_copy(
            update={
                "settings": base.settings.model_copy(
                    update={
                        "interpretation": base.settings.interpretation.model_copy(
                            update={"max_attempts": 1}
                        ),
                    }
                )
            }
        )
        accepted = await TelegramIngressService(runtime, factory).accept_message(
            message("Обсудим?")
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(503))
        ) as client:
            jev = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            await InterpretationPipeline(
                runtime, factory, interpreter=jev, owner="chat"
            ).process_one()
        async with factory() as session:
            intake = await session.get(TelegramIntakeMessage, accepted.intake_id)
            assert intake is not None and intake.status is IntakeStatus.NEEDS_CLARIFICATION
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            notice = (
                await session.scalars(
                    select(TransactionalOutbox).where(TransactionalOutbox.destination == "telegram")
                )
            ).one()
            assert notice.payload["role"] == "chat_notice"
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1
        await engine.dispose()

    asyncio.run(scenario())
