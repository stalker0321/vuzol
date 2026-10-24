"""Persistent modes through leadership turns, with no Jev or conversational Task: TEST PG."""
# ruff: noqa: RUF001

import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from tests.integration.interpretation.test_chat_boundary import chat_runtime, message
from tests.integration.providers._test_routing_helpers import profile
from tests.integration.storage.helpers import storage
from tests.unit.interpretation.test_jev_live import response
from vuzol.config import Capability, RegistryDocument, RuntimeConfiguration, build_bundle
from vuzol.config.models import ProviderRole
from vuzol.discussion.conversation import parse_conversation_command
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.persistence import CONTRACT_VERSION, LeadershipCommandReceipt, ManagedGoal
from vuzol.leadership.service import LeadershipCommandService
from vuzol.storage.models import (
    Task,
    TelegramIntakeMessage,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.telegram.domain import IngressStatus
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.workflows.application import Principal

pytestmark = pytest.mark.postgresql


def modes_runtime(tmp_path):
    runtime = chat_runtime(tmp_path)
    settings = runtime.settings.model_copy(
        update={
            "project_discussion_enabled": False,
            "project_lead": runtime.settings.project_lead.model_copy(
                update={
                    "enabled": True,
                    "profile_id": "lead-only",
                    "reviewer_profile_id": "lead-only",
                }
            ),
        }
    )
    lead = profile(
        "lead-only",
        provider="pi",
        model="vendor/current-lead",
        launch_mode="cli",
        roles=frozenset({ProviderRole.LEAD, ProviderRole.REVIEWER}),
        # Shape of the deployed Pi lead catalog: conversation must not require
        # pretending that this profile also executes architecture tasks.
        supported_task_types=frozenset({"coding", "project_lead", "general"}),
        capabilities=frozenset({Capability.REPOSITORY_READ}),
        sandbox_required=True,
        context_limit=32000,
        runtime_identity="test-lead",
        state_directory=tmp_path / "lead-state",
    )
    topics = runtime.registries.topics.items()
    document = RegistryDocument(
        projects=tuple(
            p.model_copy(
                update={
                    "allowed_capabilities": frozenset(
                        {
                            Capability.REPOSITORY_READ,
                            Capability.CODE_EDIT,
                            Capability.GIT,
                            Capability.PROJECT_SHELL,
                        }
                    )
                }
            )
            for p in runtime.registries.projects.items()
        ),
        profiles=(*runtime.registries.profiles.items(), lead),
        topics=(*topics, topics[0].model_copy(update={"message_thread_id": 20})),
        sandboxes=runtime.registries.sandboxes.items(),
    )
    return RuntimeConfiguration(settings=settings, registries=build_bundle(document, settings))


def turn(text, number=1101, thread=10):
    return message(text).model_copy(
        update={
            "update_id": number,
            "message_id": number,
            "message_thread_id": thread,
        }
    )


async def classify(runtime, factory, effect="respond", target="no_target"):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=response(effect, "new", target))
        )
    ) as client:
        jev = JevDecisionsInterpreter(
            credential=SecretStr("test-key"),
            profile_id="jev-chat",
            admitted=lambda _: True,
            client=client,
        )
        return await InterpretationPipeline(
            runtime,
            factory,
            interpreter=jev,
            owner="conversation-test",
        ).process_one()


def test_mode_survives_restart_is_topic_local_and_commands_do_not_start_work(
    postgres_dsn, tmp_path
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            switch = turn("/think", 1102)
            assert (await ingress.accept_message(switch)).status is IngressStatus.HANDLED
            assert (await ingress.accept_message(switch)).status is IngressStatus.DUPLICATE
            # A different late update containing an older command cannot revert selection.
            await ingress.accept_message(turn("/lead", 1101))
            await engine.dispose()
            engine, factory = storage(postgres_dsn)
            ingress = TelegramIngressService(runtime, factory)
            async with factory() as session:
                mapping = (await session.scalars(select(TopicMapping))).one()
                assert mapping.conversation_mode == "think"
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
            await ingress.accept_message(turn("Обсудим?", 1103))
            await ingress.accept_message(turn("А здесь?", 1104, thread=20))
            async with factory() as session:
                items = (
                    await session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "project_lead"
                        )
                    )
                ).all()
                # /think in topic 10 goes to its own discussion model, not to the lead.
                assert [i.payload["message_thread_id"] for i in items] == [20]
                thinking = (
                    await session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "discussion_classify"
                        )
                    )
                ).all()
                assert [i.payload["message_thread_id"] for i in thinking] == [10]
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_queued_message_and_reply_keep_the_original_mode(postgres_dsn, tmp_path):
    from tests.integration.leadership.test_lead_surface import _drain, _Transport
    from vuzol.leadership.composition import build_project_lead_worker
    from vuzol.leadership.persistence import LeadershipTurn

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/lead"))
            await ingress.accept_message(turn("Предложи варианты, пока не запускай.", 1102))
            await ingress.accept_message(turn("/think", 1103))
            assert not await classify(runtime, factory, "plan")
            transport = _Transport({"reply": "Только обсуждение.", "commands": []})
            worker = build_project_lead_worker(
                factory,
                settings=runtime.settings,
                profiles=runtime.registries.profiles,
                owner="mode-reply",
                adapter=transport,
            )
            assert await worker.process_one()
            assert "Только обсуждение." in await _drain(factory)
            async with factory() as session:
                saved = await session.scalar(select(LeadershipTurn))
                assert saved.model_assignments["lead"]["profile_id"] == "lead-only"
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert (await session.scalar(select(ManagedGoal))).lifecycle == "draft"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_think_answers_with_its_own_model_and_never_starts_work(postgres_dsn, tmp_path):
    from vuzol.discussion.agent import DISCUSSION_INTERNAL_TASK_TYPE

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/think"))
            await ingress.accept_message(
                turn("Добавь поиск книг; не публикуй результат в production.", 1102)
            )
            # Even a work request is only discussed: Jev runs, the lead never hears it.
            assert await classify(runtime, factory, "execute_request")
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
                assert not await session.scalar(
                    select(TransactionalOutbox.id).where(
                        TransactionalOutbox.destination == "project_lead"
                    )
                )
                tasks = (await session.scalars(select(Task))).all()
                assert [t.task_type for t in tasks] == [DISCUSSION_INTERNAL_TASK_TYPE]
                assert tasks[0].task_draft["conversation_route"]["conversation_mode"] == "think"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["lead"])
def test_explicit_work_goes_to_lead_once_and_switch_does_not_resume(postgres_dsn, tmp_path, mode):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn(f"/{mode}"))
            request = turn("Добавь поиск книг; не публикуй результат в production.", 1102)
            await ingress.accept_message(request)
            assert not await classify(runtime, factory, "execute_request")
            assert (await ingress.accept_message(request)).status is IngressStatus.DUPLICATE
            assert not await classify(runtime, factory, "execute_request")
            async with factory() as session:
                goal = (await session.scalars(select(ManagedGoal))).one()
                assert goal.lifecycle == "draft"
                assert "не публикуй" in goal.scope_text
                assert goal.criteria and goal.owner_user_id == 42
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert (
                    await session.scalar(select(func.count()).select_from(LeadershipCommandReceipt))
                    == 1
                )
                assert (
                    await session.scalar(select(TelegramIntakeMessage))
                ).affinity_kind == "discussion"
                goal_id, version = goal.id, goal.version
            service = LeadershipCommandService(factory, enabled=True)
            paused = await service.apply_human(
                Principal(42, "telegram"),
                LeadCommand(
                    name="pause_goal",
                    project_id="vuzol",
                    goal_id=goal_id,
                    idempotency_key="test-pause",
                    contract_version=CONTRACT_VERSION,
                    expected_version=version,
                    payload={},
                ),
            )
            assert paused.status == "applied"
            await ingress.accept_message(turn("/lead", 1103))
            await ingress.accept_message(turn("Теперь добавь фильтр.", 1104))
            # A live goal hears the owner: the message is a lead event, not a new goal.
            assert not await classify(runtime, factory, "execute_request")
            async with factory() as session:
                goal = (await session.scalars(select(ManagedGoal))).one()
                assert goal.lifecycle == "paused" and goal.version == paused.version
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                event = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "project_lead",
                        TransactionalOutbox.linked_entity_id == goal_id,
                        TransactionalOutbox.idempotency_key == "lead:message:-100:1104",
                    )
                )
                assert event is not None and event.payload["text"] == "Теперь добавь фильтр."
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_lead_only_profile_reaches_turn_adapter_and_pi_transport(postgres_dsn, tmp_path):
    from vuzol.leadership.composition import build_project_lead_worker
    from vuzol.providers.catalog import cli_adapter
    from vuzol.providers.ports import CodexProcessResult
    from vuzol.providers.role_cli import RoleBoundCliAdapter

    class Transport:
        invocation = None

        async def run(self, invocation, cancellation):
            self.invocation = invocation
            body = json.loads(invocation.stdin)
            assert body["role"] == "lead"
            assert body["output_schema"] is not None
            assert "--no-tools" in invocation.argv
            assert invocation.sandbox_reference.startswith("lead-turn:")
            assistant = {
                "role": "assistant",
                "stopReason": "stop",
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {"reply": "Предлагаю обсудить варианты.", "commands": []}
                        ),
                    }
                ],
                "usage": {"input": 500, "output": 20},
            }
            return CodexProcessResult(
                0,
                json.dumps({"type": "agent_end", "messages": [assistant]})
                + '\n{"type":"agent_settled"}',
                "",
                5,
            )

    class UnexpectedAdapter:
        async def execute(self, *args):
            raise AssertionError("A project message must use the leadership turn adapter")

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            await TelegramIngressService(runtime, factory).accept_message(
                turn("Обсуди варианты, не выполняй.", 1102)
            )
            transport = Transport()
            adapter = RoleBoundCliAdapter(
                UnexpectedAdapter(),
                UnexpectedAdapter(),
                cli_adapter(runtime.registries.profiles.get("lead-only"), transport),
            )
            worker = build_project_lead_worker(
                factory,
                settings=runtime.settings,
                profiles=runtime.registries.profiles,
                owner="pi-turn",
                adapter=adapter,
            )
            assert await worker.process_one()
            assert transport.invocation is not None
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert (await session.scalar(select(ManagedGoal))).lifecycle == "draft"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_model_selection_is_pinned_by_turn_and_catalog_change_fails_closed(postgres_dsn, tmp_path):
    from tests.integration.leadership.test_lead_surface import _Transport
    from tests.integration.leadership.test_model_assignment_receipts import _claim
    from tests.integration.leadership.test_stage2_contracts import expire_claim
    from vuzol.config.registries import ProfileRegistry
    from vuzol.leadership.composition import build_project_lead_worker
    from vuzol.leadership.persistence import LeadershipTurn

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/lead", 1100))
            await ingress.accept_message(turn("Первый вопрос"))
            transport = _Transport({"reply": "Обсудим.", "commands": []})
            worker = build_project_lead_worker(
                factory,
                settings=runtime.settings,
                profiles=runtime.registries.profiles,
                owner="before",
                adapter=transport,
            )
            claim = await _claim(factory, worker, "before")
            await worker._infer(claim)  # Crash after the provider receipt, before saving decision.
            await expire_claim(factory)
            profiles = ProfileRegistry(
                tuple(
                    p.model_copy(update={"model": "changed"}) if p.id == "lead-only" else p
                    for p in runtime.registries.profiles.items()
                )
            )
            restarted = build_project_lead_worker(
                factory,
                settings=runtime.settings,
                profiles=profiles,
                owner="after",
                adapter=transport,
            )
            assert await restarted.process_one()
            assert len(transport.requests) == 1
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                goal = await session.scalar(select(ManagedGoal))
                assert goal.waiting_reason == "lead_profile_unselected"
                saved = await session.scalar(select(LeadershipTurn))
                assert saved.model_assignments["lead"]["model"] == "vendor/current-lead"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_disabled_or_unauthorized_switch_never_becomes_a_task(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/lead"))
            forbidden = turn("/think", 1102).model_copy(update={"user_id": 999})
            assert (await ingress.accept_message(forbidden)).status is IngressStatus.REJECTED
            async with factory() as session:
                mapping = (await session.scalars(select(TopicMapping))).one()
                assert mapping.conversation_mode == "think"
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert (
                    await session.scalar(select(func.count()).select_from(TelegramIntakeMessage))
                    == 0
                )
            assert parse_conversation_command("/lead@bot") == "lead"
            assert parse_conversation_command("/lead сделай задачу") is None
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_accepted_discussion_reaches_lead_and_amendment_keeps_meaningful_criteria(
    postgres_dsn, tmp_path
):
    from tests.integration.leadership.test_managed_goal_flow import _worker
    from vuzol.leadership.chat import conversation_goal_context
    from vuzol.leadership.model import LeadCommandDraft, LeadDecision
    from vuzol.leadership.persistence import current_request

    proposal = "Добавить поиск книг по названию и автору. Не менять страницу профиля."

    class Model:
        async def decide(self, snapshot):
            if not snapshot.owner_event:
                return LeadDecision()
            if snapshot.owner_event["message_id"] == 1101:
                return LeadDecision(reply=proposal)
            assert any(t["text"] == proposal for t in snapshot.result_context["conversation"])
            assert "Прод не трогай" in snapshot.owner_event["text"]
            return LeadDecision(
                reply="Уточню цель по нашему обсуждению.",
                commands=(
                    LeadCommandDraft(
                        "amend_goal",
                        {
                            "scope": proposal,
                            "criteria": [{"id": "search", "description": proposal}],
                            "source_event_id": snapshot.owner_event["source_event_id"],
                            "inflight_policy": "finish_current",
                        },
                        reason="Владелец согласовал обсуждённое направление",
                    ),
                ),
            )

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            worker = _worker(factory, Model())
            await ingress.accept_message(turn("Предложи план поиска книг, без публикации."))
            assert await worker.process_one()
            await ingress.accept_message(turn("Да, реализуй этот план. Прод не трогай.", 1102))
            assert await worker.process_one()
            assert await worker.process_one()  # Apply pending amendment in the next turn.
            async with factory() as session:
                goal = (await session.scalars(select(ManagedGoal))).one()
                assert current_request(goal) == proposal
                assert goal.criteria == [{"id": "search", "description": proposal}]
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                context = await conversation_goal_context(
                    session,
                    project_id="vuzol",
                    user_id=42,
                    chat_id=-100,
                    thread_id=10,
                )
                assert proposal in context and "owner_user_id" not in context
                for user_id, thread_id in [(999, 10), (42, 20)]:
                    other = await conversation_goal_context(
                        session,
                        project_id="vuzol",
                        user_id=user_id,
                        chat_id=-100,
                        thread_id=thread_id,
                    )
                    assert str(goal.id) not in other and proposal not in other
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_crash_after_event_before_ack_rolls_back_then_recovers_once(
    postgres_dsn, tmp_path, monkeypatch
):
    from vuzol.storage.models import ConversationTurn, ExternalInbox
    from vuzol.storage.repositories.delivery import InboxRepository

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            original = InboxRepository.mark_processed

            async def crash(repository, *args, **kwargs):
                raise RuntimeError("crash before intake ack")

            monkeypatch.setattr(InboxRepository, "mark_processed", crash)
            ingress = TelegramIngressService(runtime, factory)
            update = turn("Добавь поиск книг.")
            with pytest.raises(RuntimeError, match="crash before intake ack"):
                await ingress.accept_message(update)
            async with factory() as session:
                for model in (
                    ManagedGoal,
                    LeadershipCommandReceipt,
                    ConversationTurn,
                    ExternalInbox,
                ):
                    assert await session.scalar(select(func.count()).select_from(model)) == 0
            monkeypatch.setattr(InboxRepository, "mark_processed", original)
            assert (await ingress.accept_message(update)).status is IngressStatus.HANDLED
            assert (await ingress.accept_message(update)).status is IngressStatus.DUPLICATE
            async with factory() as session:
                for model in (ManagedGoal, LeadershipCommandReceipt, ConversationTurn):
                    assert await session.scalar(select(func.count()).select_from(model)) == 1
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_pending_old_chat_intake_enters_lead_without_jev(postgres_dsn, tmp_path):
    from vuzol.interpretation.ports import InterpreterUnavailable

    class NeverClassify:
        async def interpret(self, *args, **kwargs):
            raise InterpreterUnavailable("Jev must not decide lead entry")

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            update = turn("Что здесь можно улучшить?")
            topic = runtime.registries.topics.resolve(update.chat_id, update.message_thread_id)
            accepted = await ingress._accept_discussion_message(update, topic)
            pipeline = InterpretationPipeline(
                runtime,
                factory,
                owner="old-chat-intake",
                interpreter=NeverClassify(),
            )
            assert await pipeline.process_one()
            assert not await pipeline.process_one()
            async with factory() as session:
                goal = await session.scalar(select(ManagedGoal))
                assert goal.lifecycle == "draft"
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                events = list(
                    await session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "project_lead"
                        )
                    )
                )
                assert len(events) == 1 and events[0].payload["intake_id"] == str(
                    accepted.intake_id
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())
