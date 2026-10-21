"""Persistent interlocutors, independent Jev intent, exact RO model boundary: TEST PG."""
# ruff: noqa: RUF001

import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from tests.integration.interpretation.test_chat_boundary import chat_runtime, message
from tests.integration.interpretation.test_chat_intent_context import finish_reply
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
from vuzol.projects.model_matrix import set_assignment
from vuzol.storage.models import (
    Run,
    Task,
    TelegramIntakeMessage,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.telegram.domain import IngressStatus
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.application import Principal

pytestmark = pytest.mark.postgresql


def modes_runtime(tmp_path):
    runtime = chat_runtime(tmp_path)
    settings = runtime.settings.model_copy(
        update={
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
                            TransactionalOutbox.destination == "discussion_classify"
                        )
                    )
                ).all()
                routes = {
                    i.payload["message_thread_id"]: i.payload["conversation_route"] for i in items
                }
                assert routes[10]["conversation_mode"] == "think"
                assert routes[10]["assignment"]["profile_id"] == "sol-chat"
                assert routes[20]["conversation_mode"] == "lead"
                assert routes[20]["assignment"]["profile_id"] == "lead-only"
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_queued_message_and_reply_keep_the_original_speaker(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/lead"))
            await ingress.accept_message(turn("Предложи план, пока не запускай.", 1102))
            await ingress.accept_message(turn("/think", 1103))
            assert await classify(runtime, factory, "plan")
            async with factory() as session:
                task = (await session.scalars(select(Task))).one()
                run = (await session.scalars(select(Run))).one()
                assert run.selected_route["trusted_profile_id"] == "lead-only"
                assert run.selected_route["conversation_mode"] == "lead"
                assert task.task_draft["required_capabilities"] == ["repository_read"]
                assert "project lead, speaking in read-only" in task.original_text
                assert "Managed goal in this topic: none" in task.original_text
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
                task_id = task.id
            telegram = FakeTelegramClient()
            await finish_reply(factory, task_id, "Только обсуждение.", client=telegram)
            async with factory() as session:
                reply = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["role"].as_string() == "discussion_reply"
                    )
                )
                assert (
                    reply.payload["conversation_route"]["assignment"]["model"]
                    == "vendor/current-lead"
                )
            assert any(
                "vendor/current-lead" in str(call) and "Руководитель" in str(call)
                for call in telegram.sent
            )
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["think", "lead"])
def test_explicit_work_goes_to_lead_once_and_switch_does_not_resume(postgres_dsn, tmp_path, mode):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn(f"/{mode}"))
            request = turn("Добавь поиск книг; не публикуй результат в production.", 1102)
            await ingress.accept_message(request)
            assert await classify(runtime, factory, "execute_request")
            assert (await ingress.accept_message(request)).status is IngressStatus.DUPLICATE
            assert not await classify(runtime, factory, "execute_request")
            async with factory() as session:
                goal = (await session.scalars(select(ManagedGoal))).one()
                assert goal.lifecycle == "active"
                assert "не публикуй" in goal.scope_text
                assert goal.criteria and goal.owner_user_id == 42
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                assert (
                    await session.scalar(select(func.count()).select_from(LeadershipCommandReceipt))
                    == 1
                )
                assert (
                    await session.scalar(select(TelegramIntakeMessage))
                ).affinity_kind == "managed_goal"
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
            assert await classify(runtime, factory, "execute_request")
            async with factory() as session:
                goal = (await session.scalars(select(ManagedGoal))).one()
                assert goal.lifecycle == "paused" and goal.version == paused.version
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                denied = await session.scalar(
                    select(LeadershipCommandReceipt).where(
                        LeadershipCommandReceipt.reason == "goal_already_live"
                    )
                )
                assert denied is not None
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_lead_only_profile_reaches_real_handler_and_pi_read_only_transport(postgres_dsn, tmp_path):
    from datetime import UTC, datetime, timedelta

    from vuzol.config import ScopedSecretResolver
    from vuzol.providers.catalog import cli_adapter
    from vuzol.providers.handlers import ProviderStepHandler
    from vuzol.providers.health import synchronize_profiles
    from vuzol.providers.ports import CodexProcessResult
    from vuzol.providers.registry import AdapterRegistry
    from vuzol.providers.role_cli import RoleBoundCliAdapter
    from vuzol.providers.routing import claim_routed_step
    from vuzol.storage.models import Step, Worktree
    from vuzol.storage.types import StepStatus
    from vuzol.workflows.domain import OutcomeKind
    from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

    class Transport:
        invocation = None

        async def run(self, invocation, cancellation):
            self.invocation = invocation
            body = json.loads(invocation.stdin)
            assert body["role"] == "lead"
            assert body["output_schema"] is None  # Conversation, not mutating lead command JSON.
            assert "owner_user_id" not in invocation.stdin
            assert invocation.argv[invocation.argv.index("--tools") + 1] == "read,grep,find,ls"
            assert "--no-tools" not in invocation.argv  # Repository inspection is available.
            assert invocation.sandbox_reference.startswith("worktree:")
            assistant = {
                "role": "assistant",
                "stopReason": "stop",
                "content": [{"type": "text", "text": "Предлагаю обсудить варианты."}],
                "usage": {"input": 500, "output": 20},
            }
            return CodexProcessResult(
                0,
                json.dumps({"type": "agent_end", "messages": [assistant]})
                + '\n{"type":"agent_settled"}',
                "",
                5,
            )

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/lead"))
            await ingress.accept_message(turn("Обсуди варианты, не выполняй.", 1102))
            assert await classify(runtime, factory)
            async with factory.begin() as session:
                await synchronize_profiles(
                    session,
                    runtime.registries.profiles.items(),
                    configuration_revision=runtime.registries.revision,
                )
                task = (await session.scalars(select(Task))).one()
                run = (await session.scalars(select(Run))).one()
                steps = (await session.scalars(select(Step))).all()
                for step in steps:
                    if step.step_type == "prepare_worktree":
                        step.status = StepStatus.COMPLETED
                    elif step.step_type == "execute_agent":
                        step.status = StepStatus.QUEUED
                session.add(
                    Worktree(
                        task_id=task.id,
                        run_id=run.id,
                        project_id="vuzol",
                        repository_identity_hash="a" * 64,
                        base_commit="b" * 40,
                        default_branch="main",
                        expected_target_head="b" * 40,
                        branch="test/read-only",
                        path=str(tmp_path / "checkout"),
                        owner="test",
                        retention_until=datetime.now(UTC) + timedelta(days=1),
                    )
                )
            async with factory.begin() as session:
                token = await claim_routed_step(
                    session,
                    settings=runtime.settings,
                    registries=runtime.registries,
                    owner="test",
                    lease_seconds=60,
                    candidate_limit=20,
                    step_types=frozenset({"execute_agent"}),
                )
                assert token is not None
                step = await session.get(Step, token.step.id)
                assert step.executor_profile_id == "lead-only"
                assert step.required_capabilities == ["repository_read"]
                payload = dict(step.payload)
                step.status = StepStatus.RUNNING  # Worker entry after claim, no provider stub.
            configured = runtime.registries.profiles.get("lead-only")
            transport = Transport()
            base = cli_adapter(configured, transport)

            class ForbiddenAdapter:
                async def execute(self, *args):
                    raise AssertionError("conversation entered an autonomous turn adapter")

            # Same role dispatcher used by the executor entrypoint, fake only at process boundary.
            adapter = RoleBoundCliAdapter(base, ForbiddenAdapter(), ForbiddenAdapter())
            adapters = AdapterRegistry(
                runtime.registries.profiles,
                ScopedSecretResolver(
                    access_policy={}, secret_file_root=tmp_path / "secrets", environment={}
                ),
                adapters={"lead-only": adapter},
                session_factory=factory,
            )
            outcome = await ProviderStepHandler(factory, runtime.registries, adapters).execute(
                StepExecutionRequest(
                    task_id=task.id,
                    run_id=run.id,
                    step_id=token.step.id,
                    step_type="execute_agent",
                    payload=payload,
                    timeout_seconds=60,
                    lease=token,
                ),
                CancellationContext(),
            )
            assert outcome.kind is OutcomeKind.SUCCEEDED, outcome
            assert outcome.result["structured_output"]["reply"] == "Предлагаю обсудить варианты."
            assert transport.invocation is not None
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_model_selection_is_pinned_at_intake_and_catalog_change_fails_closed(
    postgres_dsn, tmp_path
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/think", 1100))
            await ingress.accept_message(turn("Первый вопрос"))
            async with factory.begin() as session:
                await set_assignment(
                    session,
                    project_id="vuzol",
                    slot="think",
                    profile_id="other-worker",
                    expected_revision=1,
                    user_id=42,
                    registries=runtime.registries,
                )
            await ingress.accept_message(turn("/think", 1102, thread=20))
            await ingress.accept_message(turn("Второй вопрос", 1103, thread=20))
            async with factory() as session:
                items = (
                    await session.scalars(
                        select(TransactionalOutbox)
                        .where(TransactionalOutbox.destination == "discussion_classify")
                        .order_by(TransactionalOutbox.created_at)
                    )
                ).all()
                assert [
                    i.payload["conversation_route"]["assignment"]["profile_id"] for i in items
                ] == [
                    "sol-chat",
                    "other-worker",
                ]
            # Same profile id now points at another model: do not silently substitute it.
            old = runtime.registries
            document = RegistryDocument(
                projects=old.projects.items(),
                topics=old.topics.items(),
                sandboxes=old.sandboxes.items(),
                profiles=tuple(
                    p.model_copy(update={"model": "changed"}) if p.id == "sol-chat" else p
                    for p in old.profiles.items()
                ),
            )
            changed = runtime.model_copy(
                update={"registries": build_bundle(document, runtime.settings)}
            )
            assert await classify(changed, factory)
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                notice = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key.like("chat:responder:%")
                    )
                )
                assert notice is not None
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


def test_accepted_discussion_is_frozen_for_lead_and_criterion_is_not_just_yes(
    postgres_dsn, tmp_path
):
    from vuzol.leadership.chat import conversation_goal_context

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("Предложи план поиска книг, без публикации."))
            assert await classify(runtime, factory, "plan")
            async with factory() as session:
                task = (await session.scalars(select(Task))).one()
                task_id = task.id
            proposal = "Добавить поиск книг по названию и автору. Не менять страницу профиля."
            await finish_reply(factory, task_id, proposal, client=FakeTelegramClient())
            await ingress.accept_message(turn("Да, реализуй этот план. Прод не трогай.", 1102))
            assert await classify(runtime, factory, "execute_request", "conversation_context")
            async with factory() as session:
                goal = (await session.scalars(select(ManagedGoal))).one()
                scope = json.loads(goal.scope_text)
                frozen = scope["accepted_conversation_context"]
                assert proposal in frozen["content"]
                assert "Прод не трогай" in scope["current_user_request"]
                assert frozen["content_hash"] in goal.criteria[0]["description"]
                assert goal.criteria[0]["description"] != goal.original_request
                context = await conversation_goal_context(
                    session, project_id="vuzol", user_id=42, chat_id=-100, thread_id=10
                )
                assert str(goal.id) in context and '"lifecycle": "active"' in context
                assert "owner_user_id" not in context
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


def test_crash_after_command_before_ack_rolls_back_then_recovers_once(
    postgres_dsn, tmp_path, monkeypatch
):
    from datetime import UTC, datetime, timedelta

    from vuzol.interpretation import service as pipeline_module
    from vuzol.storage.models import ConversationTurn
    from vuzol.storage.types import DeliveryStatus

    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = modes_runtime(tmp_path)
        try:
            await TelegramIngressService(runtime, factory).accept_message(
                turn("Добавь поиск книг.")
            )
            complete = pipeline_module.complete_outbox_item

            async def crash(session, token):
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1
                raise RuntimeError("crash before intake ack")

            monkeypatch.setattr(pipeline_module, "complete_outbox_item", crash)
            with pytest.raises(RuntimeError, match="crash before intake ack"):
                await classify(runtime, factory, "execute_request")
            async with factory.begin() as session:
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
                assert (
                    await session.scalar(select(func.count()).select_from(LeadershipCommandReceipt))
                    == 0
                )
                assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 0
                item = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "discussion_classify"
                    )
                )
                assert item.status is DeliveryStatus.LEASED
                # Test clock advance. Runtime must recover via its ordinary expired lease claim.
                item.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            monkeypatch.setattr(pipeline_module, "complete_outbox_item", complete)
            assert await classify(runtime, factory, "execute_request")
            assert not await classify(runtime, factory, "execute_request")
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1
                assert (
                    await session.scalar(select(func.count()).select_from(LeadershipCommandReceipt))
                    == 1
                )
                item = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "discussion_classify"
                    )
                )
                assert item.status is DeliveryStatus.DELIVERED
        finally:
            await engine.dispose()

    asyncio.run(scenario())
