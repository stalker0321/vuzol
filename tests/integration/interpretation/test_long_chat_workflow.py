"""Real workflow/routing/settlement/delivery, recorded-shaped fake CLI reply."""

import asyncio
import json
import subprocess

import pytest
from sqlalchemy import select

from tests.integration.interpretation.test_chat_boundary import chat_runtime
from tests.integration.storage.helpers import storage
from tests.integration.telegram.test_delivery import service
from tests.unit.providers._test_providers_helpers import StaticCodexTransport, codex_jsonl
from tests.unit.test_telegram_reply_chunks import visible
from vuzol.config import ScopedSecretResolver
from vuzol.discussion.agent import DeliverDiscussionReplyHandler, schedule_discussion_agent
from vuzol.discussion.memory import MemoryPack
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.execution.git import LocalGit
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.execution.worktrees import WorktreeService
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.handlers import ProviderStepHandler
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.registry import AdapterRegistry
from vuzol.storage.models import (
    ConversationTurn,
    ProviderBudgetReservation,
    Run,
    Step,
    Task,
    UsageRecord,
)
from vuzol.storage.types import (
    BudgetReservationStatus,
    ConversationTurnRole,
    ConversationTurnSource,
    InteractionMode,
    QueueClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.formatting import telegram_markdown_html, telegram_markdown_parts
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.worker import CompleteHandler, RoutedWorkflowWorker, WorkflowWorker

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.mark.parametrize("rich_messages", [False, True])
@pytest.mark.parametrize("long_history", [False, True])
async def test_long_reply_through_whole_hidden_workflow_keeps_actual_token_usage(
    postgres_dsn, tmp_path, rich_messages, long_history
):
    engine, factory = storage(postgres_dsn)
    runtime = chat_runtime(tmp_path)
    runtime = runtime.model_copy(
        update={
            "settings": runtime.settings.model_copy(
                update={
                    "chat": runtime.settings.chat.model_copy(
                        update={"rich_messages": rich_messages}
                    ),
                }
            ),
        }
    )
    settings, registries = runtime.settings, runtime.registries
    repository = registries.projects.get("vuzol").repository_path
    await asyncio.to_thread(
        subprocess.run,
        ["git", "init", "-b", "main"],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    await asyncio.to_thread(
        subprocess.run,
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "base",
        ],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    text = "**Ответ**\n\n" + "я" * 4324  # longer than the former 4000-char protocol cap
    transport = StaticCodexTransport(codex_jsonl(json.dumps({"reply": text})))
    worktrees = WorktreeService(tmp_path / "worktrees", LocalGit(), retention_days=3)
    try:
        async with UnitOfWork(factory) as uow:
            session_id = await uow.discussions.create_session(
                project_id="vuzol",
                chat_id=-100,
                message_thread_id=10,
            )
            if long_history:
                # Ordinary accumulated exchanges, not duplicate failed retries.
                for index in range(6):
                    for role in (ConversationTurnRole.USER, ConversationTurnRole.ASSISTANT):
                        await uow.discussions.append_turn(
                            session_id=session_id,
                            role=role,
                            source=(
                                ConversationTurnSource.TELEGRAM_USER
                                if role is ConversationTurnRole.USER
                                else ConversationTurnSource.MODEL
                            ),
                            content=f"Exchange {index}, {role.value}: " + "контекст " * 167,
                            classifier_mode=InteractionMode.DISCUSSION,
                        )
            current = "Обсудим, ничего не запускай."
            if long_history:
                current = "Текущий запрос. " + "детали " * 450 + current
            source_id, _ = await uow.discussions.append_turn(
                session_id=session_id,
                role=ConversationTurnRole.USER,
                source=ConversationTurnSource.TELEGRAM_USER,
                content=current,
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
                memory_pack=(
                    await DiscussionMemoryService(uow).load_context(session_id=session_id)
                    if long_history
                    else MemoryPack(session_id, None, (), (), False, False, False)
                ),
                current_turn=current if long_history else None,
            )
            scheduled = await uow.session.get(Task, task_id)
            assert scheduled is not None
            if long_history:
                assert 20_000 < len(scheduled.original_text) <= 40_000
            await synchronize_profiles(
                uow.session, registries.profiles.items(), configuration_revision=registries.revision
            )
        prepare = WorkflowWorker(
            settings,
            factory,
            owner="prepare",
            handlers={
                "prepare_worktree": PrepareWorktreeHandler(
                    factory, registries, worktrees, owner="prepare", settings=settings
                ),
            },
        )
        assert await prepare.process_one()
        adapters = AdapterRegistry(
            registries.profiles,
            ScopedSecretResolver(access_policy={}, secret_file_root=settings.secret_file_root),
            adapters={"sol-chat": CodexCliAdapter(transport)},
        )
        provider = RoutedWorkflowWorker(
            settings,
            factory,
            registries=registries,
            owner="sol",
            queue_classes=frozenset({QueueClass.HEAVY}),
            handlers={
                "execute_agent": ProviderStepHandler(
                    factory, registries, adapters, worktrees=worktrees
                )
            },
        )
        assert await provider.process_one()
        assert len(transport.invocations) == 1
        envelope = json.loads(transport.invocations[0].stdin)
        if long_history:
            assert current in envelope["original_input"]
            assert "Exchange 0, user:" in envelope["original_input"]
            assert "Exchange 5, assistant:" in envelope["original_input"]
        async with factory() as session:
            execute = await session.scalar(select(Step).where(Step.step_type == "execute_agent"))
            assert execute.status is StepStatus.COMPLETED
            assert execute.result["structured_output"]["reply"] == text
            reservation = (await session.scalars(select(ProviderBudgetReservation))).one()
            usage = (await session.scalars(select(UsageRecord))).one()
            # Subscription price is unknown: keep its normal conservative cost
            # policy, but settle actual tokens instead of the full output reserve.
            assert reservation.status is BudgetReservationStatus.CONSERVATIVE
            assert reservation.reconciled_input_tokens == 143
            assert reservation.reconciled_output_tokens == 17
            assert (usage.input_tokens, usage.output_tokens, usage.outcome) == (
                143,
                17,
                "succeeded",
            )
        common = WorkflowWorker(
            settings,
            factory,
            owner="common",
            handlers={
                "deliver_discussion_reply": DeliverDiscussionReplyHandler(factory),
                "finalize": CompleteHandler(),
            },
        )
        assert await common.process_one()
        assert await common.process_one()
        client = FakeTelegramClient()
        if rich_messages:
            assert await service(factory, client, rich_messages=True).deliver_one()
            assert len(client.rich_sent) == 1 and not client.sent
            assert "я" * 4324 in client.rich_sent[0][2]
        else:
            for _ in telegram_markdown_parts(text):
                assert await service(factory, client).deliver_one()
            assert "".join(visible(m[2]) for m in client.sent) == visible(
                telegram_markdown_html(text)
            )
            assert len(client.sent) == 2
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = (await session.scalars(select(Run))).one()
            assert run.status is RunStatus.COMPLETED and task.status is TaskStatus.COMPLETED
            assert task.public_task_number is None
            turns = (
                await session.scalars(
                    select(ConversationTurn).where(
                        ConversationTurn.role == ConversationTurnRole.ASSISTANT
                    )
                )
            ).all()
            assert len(turns) == (7 if long_history else 1)
            assert sum(turn.content == text for turn in turns) == 1
    finally:
        await engine.dispose()
