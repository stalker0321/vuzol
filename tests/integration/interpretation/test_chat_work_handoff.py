"""Discussion → explicit implementation, frozen references and manual Start: TEST PG."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

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
from tests.integration.interpretation.test_chat_intent_context import finish_reply
from tests.integration.interpretation.test_chat_recovery_memory import runtime_with_memory
from tests.integration.providers._test_routing_helpers import profile
from tests.integration.providers.test_handler_request_guards import (
    _insert_reservation,
    _probe_token,
    _request,
)
from tests.integration.storage.helpers import storage
from tests.unit.interpretation.test_jev_live import response
from vuzol.config import RegistryDocument, build_bundle
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.work_context import load_work_context
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.providers.handlers import ProviderStepHandler
from vuzol.storage.attempts import snapshot_task_spec
from vuzol.storage.models import (
    Artifact,
    ConversationTurn,
    Run,
    Step,
    Task,
    TaskSpecRevision,
    TransactionalOutbox,
    Worktree,
)
from vuzol.storage.types import RunStatus, StepStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import prepare_delivery
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.dispatch import WorkflowDispatcher

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("document_snapshot", ["current", "legacy_json"])
def test_discussion_to_work_freezes_old_files_choice_and_constraints(
    postgres_dsn,
    tmp_path,
    document_snapshot,
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = runtime_with_memory(tmp_path)
        references = ("NUMERICAL REF " + "я" * 12000, "SEMANTIC REF " + "ю" * 12000)
        raw = "окей, начинай делать сайт"
        choice = "Берём book_rating_scale_prototype.md. Только локальный тест; без регистрации."
        requests = []
        telegram = FakeTelegramClient()
        ingress = TelegramIngressService(runtime, factory)

        def handler(request):
            requests.append(json.loads(request.content))
            final = requests[-1]["state"]["current_turn"] == raw
            return httpx.Response(
                200,
                json=response(
                    "execute_request" if final else "respond", "continue", "conversation_context"
                ),
            )

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
                    owner="handoff",
                    interpreter=jev,
                    downloader=Downloader(
                        {"one": references[0].encode(), "two": references[1].encode()}
                    ),
                )
                messages = [
                    message("Предложи шкалы для книг, только обсудим."),
                    message("Не нравится предложение; без искусственной иерархии. Пока не делай."),  # noqa: RUF001
                    document_message(
                        caption="Вот мои и GPT референсы; пока только обсуди.",
                        attachments=(
                            attachment(),
                            attachment("book_rating_scale_prototype.md", "text/markdown", "two"),
                        ),
                    ),
                    *[message(f"Обсуждаем детали {n}; не запускай.") for n in range(4)],
                    message(choice),
                ]
                for n, msg in enumerate(messages):
                    msg = msg.model_copy(update={"message_id": 1101 + n, "update_id": 1101 + n})
                    accepted = await ingress.accept_message(msg)
                    assert await pipeline.process_one()
                    async with factory() as session:
                        task = await session.scalar(select(Task).order_by(Task.created_at.desc()))
                        task_id = task.id
                    await finish_reply(
                        factory,
                        task_id,
                        "Тестовый сайт: 8 критериев, без общего балла и деплоя. "
                        "Реализацию здесь не начинаю.",
                        client=telegram,
                        summary="Выбран семантический прототип; локальный тест, "
                        "8 критериев, без регистрации и общего балла.",
                    )
                    if n == 2 and document_snapshot == "legacy_json":
                        # Pre-T103 stored the same prepared files in JSON form.
                        # Simulate that historical format before it leaves the
                        # summary overlap; promotion must not depend on layout.
                        async with factory.begin() as session:
                            source = await session.scalar(
                                select(ConversationTurn).where(
                                    ConversationTurn.intake_message_id == accepted.intake_id
                                )
                            )
                            outbox = await session.scalar(
                                select(TransactionalOutbox).where(
                                    TransactionalOutbox.destination == "discussion_classify",
                                    TransactionalOutbox.linked_entity_id == accepted.intake_id,
                                )
                            )
                            prepared = dict(outbox.payload["prepared_documents"])
                            legacy = json.dumps(
                                {
                                    "caption": prepared["caption"],
                                    "documents": [
                                        {
                                            k: v
                                            for k, v in doc.items()
                                            if k not in {"content_uri", "bytes"}
                                        }
                                        for doc in prepared["documents"]
                                    ],
                                },
                                ensure_ascii=False,
                            )
                            source.content = legacy
                            source.content_hash = hashlib.sha256(legacy.encode()).hexdigest()
                            outbox.payload = {
                                **outbox.payload,
                                "prepared_documents": {**prepared, "turn": legacy},
                            }
                async with UnitOfWork(factory) as uow:
                    first_turn = await uow.session.scalar(
                        select(ConversationTurn).order_by(ConversationTurn.ordinal)
                    )
                    prior = await DiscussionMemoryService(uow).load_context(
                        session_id=first_turn.session_id, chat_memory=True, delivered_only=True
                    )
                    # The original files are now older than the six-turn overlap.
                    assert not any(t.document_context for t in prior.turns)
                begin = message(raw).model_copy(update={"message_id": 1200, "update_id": 1200})
                await ingress.accept_message(begin)
                assert await pipeline.process_one()
                assert not await pipeline.process_one()
                # Telegram retry of the same update cannot promote twice.
                await ingress.accept_message(begin)
                assert not await pipeline.process_one()
            dispatcher = WorkflowDispatcher(runtime, factory, owner="handoff-dispatch")
            assert await dispatcher.process_one()
            assert not await dispatcher.process_one()
            async with factory.begin() as session:
                task = await session.scalar(select(Task).where(Task.ingress_source == "chat_work"))
                assert task.original_text == raw and task.public_task_number == 100001
                task_id = task.id
                run = await session.scalar(select(Run).where(Run.task_id == task.id))
                assert run.status is RunStatus.CREATED
                spec = await session.scalar(
                    select(TaskSpecRevision).where(
                        TaskSpecRevision.task_id == task.id,
                        TaskSpecRevision.spec_revision == task.spec_revision,
                    )
                )
                snapshot = spec.spec["chat_work_context"]
                assert snapshot["document_text_chars"] == sum(map(len, references))
                frozen = snapshot["content"]
                assert all(frozen.count(ref) == 1 for ref in references)
                assert choice in frozen and "без общего балла" in frozen
                assert json.loads(frozen)["current_user_turn"] == raw
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(Artifact)
                        .where(
                            Artifact.task_id == task.id,
                            Artifact.artifact_type == "telegram_attachment",
                        )
                    )
                    == 2
                )
                notice = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "telegram",
                        TransactionalOutbox.payload["task_id"].as_string() == str(task.id),
                    )
                )
                delivery = await prepare_delivery(session, notice)
                assert "start" in delivery.buttons
                steps = list(
                    (await session.scalars(select(Step).where(Step.run_id == run.id))).all()
                )
                assert all(
                    s.payload["chat_work_spec_revision"] == task.spec_revision for s in steps
                )
                execution = next(
                    s for s in steps if s.step_type in {"execute_agent", "execute_code"}
                )
                execution.status = StepStatus.RUNNING
                execution.lease_owner = "probe"
                execution.lease_generation = 1
                execution.executor_profile_id = "api"
                step_id, run_id = execution.id, run.id
                pinned = task.spec_revision
                session.add(
                    Worktree(
                        task_id=task.id,
                        run_id=run.id,
                        project_id=task.project_id,
                        repository_identity_hash="a" * 64,
                        base_commit="a" * 40,
                        default_branch="main",
                        expected_target_head="a" * 40,
                        branch="task-handoff",
                        path=str(tmp_path / "worktree"),
                        owner="probe",
                        retention_until=datetime.now(UTC) + timedelta(days=1),
                    )
                )
            reservation_id = await _insert_reservation(
                factory, task_id=task_id, run_id=run_id, step_id=step_id
            )
            async with factory.begin() as session:
                step = await session.get(Step, step_id)
                step.payload = {
                    **step.payload,
                    "budget_reservation_id": str(reservation_id),
                    "provider_attempt": 1,
                }
            registries = build_bundle(
                RegistryDocument(
                    projects=runtime.registries.projects.items(),
                    profiles=(*runtime.registries.profiles.items(), profile("api")),
                    sandboxes=runtime.registries.sandboxes.items(),
                ),
                runtime.settings,
                environment={},
                validate_profile_credentials=False,
            )
            provider = ProviderStepHandler(factory, registries, MagicMock())
            request, *_ = await provider._build_request(
                _request(task_id, run_id, step_id, _probe_token(step_id, run_id))
            )
            assert request.original_input == raw
            assert (
                "".join(c.content for c in request.context if c.source == "chat_work_context")
                == frozen
            )
            assert "chat_work_context" not in request.task_draft
            # Later dialogue is not consulted. Spec corrections retain old evidence,
            # while an already materialized run is fenced against the new revision.
            async with factory.begin() as session:
                task = await session.get(Task, task_id)
                task.task_draft = {**task.task_draft, "goal": "Уточнённая рабочая спецификация"}
                await snapshot_task_spec(session, task)
                assert task.spec_revision != pinned
                assert "".join(c.content for c in await load_work_context(session, task)) == frozen
                with pytest.raises(ValueError, match="revision changed"):
                    await load_work_context(session, task, expected_revision=pinned)
        finally:
            await engine.dispose()

    asyncio.run(scenario())
