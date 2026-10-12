"""Use existing request construction and continuation refs; no live providers."""

import asyncio
import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.integration.providers._test_routing_helpers import bundle, profile, storage
from tests.integration.providers.test_handler_request_guards import (
    _insert_reservation,
    _request,
    _running_step,
    _set_step_payload,
)
from tests.integration.workflows._test_runtime_helpers import simple_draft
from vuzol.interpretation.domain import TaskAction
from vuzol.providers.handlers import ProviderStepHandler
from vuzol.storage.models import Interpretation, Task, TelegramIntakeMessage, TransactionalOutbox
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    IntakeStatus,
    InteractionMode,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork

RAW = (
    "Проверь поиск: "
    + "подробности " * 400
    + "если сломан, исправь только поиск; пока не запускай деплой."
)


@pytest.mark.postgresql
@pytest.mark.parametrize(
    "case",
    ["text", "voice", "continuation", "voice_continuation", "changed_source", "wrong_target"],
)
def test_current_raw_turn_reaches_provider_request(
    postgres_dsn: str,
    tmp_path: Path,
    case: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("api"))
        task_id, run_id, step_id, token = await _running_step(factory)
        reservation_id = await _insert_reservation(
            factory,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
        )
        payload = {"budget_reservation_id": str(reservation_id), "provider_attempt": 1}
        continuation_case = case not in {"text", "voice"}
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.original_text = (
                "Initial task with its original restrictions." if continuation_case else RAW
            )
            task.task_draft = {"goal": RAW[:4000]}
            if case == "voice":
                task.original_text = "[voice attachment]"
                task.transcript = RAW
            if continuation_case:
                carrier = Task(
                    user_id=1,
                    project_id="proj",
                    task_type="general",
                    original_text="[voice attachment]" if case == "voice_continuation" else RAW,
                    transcript=RAW if case == "voice_continuation" else None,
                    parent_task_id=None if case == "wrong_target" else task_id,
                    status=TaskStatus.COMPLETED,
                )
                session.add(carrier)
                await session.flush()
                interpretation = Interpretation(
                    task_id=carrier.id,
                    original_input_hash=hashlib.sha256(RAW.encode()).hexdigest(),
                    task_draft=simple_draft()
                    .model_copy(
                        update={
                            "action": TaskAction.CONTINUE_TASK,
                            "referenced_task_id": task_id,
                        }
                    )
                    .model_dump(mode="json"),
                    profile_id="jev",
                    model="typesafe/jev-1.13",
                    prompt_version="test",
                    schema_version="1.4",
                )
                session.add(interpretation)
                await session.flush()
                payload["continuation_interpretation_id"] = str(interpretation.id)
                if case == "changed_source":
                    carrier.original_text = "Different source: conditions removed."
        await _set_step_payload(factory, step_id, payload)
        handler = ProviderStepHandler(factory, registries, MagicMock())
        try:
            if case in {"changed_source", "wrong_target"}:
                with pytest.raises(LookupError, match="does not match the bound task"):
                    await handler._build_request(_request(task_id, run_id, step_id, token))
            else:
                request, *_ = await handler._build_request(
                    _request(task_id, run_id, step_id, token)
                )
                assert request.original_input == RAW
                assert len(request.original_input) > 4000
                assert request.task_draft["goal"] != RAW
                if continuation_case:
                    assert any(
                        item.content == "Initial task with its original restrictions."
                        for item in request.context
                    )
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_work_document_partition_is_measured_from_its_durable_source(postgres_dsn, tmp_path):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        _settings, registries = bundle(tmp_path, profile("api"))
        task_id, run_id, step_id, token = await _running_step(factory)
        text = "Если поиска нет, исправь только поиск; пока не запускай деплой.\n" + "D" * 50_000
        reservation_id = await _insert_reservation(
            factory,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
        )
        try:
            async with UnitOfWork(factory) as uow:
                inbox_id, _ = await uow.inbox.receive_once(
                    source="telegram",
                    consumer="bot:main",
                    external_event_id="docs-work",
                    payload_hash="a" * 64,
                )
                intake = TelegramIntakeMessage(
                    inbox_id=inbox_id,
                    user_id=1,
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=1101,
                    original_text=text,
                    affinity_kind="new_task",
                    status=IntakeStatus.COMPLETED,
                )
                uow.session.add(intake)
                await uow.session.flush()
                session_id = await uow.discussions.create_session(
                    project_id="proj",
                    chat_id=-100,
                    message_thread_id=10,
                )
                source_id, _ = await uow.discussions.append_turn(
                    session_id=session_id,
                    role=ConversationTurnRole.USER,
                    source=ConversationTurnSource.TELEGRAM_USER,
                    content=text,
                    classifier_mode=InteractionMode.TASK_REQUEST,
                    intake_message_id=intake.id,
                )
                uow.session.add(
                    TransactionalOutbox(
                        destination="discussion_classify",
                        operation_type="classify_intake",
                        linked_entity_type="telegram_intake_message",
                        linked_entity_id=intake.id,
                        idempotency_key="test:work-docs",
                        payload={
                            "prepared_documents": {
                                "turn": text,
                                "documents": [{"text": "D" * 50_000}],
                            }
                        },
                    )
                )
                task = await uow.session.get(Task, task_id)
                task.original_text = text
                task.ingress_source = "chat_work"
                task.source_turn_id = source_id
            await _set_step_payload(
                factory,
                step_id,
                {
                    "budget_reservation_id": str(reservation_id),
                    "provider_attempt": 1,
                },
            )
            request, *_ = await ProviderStepHandler(
                factory, registries, MagicMock()
            )._build_request(
                _request(task_id, run_id, step_id, token),
            )
            assert request.original_input == text
            assert request.document_text_chars == 50_000
            assert (
                request.max_input_tokens == 1000
            )  # Existing reservation, not raised by documents.
        finally:
            await engine.dispose()

    asyncio.run(scenario())
