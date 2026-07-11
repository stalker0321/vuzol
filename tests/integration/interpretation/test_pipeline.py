import asyncio
from pathlib import Path

import anyio
import pytest
from sqlalchemy import func, select, update

from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from vuzol.interpretation.adapters import FakeInterpreter, FakeTranscriber
from vuzol.interpretation.domain import (
    InterpretationResult,
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
    TranscriptionResult,
)
from vuzol.interpretation.ports import InterpreterUnavailable
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.models import ClarificationDecision, Interpretation, Task, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus, RiskLevel, TaskStatus
from vuzol.telegram.domain import AttachmentKind, MessageUpdate, TelegramAttachment
from vuzol.telegram.ingress import TelegramIngressService

pytestmark = pytest.mark.postgresql


class FakeDownloader:
    async def download(self, file_id: str) -> bytes:
        assert file_id == "telegram-file"
        return b"voice-content"


def interpreted_result(
    *, risk: RiskLevel = RiskLevel.LOW, needs_clarification: bool = False
) -> InterpretationResult:
    return InterpretationResult(
        draft=TaskDraft(
            action=TaskAction.CREATE_TASK,
            task_type=TaskType.CODING,
            operation=TaskOperation.INSPECT,
            project_id="vuzol",
            goal="Inspect project",
            required_capabilities=frozenset(),
            suggested_complexity=SuggestedComplexity.SMALL,
            suggested_risk=risk,
            needs_planning=False,
            needs_clarification=needs_clarification,
            clarification_question=(
                "Which environment should be inspected?" if needs_clarification else None
            ),
            normalized_title="Inspect project",
        ),
        profile_id="fake-interpreter",
        model="fake-model",
        duration_ms=2,
    )


def text_update(
    update_id: int, *, attachments: tuple[TelegramAttachment, ...] = ()
) -> MessageUpdate:
    return MessageUpdate(
        bot_id="main",
        update_id=update_id,
        chat_id=-100,
        message_thread_id=10,
        message_id=update_id,
        user_id=42,
        text=None if attachments else "inspect this project",
        attachments=attachments,
    )


def test_text_interpretation_persists_draft_and_original_input(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        ingress = TelegramIngressService(runtime, factory)
        accepted = await ingress.accept_message(text_update(101))
        assert accepted.task_id is not None
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([interpreted_result()]),
            owner="interpreter-a",
        )
        assert await pipeline.process_one()
        async with factory() as session:
            task = await session.get(Task, accepted.task_id)
            interpretation = await session.scalar(
                select(Interpretation).where(Interpretation.task_id == accepted.task_id)
            )
            assert task is not None and task.original_text == "inspect this project"
            assert task.status is TaskStatus.INTERPRETED
            assert task.task_draft["normalized_title"] == "Inspect project"
            assert interpretation is not None and interpretation.transcript is None
            assert len(interpretation.original_input_hash) == 64
        await engine.dispose()

    asyncio.run(scenario())


def test_voice_download_transcription_and_interpretation_are_durable(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        attachment = TelegramAttachment(
            file_id="telegram-file",
            file_unique_id="unique-file",
            kind=AttachmentKind.VOICE,
            file_size=len(b"voice-content"),
            media_type="audio/ogg",
        )
        accepted = await TelegramIngressService(runtime, factory).accept_message(
            text_update(102, attachments=(attachment,))
        )
        assert accepted.task_id is not None
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([interpreted_result(risk=RiskLevel.HIGH)]),
            downloader=FakeDownloader(),
            transcriber=FakeTranscriber(
                TranscriptionResult(
                    transcript="inspect this project by voice",
                    profile_id="fake-transcriber",
                    model="fake-audio",
                    duration_ms=3,
                    uncertain=True,
                )
            ),
            owner="interpreter-a",
        )
        assert await pipeline.process_one()
        assert await pipeline.process_one()
        async with factory() as session:
            task = await session.get(Task, accepted.task_id)
            assert task is not None and task.transcript == "inspect this project by voice"
            assert task.status is TaskStatus.AWAITING_USER
            assert task.voice_reference is not None
            assert await anyio.Path(task.voice_reference).read_bytes() == b"voice-content"
            assert await session.scalar(select(func.count()).select_from(Interpretation)) == 1
            statuses = (
                await session.scalars(
                    select(TransactionalOutbox.status).where(
                        TransactionalOutbox.destination.in_(["telegram_file", "interpretation"])
                    )
                )
            ).all()
            assert statuses == [DeliveryStatus.DELIVERED, DeliveryStatus.DELIVERED]
            clarification = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "telegram",
                    TransactionalOutbox.payload["role"].as_string() == "semantic_clarification",
                )
            )
            assert clarification is not None and clarification.status is DeliveryStatus.PENDING
        await engine.dispose()

    asyncio.run(scenario())


def test_unavailable_interpreter_dead_letters_without_changing_original_task(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        base_runtime = telegram_runtime(tmp_path)
        runtime = base_runtime.model_copy(
            update={
                "settings": base_runtime.settings.model_copy(
                    update={
                        "interpretation": base_runtime.settings.interpretation.model_copy(
                            update={"max_attempts": 2}
                        )
                    }
                )
            }
        )
        accepted = await TelegramIngressService(runtime, factory).accept_message(text_update(103))
        assert accepted.task_id is not None
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter(
                [InterpreterUnavailable("offline"), InterpreterUnavailable("offline")]
            ),
            owner="interpreter-a",
        )
        assert await pipeline.process_one()
        async with factory() as session:
            pending = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "interpretation"
                )
            )
            assert pending is not None and pending.status is DeliveryStatus.PENDING
            assert pending.last_error_category == "provider_unavailable"
        async with factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.destination == "interpretation")
                .values(available_at=func.now())
            )
        assert await pipeline.process_one()
        async with factory() as session:
            task = await session.get(Task, accepted.task_id)
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "interpretation"
                )
            )
            assert task is not None and task.status is TaskStatus.RECEIVED
            assert task.original_text == "inspect this project"
            assert item is not None and item.status is DeliveryStatus.DEAD_LETTER
            assert item.last_error_category == "provider_unavailable"
            assert await session.scalar(select(func.count()).select_from(Interpretation)) == 0
        await engine.dispose()

    asyncio.run(scenario())


def test_clarification_answer_is_persisted_before_reinterpretation(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        interpreter = FakeInterpreter(
            [interpreted_result(needs_clarification=True), interpreted_result()]
        )
        pipeline = InterpretationPipeline(
            runtime, factory, interpreter=interpreter, owner="interpreter-a"
        )
        first = await TelegramIngressService(runtime, factory).accept_message(text_update(104))
        assert first.task_id is not None and await pipeline.process_one()
        second_update = text_update(105).model_copy(update={"text": "Use the staging environment"})
        second = await TelegramIngressService(runtime, factory).accept_message(second_update)
        assert second.task_id == first.task_id and await pipeline.process_one()
        async with factory() as session:
            decision = await session.scalar(select(ClarificationDecision))
            task = await session.get(Task, first.task_id)
            assert decision is not None
            assert decision.question == "Which environment should be inspected?"
            assert decision.answer == "Use the staging environment"
            assert task is not None and task.status is TaskStatus.INTERPRETED
        await engine.dispose()

    asyncio.run(scenario())
