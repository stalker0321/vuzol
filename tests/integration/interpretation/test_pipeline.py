import asyncio
from pathlib import Path

import anyio
import pytest
from sqlalchemy import func, select, update

from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from vuzol.interpretation.adapters import FakeInterpreter, FakeTranscriber
from vuzol.interpretation.discussion import (
    DISCUSSION_REPLY_DESTINATION,
    DiscussionInterpretation,
    DiscussionInterpretRequest,
)
from vuzol.interpretation.domain import (
    InterpretationResult,
    ProjectNameOption,
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
    TranscriptionResult,
)
from vuzol.interpretation.ports import InterpreterUnavailable
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.models import (
    ClarificationDecision,
    ConversationTurn,
    Interpretation,
    ProjectNamingRequest,
    Task,
    TelegramIntakeMessage,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    ConversationTurnRole,
    DeliveryStatus,
    IntakeStatus,
    ProjectNamingStatus,
    RiskLevel,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.domain import AttachmentKind, MessageUpdate, TelegramAttachment
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient

pytestmark = pytest.mark.postgresql


class FakeDownloader:
    async def download(self, file_id: str) -> bytes:
        assert file_id == "telegram-file"
        return b"voice-content"


class FakeDiscussionInterpreter:
    def __init__(self, results: list[DiscussionInterpretation | Exception]) -> None:
        self.results = results
        self.requests: list[DiscussionInterpretRequest] = []

    async def interpret_discussion(
        self, request: DiscussionInterpretRequest
    ) -> DiscussionInterpretation:
        self.requests.append(request)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class BlockingDiscussionInterpreter:
    def __init__(self, result: DiscussionInterpretation) -> None:
        self.result = result
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def interpret_discussion(
        self, request: DiscussionInterpretRequest
    ) -> DiscussionInterpretation:
        del request
        self.started.set()
        await self.release.wait()
        return self.result


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
            task_summary="Inspect the configured project",
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
            trace = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["role"].as_string() == "orchestration_trace",
                    TransactionalOutbox.payload["trace_kind"].as_string() == "interpreter",
                )
            )
            assert task is not None and task.original_text == "inspect this project"
            assert task.status is TaskStatus.INTERPRETED
            assert task.task_draft["normalized_title"] == "Inspect project"
            assert interpretation is not None and interpretation.transcript is None
            assert len(interpretation.original_input_hash) == 64
            assert trace is not None
            assert trace.payload["model_task_draft"]["task_type"] == "coding"
            assert trace.payload["duration_ms"] == 2
        await engine.dispose()

    asyncio.run(scenario())


def test_discussion_runtime_persists_turns_reuses_memory_and_delivers_replies(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        base_runtime = telegram_runtime(tmp_path)
        runtime = base_runtime.model_copy(
            update={
                "settings": base_runtime.settings.model_copy(
                    update={"project_discussion_enabled": True}
                )
            }
        )
        discussion = FakeDiscussionInterpreter(
            [
                DiscussionInterpretation(
                    interaction_mode="discussion",
                    confidence=0.91,
                    user_visible_summary="Давайте сначала уточним цель.",
                ),
                DiscussionInterpretation(
                    interaction_mode="query_only",
                    confidence=0.88,
                    user_visible_summary="Цель зафиксирована.",
                    clarification_question="Какой вариант интерфейса предпочтительнее?",
                ),
            ]
        )
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([]),
            discussion_interpreter=discussion,
            owner="interpreter-discussion",
        )
        ingress = TelegramIngressService(runtime, factory)

        first = await ingress.accept_message(text_update(151))
        assert first.task_id is None
        assert await pipeline.process_one()
        second = await ingress.accept_message(
            text_update(152).model_copy(update={"text": "Нужен простой интерфейс"})
        )
        assert second.task_id is None
        assert await pipeline.process_one()

        assert len(discussion.requests) == 2
        assert discussion.requests[0].memory_pack is not None
        assert discussion.requests[0].memory_pack.turns == ()
        assert discussion.requests[1].memory_pack is not None
        assert [turn.content for turn in discussion.requests[1].memory_pack.turns] == [
            "inspect this project",
            "Давайте сначала уточним цель.",
        ]

        client = FakeTelegramClient(next_message_id=500)
        delivery = TelegramDeliveryService(
            factory,
            client,
            owner="discussion-delivery",
            lease_seconds=30,
            max_attempts=3,
            retry_min_seconds=1,
            retry_max_seconds=10,
        )
        assert await delivery.deliver_one()
        assert await delivery.deliver_one()
        assert not await delivery.deliver_one()
        assert client.sent == [
            (-100, 10, "Давайте сначала уточним цель."),
            (-100, 10, "Цель зафиксирована.\n\nКакой вариант интерфейса предпочтительнее?"),  # noqa: RUF001
        ]

        async with factory() as session:
            turns = (
                await session.scalars(select(ConversationTurn).order_by(ConversationTurn.ordinal))
            ).all()
            assert [turn.role for turn in turns] == [
                ConversationTurnRole.USER,
                ConversationTurnRole.ASSISTANT,
                ConversationTurnRole.USER,
                ConversationTurnRole.ASSISTANT,
            ]
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            replies = (
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["role"].as_string()
                        == DISCUSSION_REPLY_DESTINATION
                    )
                )
            ).all()
            assert len(replies) == 2
            assert all(item.status is DeliveryStatus.DELIVERED for item in replies)
        await engine.dispose()

    asyncio.run(scenario())


def test_discussion_provider_failure_dead_letters_without_partial_turns_or_tasks(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        base_runtime = telegram_runtime(tmp_path)
        runtime = base_runtime.model_copy(
            update={
                "settings": base_runtime.settings.model_copy(
                    update={
                        "project_discussion_enabled": True,
                        "interpretation": base_runtime.settings.interpretation.model_copy(
                            update={"max_attempts": 1}
                        ),
                    }
                )
            }
        )
        accepted = await TelegramIngressService(runtime, factory).accept_message(text_update(153))
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([]),
            discussion_interpreter=FakeDiscussionInterpreter(
                [InterpreterUnavailable("provider offline")]
            ),
            owner="interpreter-discussion",
        )

        assert accepted.task_id is None
        assert await pipeline.process_one()
        async with factory() as session:
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "discussion_classify"
                )
            )
            assert item is not None
            assert item.status is DeliveryStatus.DEAD_LETTER
            assert item.last_error_category == "provider_unavailable"
            assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 0
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(TransactionalOutbox)
                    .where(
                        TransactionalOutbox.payload["role"].as_string()
                        == DISCUSSION_REPLY_DESTINATION
                    )
                )
                == 0
            )
        await engine.dispose()

    asyncio.run(scenario())


def test_concurrent_discussion_consumers_preserve_topic_order_and_refresh_memory(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        base_runtime = telegram_runtime(tmp_path)
        runtime = base_runtime.model_copy(
            update={
                "settings": base_runtime.settings.model_copy(
                    update={"project_discussion_enabled": True}
                )
            }
        )
        ingress = TelegramIngressService(runtime, factory)
        await ingress.accept_message(text_update(161))
        await ingress.accept_message(text_update(162).model_copy(update={"text": "second message"}))
        first_interpreter = BlockingDiscussionInterpreter(
            DiscussionInterpretation(
                interaction_mode="discussion",
                confidence=0.9,
                user_visible_summary="first reply",
            )
        )
        second_interpreter = FakeDiscussionInterpreter(
            [
                DiscussionInterpretation(
                    interaction_mode="discussion",
                    confidence=0.9,
                    user_visible_summary="second reply",
                )
            ]
        )
        first_pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([]),
            discussion_interpreter=first_interpreter,
            owner="discussion-first",
        )
        second_pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([]),
            discussion_interpreter=second_interpreter,
            owner="discussion-second",
        )

        first_processing = asyncio.create_task(first_pipeline.process_one())
        await first_interpreter.started.wait()
        assert await second_pipeline.process_one()
        assert second_interpreter.requests == []
        async with factory() as session:
            deferred = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "discussion_classify",
                    TransactionalOutbox.status == DeliveryStatus.PENDING,
                )
            )
            assert deferred is not None
            assert deferred.attempt_count == 0
            assert deferred.last_error_category == "discussion_context_changed"
        first_interpreter.release.set()
        assert await first_processing

        async with factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(
                    TransactionalOutbox.destination == "discussion_classify",
                    TransactionalOutbox.status == DeliveryStatus.PENDING,
                )
                .values(available_at=func.now())
            )
        assert await second_pipeline.process_one()
        assert len(second_interpreter.requests) == 1
        memory = second_interpreter.requests[0].memory_pack
        assert memory is not None
        assert [turn.content for turn in memory.turns] == [
            "inspect this project",
            "first reply",
        ]
        async with factory() as session:
            turns = (
                await session.scalars(select(ConversationTurn).order_by(ConversationTurn.ordinal))
            ).all()
            assert [turn.content for turn in turns] == [
                "inspect this project",
                "first reply",
                "second message",
                "second reply",
            ]
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


def test_project_name_regeneration_replaces_options_and_queues_new_card(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        old_options = [
            {"display_name": f"Old {index + 1}", "project_id": f"old-{index + 1}"}
            for index in range(9)
        ]
        new_options = tuple(
            ProjectNameOption(display_name=f"Fresh {index + 1}", project_id=f"fresh-{index + 1}")
            for index in range(9)
        )
        draft = TaskDraft(
            action=TaskAction.CREATE_PROJECT,
            task_type=TaskType.INFRASTRUCTURE,
            operation=TaskOperation.CREATE,
            goal="Build private notes",
            task_summary="Create a private note-taking project",
            project_name_options=new_options,
            suggested_complexity=SuggestedComplexity.SMALL,
            suggested_risk=RiskLevel.LOW,
            needs_planning=False,
            needs_clarification=False,
            normalized_title="Private notes",
        )
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="Build private notes",
                task_type="infrastructure",
            )
            assert uow.session is not None
            task_row = await uow.session.get(Task, task.id)
            assert task_row is not None
            task_row.status = TaskStatus.AWAITING_USER
            task_row.task_draft = draft.model_copy(
                update={
                    "project_name_options": tuple(
                        ProjectNameOption.model_validate(option) for option in old_options
                    )
                }
            ).model_dump(mode="json")
            inbox_id, _ = await uow.inbox.receive_once(
                source="telegram",
                consumer="bot:main",
                external_event_id="project-naming-regeneration",
                payload_hash="b" * 64,
            )
            intake = TelegramIntakeMessage(
                inbox_id=inbox_id,
                chat_id=-100,
                message_thread_id=10,
                message_id=500,
                user_id=42,
                task_id=task.id,
                original_text="Build private notes",
                affinity_kind="new_task",
                status=IntakeStatus.AWAITING_INTERPRETATION,
            )
            uow.session.add(intake)
            uow.session.add(
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=10,
                    topic_kind="inbox",
                    default_workflow="project_provisioning",
                )
            )
            naming = ProjectNamingRequest(
                task_id=task.id,
                requested_by_user_id=42,
                chat_id=-100,
                source_thread_id=10,
                description="Build private notes",
                options=old_options,
                revision=2,
                status=ProjectNamingStatus.GENERATING,
            )
            uow.session.add(naming)
            await uow.session.flush()
            uow.session.add(
                TransactionalOutbox(
                    destination="interpretation",
                    operation_type="regenerate_project_names",
                    linked_entity_type="project_naming",
                    linked_entity_id=naming.id,
                    idempotency_key=f"regenerate:{naming.id}:2",
                    payload={"revision": 2},
                )
            )
            naming_id = naming.id
        interpreter = FakeInterpreter(
            [
                InterpretationResult(
                    draft=draft,
                    profile_id="fake-interpreter",
                    model="fake-model",
                    duration_ms=2,
                )
            ]
        )
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=interpreter,
            owner="interpreter-a",
        )
        assert await pipeline.process_one()
        assert interpreter.requests[0][1] is not None
        async with factory() as session:
            persisted_naming = await session.get(ProjectNamingRequest, naming_id)
            card = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.linked_entity_type == "project_naming",
                    TransactionalOutbox.operation_type == "send_message",
                )
            )
            assert (
                persisted_naming is not None
                and persisted_naming.status is ProjectNamingStatus.PENDING
            )
            assert persisted_naming.options[0]["project_id"] == "fresh-1"
            assert card is not None and card.payload["revision"] == 2

        async with factory.begin() as session:
            persisted_naming = await session.get(
                ProjectNamingRequest, naming_id, with_for_update=True
            )
            assert persisted_naming is not None
            persisted_naming.status = ProjectNamingStatus.GENERATING
            persisted_naming.revision = 3
            session.add(
                TransactionalOutbox(
                    destination="interpretation",
                    operation_type="regenerate_project_names",
                    linked_entity_type="project_naming",
                    linked_entity_id=naming_id,
                    idempotency_key=f"regenerate:{naming_id}:3",
                    payload={"revision": 3},
                )
            )
        failing_runtime = runtime.model_copy(
            update={
                "settings": runtime.settings.model_copy(
                    update={
                        "interpretation": runtime.settings.interpretation.model_copy(
                            update={"max_attempts": 1}
                        )
                    }
                )
            }
        )
        failing_pipeline = InterpretationPipeline(
            failing_runtime,
            factory,
            interpreter=FakeInterpreter([InterpreterUnavailable("offline")]),
            owner="interpreter-b",
        )
        assert await failing_pipeline.process_one()
        async with factory() as session:
            persisted_naming = await session.get(ProjectNamingRequest, naming_id)
            fallback_card = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.linked_entity_id == naming_id,
                    TransactionalOutbox.idempotency_key.endswith(":3:fallback"),
                )
            )
            assert persisted_naming is not None
            assert persisted_naming.status is ProjectNamingStatus.PENDING
            assert persisted_naming.last_error_category == "provider_unavailable"
            assert fallback_card is not None and fallback_card.payload["revision"] == 3
        await engine.dispose()

    asyncio.run(scenario())
