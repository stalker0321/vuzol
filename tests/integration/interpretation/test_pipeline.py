import asyncio
import uuid
from pathlib import Path

import anyio
import pytest
from sqlalchemy import func, select, update

from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from vuzol.discussion.agent import DeliverDiscussionReplyHandler
from vuzol.discussion.service import WorkPackageService
from vuzol.interpretation.adapters import FakeInterpreter, FakeTranscriber
from vuzol.interpretation.discussion import (
    DISCUSSION_REPLY_DESTINATION,
    DISCUSSION_THINKING_ROLE,
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
    PlanRevision,
    ProjectNamingRequest,
    Run,
    Step,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import (
    ConversationTurnRole,
    DeliveryStatus,
    IntakeStatus,
    InteractionMode,
    ProjectNamingStatus,
    RiskLevel,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.domain import AttachmentKind, MessageUpdate, TelegramAttachment
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient, build_project_status_dashboard
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

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


def test_discussion_runtime_hands_user_turn_to_hidden_project_agent(
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
                    interaction_mode=InteractionMode.DISCUSSION,
                    confidence=0.91,
                    user_visible_summary="internal classifier summary",
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

        client = FakeTelegramClient(next_message_id=900)
        delivery = TelegramDeliveryService(
            factory,
            client,
            owner="discussion-thinking-delivery",
            lease_seconds=30,
            max_attempts=3,
            retry_min_seconds=1,
            retry_max_seconds=10,
        )
        assert await delivery.deliver_one()
        assert await delivery.deliver_one()
        assert any("Думаю" in message[2] for message in client.sent)

        assert len(discussion.requests) == 1
        assert discussion.requests[0].memory_pack is not None
        assert discussion.requests[0].memory_pack.turns == ()

        async with factory() as session:
            turns = (
                await session.scalars(select(ConversationTurn).order_by(ConversationTurn.ordinal))
            ).all()
            assert [turn.role for turn in turns] == [ConversationTurnRole.USER]
            tasks = (await session.scalars(select(Task))).all()
            assert len(tasks) == 1
            assert tasks[0].task_type == "discussion_agent_internal"
            assert tasks[0].topic_task_number is None
            dashboard = await build_project_status_dashboard(session, -100)
            assert "Сейчас нет активных задач." in dashboard.html
            steps = (
                await session.scalars(
                    select(Step)
                    .join(Run, Run.id == Step.run_id)
                    .where(Run.task_id == tasks[0].id)
                    .order_by(Step.ordinal)
                )
            ).all()
            assert [step.step_type for step in steps] == [
                "prepare_worktree",
                "execute_agent",
                "deliver_discussion_reply",
                "finalize",
            ]
            replies = (
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["role"].as_string()
                        == DISCUSSION_REPLY_DESTINATION
                    )
                )
            ).all()
            assert replies == []
            thinking_outbox = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["role"].as_string() == DISCUSSION_THINKING_ROLE
                )
            )
            thinking_link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role.like("thinking:%")
                )
            )
            assert thinking_outbox is not None
            assert thinking_link is not None and thinking_link.message_id == 901
            thinking_message_id = thinking_link.message_id

        execute_step = next(step for step in steps if step.step_type == "execute_agent")
        deliver_step = next(step for step in steps if step.step_type == "deliver_discussion_reply")
        async with factory.begin() as session:
            stored_execute = await session.get(Step, execute_step.id, with_for_update=True)
            assert stored_execute is not None
            stored_execute.status = StepStatus.COMPLETED
            stored_execute.result = {
                "profile_id": "grok-subscription-a",
                "structured_output": {"reply": "Предлагаю выбирать нужное количество."},
            }
        token = LeaseToken(
            step=StepRecord(
                id=deliver_step.id,
                run_id=deliver_step.run_id,
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
                task_id=tasks[0].id,
                run_id=deliver_step.run_id,
                step_id=deliver_step.id,
                step_type="deliver_discussion_reply",
                payload={},
                timeout_seconds=60,
                lease=token,
            ),
            CancellationContext(),
        )
        assert outcome.result["assistant_turn_id"]
        assert await delivery.deliver_one()
        assert client.edited[-1][1] == thinking_message_id
        assert "Предлагаю выбирать нужное количество" in client.edited[-1][2]
        assert await delivery.deliver_one()
        assert client.edited[-1][1] == 900
        assert "Готов к следующему сообщению" in client.edited[-1][2]
        cancelled = CancellationContext()
        cancelled.request()
        cancelled_outcome = await DeliverDiscussionReplyHandler(factory).execute(
            StepExecutionRequest(
                task_id=tasks[0].id,
                run_id=deliver_step.run_id,
                step_id=deliver_step.id,
                step_type="deliver_discussion_reply",
                payload={},
                timeout_seconds=60,
                lease=token,
            ),
            cancelled,
        )
        assert cancelled_outcome.category == "cancelled"
        async with factory() as session:
            assistant = await session.scalar(
                select(ConversationTurn).where(
                    ConversationTurn.role == ConversationTurnRole.ASSISTANT
                )
            )
            assert assistant is not None
            assert assistant.content == "Предлагаю выбирать нужное количество."

        async with factory.begin() as session:
            stored_execute = await session.get(Step, execute_step.id, with_for_update=True)
            assert stored_execute is not None
            stored_execute.result = {"structured_output": None}
        invalid_outcome = await DeliverDiscussionReplyHandler(factory).execute(
            StepExecutionRequest(
                task_id=tasks[0].id,
                run_id=deliver_step.run_id,
                step_id=deliver_step.id,
                step_type="deliver_discussion_reply",
                payload={},
                timeout_seconds=60,
                lease=token,
            ),
            CancellationContext(),
        )
        assert invalid_outcome.category == "discussion_reply_invalid"

        async with factory.begin() as session:
            stored_execute = await session.get(Step, execute_step.id, with_for_update=True)
            stored_task = await session.get(Task, tasks[0].id, with_for_update=True)
            assert stored_execute is not None and stored_task is not None
            stored_execute.result = {"structured_output": {"reply": "valid"}}
            stored_task.task_draft = {
                key: value
                for key, value in stored_task.task_draft.items()
                if key != "source_turn_id"
            }
        invalid_metadata = await DeliverDiscussionReplyHandler(factory).execute(
            StepExecutionRequest(
                task_id=tasks[0].id,
                run_id=deliver_step.run_id,
                step_id=deliver_step.id,
                step_type="deliver_discussion_reply",
                payload={},
                timeout_seconds=60,
                lease=token,
            ),
            CancellationContext(),
        )
        assert invalid_metadata.category == "discussion_reply_invalid"

        missing_task = await DeliverDiscussionReplyHandler(factory).execute(
            StepExecutionRequest(
                task_id=uuid.uuid4(),
                run_id=deliver_step.run_id,
                step_id=deliver_step.id,
                step_type="deliver_discussion_reply",
                payload={},
                timeout_seconds=60,
                lease=token,
            ),
            CancellationContext(),
        )
        assert missing_task.category == "discussion_task_invalid"
        await engine.dispose()

    asyncio.run(scenario())


def test_plan_request_materializes_package_and_queues_card_without_classifier_summary(
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
        result = DiscussionInterpretation(
            interaction_mode=InteractionMode.PLAN_REQUEST,
            confidence=0.95,
            # The classifier may omit this redundant hint. The validated structured
            # plan request itself authorizes creation of a non-executing draft.
            should_mutate_plan=False,
            user_visible_summary="internal classifier summary",
            plan_request={  # type: ignore[arg-type]
                "intent": "create_draft",
                "title": "Bill Buddy MVP",
                "items": [
                    {
                        "local_id": "prototype",
                        "summary": "Build interactive prototype",
                        "goal": "Implement the agreed bill-splitting UX",
                        "expected_outcome": "A usable static prototype",
                        "completion_criteria": ["Trusted checks pass"],
                        "allowed_scope": "index.html, app.js, styles.css",
                        "trusted_checks": ["make test"],
                        "suggested_risk": "low",
                        "needs_approval": True,
                        "estimated_complexity": "medium",
                    }
                ],
            },
        )
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([]),
            discussion_interpreter=FakeDiscussionInterpreter([result]),
            owner="interpreter-plan",
        )
        ingress = TelegramIngressService(runtime, factory)
        accepted = await ingress.accept_message(
            text_update(152).model_copy(
                update={"text": "Оформи согласованный план как пакет задач"}
            )
        )

        assert accepted.task_id is None
        assert await pipeline.process_one()
        async with factory() as session:
            package = await session.scalar(select(WorkPackage))
            projection = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "work_package_projection",
                    TransactionalOutbox.operation_type == "render_plan",
                )
            )
            leaked_summary = await session.scalar(
                select(ConversationTurn).where(
                    ConversationTurn.content == "internal classifier summary"
                )
            )
            assert package is not None and package.title == "Bill Buddy MVP"
            assert projection is not None
            assert projection.linked_entity_id == package.id
            assert leaked_summary is None
        await engine.dispose()

    asyncio.run(scenario())


def test_repeated_terminal_plan_is_regenerated_without_memory(
    postgres_dsn: str, tmp_path: Path
) -> None:
    def plan_result(title: str, summary: str, goal: str) -> DiscussionInterpretation:
        return DiscussionInterpretation(
            interaction_mode=InteractionMode.PLAN_REQUEST,
            confidence=0.95,
            user_visible_summary="plan",
            plan_request={  # type: ignore[arg-type]
                "intent": "create_draft",
                "title": title,
                "items": [
                    {
                        "local_id": "item-1",
                        "summary": summary,
                        "goal": goal,
                        "expected_outcome": "Expected result",
                        "completion_criteria": ["Trusted checks pass"],
                        "allowed_scope": "src/**",
                        "suggested_risk": "low",
                        "needs_approval": True,
                        "estimated_complexity": "small",
                    }
                ],
            },
        )

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
        repeated = plan_result("Old plan", "Build old UI", "Original wording")
        paraphrased = plan_result(" old   PLAN ", " BUILD OLD UI ", "Paraphrased wording")
        fresh = plan_result("New design plan", "Redesign the interface", "Address current request")
        discussion = FakeDiscussionInterpreter([repeated, paraphrased, fresh])
        pipeline = InterpretationPipeline(
            runtime,
            factory,
            interpreter=FakeInterpreter([]),
            discussion_interpreter=discussion,
            owner="interpreter-plan-freshness",
        )
        ingress = TelegramIngressService(runtime, factory)

        await ingress.accept_message(
            text_update(153).model_copy(update={"text": "Create the original plan"})
        )
        assert await pipeline.process_one()
        async with factory() as session:
            package = await session.scalar(select(WorkPackage))
            assert package is not None and package.head_revision_id is not None
            revision = await session.get(PlanRevision, package.head_revision_id)
            assert revision is not None
            package_id = package.id
            version = package.version
            revision_number = revision.revision_number
            h8 = revision.content_hash[:8]
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).discard(
                package_id=package_id,
                revision_number=revision_number,
                h8=h8,
                expected_status_generation=version,
                user_id=42,
            )

        await ingress.accept_message(
            text_update(154).model_copy(update={"text": "Make a completely different design"})
        )
        assert await pipeline.process_one()

        assert len(discussion.requests) == 3
        assert discussion.requests[1].memory_pack is not None
        assert discussion.requests[2].memory_pack is None
        async with factory() as session:
            packages = (await session.scalars(select(WorkPackage))).all()
            assert len(packages) == 2
            assert packages[-1].title == "New design plan"
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
                interaction_mode=InteractionMode.DISCUSSION,
                confidence=0.9,
                user_visible_summary="first reply",
            )
        )
        second_interpreter = FakeDiscussionInterpreter(
            [
                DiscussionInterpretation(
                    interaction_mode=InteractionMode.DISCUSSION,
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
        assert second_interpreter.requests == []
        async with factory() as session:
            turns = (
                await session.scalars(select(ConversationTurn).order_by(ConversationTurn.ordinal))
            ).all()
            assert [turn.content for turn in turns] == ["inspect this project"]
            deferred = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "discussion_classify",
                    TransactionalOutbox.status == DeliveryStatus.PENDING,
                )
            )
            assert deferred is not None
            assert deferred.last_error_category == "discussion_context_changed"
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
