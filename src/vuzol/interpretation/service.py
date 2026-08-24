"""Durable fenced orchestration for attachment transcription and semantic interpretation."""

import hashlib
import os
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, RuntimeConfiguration, TopicKind
from vuzol.discussion.agent import DISCUSSION_INTERNAL_TASK_TYPE, schedule_discussion_agent
from vuzol.discussion.application import apply_plan_request_in_uow
from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.service import WorkPackageService
from vuzol.interpretation.discussion import (
    DISCUSSION_CLASSIFY_DESTINATION,
    DISCUSSION_REPLY_DESTINATION,
    DISCUSSION_THINKING_ROLE,
    ControlOverride,
    ControlOverrideKind,
    DiscussionInterpretation,
    DiscussionInterpretRequest,
    EditSessionContext,
    PlanRequestIntent,
    PlanSnapshot,
    PlanSnapshotItem,
    SemanticDiscussionInterpreter,
    enforce_discussion_policy,
    plan_draft_from_interpretation,
)
from vuzol.interpretation.domain import (
    InterpretationInput,
    InterpretationResult,
    ProjectSummary,
    TaskAction,
    TaskContext,
    TranscriptionInput,
)
from vuzol.interpretation.policy import enforce_interpretation_policy
from vuzol.interpretation.ports import (
    AttachmentDownloader,
    InterpreterUnavailable,
    InvalidInterpreterOutput,
    SemanticInterpreter,
    Transcriber,
    TranscriptionUnavailable,
)
from vuzol.observability import get_logger
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    defer_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import (
    Artifact,
    ClarificationDecision,
    ConversationTurn,
    EditSession,
    Interpretation,
    PlanRevision,
    PlanRevisionItem,
    ProjectNamingRequest,
    Task,
    TelegramIntakeMessage,
    TopicMapping,
    TransactionalOutbox,
    WorkItemDraft,
    WorkPackage,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.repositories import TelegramIntakeRepository
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    ConversationTurnRole,
    ConversationTurnSource,
    DeliveryStatus,
    EditSessionStatus,
    IntakeStatus,
    InteractionMode,
    ProjectNamingStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.tracing import enqueue_interpreter_trace

INTERPRETATION_DESTINATIONS = frozenset(
    {"telegram_file", "interpretation", DISCUSSION_CLASSIFY_DESTINATION}
)


class PermanentPipelineError(RuntimeError):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


class DiscussionContextChanged(RuntimeError):
    """The model answered against memory superseded by another committed turn."""


async def interpret_with_recovery(
    primary: SemanticInterpreter,
    fallbacks: Sequence[SemanticInterpreter],
    request: InterpretationInput,
) -> InterpretationResult:
    try:
        return await primary.interpret(request)
    except InvalidInterpreterOutput as first_error:
        try:
            return await primary.interpret(request, repair_error=str(first_error)[:1_000])
        except (InvalidInterpreterOutput, InterpreterUnavailable):
            pass
    except InterpreterUnavailable:
        pass
    for fallback in fallbacks:
        try:
            return await fallback.interpret(request)
        except (InvalidInterpreterOutput, InterpreterUnavailable):
            continue
    raise InterpreterUnavailable("all_interpreters_unavailable")


async def interpret_discussion_with_recovery(
    primary: SemanticDiscussionInterpreter,
    fallbacks: Sequence[SemanticDiscussionInterpreter],
    request: DiscussionInterpretRequest,
) -> DiscussionInterpretation:
    for interpreter in (primary, *fallbacks):
        try:
            candidate = await interpreter.interpret_discussion(request)
            return enforce_discussion_policy(request, candidate)
        except (InvalidInterpreterOutput, InterpreterUnavailable):
            continue
    raise InterpreterUnavailable("all_discussion_interpreters_unavailable")


async def regenerate_project_names(
    primary: SemanticInterpreter,
    fallbacks: Sequence[SemanticInterpreter],
    request: InterpretationInput,
    *,
    previous_project_ids: frozenset[str],
) -> InterpretationResult:
    instruction = (
        "Generate exactly nine entirely new project_name_options for the same idea. "
        "Do not reuse these project_id values: " + ", ".join(sorted(previous_project_ids))
    )
    for interpreter in (primary, *fallbacks):
        try:
            result = await interpreter.interpret(request, repair_error=instruction[:1_000])
        except (InvalidInterpreterOutput, InterpreterUnavailable):
            continue
        generated_ids = {option.project_id for option in result.draft.project_name_options}
        if (
            result.draft.action is TaskAction.CREATE_PROJECT
            and len(generated_ids) == 9
            and generated_ids.isdisjoint(previous_project_ids)
        ):
            return result
    raise InterpreterUnavailable("all_interpreters_unavailable")


class InterpretationPipeline:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        interpreter: SemanticInterpreter,
        fallback_interpreters: Sequence[SemanticInterpreter] = (),
        discussion_interpreter: SemanticDiscussionInterpreter | None = None,
        fallback_discussion_interpreters: Sequence[SemanticDiscussionInterpreter] = (),
        downloader: AttachmentDownloader | None = None,
        transcriber: Transcriber | None = None,
        owner: str,
    ) -> None:
        self._runtime = runtime
        self._factory = session_factory
        self._interpreter = interpreter
        self._fallbacks = tuple(fallback_interpreters)
        self._discussion_interpreter = discussion_interpreter
        self._discussion_fallbacks = tuple(fallback_discussion_interpreters)
        self._downloader = downloader
        self._transcriber = transcriber
        self._owner = owner
        self._logger = get_logger(__name__)

    async def process_one(self) -> bool:
        settings = self._runtime.settings.interpretation
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=settings.lease_seconds,
                allowed_destinations=INTERPRETATION_DESTINATIONS,
            )
        if token is None:
            return False
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            if item is None:
                return True
            attempt_count = item.attempt_count
            destination = item.destination
            operation_type = item.operation_type
        try:
            if destination == "telegram_file":
                await self._process_attachment(token)
            elif destination == "interpretation" and operation_type == "interpret_intake":
                await self._process_interpretation(token)
            elif destination == "interpretation" and operation_type == "regenerate_project_names":
                await self._process_project_name_regeneration(token)
            elif (
                destination == DISCUSSION_CLASSIFY_DESTINATION
                and operation_type == "classify_intake"
            ):
                await self._process_discussion(token)
            else:
                raise PermanentPipelineError("unsupported_pipeline_destination")
        except (InterpreterUnavailable, TranscriptionUnavailable):
            await self._retry_or_dead_letter(token, attempt_count, "provider_unavailable")
        except DiscussionContextChanged:
            await self._defer_discussion(token)
        except OSError:
            await self._retry_or_dead_letter(token, attempt_count, "artifact_storage_unavailable")
        except PermanentPipelineError as error:
            await self._dead_letter(token, error.category)
        return True

    async def _process_discussion(self, token: OutboxLeaseToken) -> None:
        if not self._runtime.settings.project_discussion_enabled:
            raise PermanentPipelineError("project_discussion_disabled")
        if self._discussion_interpreter is None:
            raise InterpreterUnavailable("discussion_interpreter_unavailable")
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
            if (
                intake is None
                or intake.task_id is not None
                or intake.affinity_kind != "discussion"
                or not intake.original_text
            ):
                raise PermanentPipelineError("discussion_intake_invalid")
            session_id = _required_uuid(item.payload, "discussion_session_id")
            project_id = _required_string(item.payload, "project_id")
            if (
                project_id
                not in {project.id for project in self._runtime.registries.projects.items()}
                or _required_int(item.payload, "chat_id") != intake.chat_id
                or _required_int(item.payload, "message_thread_id") != intake.message_thread_id
                or _required_int(item.payload, "user_id") != intake.user_id
            ):
                raise PermanentPipelineError("discussion_context_mismatch")
            older_unfinished = await session.scalar(
                select(TransactionalOutbox.id)
                .join(
                    TelegramIntakeMessage,
                    TelegramIntakeMessage.id == TransactionalOutbox.linked_entity_id,
                )
                .where(
                    TransactionalOutbox.destination == DISCUSSION_CLASSIFY_DESTINATION,
                    TransactionalOutbox.id != item.id,
                    TransactionalOutbox.status.in_(
                        [
                            DeliveryStatus.PENDING,
                            DeliveryStatus.LEASED,
                            DeliveryStatus.AMBIGUOUS,
                        ]
                    ),
                    TelegramIntakeMessage.chat_id == intake.chat_id,
                    TelegramIntakeMessage.message_thread_id == intake.message_thread_id,
                    TelegramIntakeMessage.message_id < intake.message_id,
                )
                .limit(1)
            )
            if older_unfinished is not None:
                raise DiscussionContextChanged
            active_agent = await session.scalar(
                select(Task.id)
                .where(
                    Task.task_type == DISCUSSION_INTERNAL_TASK_TYPE,
                    Task.task_draft["discussion_session_id"].as_string() == str(session_id),
                    Task.status.not_in(USER_TERMINAL_TASK_STATUSES),
                )
                .limit(1)
            )
            if active_agent is not None:
                raise DiscussionContextChanged
        async with UnitOfWork(self._factory) as uow:
            try:
                discussion = await uow.discussions.get_session(session_id)
            except LookupError as error:
                raise PermanentPipelineError("discussion_session_missing") from error
            if (
                discussion.project_id != project_id
                or discussion.chat_id != intake.chat_id
                or discussion.message_thread_id != intake.message_thread_id
            ):
                raise PermanentPipelineError("discussion_session_mismatch")
            expected_session_version = discussion.version
            memory_pack = await DiscussionMemoryService(uow).load_context(session_id=session_id)
            plan_snapshot, edit_context = await _load_discussion_plan_context(
                uow.session,
                discussion.active_work_package_id,
                user_id=intake.user_id,
            )
        request = DiscussionInterpretRequest(
            original_input=intake.original_text,
            project_id=project_id,
            user_id=intake.user_id,
            memory_pack=memory_pack,
            plan_snapshot=plan_snapshot,
            edit_session=edit_context,
            control_override=(
                None
                if item.payload.get("control_override") is None
                else ControlOverride(kind=ControlOverrideKind(item.payload["control_override"]))
            ),
        )
        result = await interpret_discussion_with_recovery(
            self._discussion_interpreter,
            self._discussion_fallbacks,
            request,
        )
        if (
            result.interaction_mode is InteractionMode.PLAN_REQUEST
            and result.plan_request is not None
            and result.plan_request.intent is PlanRequestIntent.CREATE_DRAFT
        ):
            candidate_plan = plan_draft_from_interpretation(result)
            async with UnitOfWork(self._factory) as uow:
                repeated = await WorkPackageService(uow).is_duplicate_terminal_plan(
                    session_id=session_id,
                    project_id=project_id,
                    plan=candidate_plan,
                )
            if repeated:
                request = request.model_copy(update={"memory_pack": None})
                result = await interpret_discussion_with_recovery(
                    self._discussion_interpreter,
                    self._discussion_fallbacks,
                    request,
                )
        self._logger.info(
            "Discussion interpretation completed",
            extra={
                "event": "discussion.interpretation.completed",
                "discussion_session_id": str(session_id),
                "intake_id": str(intake.id),
                "project_id": project_id,
                "interaction_mode": result.interaction_mode.value,
                "confidence": result.confidence,
                "prompt_version": result.prompt_version,
                "schema_version": result.schema_version,
                "should_create_task": result.should_create_task,
                "should_mutate_plan": result.should_mutate_plan,
                "refusal_code": (
                    result.refusal_code.value if result.refusal_code is not None else None
                ),
            },
        )
        async with UnitOfWork(self._factory) as uow:
            current = await uow.discussions.get_session(session_id, for_update=True)
            if current.version != expected_session_version:
                raise DiscussionContextChanged
            memory = DiscussionMemoryService(uow)
            assert uow.session is not None
            existing_user_turn = await uow.session.scalar(
                select(ConversationTurn).where(ConversationTurn.intake_message_id == intake.id)
            )
            if existing_user_turn is None:
                user_turn_id, _ = await memory.append_turn(
                    session_id=session_id,
                    role=ConversationTurnRole.USER,
                    source=ConversationTurnSource.TELEGRAM_USER,
                    content=intake.original_text,
                    classifier_mode=result.interaction_mode,
                    classifier_confidence=Decimal(str(result.confidence)),
                    classifier_prompt_version=result.prompt_version,
                    should_create_task=result.should_create_task,
                    intake_message_id=intake.id,
                )
            elif (
                existing_user_turn.session_id != session_id
                or existing_user_turn.role is not ConversationTurnRole.USER
                or existing_user_turn.content != intake.original_text.strip()
            ):
                raise PermanentPipelineError("discussion_turn_idempotency_conflict")
            else:
                user_turn_id = existing_user_turn.id
            if result.interaction_mode in {
                InteractionMode.DISCUSSION,
                InteractionMode.QUERY_ONLY,
            }:
                assert uow.session is not None
                await uow.outbox.enqueue(
                    destination="telegram",
                    operation_type="send_message",
                    entity_type="conversation_turn",
                    entity_id=user_turn_id,
                    idempotency_key=f"{DISCUSSION_THINKING_ROLE}:{user_turn_id}",
                    payload={
                        "role": DISCUSSION_THINKING_ROLE,
                        "session_id": str(session_id),
                    },
                )
                fresh_memory = await memory.load_context(session_id=session_id)
                await schedule_discussion_agent(
                    uow.session,
                    runtime=self._runtime,
                    session_id=session_id,
                    source_turn_id=user_turn_id,
                    project_id=project_id,
                    chat_id=intake.chat_id,
                    thread_id=intake.message_thread_id,
                    user_id=intake.user_id,
                    interaction_mode=result.interaction_mode,
                    classifier_confidence=result.confidence,
                    memory_pack=fresh_memory,
                )
                await TelegramIntakeRepository(uow.session).set_status(
                    intake.id, IntakeStatus.COMPLETED
                )
                await complete_outbox_item(uow.session, token)
                return
            if (
                result.interaction_mode is InteractionMode.PLAN_REQUEST
                and result.should_mutate_plan
            ):
                try:
                    await apply_plan_request_in_uow(
                        uow,
                        session_id=session_id,
                        request=request,
                        result=result,
                        planner_profile=getattr(self._discussion_interpreter, "profile_id", None),
                    )
                except DomainError as error:
                    raise PermanentPipelineError(f"discussion_plan_{error}") from error
                assert uow.session is not None
                await TelegramIntakeRepository(uow.session).set_status(
                    intake.id, IntakeStatus.COMPLETED
                )
                await complete_outbox_item(uow.session, token)
                return
            assistant_turn_id, _ = await memory.append_turn(
                session_id=session_id,
                role=ConversationTurnRole.ASSISTANT,
                source=ConversationTurnSource.MODEL,
                content=_discussion_reply_text(result),
                classifier_mode=result.interaction_mode,
                classifier_confidence=Decimal(str(result.confidence)),
                classifier_prompt_version=result.prompt_version,
            )
            await uow.outbox.enqueue(
                destination="telegram",
                operation_type="send_message",
                entity_type="conversation_turn",
                entity_id=assistant_turn_id,
                idempotency_key=f"{DISCUSSION_REPLY_DESTINATION}:{assistant_turn_id}",
                payload={
                    "role": DISCUSSION_REPLY_DESTINATION,
                    "session_id": str(session_id),
                    "source_turn_id": str(user_turn_id),
                    "mode": result.interaction_mode.value,
                    "confidence": result.confidence,
                    "refusal_code": (
                        result.refusal_code.value if result.refusal_code is not None else None
                    ),
                },
            )
            assert uow.session is not None
            await TelegramIntakeRepository(uow.session).set_status(
                intake.id, IntakeStatus.COMPLETED
            )
            await complete_outbox_item(uow.session, token)

    async def _process_attachment(self, token: OutboxLeaseToken) -> None:
        if self._downloader is None:
            raise TranscriptionUnavailable("attachment_downloader_unavailable")
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
            if intake is None:
                raise PermanentPipelineError("telegram_intake_missing")
            file_id = _required_string(item.payload, "file_id")
            media_type = _required_string(item.payload, "media_type")
            kind = _required_string(item.payload, "kind")
            filename_value = item.payload.get("filename")
            filename = str(filename_value) if filename_value is not None else None
            declared_size = int(item.payload.get("declared_size", 0))
            task_id = intake.task_id
        try:
            content = await self._downloader.download(file_id)
        except Exception as error:
            raise TranscriptionUnavailable("attachment_download_failed") from error
        if len(content) > self._runtime.settings.telegram.max_attachment_bytes:
            raise PermanentPipelineError("downloaded_attachment_too_large")
        if declared_size and len(content) > declared_size + 1_024:
            raise PermanentPipelineError("attachment_size_mismatch")
        reference = self._persist_attachment(token.item_id, content)
        transcript: str | None = None
        uncertain = False
        if kind in {"voice", "audio"}:
            if self._transcriber is None:
                raise TranscriptionUnavailable("transcriber_unavailable")
            result = await self._transcriber.transcribe(
                TranscriptionInput(
                    content=content,
                    media_type=media_type,
                    filename=filename,
                    language_hint=self._runtime.settings.interpretation.language_hint,
                )
            )
            transcript = result.transcript
            uncertain = result.uncertain
        async with self._factory.begin() as session:
            if task_id is not None:
                task = await session.get(Task, task_id, with_for_update=True)
                if task is None:
                    raise PermanentPipelineError("task_missing")
                digest = hashlib.sha256(content).hexdigest()
                session.add(
                    Artifact(
                        task_id=task.id,
                        artifact_type="telegram_attachment",
                        content_uri=reference,
                        size_bytes=len(content),
                        content_hash=digest,
                        media_type=media_type,
                        sensitivity="private",
                        visibility="task",
                        retention_until=datetime.now(UTC)
                        + timedelta(days=self._runtime.settings.retention.voice_days),
                        metadata_json={"filename": filename, "kind": kind},
                    )
                )
                if kind in {"voice", "audio"}:
                    task.voice_reference = reference
                if transcript is not None:
                    task.transcript = transcript
                await _enqueue_interpretation(
                    session,
                    intake_id=intake.id,
                    transcription_uncertain=uncertain,
                )
            await complete_outbox_item(session, token)

    def _persist_attachment(self, item_id: uuid.UUID, content: bytes) -> str:
        digest = hashlib.sha256(content).hexdigest()
        root = self._runtime.settings.artifact_root / "telegram-intake"
        root.mkdir(parents=True, exist_ok=True)
        target = root / f"{item_id}-{digest}.bin"
        temporary = target.with_suffix(".tmp")
        temporary.write_bytes(content)
        os.replace(temporary, target)
        return str(target)

    async def _process_interpretation(self, token: OutboxLeaseToken) -> None:
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
            if intake is None or intake.task_id is None:
                raise PermanentPipelineError("interpretable_task_missing")
            request = await self._build_input(session, intake, item)
            task_id = intake.task_id
        result = await interpret_with_recovery(self._interpreter, self._fallbacks, request)
        policy = enforce_interpretation_policy(
            request,
            result.draft,
            known_project_ids=frozenset(
                project.id for project in self._runtime.registries.projects.items()
            ),
        )
        async with self._factory.begin() as session:
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None:
                raise PermanentPipelineError("task_missing")
            interpretation = Interpretation(
                task_id=task.id,
                original_input_hash=hashlib.sha256(request.original_input.encode()).hexdigest(),
                transcript=request.transcript,
                task_draft=policy.draft.model_dump(mode="json"),
                profile_id=result.profile_id,
                model=result.model,
                prompt_version=result.prompt_version,
                schema_version=result.schema_version,
            )
            session.add(interpretation)
            await session.flush()
            if task.status is TaskStatus.AWAITING_USER:
                previous = await session.scalar(
                    select(Interpretation)
                    .where(
                        Interpretation.task_id == task.id,
                        Interpretation.id != interpretation.id,
                    )
                    .order_by(Interpretation.created_at.desc())
                    .limit(1)
                )
                if previous is not None:
                    question = previous.task_draft.get("clarification_question")
                    if isinstance(question, str) and question:
                        session.add(
                            ClarificationDecision(
                                task_id=task.id,
                                interpretation_id=previous.id,
                                question=question,
                                answer=request.original_input,
                                deciding_user_id=intake.user_id,
                            )
                        )
            task.task_draft = policy.draft.model_dump(mode="json")
            task.task_type = policy.draft.task_type.value
            task.project_id = policy.draft.project_id
            task.interpreter_profile = result.profile_id
            task.prompt_version = result.prompt_version
            task.draft_schema_version = result.schema_version
            task.status = (
                TaskStatus.AWAITING_USER
                if policy.draft.needs_clarification
                else TaskStatus.INTERPRETED
            )
            task.version += 1
            enqueue_interpreter_trace(
                session,
                task=task,
                interpretation=interpretation,
                result=result,
            )
            if policy.draft.needs_clarification:
                await _enqueue_semantic_clarification(
                    session,
                    intake=intake,
                    interpretation_id=interpretation.id,
                )
            else:
                session.add(
                    TransactionalOutbox(
                        destination="workflow_dispatch",
                        operation_type="dispatch_interpretation",
                        linked_entity_type="interpretation",
                        linked_entity_id=interpretation.id,
                        idempotency_key=f"workflow:dispatch:{interpretation.id}",
                        payload={"task_id": str(task.id)},
                    )
                )
            await TelegramIntakeRepository(session).set_status(
                intake.id,
                IntakeStatus.NEEDS_CLARIFICATION
                if policy.draft.needs_clarification
                else IntakeStatus.COMPLETED,
            )
            await complete_outbox_item(session, token)

    async def _process_project_name_regeneration(self, token: OutboxLeaseToken) -> None:
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            naming = await session.get(ProjectNamingRequest, item.linked_entity_id)
            if naming is None:
                raise PermanentPipelineError("project_naming_missing")
            revision = int(item.payload.get("revision", 0))
            if naming.status is not ProjectNamingStatus.GENERATING or naming.revision != revision:
                async with self._factory.begin() as complete_session:
                    await complete_outbox_item(complete_session, token)
                return
            intake = await session.scalar(
                select(TelegramIntakeMessage)
                .where(TelegramIntakeMessage.task_id == naming.task_id)
                .order_by(TelegramIntakeMessage.created_at.desc())
                .limit(1)
            )
            if intake is None:
                raise PermanentPipelineError("project_naming_intake_missing")
            request = await self._build_input(session, intake, item)
            previous_project_ids = frozenset(
                str(option["project_id"])
                for option in naming.options
                if isinstance(option, dict) and option.get("project_id")
            )
        result = await regenerate_project_names(
            self._interpreter,
            self._fallbacks,
            request,
            previous_project_ids=previous_project_ids,
        )
        policy = enforce_interpretation_policy(
            request,
            result.draft,
            known_project_ids=frozenset(
                project.id for project in self._runtime.registries.projects.items()
            ),
        )
        if policy.draft.needs_clarification or len(policy.draft.project_name_options) != 9:
            raise InterpreterUnavailable("invalid_project_name_regeneration")
        async with self._factory.begin() as session:
            naming = await session.get(
                ProjectNamingRequest,
                item.linked_entity_id,
                with_for_update=True,
            )
            if naming is None:
                raise PermanentPipelineError("project_naming_missing")
            if naming.status is not ProjectNamingStatus.GENERATING or naming.revision != revision:
                await complete_outbox_item(session, token)
                return
            task = await session.get(Task, naming.task_id, with_for_update=True)
            if task is None:
                raise PermanentPipelineError("task_missing")
            interpretation = Interpretation(
                task_id=task.id,
                original_input_hash=hashlib.sha256(request.original_input.encode()).hexdigest(),
                transcript=request.transcript,
                task_draft=policy.draft.model_dump(mode="json"),
                profile_id=result.profile_id,
                model=result.model,
                prompt_version=result.prompt_version,
                schema_version=result.schema_version,
            )
            session.add(interpretation)
            naming.options = [
                option.model_dump(mode="json") for option in policy.draft.project_name_options
            ]
            naming.status = ProjectNamingStatus.PENDING
            naming.last_error_category = None
            task.task_draft = policy.draft.model_dump(mode="json")
            task.interpreter_profile = result.profile_id
            task.prompt_version = result.prompt_version
            task.draft_schema_version = result.schema_version
            task.version += 1
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="project_naming",
                    linked_entity_id=naming.id,
                    idempotency_key=f"telegram:project-naming:{naming.id}:{naming.revision}",
                    payload={"role": "project_name_options", "revision": naming.revision},
                )
            )
            await complete_outbox_item(session, token)

    async def _build_input(
        self,
        session: AsyncSession,
        intake: TelegramIntakeMessage,
        item: TransactionalOutbox,
    ) -> InterpretationInput:
        task = await session.get(Task, intake.task_id)
        assert task is not None
        topic = await session.scalar(
            select(TopicMapping).where(
                TopicMapping.chat_id == intake.chat_id,
                TopicMapping.message_thread_id == intake.message_thread_id,
            )
        )
        if topic is None:
            raise PermanentPipelineError("topic_mapping_missing")
        active = (
            await session.scalars(
                select(Task).where(
                    Task.source_chat_id == intake.chat_id,
                    Task.source_thread_id == intake.message_thread_id,
                    Task.id != task.id,
                    Task.status.not_in(
                        [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]
                    ),
                )
            )
        ).all()
        summaries = tuple(self._project_summaries())
        original = intake.original_text or task.transcript or "[attachment request]"
        return InterpretationInput(
            original_input=original,
            transcript=task.transcript,
            topic_kind=TopicKind(topic.topic_kind),
            mapped_project_id=topic.project_id,
            reply_linked_task=(
                TaskContext(task_id=task.id, title=task.original_text[:120])
                if intake.affinity_kind == "reply"
                else None
            ),
            active_tasks=tuple(
                TaskContext(task_id=value.id, title=value.original_text[:120]) for value in active
            ),
            project_summaries=summaries,
            capability_vocabulary=frozenset(Capability),
            source_is_voice=task.transcript is not None,
            transcription_uncertain=bool(item.payload.get("transcription_uncertain", False)),
        )

    def _project_summaries(self) -> Sequence[ProjectSummary]:
        summaries: list[ProjectSummary] = []
        for project in self._runtime.registries.projects.items():
            text = project.display_name
            if project.summary_path is not None:
                try:
                    text = project.summary_path.read_text()[:2_000]
                except OSError:
                    text = project.display_name
            summaries.append(ProjectSummary(project_id=project.id, summary=text))
        return summaries

    async def _retry_or_dead_letter(
        self, token: OutboxLeaseToken, attempt_count: int, category: str
    ) -> None:
        settings = self._runtime.settings.interpretation
        if attempt_count >= settings.max_attempts:
            await self._dead_letter(token, category)
            return
        delay = min(
            settings.retry_max_seconds, settings.retry_min_seconds * 2 ** (attempt_count - 1)
        )
        async with self._factory.begin() as session:
            await retry_outbox_item(session, token, delay_seconds=delay, error_category=category)

    async def _defer_discussion(self, token: OutboxLeaseToken) -> None:
        async with self._factory.begin() as session:
            await defer_outbox_item(
                session,
                token,
                delay_seconds=self._runtime.settings.interpretation.retry_min_seconds,
                reason="discussion_context_changed",
            )

    async def _dead_letter(self, token: OutboxLeaseToken, category: str) -> None:
        async with self._factory.begin() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            if item is not None and item.linked_entity_type == "project_naming":
                naming = await session.get(
                    ProjectNamingRequest,
                    item.linked_entity_id,
                    with_for_update=True,
                )
                if naming is not None:
                    naming.status = ProjectNamingStatus.PENDING
                    naming.last_error_category = category[:100]
                    session.add(
                        TransactionalOutbox(
                            destination="telegram",
                            operation_type="send_message",
                            linked_entity_type="project_naming",
                            linked_entity_id=naming.id,
                            idempotency_key=(
                                f"telegram:project-naming:{naming.id}:{naming.revision}:fallback"
                            ),
                            payload={
                                "role": "project_name_options",
                                "revision": naming.revision,
                            },
                        )
                    )
            await dead_letter_outbox_item(session, token, error_category=category)


def _required_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise PermanentPipelineError(f"invalid_{key}")
    return value


async def _load_discussion_plan_context(
    session: AsyncSession | None,
    package_id: uuid.UUID | None,
    *,
    user_id: int,
) -> tuple[PlanSnapshot | None, EditSessionContext | None]:
    if session is None or package_id is None:
        return None, None
    package = await session.get(WorkPackage, package_id)
    if package is None or package.head_revision_id is None:
        return None, None
    revision = await session.get(PlanRevision, package.head_revision_id)
    if revision is None or revision.work_package_id != package.id:
        raise PermanentPipelineError("discussion_plan_context_missing")
    rows = (
        await session.execute(
            select(PlanRevisionItem, WorkItemDraft.local_id)
            .join(WorkItemDraft, WorkItemDraft.id == PlanRevisionItem.item_id)
            .where(PlanRevisionItem.plan_revision_id == revision.id)
            .order_by(PlanRevisionItem.ordinal)
        )
    ).all()
    snapshot = PlanSnapshot(
        package_id=package.id,
        revision_id=revision.id,
        revision_number=revision.revision_number,
        revision_hash=revision.content_hash,
        status_generation=package.version,
        title=package.title,
        items=tuple(
            PlanSnapshotItem(
                item_id=item.item_id,
                local_id=local_id,
                ordinal=item.ordinal,
                summary=item.summary,
            )
            for item, local_id in rows
        ),
    )
    edit = await session.scalar(
        select(EditSession).where(
            EditSession.package_id == package.id,
            EditSession.opened_by_user_id == user_id,
            EditSession.status == EditSessionStatus.OPEN,
            EditSession.expires_at > datetime.now(UTC),
        )
    )
    if edit is None:
        return snapshot, None
    local_id = next(
        (local_id for item, local_id in rows if item.item_id == edit.item_id),
        None,
    )
    return snapshot, EditSessionContext(
        edit_session_id=edit.id,
        package_id=edit.package_id,
        revision_id=edit.plan_revision_id,
        revision_number=edit.plan_revision_number,
        revision_hash=edit.content_hash,
        session_generation=edit.session_generation,
        item_id=edit.item_id,
        item_local_id=local_id,
        opened_by_user_id=edit.opened_by_user_id,
    )


def _required_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise PermanentPipelineError(f"invalid_{key}")
    try:
        return int(value)
    except ValueError as error:
        raise PermanentPipelineError(f"invalid_{key}") from error


def _required_uuid(payload: dict[str, object], key: str) -> uuid.UUID:
    try:
        return uuid.UUID(_required_string(payload, key))
    except ValueError as error:
        raise PermanentPipelineError(f"invalid_{key}") from error


def _discussion_reply_text(result: DiscussionInterpretation) -> str:
    if not result.clarification_question:
        return result.user_visible_summary
    return f"{result.user_visible_summary}\n\n{result.clarification_question}"


async def _enqueue_interpretation(
    session: AsyncSession,
    *,
    intake_id: uuid.UUID,
    transcription_uncertain: bool,
) -> None:
    session.add(
        TransactionalOutbox(
            destination="interpretation",
            operation_type="interpret_intake",
            linked_entity_type="telegram_intake",
            linked_entity_id=intake_id,
            idempotency_key=f"interpretation:intake:{intake_id}",
            payload={"transcription_uncertain": transcription_uncertain},
        )
    )


async def _enqueue_semantic_clarification(
    session: AsyncSession,
    *,
    intake: TelegramIntakeMessage,
    interpretation_id: uuid.UUID,
) -> None:
    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="telegram_intake",
            linked_entity_id=intake.id,
            idempotency_key=f"telegram:interpretation:{interpretation_id}",
            payload={
                "role": "semantic_clarification",
                "interpretation_id": str(interpretation_id),
            },
        )
    )
