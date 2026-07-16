"""Durable fenced orchestration for attachment transcription and semantic interpretation."""

import hashlib
import os
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, RuntimeConfiguration, TopicKind
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
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import (
    Artifact,
    ClarificationDecision,
    Interpretation,
    ProjectNamingRequest,
    Task,
    TelegramIntakeMessage,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import ProjectNamingStatus, TaskStatus

INTERPRETATION_DESTINATIONS = frozenset({"telegram_file", "interpretation"})


class PermanentPipelineError(RuntimeError):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


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
        downloader: AttachmentDownloader | None = None,
        transcriber: Transcriber | None = None,
        owner: str,
    ) -> None:
        self._runtime = runtime
        self._factory = session_factory
        self._interpreter = interpreter
        self._fallbacks = tuple(fallback_interpreters)
        self._downloader = downloader
        self._transcriber = transcriber
        self._owner = owner

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
            else:
                raise PermanentPipelineError("unsupported_pipeline_destination")
        except (InterpreterUnavailable, TranscriptionUnavailable):
            await self._retry_or_dead_letter(token, attempt_count, "provider_unavailable")
        except OSError:
            await self._retry_or_dead_letter(token, attempt_count, "artifact_storage_unavailable")
        except PermanentPipelineError as error:
            await self._dead_letter(token, error.category)
        return True

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
