"""Durable fenced orchestration for attachment transcription and semantic interpretation."""

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, RuntimeConfiguration, TopicKind
from vuzol.discussion.agent import (
    DISCUSSION_INTERNAL_TASK_TYPE,
    schedule_discussion_agent,
)
from vuzol.discussion.context_budget import ChatContextError, context_failure_text
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.work_context import freeze_work_context
from vuzol.experiments.intake_shadow import IntakeEffect
from vuzol.interpretation.chat_documents import (
    DocumentsPending,
    attach_document_artifacts,
    document_failure_text,
    prepare_chat_documents,
)
from vuzol.interpretation.discussion import (
    DISCUSSION_CLASSIFY_DESTINATION,
)
from vuzol.interpretation.documents import DocumentError
from vuzol.interpretation.domain import (
    InterpretationInput,
    InterpretationResult,
    ProjectSummary,
    TaskAction,
    TaskContext,
    TranscriptionInput,
)
from vuzol.interpretation.jev import JevDecisionsInterpreter
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
from vuzol.providers.budgets import (
    accounting_for_profile,
    estimate_reservation,
)
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.attempts import snapshot_task_spec
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
    Event,
    Interpretation,
    ProjectNamingRequest,
    Run,
    Step,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.repositories import TelegramIntakeRepository
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    ConversationTurnRole,
    ConversationTurnSource,
    DeliveryStatus,
    IntakeStatus,
    InteractionMode,
    ProjectNamingStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import enqueue_chat_notice, enqueue_task_control_snapshot
from vuzol.telegram.tracing import enqueue_interpreter_trace

INTERPRETATION_DESTINATIONS = frozenset(
    {"telegram_file", "interpretation", DISCUSSION_CLASSIFY_DESTINATION}
)

# Observer signature is keyword-only and stable so callers can account every
# attempt (including failures) without importing provider internals.
InterpreterAttemptObserver = Callable[..., Awaitable[None]]


class PermanentPipelineError(RuntimeError):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


class DiscussionContextChanged(RuntimeError):
    """The model answered against memory superseded by another committed turn."""


# D4 W4: generative recovery is bounded and shares one contract. Primary and
# fallback results validate against the same TaskDraft schema, carry the same
# evidence (original input hash + transcript) and draw from the same attempt
# budget: exactly one schema-repair plus at most one fallback interpreter.
# Exhaustion routes to clarification/attention, never to execute.
RECOVERY_MAX_REPAIRS = 1
RECOVERY_MAX_FALLBACKS = 1


def _record_jev_receipt(
    session: AsyncSession, interpreter: JevDecisionsInterpreter, intake_id: uuid.UUID
) -> None:
    if interpreter.receipt is not None:
        session.add(
            Event(
                entity_type="telegram_intake",
                entity_id=intake_id,
                event_type="jev.intake_decided",
                actor_type="jev_intake",
                actor_id=interpreter.profile_id,
                payload=interpreter.receipt,
            )
        )


async def interpret_with_recovery(
    primary: SemanticInterpreter,
    fallbacks: Sequence[SemanticInterpreter],
    request: InterpretationInput,
    *,
    on_attempt: InterpreterAttemptObserver | None = None,
) -> InterpretationResult:
    async def observe(
        interpreter: object,
        result: InterpretationResult | None,
        *,
        attempt_kind: str,
        outcome: str,
    ) -> None:
        if on_attempt is None:
            return
        if isinstance(interpreter, JevDecisionsInterpreter):
            for attempt in interpreter.drain_attempts():
                await on_attempt(attempt_kind=attempt_kind, **attempt)
            return
        await on_attempt(
            profile_id=(
                result.profile_id
                if result is not None
                else getattr(interpreter, "profile_id", None)
            ),
            model=(result.model if result is not None else getattr(interpreter, "model", None)),
            attempt_kind=attempt_kind,
            outcome=outcome,
            input_tokens=result.input_tokens if result is not None else None,
            output_tokens=result.output_tokens if result is not None else None,
            duration_ms=result.duration_ms if result is not None else None,
            provider_request_id=result.provider_request_id if result is not None else None,
        )

    try:
        result = await primary.interpret(request)
        await observe(primary, result, attempt_kind="initial", outcome="succeeded")
        return result
    except InvalidInterpreterOutput as first_error:
        await observe(primary, None, attempt_kind="initial", outcome="invalid_output")
        try:
            result = await primary.interpret(request, repair_error=str(first_error)[:1_000])
            await observe(primary, result, attempt_kind="repair", outcome="succeeded")
            return result
        except (InvalidInterpreterOutput, InterpreterUnavailable):
            await observe(primary, None, attempt_kind="repair", outcome="repair_failed")
    except InterpreterUnavailable:
        await observe(primary, None, attempt_kind="initial", outcome="unavailable")
    for fallback in fallbacks[:RECOVERY_MAX_FALLBACKS]:
        try:
            result = await fallback.interpret(request)
            await observe(fallback, result, attempt_kind="retry", outcome="succeeded")
            return result
        except (InvalidInterpreterOutput, InterpreterUnavailable):
            await observe(fallback, None, attempt_kind="retry", outcome="fallback_failed")
            continue
    raise InterpreterUnavailable("all_interpreters_unavailable")


async def regenerate_project_names(
    primary: SemanticInterpreter,
    fallbacks: Sequence[SemanticInterpreter],
    request: InterpretationInput,
    *,
    previous_project_ids: frozenset[str],
    on_attempt: InterpreterAttemptObserver | None = None,
) -> InterpretationResult:
    instruction = (
        "Generate exactly nine entirely new project_name_options for the same idea. "
        "Do not reuse these project_id values: " + ", ".join(sorted(previous_project_ids))
    )
    for index, interpreter in enumerate((primary, *fallbacks)):
        attempt_kind = "initial" if index == 0 else "retry"
        try:
            result = await interpreter.interpret(request, repair_error=instruction[:1_000])
        except (InvalidInterpreterOutput, InterpreterUnavailable):
            if on_attempt is not None:
                await on_attempt(
                    profile_id=getattr(interpreter, "profile_id", None),
                    model=getattr(interpreter, "model", None),
                    attempt_kind=attempt_kind,
                    outcome="failed",
                )
            continue
        generated_ids = {option.project_id for option in result.draft.project_name_options}
        if (
            result.draft.action is TaskAction.CREATE_PROJECT
            and len(generated_ids) == 9
            and generated_ids.isdisjoint(previous_project_ids)
        ):
            if on_attempt is not None:
                await on_attempt(
                    profile_id=result.profile_id,
                    model=result.model,
                    attempt_kind=attempt_kind,
                    outcome="succeeded",
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    duration_ms=result.duration_ms,
                    provider_request_id=result.provider_request_id,
                )
            return result
        if on_attempt is not None:
            await on_attempt(
                profile_id=result.profile_id,
                model=result.model,
                attempt_kind=attempt_kind,
                outcome="rejected",
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                duration_ms=result.duration_ms,
                provider_request_id=result.provider_request_id,
            )
    raise InterpreterUnavailable("all_interpreters_unavailable")


def _tokens_or_one(usage: NormalizedUsage | None, field: str) -> int:
    value = getattr(usage, field, None) if usage is not None else None
    return value if isinstance(value, int) else 1


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
        self._logger = get_logger(__name__)

    def _attempt_observer(
        self,
        *,
        task_id: uuid.UUID | None,
        purpose: str = "intake",
        intake_id: uuid.UUID | None = None,
    ) -> InterpreterAttemptObserver:
        """Record every interpreter/transcriber attempt in the shared ledger.

        D3 step-less path (lead Q1/Q3): owner + reserve + settlement, no fake
        Step. Successful calls carry measured tokens; failed attempts are
        recorded with a conservative unknown floor so retries are visible
        without inventing a fake zero cost. An accounting failure is logged
        AND persisted as an Event row (dossier pp.3 — never logging-only);
        the caller is never broken by it.
        """

        async def observe(
            *,
            profile_id: str | None,
            model: str | None,
            attempt_kind: str,
            outcome: str,
            input_tokens: int | None = None,
            output_tokens: int | None = None,
            duration_ms: int | None = None,
            provider_request_id: str | None = None,
        ) -> None:
            if profile_id is None:
                return
            try:
                profile = self._runtime.registries.profiles.get(profile_id)
            except Exception:
                return
            usage = None
            if input_tokens is not None or output_tokens is not None or duration_ms is not None:
                usage = NormalizedUsage(
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    duration_ms=duration_ms or 0,
                )
            invocation_id = uuid.uuid4()
            try:
                async with self._factory.begin() as session:
                    from vuzol.providers.budgets import (
                        reserve_invocation_budget,
                        resolve_horizon_scope,
                        settle_invocation_budget,
                    )

                    horizon = await resolve_horizon_scope(
                        session, task_id=task_id, intake_id=intake_id
                    )
                    estimate = estimate_reservation(
                        profile,
                        input_tokens=min(
                            _tokens_or_one(usage, "input_tokens"),
                            self._runtime.settings.limits.provider_call_input_tokens,
                        ),
                        output_tokens=min(
                            _tokens_or_one(usage, "output_tokens"),
                            profile.output_limit or 1,
                        ),
                    )
                    reservation = await reserve_invocation_budget(
                        session,
                        invocation_id=invocation_id,
                        profile=profile,
                        estimate=estimate,
                        limits=self._runtime.settings.limits,
                        accounting=accounting_for_profile(
                            profile,
                            purpose=purpose,
                            attempt_kind=attempt_kind,
                            horizon_id=horizon,
                        ),
                        task_id=task_id,
                    )
                    await settle_invocation_budget(
                        session,
                        reservation=reservation,
                        profile=profile,
                        usage=usage,
                        provider_request_id=provider_request_id,
                        outcome=outcome,
                    )
            except Exception as error:
                self._logger.error(
                    "interpretation.accounting_failed",
                    extra={
                        "profile_id": profile_id,
                        "attempt_kind": attempt_kind,
                        "outcome": outcome,
                        "error_type": type(error).__name__,
                    },
                )
                try:
                    async with self._factory.begin() as session:
                        session.add(
                            Event(
                                entity_type="budget_accounting",
                                entity_id=invocation_id,
                                event_type="budget.accounting_failed",
                                actor_type="system",
                                payload={
                                    "profile_id": profile_id,
                                    "purpose": purpose,
                                    "attempt_kind": attempt_kind,
                                    "outcome": outcome,
                                    "error_type": type(error).__name__,
                                },
                            )
                        )
                except Exception:
                    self._logger.error(
                        "interpretation.accounting_event_failed",
                        extra={"profile_id": profile_id},
                    )

        return observe

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
        except (DiscussionContextChanged, DocumentsPending):
            await self._defer_discussion(token)
        except (DocumentError, ChatContextError) as error:
            await self._dead_letter(token, error.category)
        except OSError:
            await self._retry_or_dead_letter(token, attempt_count, "artifact_storage_unavailable")
        except PermanentPipelineError as error:
            await self._dead_letter(token, error.category)
        return True

    async def _process_chat(self, token: OutboxLeaseToken) -> None:
        """One Jev intake, then either a hidden RO answer or ordinary approved work."""

        try:
            # Leave headroom for classification/persistence inside the existing
            # outbox lease; a large set cannot occupy it indefinitely.
            async with asyncio.timeout(
                min(120, self._runtime.settings.interpretation.lease_seconds / 2)
            ):
                prepared = await prepare_chat_documents(
                    self._factory, self._runtime, token, self._downloader, self._persist_attachment
                )
        except TimeoutError:
            raise DocumentError("document_parser_limit") from None
        if prepared is None:
            return
        async with self._factory() as session:
            routed = await session.get(TransactionalOutbox, token.item_id)
            assert routed is not None
            route_mode = (routed.payload.get("conversation_route") or {}).get("conversation_mode")
        # With the lead on, /think is a read-only answer from its own model; all else is the lead's.
        think_only = self._runtime.settings.project_lead.enabled and route_mode == "think"
        if self._runtime.settings.project_lead.enabled and not think_only:
            from vuzol.leadership.events import ProjectEventService
            from vuzol.leadership.provenance import RequestOrigin
            from vuzol.workflows.application import Principal

            async with UnitOfWork(self._factory) as uow:
                assert uow.session is not None
                item = await uow.session.get(TransactionalOutbox, token.item_id)
                assert item is not None
                intake = await uow.session.get(TelegramIntakeMessage, item.linked_entity_id)
                if intake is None or intake.affinity_kind != "discussion" or not prepared["turn"]:
                    raise PermanentPipelineError("chat_intake_invalid")
                await ProjectEventService(self._runtime).submit_event(
                    uow,
                    Principal(user_id=intake.user_id, ingress_source="telegram"),
                    project_id=_required_string(item.payload, "project_id"),
                    session_id=_required_uuid(item.payload, "discussion_session_id"),
                    idempotency_key=f"lead:message:{intake.chat_id}:{intake.message_id}",
                    text=str(prepared["turn"]),
                    intake=intake,
                    reply_to_message_id=item.payload.get("reply_to_message_id"),
                    origin=RequestOrigin(channel="telegram", display_name="Telegram"),
                )
                await complete_outbox_item(uow.session, token)
            return
        if not isinstance(self._interpreter, JevDecisionsInterpreter):
            raise InterpreterUnavailable("chat_requires_jev_intake")
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
            if intake is None or intake.affinity_kind != "discussion" or not prepared["turn"]:
                raise PermanentPipelineError("chat_intake_invalid")
            session_id = _required_uuid(item.payload, "discussion_session_id")
            project_id = _required_string(item.payload, "project_id")
            if (
                project_id not in {p.id for p in self._runtime.registries.projects.items()}
                or _required_int(item.payload, "chat_id") != intake.chat_id
                or _required_int(item.payload, "message_thread_id") != intake.message_thread_id
                or _required_int(item.payload, "user_id") != intake.user_id
            ):
                raise PermanentPipelineError("chat_context_mismatch")
            unfinished = await session.scalar(
                select(TransactionalOutbox.id)
                .join(
                    TelegramIntakeMessage,
                    TelegramIntakeMessage.id == TransactionalOutbox.linked_entity_id,
                )
                .where(
                    TransactionalOutbox.destination == DISCUSSION_CLASSIFY_DESTINATION,
                    TransactionalOutbox.id != item.id,
                    TransactionalOutbox.status.in_(
                        [DeliveryStatus.PENDING, DeliveryStatus.LEASED, DeliveryStatus.AMBIGUOUS]
                    ),
                    TelegramIntakeMessage.chat_id == intake.chat_id,
                    TelegramIntakeMessage.message_thread_id == intake.message_thread_id,
                    TelegramIntakeMessage.message_id < intake.message_id,
                )
                .limit(1)
            )
            active = await session.scalar(
                select(Task.id)
                .where(
                    Task.task_type == DISCUSSION_INTERNAL_TASK_TYPE,
                    Task.task_draft["discussion_session_id"].as_string() == str(session_id),
                    Task.status.not_in((*USER_TERMINAL_TASK_STATUSES, TaskStatus.QUOTA_EXHAUSTED)),
                )
                .limit(1)
            )
            if unfinished is not None or active is not None:
                raise DiscussionContextChanged
            scope = (
                Task.project_id == project_id,
                Task.user_id == intake.user_id,
                Task.source_chat_id == intake.chat_id,
                Task.source_thread_id == intake.message_thread_id,
                Task.task_type != DISCUSSION_INTERNAL_TASK_TYPE,
            )
            tasks = tuple(
                (
                    await session.scalars(
                        select(Task).where(*scope).order_by(Task.created_at.desc()).limit(20)
                    )
                ).all()
            )
            reply_task = None
            reply_message_id = item.payload.get("reply_to_message_id")
            if isinstance(reply_message_id, int):
                reply_task = await session.scalar(
                    select(Task)
                    .join(TelegramMessageLink, TelegramMessageLink.task_id == Task.id)
                    .where(
                        *scope,
                        TelegramMessageLink.chat_id == intake.chat_id,
                        TelegramMessageLink.message_id == reply_message_id,
                    )
                )
            if reply_task is not None and reply_task not in tasks:
                tasks = (reply_task, *tasks[:19])
            context_lines = []
            if self._runtime.settings.project_lead.enabled:
                from vuzol.leadership.chat import conversation_goal_context

                context_lines.append(
                    await conversation_goal_context(
                        session,
                        project_id=project_id,
                        user_id=intake.user_id,
                        chat_id=intake.chat_id,
                        thread_id=intake.message_thread_id,
                    )
                )
            for task in tasks:
                context_lines.append(
                    f"Task {task.id}, number {task.public_task_number}, "
                    f"status {task.status.value}: {task.original_text[:1000]}"
                )
                outputs = tuple(
                    (
                        await session.scalars(
                            select(Step)
                            .join(Run, Run.id == Step.run_id)
                            .where(
                                Run.task_id == task.id,
                                Step.result.is_not(None),
                                Step.step_type.in_(["execute_agent", "execute_code"]),
                            )
                            .order_by(Step.created_at.desc())
                            .limit(1)
                        )
                    ).all()
                )
                for step in outputs:
                    context_lines.append("Recorded result (bounded): " + str(step.result)[:2000])
            raw = str(prepared["turn"])
            caption = str(prepared["caption"] or "Прикладываю документы для контекста.")
            request = InterpretationInput(
                original_input=caption,
                conversational_context=True,
                topic_kind=TopicKind.PROJECT,
                mapped_project_id=project_id,
                reply_linked_task=TaskContext(
                    task_id=reply_task.id, title=reply_task.original_text[:120]
                )
                if reply_task is not None
                else None,
                active_tasks=tuple(
                    TaskContext(task_id=t.id, title=t.original_text[:120]) for t in tasks
                ),
                project_summaries=tuple(self._project_summaries()),
                capability_vocabulary=frozenset(Capability),
            )
        async with UnitOfWork(self._factory) as uow:
            discussion = await uow.discussions.get_session(session_id)
            if (discussion.project_id, discussion.chat_id, discussion.message_thread_id) != (
                project_id,
                intake.chat_id,
                intake.message_thread_id,
            ):
                raise PermanentPipelineError("chat_session_mismatch")
            expected_version = discussion.version
            prior = await DiscussionMemoryService(uow).load_context(
                session_id=session_id,
                chat_memory=self._runtime.settings.chat.memory_enabled,
                chat_context=True,
                query=raw,
            )
            # Preserve legacy read-only chat behavior, but unpublished assistant
            # drafts may never become an implementation specification.
            work_prior = prior
            if not self._runtime.settings.chat.memory_enabled:
                work_prior = await DiscussionMemoryService(uow).load_context(
                    session_id=session_id,
                    chat_context=True,
                    delivered_only=True,
                )
            request = request.model_copy(
                update={
                    "discussion_context": json.dumps(
                        {
                            **(
                                {
                                    "attached_documents": [
                                        {
                                            k: v
                                            for k, v in d.items()
                                            if k not in {"content_uri", "bytes"}
                                        }
                                        for d in prepared["documents"]
                                    ]
                                }
                                if prepared["documents"]
                                else {}
                            ),
                            "summary": prior.summary.body if prior.summary is not None else None,
                            "accepted_decisions": [d.statement for d in prior.decisions],
                            "turns": [
                                {
                                    "role": t.role.value,
                                    "content": t.chat_text,
                                    "document_context": t.document_context,
                                }
                                for t in prior.turns
                            ],
                            "bounded": prior.raw_turns_truncated,
                            "recalled_memory": [
                                {"id": str(m.id), "text": m.text, "status": m.status}
                                for m in prior.recalled
                            ],
                        },
                        ensure_ascii=False,
                    )
                }
            )
        # No generative repair/fallback here: Jev classifies the complete turn once.
        try:
            result = await self._interpreter.interpret(request)
        except InvalidInterpreterOutput:
            raise InterpreterUnavailable("chat_invalid_jev_output") from None
        finally:
            observer = self._attempt_observer(task_id=None, intake_id=intake.id)
            for attempt in self._interpreter.drain_attempts():
                await observer(attempt_kind="initial", **attempt)
        receipt = self._interpreter.receipt
        if receipt is None:
            raise PermanentPipelineError("chat_receipt_missing")
        effect = IntakeEffect(receipt["effect"])
        if (
            receipt.get("selected_choices", {}).get("target") == "unresolved"
            and not receipt["abstain"]
        ):
            context_lines.append(
                "The referenced object could not be bound. This is a read-only reply: "
                "use only supplied conversation/document/task evidence, never invent the object "
                "or its state. If necessary ask a focused question about the missing reference."
            )
        work = (
            not think_only
            and effect is IntakeEffect.EXECUTE_REQUEST
            and not receipt["abstain"]
            and bool(str(prepared["caption"] or "").strip())
        )
        policy = (
            enforce_interpretation_policy(
                request,
                result.draft,
                known_project_ids=frozenset(
                    p.id for p in self._runtime.registries.projects.items()
                ),
                semantic_effect=effect.value,
            )
            if work
            and not result.draft.needs_clarification
            and result.draft.action is not TaskAction.CONTINUE_TASK
            else None
        )
        mode = (
            InteractionMode.TASK_REQUEST
            if work
            else (
                InteractionMode.QUERY_ONLY
                if effect is IntakeEffect.STATUS
                else InteractionMode.DISCUSSION
            )
        )
        async with UnitOfWork(self._factory) as uow:
            if work and self._runtime.settings.project_lead.enabled:
                from vuzol.leadership.fence import lock_project_admission

                assert uow.session is not None
                # Same order as goal/package admission: project before discussion.
                await lock_project_admission(uow.session, project_id)
            discussion = await uow.discussions.get_session(session_id, for_update=True)
            if discussion.version != expected_version:
                raise DiscussionContextChanged
            assert uow.session is not None
            session = uow.session
            memory = DiscussionMemoryService(uow)
            user_turn_id, _ = await memory.append_turn(
                session_id=session_id,
                role=ConversationTurnRole.USER,
                source=ConversationTurnSource.TELEGRAM_USER,
                content=raw,
                classifier_mode=mode,
                classifier_prompt_version=result.prompt_version,
                should_create_task=policy is not None and not policy.draft.needs_clarification,
                intake_message_id=intake.id,
            )
            _record_jev_receipt(session, self._interpreter, intake.id)
            current_intake = await session.get(
                TelegramIntakeMessage, intake.id, with_for_update=True
            )
            assert current_intake is not None
            work_context = None
            context_failure = None
            if (
                work
                and not result.draft.needs_clarification
                and receipt.get("selected_choices", {}).get("target") == "conversation_context"
            ):
                try:
                    work_context = await freeze_work_context(
                        session,
                        prior=work_prior,
                        source_turn_id=user_turn_id,
                        project_id=project_id,
                        current_document_text_chars=sum(
                            len(d["text"]) for d in prepared["documents"]
                        ),
                    )
                except ChatContextError as error:
                    context_failure = (
                        "В этом обсуждении пока нет контекста для реализации. "  # noqa: RUF001
                        "Уточни, что нужно сделать; ничего не запущено."
                        if error.category == "chat_work_context_missing"
                        else context_failure_text(error.category)
                    )
                    source_turn = await session.get(ConversationTurn, user_turn_id)
                    assert source_turn is not None
                    source_turn.should_create_task = False
            if not think_only and (
                (
                    result.draft.action is TaskAction.CONTINUE_TASK
                    and not result.draft.needs_clarification
                    and context_failure is None
                )
                or (
                    effect is IntakeEffect.CONTROL_REQUEST
                    and receipt.get("target_id") is not None
                    and not receipt.get("abstain")
                )
            ):
                target_id = result.draft.referenced_task_id
                if target_id is None and isinstance(receipt.get("target_id"), str):
                    target_id = uuid.UUID(receipt["target_id"])
                target = await session.get(Task, target_id) if target_id is not None else None
                sent = target is not None and await enqueue_task_control_snapshot(
                    session,
                    task=target,
                    chat_id=intake.chat_id,
                    thread_id=intake.message_thread_id,
                    user_id=intake.user_id,
                    key=f"chat:controls:{intake.id}",
                )
                if not sent:
                    await enqueue_chat_notice(
                        session,
                        chat_id=intake.chat_id,
                        thread_id=intake.message_thread_id,
                        key=f"chat:clarify:{intake.id}",
                        text=(
                            "Не удалось подготовить безопасное продолжение задачи в этом топике. "  # noqa: RUF001
                            "Ничего не запущено."
                        ),
                    )
            elif not think_only and (
                context_failure is not None
                or result.draft.needs_clarification
                or effect
                in {
                    IntakeEffect.MIXED,
                    IntakeEffect.CONTROL_REQUEST,
                }
                or result.draft.action is TaskAction.CONTINUE_TASK
            ):
                await enqueue_chat_notice(
                    session,
                    chat_id=intake.chat_id,
                    thread_id=intake.message_thread_id,
                    key=f"chat:clarify:{intake.id}",
                    text=context_failure
                    or result.draft.clarification_question
                    or (
                        "Не удалось подготовить безопасное продолжение. "  # noqa: RUF001
                        "Уточни запрос; ничего не запущено."
                    ),
                )
            elif work:
                assert policy is not None
                if policy.draft.needs_clarification:
                    await enqueue_chat_notice(
                        session,
                        chat_id=intake.chat_id,
                        thread_id=intake.message_thread_id,
                        key=f"chat:clarify:{intake.id}",
                        text=policy.draft.clarification_question
                        or "Уточни запрос; ничего не запущено.",
                    )
                else:
                    created = await uow.tasks.create(
                        user_id=intake.user_id,
                        chat_id=intake.chat_id,
                        thread_id=intake.message_thread_id,
                        project_id=project_id,
                        original_text=raw,
                        task_type=policy.draft.task_type.value,
                        task_draft=policy.draft.model_dump(mode="json"),
                        ingress_source="chat_work",
                        source_turn_id=user_turn_id,
                    )
                    loaded_task = await session.get(Task, created.id)
                    assert loaded_task is not None
                    task = loaded_task
                    task.status = TaskStatus.INTERPRETED
                    await attach_document_artifacts(
                        session,
                        task,
                        user_turn_id,
                        retention_days=self._runtime.settings.retention.voice_days,
                    )
                    if work_context is not None:
                        for historical_source in work_context.sources:
                            await attach_document_artifacts(
                                session,
                                task,
                                historical_source.id,
                                retention_days=self._runtime.settings.retention.voice_days,
                            )
                    task.risk = policy.draft.suggested_risk
                    task.interpreter_profile = result.profile_id
                    task.prompt_version = result.prompt_version
                    task.draft_schema_version = result.schema_version
                    interpretation = Interpretation(
                        task_id=task.id,
                        original_input_hash=hashlib.sha256(raw.encode()).hexdigest(),
                        transcript=None,
                        task_draft=dict(task.task_draft),
                        profile_id=result.profile_id,
                        model=result.model,
                        prompt_version=result.prompt_version,
                        schema_version=result.schema_version,
                    )
                    session.add(interpretation)
                    await session.flush()
                    await snapshot_task_spec(
                        session,
                        task,
                        source_turn_id=user_turn_id,
                        chat_work_context=work_context.model_dump(mode="json")
                        if work_context is not None
                        else None,
                    )
                    current_intake.task_id = task.id
                    current_intake.affinity_kind = "new_task"
                    session.add(
                        TelegramMessageLink(
                            chat_id=intake.chat_id,
                            message_thread_id=intake.message_thread_id,
                            message_id=intake.message_id,
                            task_id=task.id,
                            message_role="source_request",
                        )
                    )
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
            else:
                from vuzol.discussion.conversation import (
                    ConversationRoute,
                    select_conversation_route,
                )
                from vuzol.projects.executor_preference import ExecutorPreferenceError

                route_error = item.payload.get("conversation_route_error")
                try:
                    if route_error:
                        raise ExecutorPreferenceError(str(route_error))
                    raw_route = item.payload.get("conversation_route")
                    conversation_route = (
                        ConversationRoute.model_validate(raw_route)
                        if raw_route is not None
                        else await select_conversation_route(
                            session, self._runtime, project_id=project_id, mode="think"
                        )
                    )
                    conversation_route.validate_profile(self._runtime, project_id=project_id)
                except (ValueError, ExecutorPreferenceError) as error:
                    await enqueue_chat_notice(
                        session,
                        chat_id=intake.chat_id,
                        thread_id=intake.message_thread_id,
                        key=f"chat:responder:{intake.id}",
                        text=f"Выбранный собеседник недоступен: {error}. Проверь /model. "
                        "Ответ другой моделью не подменён; ничего не запущено.",
                    )
                    current_intake.status = IntakeStatus.COMPLETED
                    await complete_outbox_item(session, token)
                    return
                await schedule_discussion_agent(
                    session,
                    runtime=self._runtime,
                    session_id=session_id,
                    source_turn_id=user_turn_id,
                    project_id=project_id,
                    chat_id=intake.chat_id,
                    thread_id=intake.message_thread_id,
                    user_id=intake.user_id,
                    interaction_mode=mode,
                    classifier_confidence=1.0,
                    memory_pack=await memory.load_context(
                        session_id=session_id,
                        chat_memory=self._runtime.settings.chat.memory_enabled,
                        chat_context=True,
                        query=raw,
                    ),
                    current_turn=raw,
                    task_context="\n".join(context_lines),
                    conversation_route=conversation_route,
                )
            current_intake.status = IntakeStatus.COMPLETED
            await complete_outbox_item(session, token)

    async def _process_discussion(self, token: OutboxLeaseToken) -> None:
        if not self._runtime.settings.chat.enabled:
            raise PermanentPipelineError("chat_disabled")
        await self._process_chat(token)

    async def _submit_lead_voice(
        self,
        token: OutboxLeaseToken,
        *,
        project_id: str,
        intake_id: uuid.UUID,
        text: str,
        heard: str,
    ) -> None:
        """A transcribed voice note with no live goal is a lead message, like text."""
        from vuzol.leadership.events import ProjectEventService
        from vuzol.leadership.fence import lock_project_admission
        from vuzol.leadership.provenance import RequestOrigin
        from vuzol.telegram.projections import enqueue_chat_notice
        from vuzol.workflows.application import Principal

        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            await lock_project_admission(uow.session, project_id)
            intake = await uow.session.get(TelegramIntakeMessage, intake_id, with_for_update=True)
            if intake is None:
                raise PermanentPipelineError("lead_voice_intake_missing")
            session_id = await uow.discussions.active_session_id(
                chat_id=intake.chat_id, message_thread_id=intake.message_thread_id
            )
            if session_id is None:
                session_id = await uow.discussions.create_session(
                    project_id=project_id,
                    chat_id=intake.chat_id,
                    message_thread_id=intake.message_thread_id,
                )
            receipt = await ProjectEventService(self._runtime).submit_event(
                uow,
                Principal(user_id=intake.user_id, ingress_source="telegram"),
                project_id=project_id,
                session_id=session_id,
                idempotency_key=f"lead:message:{intake.chat_id}:{intake.message_id}",
                text=text,
                intake=intake,
                origin=RequestOrigin(channel="telegram", display_name="Telegram"),
            )
            if receipt.status not in {"accepted", "duplicate"}:
                raise PermanentPipelineError(f"lead_voice_{receipt.reason or receipt.status}")
            await enqueue_chat_notice(
                uow.session,
                chat_id=intake.chat_id,
                thread_id=intake.message_thread_id,
                key=f"telegram:voice-heard:{intake.id}",
                text=heard,
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
                    keywords=transcription_keywords(
                        self._runtime.settings.interpretation.transcription_keywords_file
                    ),
                )
            )
            transcript = result.transcript
            uncertain = result.uncertain
            await self._attempt_observer(task_id=task_id)(
                profile_id=getattr(self._transcriber, "profile_id", None),
                model=getattr(self._transcriber, "model", None),
                attempt_kind="initial",
                outcome="succeeded",
                duration_ms=result.duration_ms,
                provider_request_id=result.provider_request_id,
            )
        if transcript is not None and item.payload.get("lead_goal_id"):
            from vuzol.leadership.chat import enqueue_owner_message
            from vuzol.leadership.persistence import ManagedGoal
            from vuzol.telegram.projections import enqueue_chat_notice

            async with UnitOfWork(self._factory) as uow:
                assert uow.session is not None
                goal = await uow.session.get(
                    ManagedGoal, uuid.UUID(item.payload["lead_goal_id"]), with_for_update=True
                )
                current_intake = await uow.session.get(TelegramIntakeMessage, intake.id)
                if goal is None or current_intake is None:
                    raise PermanentPipelineError("lead_voice_goal_missing")
                await enqueue_owner_message(
                    uow,
                    goal=goal,
                    intake=current_intake,
                    raw="\n".join(filter(None, (intake.original_text, transcript))),
                    transcription={
                        "text": transcript,
                        "uncertain": uncertain,
                        "content_uri": reference,
                    },
                )
                # Echo what was heard so a misrecognized voice note is caught at once.
                await enqueue_chat_notice(
                    uow.session,
                    chat_id=current_intake.chat_id,
                    thread_id=current_intake.message_thread_id,
                    key=f"telegram:voice-heard:{current_intake.id}",
                    text=voice_heard_text(transcript, uncertain=uncertain),
                )
                await complete_outbox_item(uow.session, token)
            return
        if transcript is not None and item.payload.get("lead_project_id"):
            await self._submit_lead_voice(
                token,
                project_id=str(item.payload["lead_project_id"]),
                intake_id=intake.id,
                text="\n".join(filter(None, (intake.original_text, transcript))),
                heard=voice_heard_text(transcript, uncertain=uncertain),
            )
            return
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
        temporary.chmod(0o600)
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
            source_task = await session.get(Task, task_id)
            if source_task is None:
                raise PermanentPipelineError("task_missing")
            if source_task.status in USER_TERMINAL_TASK_STATUSES:
                raise PermanentPipelineError("interpretable_task_terminal")
            expected_task_version = source_task.version
        result = await interpret_with_recovery(
            self._interpreter,
            self._fallbacks,
            request,
            on_attempt=self._attempt_observer(task_id=task_id),
        )
        policy = enforce_interpretation_policy(
            request,
            result.draft,
            known_project_ids=frozenset(
                project.id for project in self._runtime.registries.projects.items()
            ),
            semantic_effect=(
                self._interpreter.receipt.get("effect")
                if isinstance(self._interpreter, JevDecisionsInterpreter)
                and self._interpreter.receipt is not None
                else None
            ),
        )
        # D4 W1/REDO: the closed decision triple is bound by policy above;
        # fail closed if the binding ever diverges from eligibility.
        assert policy.decision.policy_allowed == policy.automatic_execution_eligible
        self._logger.info(
            "Interpretation policy bound semantic decision",
            extra={
                "event": "interpretation.decision.bound",
                "semantic_effect": policy.decision.effect.value,
                "semantic_relation": policy.decision.relation.value,
                "semantic_context": policy.decision.context.value,
                "policy_allowed": policy.decision.policy_allowed,
            },
        )
        async with self._factory.begin() as session:
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None:
                raise PermanentPipelineError("task_missing")
            if task.version != expected_task_version:
                raise DiscussionContextChanged
            if isinstance(self._interpreter, JevDecisionsInterpreter):
                _record_jev_receipt(session, self._interpreter, intake.id)
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
            # D1 L2: version the spec separately from the mutating task_draft.
            await snapshot_task_spec(session, task)
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
            on_attempt=self._attempt_observer(task_id=naming.task_id),
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
            # D1 L2: version the spec separately from the mutating task_draft.
            await snapshot_task_spec(session, task)
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
            if (
                self._runtime.settings.chat.enabled
                and item is not None
                and item.destination == DISCUSSION_CLASSIFY_DESTINATION
            ):
                intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
                if intake is not None:
                    await enqueue_chat_notice(
                        session,
                        chat_id=intake.chat_id,
                        thread_id=intake.message_thread_id,
                        key=f"chat:intake-failed:{intake.id}",
                        text=(
                            context_failure_text(category)
                            if category.startswith("chat_context_")
                            else document_failure_text(category)
                            if intake.attachments
                            else (
                                "Не удалось разобрать сообщение. "  # noqa: RUF001
                                "Ничего не запущено; попробуй ещё раз."
                            )
                        ),
                    )
                    intake.status = IntakeStatus.NEEDS_CLARIFICATION
            if item is not None and (
                item.payload.get("lead_goal_id") or item.payload.get("lead_project_id")
            ):
                intake = await session.get(TelegramIntakeMessage, item.linked_entity_id)
                if intake is not None:
                    intake.status = IntakeStatus.NEEDS_CLARIFICATION
                    await enqueue_chat_notice(
                        session,
                        chat_id=intake.chat_id,
                        thread_id=intake.message_thread_id,
                        key=f"lead:voice-failed:{intake.id}",
                        text="Не удалось расшифровать сообщение. Пришлите его текстом.",  # noqa: RUF001
                    )
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


def voice_heard_text(transcript: str, *, uncertain: bool) -> str:
    note = " (не уверен в распознавании)" if uncertain else ""
    return f"🎙 Расслышал{note}: «{transcript}»\nЕсли не так — поправь текстом."  # noqa: RUF001


_MAX_KEYWORDS = 200


def transcription_keywords(path: Path | None) -> tuple[str, ...]:
    """The owner's vocabulary file; a missing or unreadable file means no hints.

    OpenAI rejects the whole request for a keyword containing <, > or a line
    break, so such lines are dropped rather than sent.
    """
    if path is None:
        return ()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    keywords: list[str] = []
    for line in lines:
        term = line.strip()
        if not term or term.startswith("#") or "<" in term or ">" in term or len(term) > 100:
            continue
        if term not in keywords:
            keywords.append(term)
    return tuple(keywords[:_MAX_KEYWORDS])
