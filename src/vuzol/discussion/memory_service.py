"""Transactional discussion memory and explicit-decision lifecycle."""

from __future__ import annotations

import uuid
from decimal import Decimal

from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory import (
    DecisionView,
    DiscussionEvent,
    ExplicitDecisionSource,
    MemoryLimits,
    MemoryPack,
    MemorySummary,
    MemoryTurn,
    build_memory_pack,
    ensure_memory_safe,
    validate_decision,
)
from vuzol.storage.models import AcceptedDecision
from vuzol.storage.types import (
    AcceptedDecisionStatus,
    ConversationSummaryGenerator,
    ConversationTurnRole,
    ConversationTurnSource,
    InteractionMode,
)
from vuzol.storage.unit_of_work import UnitOfWork


class DiscussionMemoryService:
    """No model, Telegram, package control, or Task dependency is permitted here."""

    def __init__(self, uow: UnitOfWork, *, limits: MemoryLimits | None = None) -> None:
        self._uow = uow
        self._limits = limits or MemoryLimits()

    async def append_turn(
        self,
        *,
        session_id: uuid.UUID,
        role: ConversationTurnRole,
        source: ConversationTurnSource,
        content: str,
        classifier_mode: InteractionMode,
        classifier_confidence: Decimal | None = None,
        classifier_prompt_version: str | None = None,
        should_create_task: bool = False,
        override_kind: str | None = None,
        intake_message_id: uuid.UUID | None = None,
    ) -> tuple[uuid.UUID, int]:
        normalized = content.strip()
        if not normalized or len(normalized) > 100_000:
            raise DomainError("invalid_turn", "turn must contain 1..100000 characters")
        if should_create_task and classifier_mode is not InteractionMode.TASK_REQUEST:
            raise DomainError("invalid_turn", "only task_request may request Task creation")
        turn_id, ordinal = await self._uow.discussions.append_turn(
            session_id=session_id,
            role=role,
            source=source,
            content=normalized,
            classifier_mode=classifier_mode,
            classifier_confidence=classifier_confidence,
            classifier_prompt_version=classifier_prompt_version,
            should_create_task=should_create_task,
            override_kind=override_kind,
            intake_message_id=intake_message_id,
        )
        await self._uow.events.append(
            entity_type="discussion_session",
            entity_id=session_id,
            event_type=DiscussionEvent.TURN_APPENDED.value,
            actor_type=source.value,
            payload={
                "turn_id": str(turn_id),
                "ordinal": ordinal,
                "role": role.value,
                "mode": classifier_mode.value,
                "should_create_task": should_create_task,
            },
        )
        return turn_id, ordinal

    async def update_summary(
        self,
        *,
        session_id: uuid.UUID,
        body: str,
        covered_through_turn_ordinal: int,
        generator: ConversationSummaryGenerator,
        expected_summary_revision: int,
    ) -> tuple[uuid.UUID, int]:
        normalized = ensure_memory_safe(body)
        if len(normalized) > self._limits.max_summary_chars:
            raise DomainError("summary_too_large")
        latest = await self._uow.discussions.latest_summary(session_id=session_id)
        maximum = await self._uow.discussions.max_turn_ordinal(session_id=session_id)
        if not 1 <= covered_through_turn_ordinal <= maximum + 1:
            raise DomainError("invalid_summary_coverage")
        if latest is not None and (
            covered_through_turn_ordinal < latest.covered_through_turn_ordinal
        ):
            raise DomainError("invalid_summary_coverage")
        try:
            summary_id, revision = await self._uow.discussions.add_summary(
                session_id=session_id,
                body=normalized,
                covered_through_turn_ordinal=covered_through_turn_ordinal,
                generator=generator,
                expected_summary_revision=expected_summary_revision,
            )
        except ValueError as error:
            raise DomainError("stale_summary_revision") from error
        await self._uow.events.append(
            entity_type="discussion_session",
            entity_id=session_id,
            event_type=DiscussionEvent.SUMMARY_UPDATED.value,
            actor_type=generator.value,
            payload={
                "summary_id": str(summary_id),
                "revision": revision,
                "covered_through_turn_ordinal": covered_through_turn_ordinal,
            },
        )
        return summary_id, revision

    async def load_context(self, *, session_id: uuid.UUID) -> MemoryPack:
        await self._uow.discussions.get_session(session_id)
        summary = await self._uow.discussions.latest_summary(session_id=session_id)
        start_ordinal = 1 if summary is None else summary.covered_through_turn_ordinal
        turns = await self._uow.discussions.turns_from(
            session_id=session_id,
            start_ordinal=start_ordinal,
            newest_limit=self._limits.max_raw_turns + 1,
        )
        decisions = await self._uow.discussions.active_decisions(
            session_id=session_id,
            newest_limit=self._limits.max_decisions + 1,
        )
        summary_view = (
            None
            if summary is None
            else MemorySummary(
                revision=summary.revision,
                body=summary.body,
                covered_through_turn_ordinal=summary.covered_through_turn_ordinal,
                generator=summary.generator,
                content_hash=summary.content_hash,
            )
        )
        return build_memory_pack(
            session_id=session_id,
            turns=tuple(
                MemoryTurn(
                    id=turn.id,
                    ordinal=turn.ordinal,
                    role=turn.role,
                    content=turn.content,
                    created_at=turn.created_at,
                )
                for turn in turns
            ),
            summary=summary_view,
            decisions=tuple(self._decision_view(decision) for decision in decisions),
            limits=self._limits,
        )

    async def accept_decision(
        self,
        *,
        session_id: uuid.UUID,
        key: str,
        statement: str,
        accepted_by_user_id: int,
        acceptance_source: ExplicitDecisionSource,
        source_turn_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        normalized_key, normalized_statement = validate_decision(key, statement)
        await self._uow.discussions.get_session(session_id, for_update=True)
        existing = await self._uow.discussions.active_decision(
            session_id=session_id, key=normalized_key, for_update=True
        )
        if existing is not None:
            if existing.statement == normalized_statement:
                return existing.id
            raise DomainError("decision_conflict")
        active = await self._uow.discussions.active_decisions(
            session_id=session_id,
            newest_limit=self._limits.max_decisions,
        )
        if len(active) >= self._limits.max_decisions:
            raise DomainError("decision_limit_exceeded")
        try:
            decision_id = await self._uow.discussions.accept_decision(
                session_id=session_id,
                key=normalized_key,
                statement=normalized_statement,
                source_turn_id=source_turn_id,
                accepted_by_user_id=accepted_by_user_id,
            )
        except ValueError as error:
            raise DomainError("decision_source_mismatch") from error
        await self._decision_event(
            session_id=session_id,
            decision_id=decision_id,
            event=DiscussionEvent.DECISION_ACCEPTED,
            user_id=accepted_by_user_id,
            payload={
                "key": normalized_key,
                "source_turn_id": _uuid_text(source_turn_id),
                "acceptance_source": acceptance_source.value,
            },
        )
        return decision_id

    async def supersede_decision(
        self,
        *,
        session_id: uuid.UUID,
        key: str,
        statement: str,
        accepted_by_user_id: int,
        acceptance_source: ExplicitDecisionSource,
        source_turn_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        normalized_key, normalized_statement = validate_decision(key, statement)
        await self._uow.discussions.get_session(session_id, for_update=True)
        existing = await self._uow.discussions.active_decision(
            session_id=session_id, key=normalized_key, for_update=True
        )
        if existing is None:
            raise DomainError("decision_not_found")
        if existing.statement == normalized_statement:
            return existing.id
        await self._uow.discussions.set_decision_status(existing, AcceptedDecisionStatus.SUPERSEDED)
        try:
            replacement_id = await self._uow.discussions.accept_decision(
                session_id=session_id,
                key=normalized_key,
                statement=normalized_statement,
                source_turn_id=source_turn_id,
                accepted_by_user_id=accepted_by_user_id,
            )
        except ValueError as error:
            raise DomainError("decision_source_mismatch") from error
        await self._decision_event(
            session_id=session_id,
            decision_id=existing.id,
            event=DiscussionEvent.DECISION_SUPERSEDED,
            user_id=accepted_by_user_id,
            payload={
                "key": normalized_key,
                "replacement_id": str(replacement_id),
                "acceptance_source": acceptance_source.value,
            },
        )
        await self._decision_event(
            session_id=session_id,
            decision_id=replacement_id,
            event=DiscussionEvent.DECISION_ACCEPTED,
            user_id=accepted_by_user_id,
            payload={
                "key": normalized_key,
                "supersedes_id": str(existing.id),
                "acceptance_source": acceptance_source.value,
            },
        )
        return replacement_id

    async def retract_decision(
        self,
        *,
        session_id: uuid.UUID,
        decision_id: uuid.UUID,
        retracted_by_user_id: int,
        acceptance_source: ExplicitDecisionSource,
    ) -> None:
        await self._uow.discussions.get_session(session_id, for_update=True)
        try:
            decision = await self._uow.discussions.get_decision(decision_id, for_update=True)
        except LookupError as error:
            raise DomainError("decision_not_found") from error
        if decision.session_id != session_id:
            raise DomainError("decision_not_found")
        if decision.status is AcceptedDecisionStatus.RETRACTED:
            return
        if decision.status is not AcceptedDecisionStatus.ACTIVE:
            raise DomainError("decision_not_active")
        await self._uow.discussions.set_decision_status(decision, AcceptedDecisionStatus.RETRACTED)
        await self._decision_event(
            session_id=session_id,
            decision_id=decision.id,
            event=DiscussionEvent.DECISION_RETRACTED,
            user_id=retracted_by_user_id,
            payload={"key": decision.key, "acceptance_source": acceptance_source.value},
        )

    @staticmethod
    def _decision_view(decision: AcceptedDecision) -> DecisionView:
        return DecisionView(
            id=decision.id,
            key=decision.key,
            statement=decision.statement,
            source_turn_id=decision.source_turn_id,
            accepted_by_user_id=decision.accepted_by_user_id,
            created_at=decision.created_at,
        )

    async def _decision_event(
        self,
        *,
        session_id: uuid.UUID,
        decision_id: uuid.UUID,
        event: DiscussionEvent,
        user_id: int,
        payload: dict[str, object],
    ) -> None:
        await self._uow.events.append(
            entity_type="discussion_session",
            entity_id=session_id,
            event_type=event.value,
            actor_type="user",
            payload={"decision_id": str(decision_id), "user_id": user_id, **payload},
        )


def _uuid_text(value: uuid.UUID | None) -> str | None:
    return None if value is None else str(value)
