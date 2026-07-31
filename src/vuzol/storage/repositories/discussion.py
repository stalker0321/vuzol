"""Project-discussion persistence primitives."""

import hashlib
import uuid
from decimal import Decimal
from typing import cast

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    AcceptedDecision,
    ConversationSummary,
    ConversationTurn,
    ProjectDiscussionSession,
)
from vuzol.storage.types import (
    AcceptedDecisionStatus,
    ConversationSummaryGenerator,
    ConversationTurnRole,
    ConversationTurnSource,
    DiscussionSessionStatus,
    InteractionMode,
)


def _content_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


class DiscussionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_session(
        self, *, project_id: str, chat_id: int, message_thread_id: int
    ) -> uuid.UUID:
        discussion = ProjectDiscussionSession(
            project_id=project_id,
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            status=DiscussionSessionStatus.ACTIVE,
        )
        self._session.add(discussion)
        await self._session.flush()
        return discussion.id

    async def get_session(
        self, session_id: uuid.UUID, *, for_update: bool = False
    ) -> ProjectDiscussionSession:
        statement = select(ProjectDiscussionSession).where(
            ProjectDiscussionSession.id == session_id
        )
        if for_update:
            statement = statement.with_for_update()
        discussion = await self._session.scalar(statement)
        if discussion is None:
            raise LookupError(f"discussion session {session_id} does not exist")
        return discussion

    async def active_session_id(self, *, chat_id: int, message_thread_id: int) -> uuid.UUID | None:
        return cast(
            uuid.UUID | None,
            await self._session.scalar(
                select(ProjectDiscussionSession.id).where(
                    ProjectDiscussionSession.chat_id == chat_id,
                    ProjectDiscussionSession.message_thread_id == message_thread_id,
                    ProjectDiscussionSession.status == DiscussionSessionStatus.ACTIVE,
                )
            ),
        )

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
        discussion = await self._session.scalar(
            select(ProjectDiscussionSession)
            .where(ProjectDiscussionSession.id == session_id)
            .with_for_update()
        )
        if discussion is None:
            raise LookupError(f"discussion session {session_id} does not exist")
        ordinal = (
            cast(
                int,
                await self._session.scalar(
                    select(func.coalesce(func.max(ConversationTurn.ordinal), 0)).where(
                        ConversationTurn.session_id == session_id
                    )
                ),
            )
            + 1
        )
        turn = ConversationTurn(
            session_id=session_id,
            ordinal=ordinal,
            role=role,
            source=source,
            intake_message_id=intake_message_id,
            content=content,
            content_hash=_content_hash(content),
            classifier_mode=classifier_mode,
            classifier_confidence=classifier_confidence,
            classifier_prompt_version=classifier_prompt_version,
            should_create_task=should_create_task,
            override_kind=override_kind,
        )
        self._session.add(turn)
        await self._session.execute(
            update(ProjectDiscussionSession)
            .where(ProjectDiscussionSession.id == session_id)
            .values(version=ProjectDiscussionSession.version + 1, updated_at=func.now())
        )
        await self._session.flush()
        return turn.id, ordinal

    async def add_summary(
        self,
        *,
        session_id: uuid.UUID,
        body: str,
        covered_through_turn_ordinal: int,
        generator: ConversationSummaryGenerator,
        expected_summary_revision: int,
    ) -> tuple[uuid.UUID, int]:
        discussion = await self._session.scalar(
            select(ProjectDiscussionSession)
            .where(ProjectDiscussionSession.id == session_id)
            .with_for_update()
        )
        if discussion is None:
            raise LookupError(f"discussion session {session_id} does not exist")
        if discussion.summary_revision != expected_summary_revision:
            raise ValueError(
                "discussion summary revision conflict: "
                f"expected {expected_summary_revision}, observed {discussion.summary_revision}"
            )
        revision = discussion.summary_revision + 1
        summary = ConversationSummary(
            session_id=session_id,
            revision=revision,
            body=body,
            covered_through_turn_ordinal=covered_through_turn_ordinal,
            generator=generator,
            content_hash=_content_hash(body),
        )
        self._session.add(summary)
        discussion.summary_revision = revision
        discussion.version += 1
        await self._session.flush()
        return summary.id, revision

    async def latest_summary(self, *, session_id: uuid.UUID) -> ConversationSummary | None:
        return cast(
            ConversationSummary | None,
            await self._session.scalar(
                select(ConversationSummary)
                .where(ConversationSummary.session_id == session_id)
                .order_by(ConversationSummary.revision.desc())
                .limit(1)
            ),
        )

    async def max_turn_ordinal(self, *, session_id: uuid.UUID) -> int:
        return cast(
            int,
            await self._session.scalar(
                select(func.coalesce(func.max(ConversationTurn.ordinal), 0)).where(
                    ConversationTurn.session_id == session_id
                )
            ),
        )

    async def turns_from(
        self, *, session_id: uuid.UUID, start_ordinal: int, newest_limit: int
    ) -> tuple[ConversationTurn, ...]:
        newest = tuple(
            (
                await self._session.scalars(
                    select(ConversationTurn)
                    .where(
                        ConversationTurn.session_id == session_id,
                        ConversationTurn.ordinal >= start_ordinal,
                    )
                    .order_by(ConversationTurn.ordinal.desc())
                    .limit(newest_limit)
                )
            ).all()
        )
        return tuple(reversed(newest))

    async def accept_decision(
        self,
        *,
        session_id: uuid.UUID,
        key: str,
        statement: str,
        source_turn_id: uuid.UUID | None = None,
        accepted_by_user_id: int,
    ) -> uuid.UUID:
        if source_turn_id is not None:
            source_session_id = await self._session.scalar(
                select(ConversationTurn.session_id).where(ConversationTurn.id == source_turn_id)
            )
            if source_session_id != session_id:
                raise ValueError("decision source turn does not belong to discussion session")
        decision = AcceptedDecision(
            session_id=session_id,
            key=key,
            statement=statement,
            source_turn_id=source_turn_id,
            accepted_by_user_id=accepted_by_user_id,
            status=AcceptedDecisionStatus.ACTIVE,
        )
        self._session.add(decision)
        await self._session.flush()
        return decision.id

    async def active_decision(
        self, *, session_id: uuid.UUID, key: str, for_update: bool = False
    ) -> AcceptedDecision | None:
        statement = select(AcceptedDecision).where(
            AcceptedDecision.session_id == session_id,
            AcceptedDecision.key == key,
            AcceptedDecision.status == AcceptedDecisionStatus.ACTIVE,
        )
        if for_update:
            statement = statement.with_for_update()
        return cast(AcceptedDecision | None, await self._session.scalar(statement))

    async def active_decisions(
        self, *, session_id: uuid.UUID, newest_limit: int
    ) -> tuple[AcceptedDecision, ...]:
        return tuple(
            (
                await self._session.scalars(
                    select(AcceptedDecision)
                    .where(
                        AcceptedDecision.session_id == session_id,
                        AcceptedDecision.status == AcceptedDecisionStatus.ACTIVE,
                    )
                    .order_by(AcceptedDecision.created_at.desc(), AcceptedDecision.id.desc())
                    .limit(newest_limit)
                )
            ).all()
        )

    async def get_decision(
        self, decision_id: uuid.UUID, *, for_update: bool = False
    ) -> AcceptedDecision:
        statement = select(AcceptedDecision).where(AcceptedDecision.id == decision_id)
        if for_update:
            statement = statement.with_for_update()
        decision = await self._session.scalar(statement)
        if decision is None:
            raise LookupError(f"accepted decision {decision_id} does not exist")
        return decision

    async def set_decision_status(
        self, decision: AcceptedDecision, status: AcceptedDecisionStatus
    ) -> None:
        decision.status = status
        await self._session.flush()
