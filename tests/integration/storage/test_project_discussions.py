import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from vuzol.storage.models import (
    AcceptedDecision,
    ConversationSummary,
    ConversationTurn,
    ProjectDiscussionSession,
    Task,
)
from vuzol.storage.types import (
    ConversationSummaryGenerator,
    ConversationTurnRole,
    ConversationTurnSource,
    InteractionMode,
)
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def test_discussion_identity_is_persisted_without_creating_tasks(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=73
        )
        user_turn_id, user_ordinal = await uow.discussions.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content="Давайте сначала обсудим границы MVP.",
            classifier_mode=InteractionMode.DISCUSSION,
            classifier_confidence=Decimal("0.9100"),
            classifier_prompt_version="discussion-v1",
        )
        _, assistant_ordinal = await uow.discussions.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.ASSISTANT,
            source=ConversationTurnSource.MODEL,
            content="Предлагаю оставить два пользовательских сценария.",
            classifier_mode=InteractionMode.DISCUSSION,
        )
        summary_id, summary_revision = await uow.discussions.add_summary(
            session_id=session_id,
            body="Для MVP остаются два пользовательских сценария.",
            covered_through_turn_ordinal=assistant_ordinal + 1,
            generator=ConversationSummaryGenerator.MODEL,
            expected_summary_revision=0,
        )
        decision_id = await uow.discussions.accept_decision(
            session_id=session_id,
            key="mvp-scope",
            statement="Оставить два пользовательских сценария.",
            source_turn_id=user_turn_id,
            accepted_by_user_id=42,
        )

    async with factory() as session:
        discussion = await session.get(ProjectDiscussionSession, session_id)
        summary = await session.get(ConversationSummary, summary_id)
        decision = await session.get(AcceptedDecision, decision_id)
        turns = (
            await session.scalars(
                select(ConversationTurn)
                .where(ConversationTurn.session_id == session_id)
                .order_by(ConversationTurn.ordinal)
            )
        ).all()
        task_count = await session.scalar(select(func.count()).select_from(Task))

    assert discussion is not None
    assert discussion.summary_revision == 1
    assert discussion.version == 4
    assert [user_ordinal, assistant_ordinal] == [1, 2]
    assert [turn.classifier_mode for turn in turns] == [
        InteractionMode.DISCUSSION,
        InteractionMode.DISCUSSION,
    ]
    assert all(not turn.should_create_task for turn in turns)
    assert summary is not None and summary.revision == summary_revision == 1
    assert decision is not None and decision.key == "mvp-scope"
    assert task_count == 0

    with pytest.raises(ValueError, match="summary revision conflict"):
        async with UnitOfWork(factory) as uow:
            await uow.discussions.add_summary(
                session_id=session_id,
                body="Устаревшая сводка.",
                covered_through_turn_ordinal=assistant_ordinal + 1,
                generator=ConversationSummaryGenerator.MODEL,
                expected_summary_revision=0,
            )
    await engine.dispose()


async def test_turn_ordinals_are_serialized_per_discussion(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=74
        )

    async def append(content: str) -> int:
        async with UnitOfWork(factory) as uow:
            _, ordinal = await uow.discussions.append_turn(
                session_id=session_id,
                role=ConversationTurnRole.USER,
                source=ConversationTurnSource.TELEGRAM_USER,
                content=content,
                classifier_mode=InteractionMode.DISCUSSION,
            )
            return ordinal

    ordinals = await asyncio.gather(append("Первое сообщение"), append("Второе сообщение"))

    assert sorted(ordinals) == [1, 2]
    await engine.dispose()


def test_interaction_mode_contract_has_no_unknown_fallback() -> None:
    assert {mode.value for mode in InteractionMode} == {
        "discussion",
        "plan_request",
        "task_request",
        "plan_control",
        "item_edit",
        "query_only",
        "query_refuse",
    }
