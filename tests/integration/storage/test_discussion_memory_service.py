import asyncio
import uuid

import pytest
from sqlalchemy import func, select

from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory import ExplicitDecisionSource, MemoryLimits
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.storage.models import AcceptedDecision, Event, Task
from vuzol.storage.types import (
    AcceptedDecisionStatus,
    ConversationSummaryGenerator,
    ConversationTurnRole,
    ConversationTurnSource,
    InteractionMode,
)
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def create_session(factory: object, thread_id: int) -> uuid.UUID:
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        return await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=thread_id
        )


async def append_turn(factory: object, session_id: uuid.UUID, content: str) -> uuid.UUID:
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        turn_id, _ = await DiscussionMemoryService(uow).append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content=content,
            classifier_mode=InteractionMode.DISCUSSION,
        )
        return turn_id


async def test_context_uses_latest_summary_decisions_and_bounded_raw_tail(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await create_session(factory, 92)
    first_turn = await append_turn(factory, session_id, "old turn")
    await append_turn(factory, session_id, "second turn")
    await append_turn(factory, session_id, "third turn")

    async with UnitOfWork(factory) as uow:
        service = DiscussionMemoryService(
            uow,
            limits=MemoryLimits(
                max_raw_turns=1,
                max_raw_chars=100,
                max_summary_chars=100,
                max_decisions=1,
            ),
        )
        await uow.discussions.add_summary(
            session_id=session_id,
            body="The first two turns established scope.",
            covered_through_turn_ordinal=3,
            generator=ConversationSummaryGenerator.MODEL,
            expected_summary_revision=0,
        )
        await service.accept_decision(
            session_id=session_id,
            key="mvp-scope",
            statement="Keep one primary journey.",
            source_turn_id=first_turn,
            accepted_by_user_id=42,
            acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
        )

    async with UnitOfWork(factory) as uow:
        context = await DiscussionMemoryService(
            uow,
            limits=MemoryLimits(
                max_raw_turns=1,
                max_raw_chars=100,
                max_summary_chars=100,
                max_decisions=1,
            ),
        ).load_context(session_id=session_id)

    assert context.summary is not None and context.summary.revision == 1
    assert [item.content for item in context.turns] == ["third turn"]
    assert [item.key for item in context.decisions] == ["mvp-scope"]
    assert context.raw_turns_truncated is False

    async with factory() as session:
        task_count = await session.scalar(select(func.count()).select_from(Task))
        event_types = set(
            (
                await session.scalars(select(Event.event_type).where(Event.entity_id == session_id))
            ).all()
        )
    assert task_count == 0
    assert {
        "conversation_turn.appended",
        "decision.accepted",
    }.issubset(event_types)
    await engine.dispose()


async def test_decision_conflict_and_source_fence(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await create_session(factory, 94)
    other_session_id = await create_session(factory, 95)
    source_turn = await append_turn(factory, session_id, "Explicitly choose PostgreSQL")
    foreign_turn = await append_turn(factory, other_session_id, "Foreign turn")

    async with UnitOfWork(factory) as uow:
        service = DiscussionMemoryService(uow)
        decision_id = await service.accept_decision(
            session_id=session_id,
            key="database",
            statement="Use PostgreSQL.",
            source_turn_id=source_turn,
            accepted_by_user_id=42,
            acceptance_source=ExplicitDecisionSource.BUTTON,
        )
        duplicate_id = await service.accept_decision(
            session_id=session_id,
            key="database",
            statement="Use PostgreSQL.",
            source_turn_id=source_turn,
            accepted_by_user_id=42,
            acceptance_source=ExplicitDecisionSource.BUTTON,
        )
    assert duplicate_id == decision_id

    with pytest.raises(DomainError) as conflict:
        async with UnitOfWork(factory) as uow:
            await DiscussionMemoryService(uow).accept_decision(
                session_id=session_id,
                key="database",
                statement="Use SQLite.",
                accepted_by_user_id=42,
                acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
            )
    assert conflict.value.code == "decision_conflict"

    with pytest.raises(DomainError) as source_mismatch:
        async with UnitOfWork(factory) as uow:
            await DiscussionMemoryService(uow).accept_decision(
                session_id=session_id,
                key="foreign-source",
                statement="This must fail.",
                source_turn_id=foreign_turn,
                accepted_by_user_id=42,
                acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
            )
    assert source_mismatch.value.code == "decision_source_mismatch"

    await engine.dispose()


async def test_concurrent_decision_conflict_and_physical_turn_bound(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await create_session(factory, 96)
    for number in range(4):
        await append_turn(factory, session_id, f"turn-{number}")

    async def accept(statement: str) -> str:
        try:
            async with UnitOfWork(factory) as uow:
                await DiscussionMemoryService(uow).accept_decision(
                    session_id=session_id,
                    key="runtime",
                    statement=statement,
                    accepted_by_user_id=42,
                    acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
                )
        except DomainError as error:
            return error.code
        return "accepted"

    outcomes = await asyncio.gather(accept("Use containers."), accept("Use host processes."))
    assert sorted(outcomes) == ["accepted", "decision_conflict"]

    async with UnitOfWork(factory) as uow:
        context = await DiscussionMemoryService(
            uow,
            limits=MemoryLimits(
                max_raw_turns=2,
                max_raw_chars=100,
                max_summary_chars=100,
                max_decisions=1,
            ),
        ).load_context(session_id=session_id)
    assert [turn.content for turn in context.turns] == ["turn-2", "turn-3"]
    assert context.raw_turns_truncated

    async with factory() as session:
        active_count = await session.scalar(
            select(func.count())
            .select_from(AcceptedDecision)
            .where(AcceptedDecision.status == AcceptedDecisionStatus.ACTIVE)
        )
        task_count = await session.scalar(select(func.count()).select_from(Task))
    assert active_count == 1 and task_count == 0
    await engine.dispose()


async def test_invalid_turn_and_decision_limit_fail_closed(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await create_session(factory, 97)

    with pytest.raises(DomainError) as invalid_turn:
        async with UnitOfWork(factory) as uow:
            await DiscussionMemoryService(uow).append_turn(
                session_id=session_id,
                role=ConversationTurnRole.USER,
                source=ConversationTurnSource.TELEGRAM_USER,
                content=" ",
                classifier_mode=InteractionMode.DISCUSSION,
            )
    assert invalid_turn.value.code == "invalid_turn"

    with pytest.raises(DomainError) as invalid_mode:
        async with UnitOfWork(factory) as uow:
            await DiscussionMemoryService(uow).append_turn(
                session_id=session_id,
                role=ConversationTurnRole.USER,
                source=ConversationTurnSource.TELEGRAM_USER,
                content="discussion",
                classifier_mode=InteractionMode.DISCUSSION,
                should_create_task=True,
            )
    assert invalid_mode.value.code == "invalid_turn"

    async with UnitOfWork(factory) as uow:
        limited = DiscussionMemoryService(uow, limits=MemoryLimits(max_decisions=1))
        await limited.accept_decision(
            session_id=session_id,
            key="first",
            statement="First explicit decision.",
            accepted_by_user_id=42,
            acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
        )
        with pytest.raises(DomainError) as limit:
            await limited.accept_decision(
                session_id=session_id,
                key="second",
                statement="Second explicit decision.",
                accepted_by_user_id=42,
                acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
            )
        assert limit.value.code == "decision_limit_exceeded"

    async with factory() as session:
        task_count = await session.scalar(select(func.count()).select_from(Task))
    assert task_count == 0
    await engine.dispose()
