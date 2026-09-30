"""D5 derived-memory PostgreSQL tests: writer, chains, recall, pins."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config.settings import RetentionDefaults
from vuzol.discussion.memory import ExplicitDecisionSource
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.memory_units import EXTRACTOR_VERSION, RecallQuery, extraction_scope
from vuzol.discussion.memory_writer import (
    DECISION_OPERATION,
    MemoryWriterService,
    record_hypothesis,
    redact_artifact_for_memory,
    tombstone_unit,
)
from vuzol.execution.git import LocalGit
from vuzol.ops.retention import RetentionSweeper
from vuzol.storage.models import Artifact, Event, MemoryUnit, Task, TransactionalOutbox
from vuzol.storage.types import (
    ArtifactStorageState,
    ConversationTurnRole,
    ConversationTurnSource,
    DeliveryStatus,
    InteractionMode,
    MemoryUnitStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _writer(factory: async_sessionmaker[AsyncSession]) -> MemoryWriterService:
    return MemoryWriterService(factory, owner="test-memory")


async def _session_id(factory: async_sessionmaker[AsyncSession]) -> uuid.UUID:
    async with UnitOfWork(factory) as uow:
        return await uow.discussions.create_session(
            project_id="demo", chat_id=-100, message_thread_id=7
        )


async def _accept(
    factory: async_sessionmaker[AsyncSession],
    session_id: uuid.UUID,
    *,
    key: str = "stack",
    statement: str = "Use Postgres for storage",
) -> tuple[uuid.UUID, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        service = DiscussionMemoryService(uow)
        turn_id, _ = await service.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content="we should use postgres",
            classifier_mode=InteractionMode.DISCUSSION,
        )
        decision_id = await service.accept_decision(
            session_id=session_id,
            key=key,
            statement=statement,
            accepted_by_user_id=7,
            acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
            source_turn_id=turn_id,
        )
        return decision_id, turn_id


async def _pending_jobs(
    factory: async_sessionmaker[AsyncSession],
) -> tuple[TransactionalOutbox, ...]:
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        return tuple(
            (
                await uow.session.scalars(
                    select(TransactionalOutbox)
                    .where(
                        TransactionalOutbox.destination == "memory_extract",
                        TransactionalOutbox.status == DeliveryStatus.PENDING,
                    )
                    .order_by(TransactionalOutbox.created_at)
                )
            ).all()
        )


async def _requeue(factory: async_sessionmaker[AsyncSession], item_id: uuid.UUID) -> None:
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        await uow.session.execute(
            update(TransactionalOutbox)
            .where(TransactionalOutbox.id == item_id)
            .values(status=DeliveryStatus.PENDING, lease_owner=None)
        )


async def _units(factory: async_sessionmaker[AsyncSession]) -> tuple[MemoryUnit, ...]:
    async with UnitOfWork(factory) as uow:
        return await uow.memory_units.units_by_status(tuple(MemoryUnitStatus))


async def test_decision_extracts_verified_template_and_recalls(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await _session_id(factory)
    decision_id, turn_id = await _accept(factory, session_id)

    assert len(await _pending_jobs(factory)) == 1
    assert await _writer(factory).process_one() is True
    assert await _writer(factory).process_one() is False

    rows = await _units(factory)
    assert len(rows) == 1
    (unit,) = rows
    assert unit.status is MemoryUnitStatus.VERIFIED
    assert unit.unit_type == "decision_template"
    assert unit.source_decision_id == decision_id
    assert unit.source_turn_id == turn_id
    assert "stack" in unit.text and "Use Postgres for storage" in unit.text

    async with UnitOfWork(factory) as uow:
        found = await uow.memory_units.recall(RecallQuery(project_id="demo", query="postgres"))
        assert [row.id for row in found] == [unit.id]
        assert await uow.memory_units.recall(RecallQuery(query="postgres")) != ()
        assert await uow.memory_units.recall(RecallQuery(project_id="foreign")) == ()
        assert (
            await uow.memory_units.recall(RecallQuery(unit_types=frozenset({"outcome_template"})))
            == ()
        )
        assert await uow.memory_units.recall(RecallQuery(query="unrelated words here")) == ()
    await engine.dispose()


async def test_redelivery_does_not_duplicate_units(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await _session_id(factory)
    await _accept(factory, session_id)

    (job,) = await _pending_jobs(factory)
    assert await _writer(factory).process_one() is True
    first = await _units(factory)
    assert len(first) == 1

    await _requeue(factory, job.id)
    assert await _writer(factory).process_one() is True
    second = await _units(factory)
    assert len(second) == 1
    assert second[0].id == first[0].id
    await engine.dispose()


async def test_delayed_writer_does_not_displace_current(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await _session_id(factory)
    async with UnitOfWork(factory) as uow:
        service = DiscussionMemoryService(uow)
        await service.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content="use postgres",
            classifier_mode=InteractionMode.DISCUSSION,
        )
        await service.accept_decision(
            session_id=session_id,
            key="stack",
            statement="Use Postgres for storage",
            accepted_by_user_id=7,
            acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
        )
    async with UnitOfWork(factory) as uow:
        service = DiscussionMemoryService(uow)
        await service.supersede_decision(
            session_id=session_id,
            key="stack",
            statement="Use Postgres with read replicas",
            accepted_by_user_id=7,
            acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
        )
    # Process the newer job first by completing the stale job's turn: drain
    # the queue in order would be fresh-first, so simulate drill 11 by
    # crafting a delayed job with its own trigger for the old statement.
    assert await _writer(factory).process_one() is True  # v1 unit
    assert await _writer(factory).process_one() is True  # v2 unit, v1 superseded

    rows = await _units(factory)
    assert len(rows) == 2
    by_status = {row.status for row in rows}
    assert by_status == {MemoryUnitStatus.VERIFIED, MemoryUnitStatus.SUPERSEDED}
    old = next(row for row in rows if row.status is MemoryUnitStatus.SUPERSEDED)
    new = next(row for row in rows if row.status is MemoryUnitStatus.VERIFIED)
    assert old.superseded_by == new.id
    assert old.superseded_at is not None
    assert "read replicas" in new.text

    # Delayed v1 job with a fresh trigger arrives late: must be a no-op.
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        late_trigger = await uow.events.append(
            entity_type="discussion_session",
            entity_id=session_id,
            event_type="decision.accepted",
            actor_type="user",
            payload={"late": True},
        )
        scope = extraction_scope(project_id="demo", session_id=session_id)
        await uow.outbox.enqueue(
            destination="memory_extract",
            operation_type=DECISION_OPERATION,
            entity_type="accepted_decision",
            entity_id=old.source_decision_id or uuid.uuid4(),
            idempotency_key=f"test:late:{late_trigger}",
            payload={
                "trigger_event_id": str(late_trigger),
                "extractor_version": EXTRACTOR_VERSION,
                "scope": scope,
                "project_id": "demo",
                "session_id": str(session_id),
                "decision_id": str(old.source_decision_id),
                "source_event_id": str(late_trigger),
            },
        )
    assert await _writer(factory).process_one() is True
    assert len(await _units(factory)) == 2
    async with UnitOfWork(factory) as uow:
        current = await uow.memory_units.get(new.id)
        assert current is not None and current.status is MemoryUnitStatus.VERIFIED
    await engine.dispose()


async def test_retraction_excludes_recall_but_keeps_row(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await _session_id(factory)
    decision_id, _ = await _accept(factory, session_id)
    assert await _writer(factory).process_one() is True

    async with UnitOfWork(factory) as uow:
        service = DiscussionMemoryService(uow)
        await service.retract_decision(
            session_id=session_id,
            decision_id=decision_id,
            retracted_by_user_id=7,
            acceptance_source=ExplicitDecisionSource.USER_CONFIRM,
        )
    assert await _writer(factory).process_one() is True

    rows = await _units(factory)
    assert len(rows) == 1
    assert rows[0].status is MemoryUnitStatus.RETRACTED
    assert rows[0].source_decision_id == decision_id
    async with UnitOfWork(factory) as uow:
        assert await uow.memory_units.recall(RecallQuery(project_id="demo")) == ()
        # Addressable by id with provenance intact.
        same = await uow.memory_units.get(rows[0].id)
        assert same is not None and same.text != ""
    await engine.dispose()


async def test_tombstone_and_artifact_redaction_keep_provenance(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await _session_id(factory)
    await _accept(factory, session_id)
    assert await _writer(factory).process_one() is True
    (unit,) = await _units(factory)

    async with UnitOfWork(factory) as uow:
        event_id = await tombstone_unit(uow, unit_id=unit.id, actor="user", reason="secret")
        row = await uow.memory_units.get(unit.id)
        assert row is not None
        assert row.status is MemoryUnitStatus.TOMBSTONED
        assert row.text == "[tombstoned]"
        assert row.tombstone_event_id == event_id
        assert await uow.memory_units.recall(RecallQuery(project_id="demo")) == ()

    task_record, run_id, _step = await seed_task_run_step(factory)
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        artifact = Artifact(
            task_id=task_record.id,
            run_id=run_id,
            step_id=None,
            artifact_type="notes",
            content_uri="artifact:notes/abc",
            size_bytes=3,
            content_hash="c" * 64,
            media_type="text/plain",
            sensitivity="private",
            visibility="private",
            retention_until=datetime.now(UTC) - timedelta(days=1),
            storage_state=ArtifactStorageState.AVAILABLE,
        )
        uow.session.add(artifact)
        await uow.session.flush()
        before = artifact.content_hash
        event_id = await redact_artifact_for_memory(
            uow, artifact_id=artifact.id, actor="user", reason="secret"
        )
        assert artifact.redaction_revision is not None
        assert artifact.content_hash == before
        event = await uow.session.get(Event, event_id)
        assert event is not None and event.event_type == "artifact.redacted"
    await engine.dispose()


async def test_memory_provenance_pins_artifact_against_sweep(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    task_record, _run_id, _step = await seed_task_run_step(factory)
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        task = await uow.session.get(Task, task_record.id)
        assert task is not None
        task.status = TaskStatus.COMPLETED
        artifact = Artifact(
            task_id=task_record.id,
            run_id=None,
            step_id=None,
            artifact_type="notes",
            content_uri="artifact:notes/pinned",
            size_bytes=3,
            content_hash="d" * 64,
            media_type="text/plain",
            sensitivity="internal",
            visibility="private",
            retention_until=datetime.now(UTC) - timedelta(days=1),
            storage_state=ArtifactStorageState.AVAILABLE,
        )
        uow.session.add(artifact)
        await uow.session.flush()
        await uow.memory_units.create_unit(
            text="Decision stack: use Postgres",
            unit_type="decision_template",
            status=MemoryUnitStatus.VERIFIED,
            extractor_version=EXTRACTOR_VERSION,
            extraction_identity=f"memory:test-pin-{artifact.id}",
            effective_at=datetime.now(UTC),
            project_id="demo",
            session_id=None,
            source_artifact_id=artifact.id,
        )
        sweeper = RetentionSweeper(
            factory,
            worktree_root=tmp_path / "worktrees",
            artifact_root=tmp_path / "artifacts",
            repository_root=tmp_path / "repo",
            retention=RetentionDefaults(
                completed_worktree_days=3,
                failed_worktree_days=14,
                artifact_days=14,
                sweep_batch_size=50,
                sweep_lock_timeout_seconds=1.0,
            ),
            owner="test-memory",
            git=LocalGit(),
        )
        reason = await sweeper._artifact_skip_reason(uow.session, artifact)
        assert reason is not None
        assert reason[0] == "referenced_by_memory_provenance"
    await engine.dispose()


async def test_missing_provenance_artifact_dead_letters(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await _session_id(factory)
    trigger = uuid.uuid4()
    scope = extraction_scope(project_id="demo", session_id=session_id)
    async with UnitOfWork(factory) as uow:
        await uow.outbox.enqueue(
            destination="memory_extract",
            operation_type="extract_outcome",
            entity_type="work_package",
            entity_id=uuid.uuid4(),
            idempotency_key=f"test:missing:{trigger}",
            payload={
                "trigger_event_id": str(trigger),
                "extractor_version": EXTRACTOR_VERSION,
                "scope": scope,
                "project_id": "demo",
                "session_id": str(session_id),
                "package_id": str(uuid.uuid4()),
                "revision_number": 1,
                "accepted_by_user_id": 7,
                "artifact_id": str(uuid.uuid4()),
            },
        )
    assert await _writer(factory).process_one() is True
    assert await _units(factory) == ()
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        dead = tuple(
            (
                await uow.session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "memory_extract",
                        TransactionalOutbox.status == DeliveryStatus.DEAD_LETTER,
                    )
                )
            ).all()
        )
        assert len(dead) == 1
    await engine.dispose()


async def test_hypotheses_and_filters_in_recall(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    session_id = await _session_id(factory)
    async with UnitOfWork(factory) as uow:
        await record_hypothesis(
            uow, project_id="demo", session_id=session_id, body="Maybe use SQLite"
        )
        await uow.memory_units.create_unit(
            text="Postgres is fast",
            unit_type="observation",
            status=MemoryUnitStatus.OBSERVATION,
            extractor_version=EXTRACTOR_VERSION,
            extraction_identity="memory:test-obs-1",
            effective_at=datetime.now(UTC),
            project_id="demo",
            session_id=session_id,
        )
        found = await uow.memory_units.recall(RecallQuery(project_id="demo"))
        assert [row.unit_type for row in found] == ["observation"]
        assert await uow.memory_units.recall(RecallQuery(project_id="demo", query="sqlite")) == ()
        assert await uow.memory_units.recall(RecallQuery(project_id="other")) == ()
    await engine.dispose()
