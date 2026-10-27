"""Explicit recall scopes and idempotent notebook history operations."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.integration.storage.helpers import storage
from vuzol.discussion.memory_units import RecallQuery
from vuzol.storage.types import MemoryScope, MemoryUnitStatus
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def test_recall_does_not_cross_project_session_or_global_scope(postgres_dsn):
    engine, factory = storage(postgres_dsn)
    try:
        async with UnitOfWork(factory) as uow:
            first = await uow.discussions.create_session(
                project_id="demo", chat_id=-100, message_thread_id=10
            )
            second = await uow.discussions.create_session(
                project_id="demo", chat_id=-100, message_thread_id=11
            )
            for scope, project, session_id, text in (
                (MemoryScope.SESSION, None, first, "first session"),
                (MemoryScope.SESSION, None, second, "second session"),
                (MemoryScope.PROJECT, "demo", None, "demo project"),
                (MemoryScope.PROJECT, "other", None, "other project"),
                (MemoryScope.GLOBAL, None, None, "global"),
            ):
                await uow.memory_units.create_unit(
                    scope=scope,
                    project_id=project,
                    session_id=session_id,
                    text=text,
                    unit_type="observation",
                    status=MemoryUnitStatus.OBSERVATION,
                    extractor_version="test",
                    extraction_identity=str(uuid.uuid4()),
                    effective_at=datetime.now(UTC),
                )
        async with UnitOfWork(factory) as uow:
            for query, text in (
                (RecallQuery(scope=MemoryScope.SESSION, session_id=first), "first session"),
                (RecallQuery(scope=MemoryScope.SESSION, session_id=second), "second session"),
                (RecallQuery(scope=MemoryScope.PROJECT, project_id="demo"), "demo project"),
                (RecallQuery(scope=MemoryScope.PROJECT, project_id="other"), "other project"),
                (RecallQuery(scope=MemoryScope.GLOBAL), "global"),
            ):
                assert [row.text for row in await uow.memory_units.recall(query)] == [text]
    finally:
        await engine.dispose()


async def test_notebook_order_totals_and_idempotent_history(postgres_dsn):
    from tests.integration.leadership.test_managed_goal_flow import PROJECT, _enable, _session

    engine, factory = storage(postgres_dsn)
    try:
        goal_id = await _enable(factory, await _session(factory))
        async with UnitOfWork(factory) as uow:
            units = uow.memory_units
            await units.lock_notebook(MemoryScope.PROJECT, PROJECT)
            args = dict(scope=MemoryScope.PROJECT, project_id=PROJECT, goal_id=goal_id)
            old = await units.create_note(**args, text="я", extraction_identity="first")
            old.created_at = datetime.now(UTC) - timedelta(days=1)
            current = await units.create_note(**args, text="новое", extraction_identity="second")
            replay = await units.create_note(**args, text="новое", extraction_identity="second")
            assert replay.id == current.id
            notebook = await units.notebook(scope=MemoryScope.PROJECT, project_id=PROJECT)
            assert [row.id for row in notebook.notes] == [old.id, current.id]
            assert notebook.text_bytes == len("яновое".encode())
            assert await units.supersede_note(old, superseded_by=current.id)
            replaced_at = old.superseded_at
            assert not await units.supersede_note(old, superseded_by=current.id)
            assert old.superseded_at == replaced_at
            assert not await units.retract_note(old)
            assert await units.retract_note(current)
            assert not await units.retract_note(current)
            assert (await units.notebook(scope=MemoryScope.PROJECT, project_id=PROJECT)).notes == ()
            assert await units.count_all() == 2
    finally:
        await engine.dispose()
