"""Nightly scheduling, durable receipts and atomic notebook history in PostgreSQL."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from tests.integration.leadership import test_managed_goal_flow as flow
from tests.integration.leadership.test_notebooks_pg import remember

from vuzol.config.settings import ProjectLeadSettings
from vuzol.leadership.dreaming import DESTINATION, DreamConsumer, enqueue_dreams
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import MemoryUnit, TransactionalOutbox
from vuzol.storage.types import DeliveryStatus, MemoryUnitStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
NIGHT = datetime(2026, 11, 9, 3, tzinfo=UTC)
SETTINGS = ProjectLeadSettings(enabled=True, dreaming_enabled=True)


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def tick(factory, now=NIGHT, settings=SETTINGS):
    async with factory.begin() as session:
        await enqueue_dreams(session, settings=settings, timezone="Europe/Amsterdam", now=now)


async def jobs(factory):
    async with factory() as session:
        return list(
            await session.scalars(
                select(TransactionalOutbox).where(TransactionalOutbox.destination == DESTINATION)
            )
        )


class Model:
    def __init__(self, mode="merge", entered=None, release=None):
        self.mode, self.entered, self.release = mode, entered, release
        self.calls = []

    async def dream(self, project_id, context, *, invocation_id):
        self.calls.append(context)
        if self.entered:
            self.entered.set()
            await self.release.wait()
        ids = [note["id"] for note in context["notes"]]
        if self.mode == "keep":
            return {"keep_ids": ids, "merge": [], "drop": []}
        if self.mode == "invalid":
            return {"keep_ids": ids[:-1], "merge": [], "drop": []}
        return {
            "keep_ids": [],
            "merge": [{"replaces": ids[:2], "text": "Факт"}],
            "drop": [{"id": key, "reason": "Устарело"} for key in ids[2:]],
        }


async def seed(factory):
    goal_id = await flow._enable(factory, await flow._session(factory))
    ids = [await remember(factory, goal_id, text) for text in ("Факт один", "Факт два", "Старое")]
    await remember(factory, goal_id, "Общее", scope="global")
    return goal_id, ids


async def test_schedule_hour_flag_lease_date_and_unchanged_receipt(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id, _ = await seed(factory)
        await tick(factory, NIGHT - timedelta(minutes=1))
        await tick(factory, settings=ProjectLeadSettings(enabled=True))
        assert not await jobs(factory)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lease_owner, goal.lease_until = "busy", datetime.now(UTC) + timedelta(minutes=5)
        await tick(factory)
        assert not await jobs(factory)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lease_owner, goal.lease_until = None, None
        await tick(factory)
        await tick(factory)
        assert len(await jobs(factory)) == 1
        model = Model("keep")
        assert await DreamConsumer(factory, owner="dream", model=model).process_one()
        assert len(model.calls[0]["notes"]) == 3
        assert all("created_at" in note for note in model.calls[0]["notes"])
        await tick(factory, NIGHT + timedelta(days=1))
        assert len(await jobs(factory)) == 1
        await remember(factory, goal_id, "Новое")
        await tick(factory)
        assert len(await jobs(factory)) == 1
        await tick(factory, NIGHT + timedelta(days=1))
        assert len(await jobs(factory)) == 2
        async with factory() as session:
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.idempotency_key.startswith("lead:dream-summary:")
                )
            )


async def test_merge_drop_history_notice_and_replay(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        _, ids = await seed(factory)
        await tick(factory)
        consumer = DreamConsumer(factory, owner="dream", model=Model())
        assert await consumer.process_one()
        async with factory.begin() as session:
            notes = list(await session.scalars(select(MemoryUnit).where(MemoryUnit.id.in_(ids))))
            merged = await session.scalar(select(MemoryUnit).where(MemoryUnit.text == "Факт"))
            assert all(
                note.status == MemoryUnitStatus.SUPERSEDED and note.superseded_by == merged.id
                for note in notes
                if note.id in ids[:2]
            )
            assert (await session.get(MemoryUnit, ids[2])).status == MemoryUnitStatus.RETRACTED
            global_note = await session.scalar(select(MemoryUnit).where(MemoryUnit.text == "Общее"))
            assert global_note.status == MemoryUnitStatus.OBSERVATION
            notices = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key.startswith("lead:dream-summary:")
                    )
                )
            )
            assert len(notices) == 1
            assert "объединил 2, убрал 1" in str(notices[0].payload)
            item = await session.get(TransactionalOutbox, (await jobs(factory))[0].id)
            item.status = DeliveryStatus.PENDING
        assert await consumer.process_one()
        await tick(factory, NIGHT + timedelta(days=1))
        assert len(await jobs(factory)) == 1


@pytest.mark.parametrize("mode", ["invalid", "changed", "leased", "lost"])
async def test_invalid_or_stale_dream_does_not_write(postgres_dsn, mode):
    async with flow._factory(postgres_dsn) as factory:
        goal_id, ids = await seed(factory)
        await tick(factory)
        entered, release = asyncio.Event(), asyncio.Event()
        model = Model("invalid" if mode == "invalid" else "merge", entered, release)
        consumer = DreamConsumer(factory, owner="dream", model=model, retry_min_seconds=0)
        task = asyncio.create_task(consumer.process_one())
        await asyncio.wait_for(entered.wait(), timeout=10)
        if mode == "changed":
            await remember(factory, goal_id, "Новое")
        elif mode == "leased":
            async with factory.begin() as session:
                goal = await session.get(ManagedGoal, goal_id)
                goal.lease_owner, goal.lease_until = (
                    "busy",
                    datetime.now(UTC) + timedelta(minutes=5),
                )
        elif mode == "lost":
            async with factory.begin() as session:
                item = await session.get(TransactionalOutbox, (await jobs(factory))[0].id)
                item.lease_generation += 1
        release.set()
        assert await task
        async with factory() as session:
            assert all(
                [
                    (await session.get(MemoryUnit, key)).status == MemoryUnitStatus.OBSERVATION
                    for key in ids
                ]
            )
        if mode == "invalid":
            assert (await jobs(factory))[0].status == DeliveryStatus.PENDING
            model.mode, model.entered = "merge", None
            assert await consumer.process_one()
            assert (await jobs(factory))[0].status == DeliveryStatus.DELIVERED


async def test_composed_worker_uses_lead_assignment_and_accounts_call(postgres_dsn, monkeypatch):
    from tests.integration.leadership.test_lead_surface import _profile, _settings, _Transport

    from vuzol.config.registries import ProfileRegistry
    from vuzol.leadership.composition import build_dream_worker
    from vuzol.storage.models import UsageRecord

    async with flow._factory(postgres_dsn) as factory:
        _, ids = await seed(factory)
        await tick(factory)
        settings = _settings(monkeypatch, "lead-local")
        settings = settings.model_copy(
            update={
                "project_lead": settings.project_lead.model_copy(update={"dreaming_enabled": True})
            }
        )
        transport = _Transport({"keep_ids": [str(key) for key in ids], "merge": [], "drop": []})
        consumer = build_dream_worker(
            factory,
            settings=settings,
            profiles=ProfileRegistry((_profile(),)),
            owner="dream",
            adapter=transport,
        )
        assert await consumer.process_one()
        assert (await jobs(factory))[0].status == DeliveryStatus.DELIVERED
        assert transport.requests[0].output_schema_name == "lead_dreaming"
        async with factory() as session:
            usage = await session.scalar(select(UsageRecord))
            assert usage.purpose == "lead_dreaming"


async def test_note_controls_wait_for_notebook_transaction(postgres_dsn):
    from vuzol.storage.repositories.memory_units import MemoryUnitRepository
    from vuzol.storage.types import MemoryScope

    async with flow._factory(postgres_dsn) as factory:
        _, ids = await seed(factory)
        entered = asyncio.Event()

        async def retract():
            async with factory.begin() as session:
                units = MemoryUnitRepository(session)
                entered.set()
                note = await units.lock_note(ids[0])
                await units.retract_note(note)

        async with factory.begin() as session:
            await MemoryUnitRepository(session).lock_notebook(MemoryScope.PROJECT, flow.PROJECT)
            task = asyncio.create_task(retract())
            await entered.wait()
            try:
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
            finally:
                if task.done():
                    await task
        await task


async def test_idle_check_refreshes_goal_lease_before_apply(postgres_dsn):
    from vuzol.leadership.dreaming import project_idle

    async with flow._factory(postgres_dsn) as factory:
        goal_id, _ = await seed(factory)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lease_until is None
            async with factory.begin() as concurrent:
                leased = await concurrent.get(ManagedGoal, goal_id)
                leased.lease_owner = "lead"
                leased.lease_until = datetime.now(UTC) + timedelta(minutes=5)
            assert not await project_idle(session, flow.PROJECT, lock=True)


async def test_write_failure_rolls_back_notes_notice_and_receipt(postgres_dsn, monkeypatch):
    from vuzol.leadership import dreaming

    async with flow._factory(postgres_dsn) as factory:
        _, ids = await seed(factory)
        await tick(factory)

        async def fail_notice(*args, **kwargs):
            raise RuntimeError("notice storage unavailable")

        monkeypatch.setattr(dreaming, "enqueue_chat_notice", fail_notice)
        assert await DreamConsumer(factory, owner="dream", model=Model()).process_one()
        async with factory() as session:
            assert all(
                [
                    (await session.get(MemoryUnit, key)).status == MemoryUnitStatus.OBSERVATION
                    for key in ids
                ]
            )
            assert not await session.scalar(select(MemoryUnit.id).where(MemoryUnit.text == "Факт"))
        item = (await jobs(factory))[0]
        assert item.status == DeliveryStatus.PENDING
        assert not item.payload["applied"]


async def test_finished_goal_context_is_compact_scoped_and_since_last_dream(postgres_dsn):
    from tests.integration.leadership.test_notebooks_pg import other_goal
    from tests.integration.leadership.test_retrospective_pg import terminal

    from vuzol.leadership.dreaming import dream_input

    async with flow._factory(postgres_dsn) as factory:
        goal_id, _ = await seed(factory)
        await terminal(factory, goal_id)
        other = await other_goal(factory)
        await terminal(factory, other)
        await tick(factory)
        model = Model("keep")
        assert await DreamConsumer(factory, owner="dream", model=model).process_one()
        assert len(model.calls[0]["finished_goals"]) == 1
        assert model.calls[0]["finished_goals"][0]["outcome"] == "accepted"
        async with factory() as session:
            assert (await dream_input(session, flow.PROJECT))["finished_goals"] == []
