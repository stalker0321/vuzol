"""Notebook writes, complete snapshots and delivered Undo through real PostgreSQL."""

import asyncio
import uuid
from dataclasses import replace

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_managed_goal_flow import (
    CHAT,
    OWNER,
    PROJECT,
    THREAD,
    _enable,
    _factory,
    _lead_service,
    _session,
    _worker,
)
from tests.integration.leadership.test_owner_ux import runtime  # noqa: F401
from tests.integration.leadership.test_readable_cards import drain
from tests.integration.leadership.test_task_control import command

from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.model import LeadCommandDraft, LeadDecision
from vuzol.leadership.persistence import GoalDelegation, LeadershipCommandReceipt, ManagedGoal
from vuzol.storage.models import MemoryUnit, TelegramMessageLink, TransactionalOutbox
from vuzol.storage.types import MemoryScope, MemoryUnitStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.adapter import _callback_answer
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, IngressStatus
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.application import Principal

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def remember(factory, goal_id, text="Коммиты по-русски", scope="project", **extra):
    result = await command(factory, goal_id, "remember", {"scope": scope, "text": text, **extra})
    assert result.status == "applied", result.reason
    return uuid.UUID(result.result["note_id"])


async def other_goal(factory):
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="other", chat_id=CHAT, message_thread_id=11
        )
    result = await _lead_service(factory).apply_human(
        Principal(OWNER, "telegram"),
        LeadCommand(
            name="enable_managed_goal",
            project_id="other",
            idempotency_key="other-goal",
            payload={
                "session_id": str(session_id),
                "request": "other task",
                "integration_branch": "vuzol-managed/other/notebook",
                "scope": "files",
                "criteria": [{"id": "done", "description": "done"}],
            },
        ),
    )
    assert result.status == "applied"
    return result.goal_id


@pytest.mark.parametrize("lifecycle", ["draft", "paused"])
async def test_memory_without_action_grant_and_replay_is_atomic(postgres_dsn, lifecycle):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle = lifecycle
            goal.constraints = {**goal.constraints, "owner_mandate": False}
            delegation = await session.get(GoalDelegation, goal.delegation_id)
            delegation.allowed_actions = []
            version, fence = goal.version, goal.fence_epoch
        service = _lead_service(factory)
        saved = LeadCommand(
            name="remember",
            project_id=PROJECT,
            goal_id=goal_id,
            expected_version=version,
            idempotency_key="save",
            payload={"scope": "project", "text": "Русские коммиты"},
        )
        principal = server_agent_principal(PROJECT)
        result = await service.apply_agent(principal, "worker", saved)
        assert result.status == "applied" and result.version == version + 1
        replay = await service.apply_agent(principal, "worker", saved)
        assert replay.status == "duplicate" and replay.result == result.result
        forgotten = replace(
            saved,
            name="forget",
            idempotency_key="forget",
            expected_version=result.version,
            payload={"note_id": result.result["note_id"]},
        )
        assert (await service.apply_agent(principal, "worker", forgotten)).version == version + 2
        assert (await service.apply_agent(principal, "worker", forgotten)).status == "duplicate"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert (goal.version, goal.fence_epoch, goal.lifecycle) == (
                version + 2,
                fence + 2,
                lifecycle,
            )
            notes = list(await session.scalars(select(MemoryUnit)))
            assert len(notes) == 1 and notes[0].status == MemoryUnitStatus.RETRACTED
            assert notes[0].goal_id == goal_id
            notices = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key.startswith("memory:")
                    )
                )
            )
            assert len(notices) == 2
            assert (
                sum(
                    "📌 Запомнил (проект vuzol): Русские коммиты" in n.payload["html"]
                    for n in notices
                )
                == 1
            )
            assert sum(n.payload["html"] == "🗑 Забыл: Русские коммиты" for n in notices) == 1
            assert sum("callback_buttons" in n.payload for n in notices) == 1


async def test_replaces_history_snapshot_and_project_isolation(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        old = await remember(factory, goal_id, "Старое правило")
        current = await remember(factory, goal_id, "Новое правило", replaces=str(old))
        shared = await remember(factory, goal_id, "Общее правило", scope="global")
        sibling = await other_goal(factory)
        worker = _worker(factory, None)
        snapshot = await worker._snapshot(goal_id)
        other = await worker._snapshot(sibling)
        assert [n["id"] for n in snapshot.project_notebook] == [str(current)]
        assert other.project_notebook == ()
        assert snapshot.global_notebook == other.global_notebook
        assert snapshot.global_notebook[0]["id"] == str(shared)
        async with factory() as session:
            previous = await session.get(MemoryUnit, old)
            assert previous.status == MemoryUnitStatus.SUPERSEDED
            assert previous.superseded_by == current and previous.superseded_at is not None
        async with UnitOfWork(factory) as uow:
            notes = await uow.memory_units.notebook(scope=MemoryScope.PROJECT, project_id=PROJECT)
            assert notes.text_bytes == len("Новое правило".encode())
        for name, payload in (
            ("forget", {"note_id": str(current)}),
            ("remember", {"scope": "project", "text": "wrong", "replaces": str(current)}),
        ):
            result = await _lead_service(factory).apply_agent(
                server_agent_principal("other"),
                "worker",
                LeadCommand(
                    name=name,
                    project_id="other",
                    goal_id=sibling,
                    expected_version=1,
                    idempotency_key=name,
                    payload=payload,
                ),
            )
            assert result.reason == "note_outside_scope"
        result = await command(
            factory,
            goal_id,
            "remember",
            {
                "scope": "project",
                "text": "wrong scope",
                "replaces": str(shared),
            },
        )
        assert result.reason == "note_outside_scope"
        for name, payload in (
            ("forget", {"note_id": str(old)}),
            ("remember", {"scope": "project", "text": "old", "replaces": str(old)}),
        ):
            assert (await command(factory, goal_id, name, payload)).reason == "note_not_active"


@pytest.mark.parametrize("scope,limit", [("project", 8000), ("global", 4000)])
async def test_budget_refusal_and_replacement_at_limit(postgres_dsn, scope, limit):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        first = None
        remaining = limit
        while remaining:
            size = min(remaining, 1200)
            note_id = await remember(factory, goal_id, "я" * (size // 2), scope=scope)
            first = first or note_id
            remaining -= size
        async with factory() as session:
            before = (await session.get(ManagedGoal, goal_id)).version
        result = await command(factory, goal_id, "remember", {"scope": scope, "text": "x"})
        assert result.reason == "notebook_full" and result.version == before
        replacement = await remember(factory, goal_id, "я" * 600, scope=scope, replaces=str(first))
        await command(factory, goal_id, "forget", {"note_id": str(replacement)})
        await remember(factory, goal_id, "x", scope=scope)
        async with UnitOfWork(factory) as uow:
            notebook = await uow.memory_units.notebook(
                scope=MemoryScope(scope),
                project_id=PROJECT if scope == "project" else None,
            )
            assert notebook.text_bytes == limit - 1200 + 1


@pytest.mark.parametrize(
    "name,payload,reason",
    [
        ("remember", {"scope": "session", "text": "fact"}, "invalid_remember_payload"),
        ("remember", {"scope": "global", "text": " "}, "invalid_remember_payload"),
        ("remember", {"scope": "project", "text": "я" * 601}, "invalid_remember_payload"),
        (
            "remember",
            {"scope": "global", "text": "password=" + "test-only-value"},
            "sensitive_memory",
        ),
        (
            "remember",
            {"scope": "project", "text": "fact", "replaces": str(uuid.uuid4())},
            "note_not_found",
        ),
        ("forget", {"note_id": "invalid"}, "invalid_forget_payload"),
        ("forget", {"note_id": str(uuid.uuid4())}, "note_not_found"),
    ],
)
async def test_service_validates_untyped_commands(postgres_dsn, name, payload, reason):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        result = await command(factory, goal_id, name, payload)
        assert result.status == "denied" and result.reason == reason and result.version == 1
        async with factory() as session:
            assert not await session.scalar(select(MemoryUnit.id))
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.idempotency_key.startswith("memory:")
                )
            )


async def test_worker_batch_keeps_frozen_versions(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        class Model:
            async def decide(self, snapshot):
                return LeadDecision(
                    commands=(
                        LeadCommandDraft("remember", {"scope": "project", "text": "First"}),
                        LeadCommandDraft("remember", {"scope": "global", "text": "Second"}),
                        LeadCommandDraft("pause_goal", {}),
                    )
                )

        assert await _worker(factory, Model()).process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.version == 4 and goal.lifecycle == "paused"
            receipts = list(
                await session.scalars(
                    select(LeadershipCommandReceipt)
                    .where(LeadershipCommandReceipt.actor_kind == "agent")
                    .order_by(LeadershipCommandReceipt.expected_version)
                )
            )
            assert [r.expected_version for r in receipts] == [1, 2, 3]
            assert all(r.status == "applied" for r in receipts)


async def test_global_budget_is_serialized_across_goals(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        for _ in range(3):
            await remember(factory, goal_id, "я" * 600, scope="global")
        await remember(factory, goal_id, "x" * 399, scope="global")
        sibling = await other_goal(factory)

        async def save(target, project, version):
            return await _lead_service(factory).apply_agent(
                server_agent_principal(project),
                "worker",
                LeadCommand(
                    name="remember",
                    project_id=project,
                    goal_id=target,
                    expected_version=version,
                    idempotency_key="concurrent",
                    payload={"scope": "global", "text": "x"},
                ),
            )

        results = await asyncio.gather(save(goal_id, PROJECT, 5), save(sibling, "other", 1))
        assert sorted(r.status for r in results) == ["applied", "denied"]
        assert next(r for r in results if r.status == "denied").reason == "notebook_full"


@pytest.mark.parametrize("scope", ["project", "global"])
@pytest.mark.parametrize("note_state", ["active", "forgotten", "superseded"])
async def test_delivered_undo_is_owner_and_message_bound_and_repeatable(
    postgres_dsn,
    runtime,  # noqa: F811
    scope,
    note_state,
):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        note_id = await remember(factory, goal_id, scope=scope)
        other = await remember(factory, goal_id, "Другое правило", scope=scope)
        if note_state == "forgotten":
            await command(factory, goal_id, "forget", {"note_id": str(note_id)})
        elif note_state == "superseded":
            await remember(factory, goal_id, "Замена", scope=scope, replaces=str(note_id))
        client = FakeTelegramClient()
        await drain(factory, client)
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == f"mu:{note_id.hex}"
                )
            )
            assert link is not None
        button = ("Отменить", f"v1:mu:{note_id.hex}:undo")
        assert any(button in row for keyboard in client.sent_keyboards for row in keyboard)
        update = ControlUpdate(
            bot_id="test",
            update_id=500,
            callback_query_id="undo",
            chat_id=CHAT,
            message_thread_id=THREAD,
            message_id=link.message_id,
            user_id=OWNER,
            action_kind="memory_undo",
            note_id=note_id,
        )
        controls = TelegramControlService(runtime, factory)
        for change in (
            {"user_id": 12345},
            {"user_id": 99},
            {"message_id": link.message_id + 999},
            {"note_id": other},
            {"message_thread_id": 11},
        ):
            refused = await controls.accept(update.model_copy(update=change))
            assert refused.status == IngressStatus.REJECTED
        expected = "Отменено" if note_state == "active" else "Уже отменено"
        assert _callback_answer(await controls.accept(update)) == (expected, False)
        assert _callback_answer(await controls.accept(update)) == ("Уже отменено", False)
        async with factory() as session:
            expected_status = (
                MemoryUnitStatus.SUPERSEDED
                if note_state == "superseded"
                else MemoryUnitStatus.RETRACTED
            )
            assert (await session.get(MemoryUnit, note_id)).status == expected_status
            assert (await session.get(MemoryUnit, other)).status == MemoryUnitStatus.OBSERVATION
            assert await session.scalar(select(func.count()).select_from(MemoryUnit)) == (
                3 if note_state == "superseded" else 2
            )


async def test_missing_notice_destination_does_not_lose_note(postgres_dsn, caplog):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            (await session.get(ManagedGoal, goal_id)).source_chat_id = None
        note_id = await remember(factory, goal_id)
        async with factory() as session:
            assert await session.get(MemoryUnit, note_id)
        assert "no chat destination" in caplog.text


async def test_notes_survive_into_next_goal_in_same_project(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        old_goal = await _enable(factory, await _session(factory))
        project_note = await remember(factory, old_goal)
        global_note = await remember(factory, old_goal, "Общее правило", scope="global")
        assert (await command(factory, old_goal, "cancel_goal", {})).status == "applied"
        next_goal = await _enable(factory, await _session(factory, thread_id=11))
        snapshot = await _worker(factory, None)._snapshot(next_goal)
        assert [n["id"] for n in snapshot.project_notebook] == [str(project_note)]
        assert [n["id"] for n in snapshot.global_notebook] == [str(global_note)]
        async with factory() as session:
            assert (await session.get(MemoryUnit, project_note)).goal_id == old_goal


async def test_receipt_failure_rolls_back_note_notice_and_version(postgres_dsn, monkeypatch):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        async def fail_receipt(*_args):
            raise RuntimeError("receipt failure")

        monkeypatch.setattr("vuzol.leadership.notices.command_notice", fail_receipt)
        with pytest.raises(RuntimeError, match="receipt failure"):
            await command(factory, goal_id, "remember", {"scope": "project", "text": "fact"})
        async with factory() as session:
            assert not await session.scalar(select(MemoryUnit.id))
            assert not await session.scalar(
                select(LeadershipCommandReceipt.id).where(
                    LeadershipCommandReceipt.command_name == "remember"
                )
            )
            assert not await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.idempotency_key.startswith("memory:")
                )
            )
            assert (await session.get(ManagedGoal, goal_id)).version == 1


async def test_undo_of_a_replacement_restores_the_replaced_note(
    postgres_dsn,
    runtime,  # noqa: F811
):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        original = await remember(factory, goal_id, "Деплой через make deploy")
        replacement = await remember(
            factory, goal_id, "Деплой через ./deploy.sh", replaces=str(original)
        )
        client = FakeTelegramClient()
        await drain(factory, client)
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == f"mu:{replacement.hex}"
                )
            )
        update = ControlUpdate(
            bot_id="test",
            update_id=501,
            callback_query_id="undo-replacement",
            chat_id=CHAT,
            message_thread_id=THREAD,
            message_id=link.message_id,
            user_id=OWNER,
            action_kind="memory_undo",
            note_id=replacement,
        )
        controls = TelegramControlService(runtime, factory)
        assert _callback_answer(await controls.accept(update)) == ("Отменено", False)
        assert _callback_answer(await controls.accept(update)) == ("Уже отменено", False)
        async with factory() as session:
            restored = await session.get(MemoryUnit, original)
            assert restored.status == MemoryUnitStatus.OBSERVATION
            assert restored.superseded_by is None and restored.superseded_at is None
            assert (await session.get(MemoryUnit, replacement)).status == MemoryUnitStatus.RETRACTED
        async with UnitOfWork(factory) as uow:
            notebook = await uow.memory_units.notebook(
                scope=MemoryScope.PROJECT, project_id=PROJECT
            )
        assert [note.id for note in notebook.notes] == [original]
