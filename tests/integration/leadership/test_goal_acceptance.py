"""The owner's one decision on a finished goal (live pilot 2026-10-25)."""

import subprocess

import pytest
from sqlalchemy import select
from tests.integration.leadership import test_managed_goal_flow as flow

from vuzol.leadership.acceptance import (
    REWORK_QUESTION,
    accept_goal,
    acceptance_ready,
    request_rework,
)
from vuzol.leadership.evidence import _current_head
from vuzol.leadership.persistence import ManagedGoal
from vuzol.leadership.telegram_commands import final_card_buttons
from vuzol.storage.models import TransactionalOutbox

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _main(tmp_path):
    repository = tmp_path / "repositories" / flow.PROJECT
    return subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "refs/heads/main"),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _owner_commit(tmp_path):
    repository = tmp_path / "repositories" / flow.PROJECT
    (repository / "OTHER.txt").write_text("owner change\n")
    for argv in (("add", "OTHER.txt"), ("commit", "-m", "owner change")):
        subprocess.run(("git", "-C", str(repository), *argv), capture_output=True, check=True)


async def test_accepting_a_finished_goal_fast_forwards_main(postgres_dsn, tmp_path, monkeypatch):
    await flow.run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch)
    before = _main(tmp_path)
    async with flow._factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            goal = await session.scalar(select(ManagedGoal).with_for_update())
            assert goal.lifecycle == "completed"
            assert await acceptance_ready(session, goal)
            labels = [label for row in final_card_buttons(goal, ready=True) for label, _ in row]
            assert labels == ["✅ Принять — влить в main", "✏️ Доработать"]
            head = await _current_head(session, goal, check_live_head=True)
            assert head is not None and head != before
            assert await accept_goal(session, goal, user_id=goal.owner_user_id + 1) == (
                "goal_not_available"
            )
            assert await accept_goal(session, goal, user_id=goal.owner_user_id) == "accepted"
        assert _main(tmp_path) == head
        async with factory.begin() as session:
            goal = await session.scalar(select(ManagedGoal).with_for_update())
            assert goal.constraints["owner_acceptance"]["commit"] == head
            assert not await acceptance_ready(session, goal)
            # A second press changes nothing.
            assert await accept_goal(session, goal, user_id=goal.owner_user_id) == "accepted"
            assert await request_rework(session, goal, user_id=goal.owner_user_id) == (
                "goal_not_finished"
            )
            from vuzol.leadership.retrospective import DESTINATION, enqueue_retrospective

            await enqueue_retrospective(session, goal, outcome="accepted")
            jobs = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == DESTINATION
                    )
                )
            )
            assert len(jobs) == 1 and jobs[0].linked_entity_id == goal.id


async def test_a_moved_main_is_never_overwritten(postgres_dsn, tmp_path, monkeypatch):
    await flow.run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch)
    _owner_commit(tmp_path)
    moved = _main(tmp_path)
    async with flow._factory(postgres_dsn) as factory, factory.begin() as session:
        goal = await session.scalar(select(ManagedGoal).with_for_update())
        assert await accept_goal(session, goal, user_id=goal.owner_user_id) == "target_moved"
        assert "owner_acceptance" not in goal.constraints
    assert _main(tmp_path) == moved


async def test_rework_reopens_the_goal_with_one_question(postgres_dsn, tmp_path, monkeypatch):
    from tests.integration.leadership.test_task_control import command

    await flow.run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch)
    async with flow._factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            goal = await session.scalar(select(ManagedGoal).with_for_update())
            assert await request_rework(session, goal, user_id=goal.owner_user_id) == (
                "rework_requested"
            )
            assert goal.lifecycle == "waiting" and goal.waiting_reason == "owner"
            assert goal.wakeup_condition["question"] == REWORK_QUESTION
            question = await session.scalar(
                select(TransactionalOutbox.id).where(
                    TransactionalOutbox.idempotency_key.like(f"%lead:question:{goal.id}:%")
                )
            )
            assert question is not None
            goal.lifecycle, goal.waiting_reason, goal.wakeup_condition = "active", None, None
            goal_id, version = goal.id, goal.version
        # The same reviewed head cannot close the reopened goal.
        del version
        refused = await command(factory, goal_id, "propose_completion", {})
        assert refused.reason == "rework_pending"


async def test_the_final_card_carries_the_decision_buttons(postgres_dsn, tmp_path, monkeypatch):
    from vuzol.leadership.service import enqueue_goal_status
    from vuzol.leadership.telegram_commands import apply_goal_button
    from vuzol.storage.models import TelegramMessageLink
    from vuzol.telegram.domain import ControlUpdate, IngressStatus

    await flow.run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch)
    async with flow._factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            goal = await session.scalar(select(ManagedGoal).with_for_update())
            await enqueue_goal_status(session, goal)
            card = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["projection_role"].astext == f"lf:{goal.id.hex}"
                )
            )
            assert card is not None
            assert "Прими результат" in card.payload["html"]
            data = [data for row in card.payload["callback_buttons"] for _, data in row]
            assert data == [f"v1:lg:{goal.version}:accept", f"v1:lg:{goal.version}:rework"]
            session.add(
                TelegramMessageLink(
                    chat_id=flow.CHAT,
                    message_thread_id=flow.THREAD,
                    message_id=900,
                    message_role=f"lf:{goal.id.hex}",
                    projection_revision=goal.version,
                )
            )
            version = goal.version
        update = ControlUpdate(
            bot_id="main",
            update_id=1,
            callback_query_id="accept-1",
            chat_id=flow.CHAT,
            user_id=flow.OWNER,
            message_thread_id=flow.THREAD,
            message_id=900,
            action_kind="lead_accept",
            goal_version=version,
        )
        runtime = flow._runtime(tmp_path, tmp_path / "repositories" / flow.PROJECT, lead=True)
        result = await apply_goal_button(
            session_factory=factory,
            settings=runtime.settings,
            project_id=flow.PROJECT,
            update=update,
        )
        assert result.status is IngressStatus.HANDLED and result.reason == "goal_accept_queued"
        # Ingress has no git: the host applier performs the merge.
        from vuzol.cli.applier import ApplierChain
        from vuzol.leadership.acceptance import GoalAcceptanceConsumer

        consumer = GoalAcceptanceConsumer(factory, owner="test-applier")
        assert await consumer.process_one()
        assert not await consumer.process_one()
        assert ApplierChain is not None
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert _main(tmp_path) == goal.constraints["owner_acceptance"]["commit"]
            accepted_card = await session.scalar(
                select(TransactionalOutbox)
                .where(TransactionalOutbox.payload["projection_role"].astext == f"lf:{goal.id.hex}")
                .order_by(TransactionalOutbox.created_at.desc())
                .limit(1)
            )
            assert "ПРИНЯТО" in accepted_card.payload["html"]
            assert not accepted_card.payload.get("callback_buttons")
        again = await apply_goal_button(
            session_factory=factory,
            settings=runtime.settings,
            project_id=flow.PROJECT,
            update=update.model_copy(update={"callback_query_id": "accept-2"}),
        )
        assert again.status is IngressStatus.DUPLICATE
