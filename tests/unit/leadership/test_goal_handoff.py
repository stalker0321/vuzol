"""Exercise the cancellation boundary even without PostgreSQL."""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import inspect
from sqlalchemy.orm.attributes import set_committed_value

from vuzol.discussion.domain import DomainError
from vuzol.leadership.actor import Actor
from vuzol.leadership.commands import LeadCommand, LeadCommandName
from vuzol.leadership.persistence import GoalDelegation, ManagedGoal
from vuzol.leadership.service import LeadershipCommandService
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.storage.models import ProjectDiscussionSession, TransactionalOutbox
from vuzol.storage.types import DiscussionSessionStatus

CHOICE = "Новая задача, текущую закрыть"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def handoff(monkeypatch):
    delegation = GoalDelegation(id=uuid.uuid4())
    goal = ManagedGoal(
        id=uuid.uuid4(),
        project_id="demo",
        owner_user_id=42,
        lifecycle="active",
        version=3,
        fence_epoch=3,
        scope_revision=1,
        constraints={},
        discussion_session_id=uuid.uuid4(),
        delegation_id=delegation.id,
    )
    source = TransactionalOutbox(
        id=uuid.uuid4(),
        destination="project_lead",
        linked_entity_type="managed_goal",
        linked_entity_id=goal.id,
        payload={
            "wakeup_kind": "owner_message",
            "goal_id": str(goal.id),
            "owner_user_id": 42,
            "session_id": str(goal.discussion_session_id),
            "delegation_id": str(delegation.id),
            "scope_revision": 1,
            "goal_version": 1,
            "text": "New request",
            "attachments": [],
        },
    )
    goal.constraints = {
        "owner_answer": {
            "answer": CHOICE,
            "restart_from_event_id": str(source.id),
            "question_version": 2,
            "scope_revision": 1,
            "version": 2,
        }
    }
    discussion = ProjectDiscussionSession(
        id=goal.discussion_session_id,
        project_id=goal.project_id,
        status=DiscussionSessionStatus.ACTIVE,
    )
    session = AsyncMock()
    session.begin_nested = MagicMock(return_value=AsyncMock())
    session.get.side_effect = lambda model, key, **kwargs: {
        TransactionalOutbox: source,
        ProjectDiscussionSession: discussion,
    }.get(model)
    session.scalar.return_value = source
    settle, retro = AsyncMock(), AsyncMock()
    monkeypatch.setattr("vuzol.leadership.service._settle_children", settle)
    monkeypatch.setattr("vuzol.leadership.retrospective.enqueue_retrospective", retro)
    monkeypatch.setattr("vuzol.leadership.service._enqueue_owner_question", AsyncMock())
    return SimpleNamespace(
        goal=goal,
        source=source,
        delegation=delegation,
        session=session,
        service=LeadershipCommandService(AsyncMock(), enabled=True),
        actor=Actor.agent("agent:demo", "worker"),
        settle=settle,
        retro=retro,
    )


async def perform(h, name="cancel_goal", payload=None):
    return await h.service._perform(
        SimpleNamespace(session=h.session),
        h.actor,
        LeadCommandName(name),
        LeadCommand(name=name, project_id="demo", idempotency_key="test", payload=payload or {}),
        h.goal,
        h.delegation,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "fault",
    [
        "unknown",
        "goal",
        "owner",
        "session",
        "project",
        "missing",
        "wrong_answer",
        "old_answer",
        "newer_event",
    ],
)
async def test_invalid_handoff_is_denied_before_cancelling(handoff, fault):
    h = handoff
    if fault == "unknown":
        h.session.get.side_effect = None
        h.session.get.return_value = None
    elif fault == "goal":
        h.source.linked_entity_id = uuid.uuid4()
    elif fault in {"owner", "session", "project"}:
        key = {"owner": "owner_user_id", "session": "session_id", "project": "project_id"}[fault]
        h.source.payload = {**h.source.payload, key: "foreign"}
    elif fault == "missing":
        h.goal.constraints = {}
    elif fault == "wrong_answer":
        h.goal.constraints["owner_answer"]["answer"] = "Да"
    elif fault == "old_answer":
        h.goal.constraints["owner_answer"]["restart_from_event_id"] = str(uuid.uuid4())
    else:
        h.source.payload["goal_version"] = 2
    with pytest.raises(DomainError, match="restart_"):
        await perform(h, payload={"restart_from_event_id": str(h.source.id)})
    h.settle.assert_not_awaited()
    h.retro.assert_not_awaited()
    assert h.goal.lifecycle == "active"


@pytest.mark.anyio
async def test_routing_question_remembers_the_source(handoff):
    h = handoff
    result = await perform(
        h, "request_permission", {"question": "Новая задача?", "options": [CHOICE]}
    )
    assert result.status.value == "waiting"
    assert h.goal.wakeup_condition["restart_from_event_id"] == str(h.source.id)


@pytest.mark.anyio
async def test_question_binding_stays_dirty_after_source_lookup_autoflush(handoff):
    h = handoff

    async def lookup(*args, **kwargs):
        set_committed_value(h.goal, "wakeup_condition", h.goal.wakeup_condition)
        return h.source

    h.session.scalar.side_effect = lookup
    await perform(h, "request_permission", {"question": "Новая задача?", "options": [CHOICE]})
    assert inspect(h.goal).attrs.wakeup_condition.history.has_changes()


@pytest.mark.anyio
async def test_plain_cancel_retains_existing_behavior(handoff):
    result = await perform(handoff)
    assert result.status.value == "applied"
    assert handoff.goal.lifecycle == "cancelled" and handoff.goal.waiting_reason == "cancelled"
    handoff.settle.assert_awaited_once()
    handoff.retro.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize("default_cost", [None, 1.5])
async def test_confirmed_handoff_creates_a_draft_and_one_source_keyed_wakeup(
    handoff, monkeypatch, default_cost
):
    h = handoff
    h.service = LeadershipCommandService(
        AsyncMock(), enabled=True, default_goal_max_cost=default_cost
    )
    new = ManagedGoal(id=uuid.uuid4(), version=1, scope_revision=1, delegation_id=uuid.uuid4())
    create = AsyncMock(return_value=new)
    monkeypatch.setattr("vuzol.leadership.events.live_or_draft", create)
    h.session.add = MagicMock()
    result = await perform(h, payload={"restart_from_event_id": str(h.source.id)})
    assert result.result["new_goal_id"] == str(new.id)
    assert h.goal.lifecycle == "cancelled"
    create.assert_awaited_once()
    assert create.call_args.kwargs["text"] == "New request"
    assert create.call_args.kwargs["default_goal_max_cost"] == default_cost
    wakeup = h.session.add.call_args.args[0]
    assert wakeup.linked_entity_id == new.id
    assert wakeup.idempotency_key == f"lead:restart:{h.source.id}"
    assert wakeup.payload["text"] == "New request"
    assert wakeup.payload["attachments"] == []
    h.settle.assert_awaited_once()
    h.retro.assert_awaited_once()


@pytest.mark.anyio
async def test_owner_answer_preserves_server_bound_source(handoff):
    h = handoff
    h.goal.wakeup_condition = {
        "kind": "owner",
        "question": "Новая задача?",
        "asked_version": 2,
        "scope_revision": 1,
        "restart_from_event_id": str(h.source.id),
    }
    result = await perform(h, "answer_permission", {"answer": CHOICE, "question_version": 2})
    assert result.status.value == "applied"
    assert h.goal.constraints["owner_answer"]["restart_from_event_id"] == str(h.source.id)


@pytest.mark.anyio
async def test_paused_goal_can_ask_routing_without_resuming(handoff):
    h = handoff
    h.goal.lifecycle = "paused"
    result = await perform(
        h, "request_permission", {"question": "Новая задача?", "options": [CHOICE]}
    )
    assert result.status.value == "waiting"
    assert h.goal.lifecycle == "paused"
    assert h.goal.wakeup_condition["restart_from_event_id"] == str(h.source.id)


@pytest.mark.anyio
@pytest.mark.parametrize("answer", [CHOICE, "Дополнение к текущей"])
async def test_paused_routing_answer_wakes_lead_without_resuming(handoff, monkeypatch, answer):
    # Any answer to a routing question needs a lead turn, not only the restart choice.
    h = handoff
    h.goal.lifecycle = "paused"
    h.goal.wakeup_condition = {
        "kind": "owner",
        "question": "Новая задача?",
        "asked_version": 2,
        "scope_revision": 1,
        "restart_from_event_id": str(h.source.id),
    }
    wakeup = AsyncMock()
    monkeypatch.setattr("vuzol.leadership.service.enqueue_goal_wakeup", wakeup)
    await perform(h, "answer_permission", {"answer": answer, "question_version": 2})
    assert h.goal.lifecycle == "paused"
    wakeup.assert_awaited_once()
    assert wakeup.call_args.kwargs["kind"] == "owner_routing_answer"


@pytest.mark.anyio
async def test_paused_routing_answer_is_a_conversational_model_invocation(handoff, monkeypatch):
    from vuzol.leadership.invocation_context import current_turn

    h = handoff
    h.goal.lifecycle = "paused"
    h.source.payload = {"wakeup_kind": "owner_routing_answer"}
    h.session.__aenter__.return_value = h.session
    h.session.get.side_effect = lambda model, key: h.goal if model is ManagedGoal else h.source
    turn = SimpleNamespace(id=uuid.uuid4(), goal_version=3, fence_epoch=3, scope_revision=1)
    monkeypatch.setattr("vuzol.leadership.worker._turn", AsyncMock(return_value=turn))

    class Model:
        async def decide(self, snapshot):
            assert current_turn.get().conversational is True

    worker = ProjectLeadWorker(
        MagicMock(return_value=h.session), owner="test", model=Model(), enabled=lambda: True
    )
    monkeypatch.setattr(worker, "_snapshot", AsyncMock())
    await worker._infer(SimpleNamespace(goal_id=h.goal.id, wakeup_id=h.source.id))


@pytest.mark.anyio
async def test_cancelled_successor_cannot_readmit_its_unprocessed_handoff(handoff, monkeypatch):
    h = handoff
    h.goal.lifecycle = "cancelled"
    origin = str(uuid.uuid4())
    h.goal.constraints = {"restart_from_event_id": origin}
    h.source.payload = {**h.source.payload, "restart_from_event_id": origin}
    get = h.session.get.side_effect
    h.session.get.side_effect = lambda model, key, **kwargs: (
        h.goal if model is ManagedGoal else get(model, key, **kwargs)
    )
    h.session.scalar.side_effect = ["demo", None]
    create = AsyncMock(side_effect=AssertionError("consumed handoff was readmitted"))
    monkeypatch.setattr("vuzol.leadership.events.live_or_draft", create)
    worker = ProjectLeadWorker(
        MagicMock(return_value=h.session), owner="test", model=AsyncMock(), enabled=lambda: True
    )
    complete = AsyncMock()
    monkeypatch.setattr(worker, "_complete", complete)
    assert await worker._prepare_turn(SimpleNamespace(item_id=h.source.id)) is None
    create.assert_not_awaited()
    complete.assert_awaited_once()
