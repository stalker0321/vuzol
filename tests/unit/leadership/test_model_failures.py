"""Retry real turn failures with a deterministic outbox and no provider or database."""

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.unit.leadership.test_goal_budget import call
from tests.unit.leadership.test_goal_budget import ledger as ledger
from tests.unit.leadership.test_tool_loop import snapshot

from vuzol.config.settings import ProjectLeadSettings
from vuzol.leadership import worker as worker_module
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.model import LeadDecision, LeadModelUnavailable
from vuzol.leadership.persistence import LeadershipTurn, ManagedGoal
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.providers.domain import (
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderFailureDetails,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import DeliveryStatus


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize("purpose", ["project_lead", "project_goal_review"])
@pytest.mark.parametrize("sent,completed", [(False, False), (True, True), (True, False)])
async def test_inference_keeps_failure_category_and_resend_evidence(
    ledger, purpose, sent, completed
):
    ledger.send.side_effect = ProviderFailure(
        ProviderErrorCategory.PROVIDER_UNAVAILABLE,
        retryable=True,
        request_sent=sent,
        execution_completed=completed,
    )
    with pytest.raises(LeadModelUnavailable) as raised:
        await call(ledger, purpose)
    assert raised.value.failure_category == "provider_unavailable"
    assert raised.value.definitive == (not sent or completed)


@pytest.mark.anyio
async def test_failed_result_retains_definitive_infrastructure_classification(ledger):
    ledger.send.return_value = ProviderResult(
        status=ProviderResultStatus.FAILED,
        usage=NormalizedUsage(duration_ms=1),
        adapter_version="fixture",
        failure=ProviderFailureDetails(
            category=ProviderErrorCategory.RATE_LIMITED,
            retryable=True,
            request_sent=True,
            execution_completed=True,
        ),
    )
    with pytest.raises(LeadModelUnavailable) as raised:
        await call(ledger, "project_goal_review")
    assert raised.value.failure_category == "rate_limited"
    assert raised.value.definitive and raised.value.retryable


@pytest.fixture
def turns(monkeypatch):
    goal = ManagedGoal(
        id=uuid.uuid4(),
        project_id="p",
        lifecycle="active",
        version=1,
        fence_epoch=1,
        scope_revision=1,
        constraints={},
    )
    rows = {}
    session = AsyncMock()
    session.__aenter__.return_value = session

    def add(row):
        if row.id is None:
            row.id = uuid.uuid4()
        rows[row.id] = row

    async def get(model, identity, **kwargs):
        return goal if model is ManagedGoal else rows.get(identity)

    session.add = add
    session.get.side_effect = get
    session.scalar.return_value = None
    factory = Mock(return_value=session)
    current = None

    async def turn(*args):
        return current

    monkeypatch.setattr(worker_module, "_turn", turn)
    monkeypatch.setattr(worker_module, "claim_current", AsyncMock(return_value=True))
    recovery = AsyncMock()
    monkeypatch.setattr(worker_module, "request_owner_recovery", recovery)
    model, verifier = AsyncMock(), AsyncMock()
    model.decide.return_value = LeadDecision(reply="Готово")
    worker = ProjectLeadWorker(
        factory, owner="worker", model=model, verifier=verifier, enabled=lambda: True
    )
    worker._snapshot = AsyncMock(return_value=snapshot())
    worker._save_decision = AsyncMock(return_value=True)
    worker._saved_decision_admissible = AsyncMock(return_value=True)
    worker._apply_if_current = AsyncMock()
    worker._ack = AsyncMock()

    async def complete(session, token):
        rows[token.item_id].status = DeliveryStatus.DELIVERED

    worker._complete = complete

    async def run(item):
        nonlocal current
        current = LeadershipTurn(
            id=uuid.uuid4(),
            goal_id=goal.id,
            wakeup_id=item.id,
            status="claimed",
            goal_version=goal.version,
            fence_epoch=goal.fence_epoch,
            scope_revision=1,
            owner="worker",
            lease_epoch=1,
        )
        item.status = DeliveryStatus.LEASED
        claim = TurnClaim(goal.id, item.id, "worker", 1, 1)
        token = OutboxLeaseToken(item.id, DeliveryStatus.LEASED, "worker", 1, datetime.now(UTC))
        await worker._run_turn(token, claim, "claimed")

    first = TransactionalOutbox(
        id=uuid.uuid4(),
        payload={"goal_id": str(goal.id), "wakeup_kind": "task_finished", "text": "same event"},
        destination="project_lead",
        status=DeliveryStatus.PENDING,
    )
    add(first)
    return SimpleNamespace(
        goal=goal,
        rows=rows,
        session=session,
        factory=factory,
        worker=worker,
        model=model,
        verifier=verifier,
        recovery=recovery,
        first=first,
        run=run,
        pending=lambda: [
            r
            for r in rows.values()
            if isinstance(r, TransactionalOutbox) and r.status is DeliveryStatus.PENDING
        ],
    )


def outage(category="provider_unavailable", *, definitive=True, retryable=True):
    return LeadModelUnavailable(
        "lead_model_unavailable",
        retryable=retryable,
        definitive=definitive,
        failure_category=category,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["lead", "review"])
async def test_two_outages_then_success_preserve_event_and_do_not_ask_owner(turns, phase):
    failing = turns.model.decide if phase == "lead" else turns.verifier.verify
    failure = outage()
    if phase == "review":
        failure = LeadModelUnavailable(
            "goal_review_unavailable", definitive=True, failure_category="provider_unavailable"
        )
    failing.side_effect = [failure, failure, LeadDecision(reply="Готово")]
    item = turns.first
    for attempt, delay in enumerate((30, 120), start=1):
        before = datetime.now(UTC)
        await turns.run(item)
        (retry,) = turns.pending()
        assert retry.payload == {
            **item.payload,
            "retry_of": str(item.id),
            "model_retry_count": attempt,
        }
        assert before + timedelta(seconds=delay) <= retry.available_at
        assert retry.available_at <= datetime.now(UTC) + timedelta(seconds=delay)
        assert turns.goal.lifecycle == "active" and turns.goal.waiting_reason is None
        turns.recovery.assert_not_awaited()
        item = retry
    await turns.run(item)
    turns.worker._apply_if_current.assert_awaited_once()
    turns.recovery.assert_not_awaited()
    assert not turns.pending()


@pytest.mark.anyio
async def test_exhaustion_asks_owner_once_after_three_deferred_retries(turns):
    turns.model.decide.side_effect = outage()
    item = turns.first
    for delay in (30, 120, 600):
        before = datetime.now(UTC)
        await turns.run(item)
        (item,) = turns.pending()
        assert item.available_at >= before + timedelta(seconds=delay)
        turns.recovery.assert_not_awaited()
    await turns.run(item)
    assert not turns.pending()
    assert turns.goal.lifecycle == "waiting"
    turns.recovery.assert_awaited_once()
    assert turns.model.decide.await_count == 4


@pytest.mark.anyio
@pytest.mark.parametrize("category", ["provider_unavailable", "rate_limited", "timeout"])
@pytest.mark.parametrize("definitive,retryable", [(True, True), (False, True), (True, False)])
async def test_only_safe_transient_failures_are_retried(turns, category, definitive, retryable):
    turns.model.decide.side_effect = outage(category, definitive=definitive, retryable=retryable)
    await turns.run(turns.first)
    assert bool(turns.pending()) == (definitive and retryable)
    if not definitive:
        assert turns.goal.waiting_reason == "lead_inference_reconciliation"


def test_retry_settings_have_bounded_configurable_schedule():
    assert ProjectLeadSettings().model_retry_delays_seconds == (30, 120, 600)
    assert ProjectLeadSettings(model_retry_delays_seconds=(1, 2)).model_retry_delays_seconds == (
        1,
        2,
    )
    for schedule in ((1, 2, 3, 4), (-1,), (float("nan"),)):
        with pytest.raises(ValueError):
            ProjectLeadSettings(model_retry_delays_seconds=schedule)
