"""Goal attribution and admission through real reservation/settlement code."""

import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.unit.leadership.test_tool_loop import profile, snapshot

from vuzol.config.settings import HardLimits
from vuzol.leadership import inference
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import TurnInvocationContext, current_turn
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.persistence import LeadershipInference, ManagedGoal
from vuzol.leadership.provider_model import _request
from vuzol.providers import budgets
from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus
from vuzol.storage.models import ProviderBudgetReservation, UsageRecord, WorkPackage
from vuzol.storage.types import BudgetReservationStatus


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def ledger(monkeypatch):
    goal = ManagedGoal(id=uuid.uuid4(), constraints={"work_limits": {"max_cost": 1.0}})
    rows = []
    session = AsyncMock()
    session.scalar.return_value = None
    session.scalars.return_value = []

    def add(row):
        if row.id is None:
            row.id = uuid.uuid4()
        rows.append(row)

    def get(model, identity, **kwargs):
        if model is ManagedGoal:
            return goal if identity == goal.id else None
        if model is WorkPackage:
            return None
        return next((r for r in rows if isinstance(r, model) and r.id == identity), None)

    async def execute(query):
        sql = str(query)
        if "sum(" not in sql:
            return Mock()
        scope = query.compile().params["horizon_id_1"]
        scopes = scope if isinstance(scope, list) else [scope]
        usage = "usage_records" in sql
        selected = [
            r
            for r in rows
            if isinstance(r, UsageRecord if usage else ProviderBudgetReservation)
            and r.horizon_id in scopes
            and (usage or r.status is BudgetReservationStatus.RESERVED)
        ]
        prefix = "" if usage else "reserved_"
        totals = tuple(
            sum((getattr(r, prefix + field) or 0 for r in selected), Decimal(0))
            for field in ("input_tokens", "output_tokens", "cost_units", "quota_units")
        )
        return Mock(one=lambda: (*totals, len(selected)))

    session.add = add
    session.get.side_effect = get
    session.execute.side_effect = execute
    zero = (0, 0, Decimal(0), Decimal(0))
    monkeypatch.setattr(budgets, "_daily_usage_totals", AsyncMock(return_value=zero))
    monkeypatch.setattr(budgets, "_reserved_totals", AsyncMock(return_value=zero))
    monkeypatch.setattr(inference, "invocation_current", AsyncMock(return_value=True))
    result = ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        usage=NormalizedUsage(duration_ms=1, cost_units=Decimal("0.2")),
        adapter_version="fake",
    )
    send = AsyncMock(return_value=result)
    monkeypatch.setattr(inference, "_send_guarded", send)

    class Factory:
        @asynccontextmanager
        async def begin(self):
            yield session

    context = TurnInvocationContext(
        TurnClaim(goal.id, uuid.uuid4(), "owner", 1, 1), uuid.uuid4(), "p", 1, 1, 1
    )
    token = current_turn.set(context)
    try:
        yield SimpleNamespace(goal=goal, rows=rows, session=session, factory=Factory(), send=send)
    finally:
        current_turn.reset(token)


async def call(ledger, purpose, round=0):
    return await inference.invoke(
        ledger.factory,
        profile(),
        AsyncMock(),
        _request(snapshot(), uuid.uuid4()),
        purpose=purpose,
        limits=HardLimits(),
        round=round,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "purpose", ["project_lead", "planning", "project_goal_review", "lead_research_job"]
)
async def test_goal_owns_reservation_and_settlement_including_read_rounds(ledger, purpose):
    await call(ledger, purpose, round=1)
    reservation = next(r for r in ledger.rows if isinstance(r, ProviderBudgetReservation))
    usage = next(r for r in ledger.rows if isinstance(r, UsageRecord))
    assert reservation.horizon_id == usage.horizon_id == ledger.goal.id
    assert reservation.purpose == usage.purpose == purpose
    assert (await budgets._lifetime_totals(ledger.session, ledger.goal.id))[2] == Decimal("0.2")
    assert next(r for r in ledger.rows if isinstance(r, LeadershipInference)).round == 1


@pytest.mark.anyio
@pytest.mark.parametrize("purpose", ["planning", "project_goal_review", "lead_research_job"])
async def test_paid_work_is_refused_before_send_when_goal_cap_cannot_fit_estimate(ledger, purpose):
    ledger.goal.constraints = {"work_limits": {"max_cost": 0.0}}
    with pytest.raises(LeadModelUnavailable, match=r"^goal_budget_exhausted$") as raised:
        await call(ledger, purpose)
    assert not raised.value.retryable
    ledger.send.assert_not_awaited()
    assert ledger.rows == []


@pytest.mark.anyio
@pytest.mark.parametrize("purpose", ["project_lead", "lead_retrospective"])
async def test_lead_and_retrospective_are_counted_even_after_goal_cap_is_exhausted(ledger, purpose):
    ledger.goal.constraints = {"work_limits": {"max_cost": 0.0, "max_attempts": 1}}
    await call(ledger, purpose)
    ledger.send.assert_awaited_once()
    assert (await budgets._lifetime_totals(ledger.session, ledger.goal.id))[2] == Decimal("0.2")


@pytest.mark.anyio
async def test_lead_spend_and_outstanding_reservations_reduce_available_goal_budget(ledger):
    await call(ledger, "project_lead")
    ledger.session.add(
        ProviderBudgetReservation(
            horizon_id=ledger.goal.id,
            status=BudgetReservationStatus.RESERVED,
            reserved_cost_units=Decimal("0.8"),
        )
    )
    with pytest.raises(LeadModelUnavailable, match=r"^goal_budget_exhausted$"):
        await call(ledger, "planning")
    assert ledger.send.await_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize("purpose", ["project_lead", "lead_retrospective"])
async def test_goal_exemption_preserves_provider_daily_limit(ledger, monkeypatch, purpose):
    monkeypatch.setattr(
        budgets, "_daily_usage_totals", AsyncMock(return_value=(0, 0, Decimal("1e9"), 0))
    )
    with pytest.raises(LeadModelUnavailable, match=r"^lead_budget_exceeded$"):
        await call(ledger, purpose)
    ledger.send.assert_not_awaited()
