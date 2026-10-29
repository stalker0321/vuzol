"""Goal attribution and admission through real reservation/settlement code."""

import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import Column, MetaData, Numeric, Table, Uuid, create_engine
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
    goal = ManagedGoal(
        id=uuid.uuid4(), scope_revision=1, constraints={"work_limits": {"max_cost": 1.0}}
    )
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

    async def scalar(query):
        return (await execute(query)).one()[2] if "sum(" in str(query) else None

    session.add = add
    session.get.side_effect = get
    session.execute.side_effect = execute
    session.scalar.side_effect = scalar
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


@pytest.mark.anyio
async def test_budget_totals_include_goal_and_all_packages_without_double_counting(ledger):
    package_id = uuid.uuid4()
    ledger.session.scalars.return_value = [package_id]
    for scope, cost in [(ledger.goal.id, "0.2"), (package_id, "0.3"), (uuid.uuid4(), "99")]:
        ledger.session.add(UsageRecord(horizon_id=scope, cost_units=Decimal(cost)))
    for status, cost in [
        (BudgetReservationStatus.RESERVED, "0.4"),
        (BudgetReservationStatus.RECONCILED, "0.3"),
        (BudgetReservationStatus.RELEASED, "99"),
    ]:
        ledger.session.add(
            ProviderBudgetReservation(
                horizon_id=package_id, status=status, reserved_cost_units=Decimal(cost)
            )
        )
    assert await budgets.goal_cost_totals(ledger.session, ledger.goal.id) == (
        Decimal("0.5"),
        Decimal("0.4"),
    )
    estimate = budgets.estimate_reservation(profile(), input_tokens=1, output_tokens=1)
    ledger.goal.constraints = {"work_limits": {"max_cost": 0.9}}
    with pytest.raises(budgets.GoalBudgetExceeded):
        await budgets._enforce_lifetime_budget(
            ledger.session, horizon_id=ledger.goal.id, estimate=estimate
        )


@pytest.mark.anyio
async def test_goal_review_refusal_reaches_the_leads_same_turn(ledger):
    from vuzol.leadership.worker import ProjectLeadWorker

    context = current_turn.get()
    goal, turn = (
        ledger.goal,
        SimpleNamespace(id=context.turn_id, goal_version=1, fence_epoch=1, scope_revision=1),
    )
    goal.project_id = "p"
    ledger.session.scalar.side_effect = None
    ledger.session.scalar.return_value = turn
    ledger.factory.__class__.__call__ = ledger.factory.__class__.begin
    verifier, model = AsyncMock(), AsyncMock()
    verifier.verify.side_effect = LeadModelUnavailable("goal_budget_exhausted", retryable=False)
    worker = ProjectLeadWorker(
        ledger.factory, owner="owner", model=model, verifier=verifier, enabled=lambda: True
    )
    worker._snapshot = AsyncMock(return_value=snapshot())
    await worker._infer(context.claim)
    model.decide.assert_awaited_once()
    assert model.decide.call_args.args[0].result_context["goal_review_failure"] == (
        "goal_budget_exhausted"
    )


@pytest.mark.anyio
async def test_snapshot_budget_matches_lifetime_ledger(ledger, monkeypatch):
    from vuzol.leadership.worker import ProjectLeadWorker

    await call(ledger, "project_lead")
    package_id = uuid.uuid4()
    ledger.session.scalars.return_value = [package_id]
    ledger.session.add(
        ProviderBudgetReservation(
            horizon_id=package_id,
            status=BudgetReservationStatus.RESERVED,
            reserved_cost_units=Decimal("0.3"),
        )
    )
    ledger.goal.original_request = ledger.goal.scope_text = "task"
    ledger.goal.criteria = []

    @asynccontextmanager
    async def uow(_factory):
        yield SimpleNamespace(
            session=ledger.session,
            memory_units=SimpleNamespace(
                notebook=AsyncMock(return_value=SimpleNamespace(notes=[]))
            ),
        )

    monkeypatch.setattr("vuzol.leadership.worker.UnitOfWork", uow)
    monkeypatch.setattr(
        "vuzol.leadership.worker.assess_completion",
        AsyncMock(return_value=SimpleNamespace(covered=())),
    )
    monkeypatch.setattr("vuzol.leadership.worker.result_context", AsyncMock(return_value={}))
    monkeypatch.setattr("vuzol.leadership.planning.source_commit", AsyncMock(return_value=None))
    monkeypatch.setattr("vuzol.leadership.jobs.list_jobs", AsyncMock(return_value=[]))
    monkeypatch.setattr("vuzol.leadership.tasks.task_context", AsyncMock(return_value={}))
    worker = ProjectLeadWorker(
        ledger.factory, owner="owner", model=AsyncMock(), enabled=lambda: True
    )
    state = await worker._snapshot(ledger.goal.id)
    assert state.budget == {"spent": 0.2, "reserved": 0.3, "cap": 1.0}


@pytest.mark.anyio
async def test_goal_cost_queries_sum_only_its_scopes_and_open_reservations():
    # Execute the production aggregate SQL on a minimal, disposable ledger.
    metadata = MetaData()
    packages = Table(
        WorkPackage.__tablename__,
        metadata,
        Column("id", Uuid),
        Column("managed_goal_id", Uuid),
    )
    usage = Table(
        UsageRecord.__tablename__,
        metadata,
        Column("horizon_id", Uuid),
        Column("cost_units", Numeric),
    )
    reservations = Table(
        ProviderBudgetReservation.__tablename__,
        metadata,
        Column("horizon_id", Uuid),
        Column("reserved_cost_units", Numeric),
        Column("status", ProviderBudgetReservation.__table__.c.status.type),
    )
    goal_id, package_id, other_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    engine = create_engine("sqlite://")
    try:
        metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(packages.insert(), [{"id": package_id, "managed_goal_id": goal_id}])
            connection.execute(
                usage.insert(),
                [
                    {"horizon_id": scope, "cost_units": Decimal(cost)}
                    for scope, cost in [(goal_id, "0.2"), (package_id, "0.3"), (other_id, "99")]
                ],
            )
            connection.execute(
                reservations.insert(),
                [
                    {"horizon_id": scope, "reserved_cost_units": Decimal(cost), "status": status}
                    for scope, cost, status in [
                        (goal_id, "0.1", BudgetReservationStatus.RESERVED),
                        (package_id, "0.4", BudgetReservationStatus.RESERVED),
                        (package_id, "0.3", BudgetReservationStatus.RECONCILED),
                        (goal_id, "99", BudgetReservationStatus.RELEASED),
                        (other_id, "99", BudgetReservationStatus.RESERVED),
                    ]
                ],
            )
            session = SimpleNamespace(
                scalar=AsyncMock(side_effect=connection.scalar),
                scalars=AsyncMock(side_effect=connection.scalars),
            )
            assert await budgets.goal_cost_totals(session, goal_id) == (
                Decimal("0.5"),
                Decimal("0.5"),
            )
    finally:
        engine.dispose()
