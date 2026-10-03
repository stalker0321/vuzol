"""J5 accounting and late-result integration against the existing ledger."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from vuzol.context.decision_binding import (
    Coverage,
    DecisionBinding,
    DecisionOutput,
    DecisionStatus,
)
from vuzol.experiments.canary import CanaryPolicy
from vuzol.experiments.decision import WhitelistGate
from vuzol.experiments.decision_eval import load_decision_accounting
from vuzol.interpretation.decision_chain import LATE_DECISION_EVENT, record_late_decision
from vuzol.storage.models import Event, Run, Task, UsageRecord, WorkPackage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _usage(
    *,
    purpose: str,
    attempt_kind: str,
    cost: str,
    outcome: str = "succeeded",
) -> UsageRecord:
    return UsageRecord(
        provider="openai-compatible",
        profile_id="api",
        model="model",
        duration_ms=1,
        outcome=outcome,
        purpose=purpose,
        attempt_kind=attempt_kind,
        cost_units=Decimal(cost),
        cost_known=True,
        pricing_revision="test",
        currency="USD",
    )


async def test_accounting_includes_retries_and_purposes(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with factory.begin() as session:
        session.add(_usage(purpose="intake", attempt_kind="initial", cost="0.010"))
        session.add(_usage(purpose="intake", attempt_kind="retry", cost="0.020"))
        session.add(_usage(purpose="target_resolution", attempt_kind="initial", cost="0.005"))

    async with factory() as session:
        accounting = await load_decision_accounting(session)
    assert accounting.total_calls == 3
    assert accounting.total_cost_units == Decimal("0.035")
    assert accounting.retry_calls == 1
    assert accounting.retry_cost_units == Decimal("0.020")
    by_purpose = {row[0]: (row[1], row[2]) for row in accounting.by_purpose}
    assert by_purpose["intake"] == (Decimal("0.030"), 2)
    assert by_purpose["target_resolution"] == (Decimal("0.005"), 1)
    await engine.dispose()


async def test_late_result_after_switch_off_does_not_change_state(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    policy = CanaryPolicy(
        enabled_kind="intake",
        cohort_percent=100,
        allowlist=WhitelistGate(enabled_kinds=frozenset({"intake"})),
    )
    assert policy.admit(decision_kind="intake", opportunity_id="op-1").allowed is True
    policy.rollback()
    assert policy.admit(decision_kind="intake", opportunity_id="op-1").allowed is False

    binding = DecisionBinding(
        decision_kind="intake",
        request_hash="a" * 64,
        schema_hash="b" * 64,
        prompt_hash="c" * 64,
        packet_hash="d" * 64,
        coverage=Coverage.COMPLETE,
    )
    output = DecisionOutput(
        decision_kind="intake", status=DecisionStatus.ABSTAIN, reason="late_after_switch_off"
    )
    async with factory.begin() as session:
        await record_late_decision(
            session, binding=binding, output=output, correlation_id="j5-late"
        )

    async with factory() as session:
        event = await session.scalar(select(Event).where(Event.event_type == LATE_DECISION_EVENT))
        assert event is not None and event.payload["late"] is True
        for model in (Task, Run, WorkPackage):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    await engine.dispose()
