"""J3 INTAKE shadow integration: advisory event, zero transitions, ledger cost."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select

from tests.integration.providers._test_routing_helpers import (
    NormalizedUsage,
    bundle,
    estimate_reservation,
    profile,
    storage,
)
from vuzol.context.assembler import (
    AssembledContext,
    PendingInteractionSet,
    RecentWorkEntry,
    RecentWorkWindow,
    WorkItemKind,
    WorkOutcome,
    assemble_context,
)
from vuzol.context.decision_binding import Coverage
from vuzol.experiments.intake_shadow import (
    ADVISORY_EVENT_TYPE,
    IntakeEffect,
    run_intake_shadow,
)
from vuzol.interpretation.decision_chain import execute_decision_step
from vuzol.storage.models import Approval, Event, Run, Task, UsageRecord, WorkPackage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]

_HASH = "a" * 64


@dataclass(frozen=True, slots=True)
class _Result:
    usage: NormalizedUsage | None
    provider_request_id: str | None


def _valid_output() -> dict[str, Any]:
    return {
        "schema": "decision.v3",
        "decision_kind": "intake",
        "state_revision": 0,
        "effect": "execute_request",
        "relation": "new",
        "target_id": None,
        "support_refs": ["turn:abc"],
        "coverage": "complete",
        "abstain": False,
        "reason_code": "clear_match",
        "input_fingerprint": _HASH,
    }


def _context() -> AssembledContext:
    entry = RecentWorkEntry(
        ref="task:00000000-0000-0000-0000-000000000001",
        kind=WorkItemKind.TASK,
        outcome=WorkOutcome.ACTIVE,
        content_hash=_HASH,
    )
    return assemble_context(
        decision_kind="intake",
        recent=RecentWorkWindow(entries=(entry,), limit=5),
        pending=PendingInteractionSet(interactions=()),
        coverage=Coverage.COMPLETE,
    )


async def test_shadow_records_advisory_event_with_zero_production_transitions(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)

    async def provider(prompt: str) -> dict[str, Any]:
        return _valid_output()

    async with factory.begin() as session:
        run = await run_intake_shadow(
            session,
            context=_context(),
            correlation_id="j3-advisory",
            provider_call=provider,
            allowed_refs=frozenset({"turn:abc"}),
        )
    assert run.provider_calls == 1
    assert run.decision is not None and run.decision.effect is IntakeEffect.EXECUTE_REQUEST

    async with factory() as session:
        event = await session.scalar(select(Event).where(Event.event_type == ADVISORY_EVENT_TYPE))
        assert event is not None
        assert event.payload["advisory"] is True
        assert event.payload["production_transition"] is False
        assert event.payload["decision_kind"] == "intake"
        assert event.payload["route_hint"] == "execute_request"
        assert len(event.payload["prompt_hash"]) == 64
        for model in (Task, Run, WorkPackage, Approval):
            count = await session.scalar(select(func.count()).select_from(model))
            assert count == 0
    await engine.dispose()


async def test_shadow_call_cost_recorded_in_shared_ledger(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    api = profile(
        "api",
        input_cost_units_per_million=1_000_000,
        output_cost_units_per_million=1_000_000,
    )
    settings, _registries = bundle(tmp_path, api)
    estimate = estimate_reservation(api, input_tokens=1, output_tokens=1)
    result = _Result(
        usage=NormalizedUsage(input_tokens=1, output_tokens=1, duration_ms=1),
        provider_request_id="req-1",
    )

    async def provider(prompt: str) -> dict[str, Any]:
        async def call() -> _Result:
            return result

        await execute_decision_step(
            factory,
            profile=api,
            limits=settings.limits,
            decision_kind="intake",
            estimate=estimate,
            call=call,
        )
        return _valid_output()

    try:
        async with factory.begin() as session:
            run = await run_intake_shadow(
                session,
                context=_context(),
                correlation_id="j3-ledger",
                provider_call=provider,
                allowed_refs=frozenset({"turn:abc"}),
            )
        assert run.provider_calls == 1
        async with factory() as session:
            record = await session.scalar(
                select(UsageRecord).where(UsageRecord.purpose == "intake")
            )
            assert record is not None
            assert record.cost_known is True
    finally:
        await engine.dispose()
