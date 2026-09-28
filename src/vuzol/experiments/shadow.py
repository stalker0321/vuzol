"""Shadow decision records and route reports (WP11).

Shadow records live in the existing ``Event`` ledger (no new tables, no
migrations) next to the rules decisions they observe. A shadow record never
changes execution: production transitions read only the rules table.
Route reports reuse the WP13 analysis patterns (``summarize_arm``) with the
downstream cost folded into each record — no duplicated statistics.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.experiments.analysis import TrialRecord, summarize_arm
from vuzol.experiments.decision import TriageDecision
from vuzol.storage.models import Event

EVENT_TYPE = "jev.shadow_recorded"
ENTITY_TYPE = "jev_shadow_decision"


async def record_shadow_decision(
    session: AsyncSession,
    decision: TriageDecision,
    *,
    correlation_id: str,
    rules_action: str,
    actor_id: str = "jev-shadow",
) -> uuid.UUID:
    """Persist one shadow record; the rules action is stored for comparison."""

    event = Event(
        entity_type=ENTITY_TYPE,
        entity_id=uuid.uuid5(uuid.NAMESPACE_URL, f"{correlation_id}:{decision.decision_hash}"),
        event_type=EVENT_TYPE,
        actor_type="jev_shadow_recorder",
        actor_id=actor_id,
        correlation_id=correlation_id,
        payload={
            "decision": decision.canonical(),
            "decision_sha256": decision.decision_hash,
            "rules_action": rules_action,
            "agrees_with_rules": rules_action == decision.choice.value,
        },
    )
    session.add(event)
    await session.flush()
    return event.id


async def load_shadow_records(
    session: AsyncSession, correlation_id: str
) -> tuple[dict[str, Any], ...]:
    from sqlalchemy import select

    events = await session.scalars(
        select(Event)
        .where(Event.event_type == EVENT_TYPE, Event.correlation_id == correlation_id)
        .order_by(Event.created_at, Event.id)
    )
    return tuple(dict(event.payload) for event in events)


def report_routes(
    rows: tuple[tuple[str, bool, str, str], ...],
) -> dict[str, Any]:
    """Compare rules/cheap/strong routes on downstream total cost.

    Each row is (route, verified, cost, downstream_cost); totals fold
    downstream retries/repairs in. Summaries reuse ``summarize_arm`` so the
    denominator rules (failures stay in, 0 successes → undefined) match WP13.
    """

    by_route: dict[str, list[TrialRecord]] = {}
    for index, (route, verified, cost, downstream) in enumerate(rows):
        total = Decimal(cost) + Decimal(downstream)
        by_route.setdefault(route, []).append(
            TrialRecord(
                pair_id=f"{route}:{index}",
                task_id=f"{route}:{index}",
                family="repair-triage",
                arm=route,
                status="verified_success" if verified else "failed",
                verified=verified,
                cost=total,
            )
        )
    routes = {route: summarize_arm(tuple(members)) for route, members in sorted(by_route.items())}
    return {
        "schema_version": "jev-route-report.v1",
        "routes": routes,
        "downstream_note": "totals include downstream retry/repair cost",
        "inconclusive": any(summary["c_success_undefined"] for summary in routes.values()),
    }
