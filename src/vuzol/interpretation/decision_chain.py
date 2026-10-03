"""Budgeted decision-chain primitive: pre-reserve, one shared limiter (J1).

A decision chain is the sequence of provider calls behind one semantic decision
(initial, recontext, schema repair, provider fallback). J1 requires that the
call is reserved *before* provider I/O and that every step of the chain draws
from one lifetime limiter — changing ``decision_kind`` never resets it. This
module reuses the existing step-less budget owner
(:func:`vuzol.providers.budgets.reserve_invocation_budget` /
:func:`settle_invocation_budget`): no new ledger, no fake step, no new table.

The chain refuses to call the provider when budget admission fails, when the
kill switch is on, or when apply-time snapshot checks reject the binding. A
late shadow result is recorded as an ``Event`` and never advances state.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.settings import HardLimits
from vuzol.context.decision_binding import (
    DecisionBinding,
    DecisionOutput,
    DecisionSnapshot,
    assert_applicable,
)
from vuzol.providers.budgets import (
    AccountingContext,
    BudgetExceeded,
    ReservationEstimate,
    accounting_for_profile,
    reserve_invocation_budget,
    settle_invocation_budget,
)
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.models import Event, ProviderBudgetReservation

LATE_DECISION_EVENT = "jev.decision_late_recorded"
LATE_DECISION_ENTITY = "jev_decision_late"


class DecisionChainFrozen(RuntimeError):
    """Kill switch is active: no provider call and no state transition."""


class DecisionChainExhausted(BudgetExceeded):
    """The chain-level call/cost limiter is exhausted (shared across kinds)."""


@dataclass(frozen=True, slots=True)
class DecisionChainLimits:
    max_calls: int
    max_cost_units: Decimal


class DecisionChainState:
    """In-process admission guard for one decision chain.

    Counters are monotonic: registering a different ``decision_kind`` or an
    ``attempt_kind`` never resets them (dossier: no budget reset on kind change).
    """

    def __init__(self, limits: DecisionChainLimits) -> None:
        self._limits = limits
        self._calls = 0
        self._cost = Decimal("0")
        self._kinds: set[str] = set()

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def cost_units(self) -> Decimal:
        return self._cost

    @property
    def kinds(self) -> frozenset[str]:
        return frozenset(self._kinds)

    def admit(self, *, decision_kind: str, estimate: ReservationEstimate) -> None:
        if self._calls >= self._limits.max_calls:
            raise DecisionChainExhausted("decision chain call limit exceeded")
        if self._cost + estimate.cost_units > self._limits.max_cost_units:
            raise DecisionChainExhausted("decision chain cost limit exceeded")

    def record(self, *, decision_kind: str, estimate: ReservationEstimate) -> None:
        self._calls += 1
        self._cost += estimate.cost_units
        self._kinds.add(decision_kind)


async def _load_open_reservation(
    session: AsyncSession, invocation_id: uuid.UUID
) -> ProviderBudgetReservation:
    reservation = await session.scalar(
        select(ProviderBudgetReservation).where(
            ProviderBudgetReservation.invocation_id == invocation_id
        )
    )
    if reservation is None:
        raise LookupError(f"unknown decision invocation: {invocation_id}")
    return reservation


async def _settle(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    invocation_id: uuid.UUID,
    profile: ProviderProfileConfig,
    usage: NormalizedUsage | None,
    provider_request_id: str | None,
    outcome: str,
) -> None:
    async with session_factory.begin() as session:
        reservation = await _load_open_reservation(session, invocation_id)
        await settle_invocation_budget(
            session,
            reservation=reservation,
            profile=profile,
            usage=usage,
            provider_request_id=provider_request_id,
            outcome=outcome,
        )


async def execute_decision_step[T](
    session_factory: async_sessionmaker[AsyncSession],
    *,
    profile: ProviderProfileConfig,
    limits: HardLimits,
    decision_kind: str,
    estimate: ReservationEstimate,
    call: Callable[[], Awaitable[T]],
    task_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
    horizon_id: uuid.UUID | None = None,
    chain: DecisionChainState | None = None,
    kill_switch: bool = False,
    invocation_id: uuid.UUID | None = None,
) -> T:
    """Reserve, then call, then settle one step of a decision chain.

    The provider ``call`` is invoked only after a step-less reservation is
    committed. A budget refusal, chain exhaustion or active kill switch raises
    before ``call`` ever runs. A provider crash settles the reservation as an
    unknown-but-non-zero cost (conservative floor), never as zero.
    """

    if kill_switch:
        raise DecisionChainFrozen("decision chain is frozen")
    if chain is not None:
        chain.admit(decision_kind=decision_kind, estimate=estimate)
    invocation_id = invocation_id or uuid.uuid4()
    accounting = accounting_for_profile(profile, purpose=decision_kind, horizon_id=horizon_id)
    async with session_factory.begin() as session:
        await reserve_invocation_budget(
            session,
            invocation_id=invocation_id,
            profile=profile,
            estimate=estimate,
            limits=limits,
            accounting=accounting,
            task_id=task_id,
            run_id=run_id,
        )
    try:
        result = await call()
    except Exception:
        await _settle(
            session_factory,
            invocation_id=invocation_id,
            profile=profile,
            usage=None,
            provider_request_id=None,
            outcome="crashed",
        )
        raise
    await _settle(
        session_factory,
        invocation_id=invocation_id,
        profile=profile,
        usage=getattr(result, "usage", None),
        provider_request_id=getattr(result, "provider_request_id", None),
        outcome="succeeded",
    )
    if chain is not None:
        chain.record(decision_kind=decision_kind, estimate=estimate)
    return result


async def apply_decision[T](
    *,
    binding: DecisionBinding,
    snapshot: DecisionSnapshot,
    apply: Callable[[], Awaitable[T]],
    kill_switch: bool = False,
) -> T:
    """Apply a validated decision only if the snapshot is still current.

    Args:
        binding: runtime-only provenance the decision was made against.
        snapshot: current refs/candidate set/kill switch at the apply point.
        apply: the existing transition callback (never called on refusal).
        kill_switch: explicit override checked before the snapshot.
    """

    if kill_switch:
        raise DecisionChainFrozen("decision chain is frozen")
    assert_applicable(binding, snapshot)
    return await apply()


async def record_late_decision(
    session: AsyncSession,
    *,
    binding: DecisionBinding,
    output: DecisionOutput,
    correlation_id: str,
    actor_id: str = "jev-shadow",
) -> uuid.UUID:
    """Record a late shadow result as an Event; never advances execution."""

    event = Event(
        entity_type=LATE_DECISION_ENTITY,
        entity_id=uuid.uuid5(uuid.NAMESPACE_URL, f"{correlation_id}:{binding.packet_hash}"),
        event_type=LATE_DECISION_EVENT,
        actor_type="jev_shadow_recorder",
        actor_id=actor_id,
        correlation_id=correlation_id,
        payload={
            "binding": binding.model_dump(mode="json"),
            "decision": output.model_dump(mode="json"),
            "late": True,
        },
    )
    session.add(event)
    await session.flush()
    return event.id


def chain_limits_from_context(
    *,
    max_calls: int,
    max_cost_units: object,
    accounting: AccountingContext | None = None,
) -> DecisionChainLimits:
    """Build chain limits; ``accounting`` is accepted for future scoping."""

    del accounting
    return DecisionChainLimits(max_calls=max_calls, max_cost_units=Decimal(str(max_cost_units)))


def snapshot_from_refs(
    refs: Mapping[str, object],
    *,
    candidate_ids: frozenset[str],
    kill_switch: bool = False,
) -> DecisionSnapshot:
    """Test/consumer helper: coerce plain mappings into a snapshot."""

    from vuzol.context.decision_binding import SnapshotRef

    coerced: dict[str, SnapshotRef] = {}
    for ref_id, raw in refs.items():
        if isinstance(raw, SnapshotRef):
            coerced[ref_id] = raw
            continue
        if not isinstance(raw, Mapping):
            raise TypeError(f"snapshot ref {ref_id} must be a mapping")
        revision = raw.get("revision")
        coerced[ref_id] = SnapshotRef(
            ref_id=ref_id,
            revision=revision if isinstance(revision, str) else None,
            content_hash=str(raw["content_hash"]),
            consumed=bool(raw.get("consumed", False)),
        )
    return DecisionSnapshot(refs=coerced, candidate_ids=candidate_ids, kill_switch=kill_switch)
