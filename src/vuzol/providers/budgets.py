"""Atomic provider budget reservation and idempotent usage reconciliation."""

import uuid
from dataclasses import dataclass
from decimal import ROUND_UP, Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.settings import HardLimits
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import ProviderBudgetReservation, Step, UsageRecord
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import BudgetReservationStatus, StepStatus

BUDGET_LOCK_KEY = 8_946_527_101
MONEY_QUANTUM = Decimal("0.000001")


class BudgetExceeded(RuntimeError):
    """A hard budget cannot accommodate another provider call."""


@dataclass(frozen=True, slots=True)
class ReservationEstimate:
    input_tokens: int
    output_tokens: int
    cost_units: Decimal
    quota_units: Decimal


def estimate_reservation(
    profile: ProviderProfileConfig,
    *,
    input_tokens: int,
    output_tokens: int,
) -> ReservationEstimate:
    input_rate = Decimal(str(profile.input_cost_units_per_million or 0))
    output_rate = Decimal(str(profile.output_cost_units_per_million or 0))
    calculated = (
        Decimal(input_tokens) * input_rate + Decimal(output_tokens) * output_rate
    ) / Decimal(1_000_000)
    conservative = Decimal(str(profile.minimum_unknown_usage_cost))
    cost = max(calculated, conservative).quantize(MONEY_QUANTUM, rounding=ROUND_UP)
    quota = Decimal(str(profile.quota_units_per_call or 0)).quantize(MONEY_QUANTUM)
    return ReservationEstimate(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_units=cost,
        quota_units=quota,
    )


def account_usage(profile: ProviderProfileConfig, usage: NormalizedUsage) -> NormalizedUsage:
    """Attach configured accounting without treating unknown rates as zero."""

    cost = usage.cost_units
    if (
        cost is None
        and profile.input_cost_units_per_million is not None
        and profile.output_cost_units_per_million is not None
        and usage.input_tokens is not None
        and usage.output_tokens is not None
    ):
        input_rate = Decimal(str(profile.input_cost_units_per_million))
        output_rate = Decimal(str(profile.output_cost_units_per_million))
        cost = (
            Decimal(usage.input_tokens) * input_rate + Decimal(usage.output_tokens) * output_rate
        ) / Decimal(1_000_000)
        cost = cost.quantize(MONEY_QUANTUM, rounding=ROUND_UP)
    quota = usage.quota_units
    if quota is None and profile.quota_units_per_call is not None:
        quota = Decimal(str(profile.quota_units_per_call)).quantize(MONEY_QUANTUM)
    return usage.model_copy(update={"cost_units": cost, "quota_units": quota})


async def reserve_budget(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    profile_id: str,
    provider_attempt: int,
    estimate: ReservationEstimate,
    limits: HardLimits,
) -> ProviderBudgetReservation:
    await session.execute(select(func.pg_advisory_xact_lock(BUDGET_LOCK_KEY)))
    existing = await session.scalar(
        select(ProviderBudgetReservation).where(
            ProviderBudgetReservation.step_id == step_id,
            ProviderBudgetReservation.provider_attempt == provider_attempt,
        )
    )
    if existing is not None:
        return existing

    task_usage = await _usage_totals(session, task_id=task_id)
    step_usage = await _usage_totals(session, step_id=step_id)
    daily_usage = await _daily_usage_totals(session)
    task_reserved = await _reserved_totals(session, task_id=task_id)
    step_reserved = await _reserved_totals(session, step_id=step_id)
    daily_reserved = await _reserved_totals(session)

    if estimate.input_tokens > limits.provider_call_input_tokens:
        raise BudgetExceeded("provider call input-token limit exceeded")
    if estimate.output_tokens > limits.provider_call_output_tokens:
        raise BudgetExceeded("provider call output-token limit exceeded")
    if step_usage[0] + step_reserved[0] + estimate.input_tokens > limits.step_input_tokens:
        raise BudgetExceeded("step input-token limit exceeded")
    if step_usage[1] + step_reserved[1] + estimate.output_tokens > limits.step_output_tokens:
        raise BudgetExceeded("step output-token limit exceeded")
    if task_usage[0] + task_reserved[0] + estimate.input_tokens > limits.task_input_tokens:
        raise BudgetExceeded("task input-token limit exceeded")
    if task_usage[1] + task_reserved[1] + estimate.output_tokens > limits.task_output_tokens:
        raise BudgetExceeded("task output-token limit exceeded")
    if step_usage[2] + step_reserved[2] + estimate.cost_units > Decimal(
        str(limits.step_cost_units)
    ):
        raise BudgetExceeded("step cost limit exceeded")
    if task_usage[2] + task_reserved[2] + estimate.cost_units > Decimal(
        str(limits.task_cost_units)
    ):
        raise BudgetExceeded("task cost limit exceeded")
    if daily_usage[2] + daily_reserved[2] + estimate.cost_units > Decimal(
        str(limits.daily_cost_units)
    ):
        raise BudgetExceeded("daily cost limit exceeded")
    if daily_usage[3] + daily_reserved[3] + estimate.quota_units > Decimal(
        str(limits.daily_quota_units)
    ):
        raise BudgetExceeded("daily quota limit exceeded")

    reservation = ProviderBudgetReservation(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        profile_id=profile_id,
        provider_attempt=provider_attempt,
        reserved_input_tokens=estimate.input_tokens,
        reserved_output_tokens=estimate.output_tokens,
        reserved_cost_units=estimate.cost_units,
        reserved_quota_units=estimate.quota_units,
        status=BudgetReservationStatus.RESERVED,
    )
    session.add(reservation)
    await session.flush()
    return reservation


async def reconcile_usage(
    session: AsyncSession,
    *,
    reservation_id: uuid.UUID,
    token: LeaseToken,
    provider: str,
    model: str,
    usage: NormalizedUsage | None,
    provider_request_id: str | None,
    outcome: str,
    conservative: bool = False,
) -> UsageRecord:
    reservation = await session.scalar(
        select(ProviderBudgetReservation)
        .where(ProviderBudgetReservation.id == reservation_id)
        .with_for_update()
    )
    if reservation is None:
        raise LookupError(f"unknown budget reservation: {reservation_id}")
    existing = await session.scalar(
        select(UsageRecord).where(UsageRecord.reservation_id == reservation_id)
    )
    if existing is not None:
        return existing
    step = await session.scalar(
        select(Step).where(
            Step.id == token.step.id,
            Step.lease_owner == token.owner,
            Step.lease_generation == token.generation,
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
        )
    )
    if step is None or reservation.step_id != step.id:
        raise LeaseLost(f"step lease lost before usage reconciliation: {token.step.id}")

    unknown = usage is None or usage.cost_units is None
    input_tokens = (
        usage.input_tokens
        if usage is not None and usage.input_tokens is not None
        else reservation.reserved_input_tokens
    )
    output_tokens = (
        usage.output_tokens
        if usage is not None and usage.output_tokens is not None
        else reservation.reserved_output_tokens
    )
    cost = (
        usage.cost_units
        if usage is not None and usage.cost_units is not None
        else reservation.reserved_cost_units
    )
    quota = (
        usage.quota_units
        if usage is not None and usage.quota_units is not None
        else reservation.reserved_quota_units
    )
    duration_ms = usage.duration_ms if usage is not None else 0
    reservation.reconciled_input_tokens = input_tokens
    reservation.reconciled_output_tokens = output_tokens
    reservation.reconciled_cost_units = cost
    reservation.reconciled_quota_units = quota
    reservation.provider_request_id = provider_request_id
    reservation.status = (
        BudgetReservationStatus.CONSERVATIVE
        if conservative or unknown
        else BudgetReservationStatus.RECONCILED
    )
    reservation.reconciled_at = func.now()
    record = UsageRecord(
        provider=provider,
        profile_id=reservation.profile_id,
        model=model,
        task_id=reservation.task_id,
        run_id=reservation.run_id,
        step_id=reservation.step_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=usage.cached_tokens if usage is not None else None,
        cost_units=cost,
        quota_units=quota,
        duration_ms=duration_ms,
        provider_request_id=provider_request_id,
        reservation_id=reservation.id,
        outcome=outcome,
    )
    session.add(record)
    await session.flush()
    return record


async def release_reservation(
    session: AsyncSession, *, reservation_id: uuid.UUID, token: LeaseToken
) -> None:
    reservation = await session.scalar(
        select(ProviderBudgetReservation)
        .where(ProviderBudgetReservation.id == reservation_id)
        .with_for_update()
    )
    if reservation is None:
        raise LookupError(f"unknown budget reservation: {reservation_id}")
    step = await session.scalar(
        select(Step).where(
            Step.id == token.step.id,
            Step.lease_owner == token.owner,
            Step.lease_generation == token.generation,
            Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
        )
    )
    if step is None or reservation.step_id != step.id:
        raise LeaseLost(f"step lease lost before budget release: {token.step.id}")
    if reservation.status is BudgetReservationStatus.RESERVED:
        reservation.status = BudgetReservationStatus.RELEASED
        reservation.reconciled_at = func.now()


async def _usage_totals(
    session: AsyncSession,
    *,
    task_id: uuid.UUID | None = None,
    step_id: uuid.UUID | None = None,
) -> tuple[int, int, Decimal, Decimal]:
    statement = select(
        func.coalesce(func.sum(UsageRecord.input_tokens), 0),
        func.coalesce(func.sum(UsageRecord.output_tokens), 0),
        func.coalesce(func.sum(UsageRecord.cost_units), 0),
        func.coalesce(func.sum(UsageRecord.quota_units), 0),
    )
    if task_id is not None:
        statement = statement.where(UsageRecord.task_id == task_id)
    if step_id is not None:
        statement = statement.where(UsageRecord.step_id == step_id)
    row = (await session.execute(statement)).one()
    return int(row[0]), int(row[1]), Decimal(row[2]), Decimal(row[3])


async def _daily_usage_totals(session: AsyncSession) -> tuple[int, int, Decimal, Decimal]:
    statement = select(
        func.coalesce(func.sum(UsageRecord.input_tokens), 0),
        func.coalesce(func.sum(UsageRecord.output_tokens), 0),
        func.coalesce(func.sum(UsageRecord.cost_units), 0),
        func.coalesce(func.sum(UsageRecord.quota_units), 0),
    ).where(UsageRecord.created_at >= func.date_trunc("day", func.now()))
    row = (await session.execute(statement)).one()
    return int(row[0]), int(row[1]), Decimal(row[2]), Decimal(row[3])


async def _reserved_totals(
    session: AsyncSession,
    *,
    task_id: uuid.UUID | None = None,
    step_id: uuid.UUID | None = None,
) -> tuple[int, int, Decimal, Decimal]:
    statement = select(
        func.coalesce(func.sum(ProviderBudgetReservation.reserved_input_tokens), 0),
        func.coalesce(func.sum(ProviderBudgetReservation.reserved_output_tokens), 0),
        func.coalesce(func.sum(ProviderBudgetReservation.reserved_cost_units), 0),
        func.coalesce(func.sum(ProviderBudgetReservation.reserved_quota_units), 0),
    ).where(ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED)
    if task_id is not None:
        statement = statement.where(ProviderBudgetReservation.task_id == task_id)
    if step_id is not None:
        statement = statement.where(ProviderBudgetReservation.step_id == step_id)
    row = (await session.execute(statement)).one()
    return int(row[0]), int(row[1]), Decimal(row[2]), Decimal(row[3])
