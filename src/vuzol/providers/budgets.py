"""Atomic provider budget reservation and idempotent usage reconciliation."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_UP, Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.revision import content_revision
from vuzol.config.settings import HardLimits
from vuzol.discussion.horizon import parse_budget
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import (
    MaterializationLink,
    PlanRevision,
    ProviderBudgetReservation,
    Run,
    Step,
    Task,
    UsageRecord,
    WorkAttempt,
    WorkPackage,
)
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import (
    AttemptKind,
    BudgetReservationStatus,
    StepStatus,
)

BUDGET_LOCK_KEY = 8_946_527_101
MONEY_QUANTUM = Decimal("0.000001")
DEFAULT_ACCOUNTING_CURRENCY = "USD"
# Rows written before the accounting ledger existed are marked legacy; orphan
# settlement without a live profile is marked unknown.
LEGACY_PRICING_REVISION = "legacy"
UNKNOWN_PRICING_REVISION = "unknown"
ORPHAN_PROVIDER = "unknown"
ORPHAN_MODEL = "unknown"


class BudgetExceeded(RuntimeError):
    """A hard budget cannot accommodate another provider call."""


@dataclass(frozen=True, slots=True)
class AccountingContext:
    """Orthogonal attribution for one provider invocation (ADR-A02).

    ``purpose`` and ``attempt_kind`` are independent dimensions; the same rows
    produce both the purpose breakdown and the retry subtotal without double
    counting.
    """

    purpose: str
    attempt_kind: str = AttemptKind.INITIAL.value
    pricing_revision: str | None = None
    currency: str = DEFAULT_ACCOUNTING_CURRENCY
    horizon_id: uuid.UUID | None = None


def accounting_for_profile(
    profile: ProviderProfileConfig,
    *,
    purpose: str,
    attempt_kind: str = AttemptKind.INITIAL.value,
    horizon_id: uuid.UUID | None = None,
) -> AccountingContext:
    """Bind a profile to its pricing revision (the current config content hash)."""

    return AccountingContext(
        purpose=purpose,
        attempt_kind=attempt_kind,
        pricing_revision=content_revision(profile),
        currency=DEFAULT_ACCOUNTING_CURRENCY,
        horizon_id=horizon_id,
    )


_STEP_PURPOSE = {
    "plan": "planning",
    "execute_model": "coding",
    "execute_code": "coding",
    "execute_agent": "coding",
    "research_execute": "research",
    "synthesize": "research",
    "privileged_execute": "setup",
    "ensure_capabilities": "setup",
    "ensure_dependencies": "setup",
    # D3: every routed step type is classified explicitly; the "coding"
    # default below covers only legacy/unknown types, never new steps.
    "acceptance": "review",
    "scout": "scout",
}


def purpose_for_step_type(step_type: str) -> str:
    """Map a provider step type to its accounting purpose."""

    return _STEP_PURPOSE.get(step_type, "coding")


async def resolve_horizon_scope(
    session: AsyncSession,
    *,
    task_id: uuid.UUID | None = None,
    intake_id: uuid.UUID | None = None,
) -> uuid.UUID | None:
    """Lifetime owner scope for one invocation (D3, lead Q1).

    Materialized tasks resolve to their WorkPackage id — the same scope the
    lifetime budget and ``_lifetime_spend`` account against. Pre-Task calls
    scope to the intake row identity (project affinity lives on that row).
    ``None`` means no lifetime owner (task caps still apply); it is never
    fabricated.
    """

    if task_id is not None:
        package_id = await resolve_package_id(session, task_id)
        if package_id is not None:
            return package_id
        return None
    return intake_id


async def resolve_package_id(
    session: AsyncSession, task_id: uuid.UUID
) -> uuid.UUID | None:
    """Owning package of a materialized task, if any (D3 counters/fences)."""

    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task_id)
    )
    return link.work_package_id if link is not None else None


async def _lifetime_task_count(session: AsyncSession, horizon_id: uuid.UUID) -> int:
    """Tasks plus attempt rows for a lifetime owner (REDO-3 unit).

    Same unit as ``_lifetime_spend``/``budget_state`` (task-set size), plus
    the finer-grained WorkAttempt rows the lead decision names. Epochs,
    retries and revisions never shrink it.
    """

    usage_tasks = (
        await session.scalars(
            select(UsageRecord.task_id).where(
                UsageRecord.horizon_id == horizon_id,
                UsageRecord.task_id.is_not(None),
            )
        )
    ).all()
    reserved_tasks = (
        await session.scalars(
            select(ProviderBudgetReservation.task_id).where(
                ProviderBudgetReservation.horizon_id == horizon_id,
                ProviderBudgetReservation.task_id.is_not(None),
            )
        )
    ).all()
    tasks = {task_id for task_id in (*usage_tasks, *reserved_tasks)}
    attempts = 0
    if tasks:
        attempts = (
            await session.scalar(
                select(func.count())
                .select_from(WorkAttempt)
                .where(WorkAttempt.task_id.in_(tasks))
            )
        ) or 0
    return len(tasks) + int(attempts)


async def _lifetime_totals(
    session: AsyncSession, horizon_id: uuid.UUID
) -> tuple[int, int, Decimal, Decimal, int]:
    """Settled + outstanding spend for a lifetime owner, no epoch filter.

    Canonical lifetime math (lead Q2, like ``_lifetime_spend``): retry, goal
    revision and epoch changes never erase it. ``budget_epoch`` keeps
    resetting only task/step caps (documented in ACCOUNTING_LEDGER).
    """

    usage = (
        await session.execute(
            select(
                func.coalesce(func.sum(UsageRecord.input_tokens), 0),
                func.coalesce(func.sum(UsageRecord.output_tokens), 0),
                func.coalesce(func.sum(UsageRecord.cost_units), 0),
                func.coalesce(func.sum(UsageRecord.quota_units), 0),
                func.count(),
            ).where(UsageRecord.horizon_id == horizon_id)
        )
    ).one()
    reserved = (
        await session.execute(
            select(
                func.coalesce(
                    func.sum(ProviderBudgetReservation.reserved_input_tokens), 0
                ),
                func.coalesce(
                    func.sum(ProviderBudgetReservation.reserved_output_tokens), 0
                ),
                func.coalesce(
                    func.sum(ProviderBudgetReservation.reserved_cost_units), 0
                ),
                func.coalesce(
                    func.sum(ProviderBudgetReservation.reserved_quota_units), 0
                ),
                func.count(),
            ).where(
                ProviderBudgetReservation.horizon_id == horizon_id,
                ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED,
            )
        )
    ).one()
    return (
        int(usage[0]) + int(reserved[0]),
        int(usage[1]) + int(reserved[1]),
        Decimal(usage[2]) + Decimal(reserved[2]),
        Decimal(usage[3]) + Decimal(reserved[3]),
        int(usage[4]) + int(reserved[4]),
    )


async def _task_lifetime_calls(session: AsyncSession, task_id: uuid.UUID) -> int:
    """Provider invocations for a task across all epochs (D3 counters)."""

    usage_calls = (
        await session.scalar(
            select(func.count())
            .select_from(UsageRecord)
            .where(UsageRecord.task_id == task_id)
        )
    ) or 0
    reserved_calls = (
        await session.scalar(
            select(func.count())
            .select_from(ProviderBudgetReservation)
            .where(
                ProviderBudgetReservation.task_id == task_id,
                ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED,
            )
        )
    ) or 0
    return int(usage_calls) + int(reserved_calls)


async def _task_review_allowance(
    session: AsyncSession, task_id: uuid.UUID
) -> tuple[int, int, Decimal]:
    """Already-consumed review allowance for a task (D3 Q4, shared ledger)."""

    row = (
        await session.execute(
            select(
                func.coalesce(func.sum(ProviderBudgetReservation.allowance_input_tokens), 0),
                func.coalesce(func.sum(ProviderBudgetReservation.allowance_output_tokens), 0),
                func.coalesce(func.sum(ProviderBudgetReservation.allowance_cost_units), 0),
            ).where(ProviderBudgetReservation.task_id == task_id)
        )
    ).one()
    return int(row[0]), int(row[1]), Decimal(row[2])


def attempt_kind_for_payload(payload: object, *, attempt_count: int = 1) -> str:
    """Classify an invocation as initial/repair/retry from persisted facts."""

    mapping = payload if isinstance(payload, dict) else {}
    if mapping.get("repair_context") is not None:
        return AttemptKind.REPAIR.value
    if attempt_count > 1:
        return AttemptKind.RETRY.value
    return AttemptKind.INITIAL.value


def attempt_kind_for_step(step: Step) -> str:
    """Classify an invocation as initial/repair/retry from persisted step facts."""

    payload = step.payload if isinstance(step.payload, dict) else {}
    return attempt_kind_for_payload(payload, attempt_count=step.attempt_count)


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
    task_id: uuid.UUID | None,
    run_id: uuid.UUID | None,
    step_id: uuid.UUID | None,
    profile_id: str,
    provider_attempt: int,
    estimate: ReservationEstimate,
    limits: HardLimits,
    review_allowance: bool = False,
    accounting: AccountingContext | None = None,
    invocation_id: uuid.UUID | None = None,
) -> ProviderBudgetReservation:
    await session.execute(select(func.pg_advisory_xact_lock(BUDGET_LOCK_KEY)))
    if step_id is None:
        # Step-less idempotency keys on the invocation, never on NULL steps
        # (NULL never equals NULL in the unique constraint).
        if invocation_id is None:
            raise ValueError("step-less reserve requires invocation_id")
        existing = await session.scalar(
            select(ProviderBudgetReservation).where(
                ProviderBudgetReservation.invocation_id == invocation_id
            )
        )
        if existing is not None:
            assert isinstance(existing, ProviderBudgetReservation)
            return existing
    else:
        existing = await session.scalar(
            select(ProviderBudgetReservation).where(
                ProviderBudgetReservation.step_id == step_id,
                ProviderBudgetReservation.provider_attempt == provider_attempt,
            )
        )
        if existing is not None:
            assert isinstance(existing, ProviderBudgetReservation)
            return existing

    if task_id is None:
        raise LookupError("workflow budget reserve requires a task")
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise LookupError(f"unknown budget task: {task_id}")
    budget_epoch = task.budget_epoch
    task_usage = await _usage_totals(session, task_id=task_id, budget_epoch=budget_epoch)
    step_usage = await _usage_totals(session, step_id=step_id, budget_epoch=budget_epoch)
    daily_usage = await _daily_usage_totals(session)
    task_reserved = await _reserved_totals(session, task_id=task_id, budget_epoch=budget_epoch)
    step_reserved = await _reserved_totals(session, step_id=step_id, budget_epoch=budget_epoch)
    daily_reserved = await _reserved_totals(session)

    if estimate.input_tokens > limits.provider_call_input_tokens:
        raise BudgetExceeded("provider call input-token limit exceeded")
    if estimate.output_tokens > limits.provider_call_output_tokens:
        raise BudgetExceeded("provider call output-token limit exceeded")
    if step_usage[0] + step_reserved[0] + estimate.input_tokens > limits.step_input_tokens:
        raise BudgetExceeded("step input-token limit exceeded")
    if step_usage[1] + step_reserved[1] + estimate.output_tokens > limits.step_output_tokens:
        raise BudgetExceeded("step output-token limit exceeded")
    # D3 Q4: review calls are subject to task caps like everything else; the
    # old bypass is gone. A deductible allowance pool per task
    # (HardLimits.review_allowance_*) covers the overage part and is
    # recorded on the reservation inside the shared ledger.
    allowed_in = allowed_out = 0
    allowed_cost = Decimal("0")
    if review_allowance:
        used_in, used_out, used_cost = await _task_review_allowance(session, task_id)
        headroom_in = max(limits.review_allowance_input_tokens - used_in, 0)
        headroom_out = max(limits.review_allowance_output_tokens - used_out, 0)
        headroom_cost = max(
            Decimal(str(limits.review_allowance_cost_units)) - used_cost, Decimal("0")
        )
        over_in = max(
            task_usage[0] + task_reserved[0] + estimate.input_tokens
            - limits.task_input_tokens,
            0,
        )
        over_out = max(
            task_usage[1] + task_reserved[1] + estimate.output_tokens
            - limits.task_output_tokens,
            0,
        )
        over_cost = max(
            task_usage[2] + task_reserved[2] + estimate.cost_units
            - Decimal(str(limits.task_cost_units)),
            Decimal("0"),
        )
        allowed_in, allowed_out, allowed_cost = (
            min(estimate.input_tokens, over_in, headroom_in),
            min(estimate.output_tokens, over_out, headroom_out),
            min(estimate.cost_units, over_cost, headroom_cost),
        )
    if (
        task_usage[0] + task_reserved[0] + estimate.input_tokens - allowed_in
        > limits.task_input_tokens
    ):
        raise BudgetExceeded("task input-token limit exceeded")
    if (
        task_usage[1] + task_reserved[1] + estimate.output_tokens - allowed_out
        > limits.task_output_tokens
    ):
        raise BudgetExceeded("task output-token limit exceeded")
    if step_usage[2] + step_reserved[2] + estimate.cost_units > Decimal(
        str(limits.step_cost_units)
    ):
        raise BudgetExceeded("step cost limit exceeded")
    if (
        task_usage[2] + task_reserved[2] + estimate.cost_units - allowed_cost
        > Decimal(str(limits.task_cost_units))
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
    # D3 lifetime gate (lead Q1/Q2): settled + outstanding for the lifetime
    # owner, no epoch filter, checked against the package lifetime budget.
    horizon_id = accounting.horizon_id if accounting is not None else None
    if horizon_id is not None:
        await _enforce_lifetime_budget(
            session,
            horizon_id=horizon_id,
            estimate=estimate,
        )
    # D3 admission counters (lead W4 + REDO-4): enforced alongside caps,
    # 0 = unlimited. Units are documented in ACCOUNTING_LEDGER/ADR-0014.
    if limits.max_provider_calls > 0:
        calls = await _task_lifetime_calls(session, task_id)
        if calls >= limits.max_provider_calls:
            raise BudgetExceeded("task provider-call limit exceeded")
    if limits.max_work_attempts > 0:
        attempt_rows = (
            await session.scalar(
                select(func.count())
                .select_from(WorkAttempt)
                .where(WorkAttempt.task_id == task_id)
            )
        ) or 0
        step_attempts = (
            await session.scalar(
                select(func.coalesce(func.sum(Step.attempt_count), 0))
                .select_from(Step)
                .join(Run, Run.id == Step.run_id)
                .where(Run.task_id == task_id)
            )
        ) or 0
        if int(attempt_rows) + int(step_attempts) >= limits.max_work_attempts:
            raise BudgetExceeded("task work-attempt limit exceeded")
    if limits.max_replans > 0:
        package_id = await resolve_package_id(session, task_id)
        if package_id is not None:
            revisions = (
                await session.scalar(
                    select(func.count())
                    .select_from(PlanRevision)
                    .where(PlanRevision.work_package_id == package_id)
                )
            ) or 0
            if int(revisions) >= limits.max_replans:
                raise BudgetExceeded("package replan limit exceeded")

    reservation = ProviderBudgetReservation(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        invocation_id=invocation_id,
        profile_id=profile_id,
        budget_epoch=budget_epoch,
        provider_attempt=provider_attempt,
        reserved_input_tokens=estimate.input_tokens,
        reserved_output_tokens=estimate.output_tokens,
        reserved_cost_units=estimate.cost_units,
        reserved_quota_units=estimate.quota_units,
        status=BudgetReservationStatus.RESERVED,
        purpose=accounting.purpose if accounting is not None else None,
        attempt_kind=accounting.attempt_kind if accounting is not None else None,
        horizon_id=horizon_id,
        pricing_revision=accounting.pricing_revision if accounting is not None else None,
        currency=accounting.currency if accounting is not None else None,
        allowance_input_tokens=allowed_in,
        allowance_output_tokens=allowed_out,
        allowance_cost_units=allowed_cost,
    )
    session.add(reservation)
    await session.flush()
    return reservation


async def _enforce_lifetime_budget(
    session: AsyncSession,
    *,
    horizon_id: uuid.UUID,
    estimate: ReservationEstimate,
) -> None:
    """Admission-budget check for a lifetime owner (D3 W4, DELTA §D3).

    Totals are canonical lifetime (settled + outstanding, no epoch filter).
    Enforced against the package lifetime budget when the horizon scope is a
    package; other scopes are totaled but uncapped (documented).
    """

    _spent_in, _spent_out, spent_cost, _spent_quota, _spent_calls = await _lifetime_totals(
        session, horizon_id
    )
    package = await session.get(WorkPackage, horizon_id)
    if package is None or package.lifetime_budget is None:
        return
    budget = parse_budget(package.lifetime_budget)
    if budget is None:
        return
    if budget.max_cost is not None and spent_cost + estimate.cost_units > Decimal(
        str(budget.max_cost)
    ):
        raise BudgetExceeded("lifetime owner cost budget exhausted")
    # REDO-3 (lead decision): the attempt gate counts in _lifetime_spend
    # units — tasks plus attempt rows for the owner — never provider calls.
    if budget.max_attempts is not None:
        units = await _lifetime_task_count(session, horizon_id)
        if units >= budget.max_attempts:
            raise BudgetExceeded("lifetime owner attempt budget exhausted")


async def reserve_invocation_budget(
    session: AsyncSession,
    *,
    invocation_id: uuid.UUID,
    profile: ProviderProfileConfig,
    estimate: ReservationEstimate,
    limits: HardLimits,
    accounting: AccountingContext,
    task_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
) -> ProviderBudgetReservation:
    """Reserve a step-less invocation (D3, lead Q3): intake/planning/scout.

    Same atomic lock and idempotency as workflow reserves, keyed by
    ``invocation_id`` (partial unique) instead of ``(step_id, attempt)``.
    No fake Step is created. Task/step caps apply only when a task is bound;
    call, daily and lifetime-owner caps always apply.
    """

    await session.execute(select(func.pg_advisory_xact_lock(BUDGET_LOCK_KEY)))
    existing = await session.scalar(
        select(ProviderBudgetReservation).where(
            ProviderBudgetReservation.invocation_id == invocation_id
        )
    )
    if existing is not None:
        return existing
    if estimate.input_tokens > limits.provider_call_input_tokens:
        raise BudgetExceeded("provider call input-token limit exceeded")
    if estimate.output_tokens > limits.provider_call_output_tokens:
        raise BudgetExceeded("provider call output-token limit exceeded")
    daily_usage = await _daily_usage_totals(session)
    daily_reserved = await _reserved_totals(session)
    if daily_usage[2] + daily_reserved[2] + estimate.cost_units > Decimal(
        str(limits.daily_cost_units)
    ):
        raise BudgetExceeded("daily cost limit exceeded")
    if daily_usage[3] + daily_reserved[3] + estimate.quota_units > Decimal(
        str(limits.daily_quota_units)
    ):
        raise BudgetExceeded("daily quota limit exceeded")
    if task_id is not None:
        task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
        if task is None:
            raise LookupError(f"unknown budget task: {task_id}")
        budget_epoch = task.budget_epoch
        task_usage = await _usage_totals(session, task_id=task_id, budget_epoch=budget_epoch)
        task_reserved = await _reserved_totals(
            session, task_id=task_id, budget_epoch=budget_epoch
        )
        if (
            task_usage[0] + task_reserved[0] + estimate.input_tokens
            > limits.task_input_tokens
        ):
            raise BudgetExceeded("task input-token limit exceeded")
        if (
            task_usage[1] + task_reserved[1] + estimate.output_tokens
            > limits.task_output_tokens
        ):
            raise BudgetExceeded("task output-token limit exceeded")
        if (
            task_usage[2] + task_reserved[2] + estimate.cost_units
            > Decimal(str(limits.task_cost_units))
        ):
            raise BudgetExceeded("task cost limit exceeded")
    if accounting.horizon_id is not None:
        await _enforce_lifetime_budget(
            session,
            horizon_id=accounting.horizon_id,
            estimate=estimate,
        )
    reservation = ProviderBudgetReservation(
        task_id=task_id,
        run_id=run_id,
        step_id=None,
        invocation_id=invocation_id,
        profile_id=profile.id,
        budget_epoch=0,
        provider_attempt=1,
        reserved_input_tokens=estimate.input_tokens,
        reserved_output_tokens=estimate.output_tokens,
        reserved_cost_units=estimate.cost_units,
        reserved_quota_units=estimate.quota_units,
        status=BudgetReservationStatus.RESERVED,
        purpose=accounting.purpose,
        attempt_kind=accounting.attempt_kind,
        horizon_id=accounting.horizon_id,
        pricing_revision=accounting.pricing_revision,
        currency=accounting.currency,
    )
    session.add(reservation)
    await session.flush()
    return reservation


async def settle_invocation_budget(
    session: AsyncSession,
    *,
    reservation: ProviderBudgetReservation,
    profile: ProviderProfileConfig,
    usage: NormalizedUsage | None,
    provider_request_id: str | None,
    outcome: str,
    conservative: bool = False,
) -> UsageRecord:
    """Settle a step-less reservation with measured (or unknown-floor) usage.

    Money-only like ``_settle_reservation``; no lease fencing (there is no
    step). Unknown stays unknown (conservative floor + ``cost_known=false``).
    """

    if reservation.status is not BudgetReservationStatus.RESERVED:
        raise ValueError(f"invocation reservation is not open: {reservation.id}")
    accounted = account_usage(profile, usage) if usage is not None else None
    record = _settle_reservation(
        reservation,
        provider=profile.provider,
        model=profile.model,
        usage=accounted,
        provider_request_id=provider_request_id,
        outcome=outcome,
        conservative=conservative or accounted is None,
        late_receipt=False,
    )
    record.invocation_id = reservation.invocation_id
    session.add(record)
    await session.flush()
    return record


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
    accounting: AccountingContext | None = None,
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
    if accounting is not None:
        _apply_accounting(reservation, accounting)
    record = _settle_reservation(
        reservation,
        provider=provider,
        model=model,
        usage=usage,
        provider_request_id=provider_request_id,
        outcome=outcome,
        conservative=conservative,
        late_receipt=False,
    )
    session.add(record)
    await session.flush()
    return record


def _apply_accounting(
    reservation: ProviderBudgetReservation, accounting: AccountingContext
) -> None:
    reservation.purpose = accounting.purpose
    reservation.attempt_kind = accounting.attempt_kind
    # D3 sticky owner: the reservation row is the persisted intent written at
    # reserve time. A reconcile context without an explicit horizon inherits
    # it instead of wiping it back to NULL.
    if accounting.horizon_id is not None:
        reservation.horizon_id = accounting.horizon_id
    reservation.pricing_revision = accounting.pricing_revision
    reservation.currency = accounting.currency


def _settle_reservation(
    reservation: ProviderBudgetReservation,
    *,
    provider: str,
    model: str,
    usage: NormalizedUsage | None,
    provider_request_id: str | None,
    outcome: str,
    conservative: bool,
    late_receipt: bool,
) -> UsageRecord:
    """Write one invocation row for a locked reservation.

    Money-only: the caller owns any lease/step fencing. Unknown usage falls back
    to the conservative reservation floor and is explicitly marked ``cost_known
    = false`` so that unknown is never reported as zero.
    """

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
    return UsageRecord(
        provider=provider,
        profile_id=reservation.profile_id,
        model=model,
        task_id=reservation.task_id,
        run_id=reservation.run_id,
        step_id=reservation.step_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        # cached_tokens are recorded for provenance but never priced or added on
        # top of input_tokens, so cached usage cannot be double-charged.
        cached_tokens=usage.cached_tokens if usage is not None else None,
        cost_units=cost,
        quota_units=quota,
        duration_ms=duration_ms,
        provider_request_id=provider_request_id,
        reservation_id=reservation.id,
        outcome=outcome,
        purpose=reservation.purpose,
        attempt_kind=reservation.attempt_kind,
        horizon_id=reservation.horizon_id,
        pricing_revision=reservation.pricing_revision,
        currency=reservation.currency,
        cost_known=not unknown,
        late_receipt=late_receipt,
    )


async def record_late_receipt(
    session: AsyncSession,
    *,
    reservation_id: uuid.UUID,
    provider: str,
    model: str,
    usage: NormalizedUsage | None,
    provider_request_id: str | None,
    outcome: str,
    conservative: bool = True,
    accounting: AccountingContext | None = None,
) -> UsageRecord | None:
    """Settle money for a provider call whose originating lease was lost.

    Deliberately NOT fenced by the step lease and deliberately does not touch
    Step/Run/Task business state (report §22): a late receipt accounts cost but
    never advances or rewinds execution. Idempotent by ``reservation_id``.
    """

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
    if reservation.status is BudgetReservationStatus.RELEASED:
        return None
    if accounting is not None:
        _apply_accounting(reservation, accounting)
    record = _settle_reservation(
        reservation,
        provider=provider,
        model=model,
        usage=usage,
        provider_request_id=provider_request_id,
        outcome=outcome,
        conservative=conservative,
        late_receipt=True,
    )
    session.add(record)
    await session.flush()
    return record


async def record_intake_usage(
    session: AsyncSession,
    *,
    profile: ProviderProfileConfig,
    usage: NormalizedUsage | None,
    purpose: str,
    task_id: uuid.UUID | None,
    provider_request_id: str | None = None,
    outcome: str = "succeeded",
    attempt_kind: str = AttemptKind.INITIAL.value,
    horizon_id: uuid.UUID | None = None,
    invocation_id: uuid.UUID | None = None,
) -> UsageRecord:
    """Write one intake/review invocation that has no workflow reservation.

    Uses the same ledger table as workflow calls so intake cost is visible in
    the shared breakdown. Because there is no reservation, an unknown price is
    charged the profile's conservative unknown floor and marked ``cost_known =
    false`` instead of being silently treated as zero.
    """

    accounted = account_usage(profile, usage) if usage is not None else None
    cost_known = accounted is not None and accounted.cost_units is not None
    cost = (
        accounted.cost_units
        if accounted is not None and accounted.cost_units is not None
        else Decimal(str(profile.minimum_unknown_usage_cost)).quantize(
            MONEY_QUANTUM, rounding=ROUND_UP
        )
    )
    quota = accounted.quota_units if accounted is not None else None
    record = UsageRecord(
        provider=profile.provider,
        profile_id=profile.id,
        model=profile.model,
        task_id=task_id,
        run_id=None,
        step_id=None,
        input_tokens=usage.input_tokens if usage is not None else None,
        output_tokens=usage.output_tokens if usage is not None else None,
        cached_tokens=usage.cached_tokens if usage is not None else None,
        cost_units=cost,
        quota_units=quota,
        duration_ms=usage.duration_ms if usage is not None else 0,
        provider_request_id=provider_request_id,
        reservation_id=None,
        outcome=outcome,
        purpose=purpose,
        attempt_kind=attempt_kind,
        horizon_id=horizon_id,
        invocation_id=invocation_id,
        pricing_revision=content_revision(profile),
        currency=DEFAULT_ACCOUNTING_CURRENCY,
        cost_known=cost_known,
        late_receipt=False,
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


async def release_reservation_unfenced(
    session: AsyncSession, *, reservation_id: uuid.UUID
) -> None:
    """Release a reservation whose lease is already gone.

    Used only when it is known that no provider request was sent (e.g. an
    authentication rejection), so the reservation must not be charged by the
    orphan sweep. Never mutates Step/Run/Task state.
    """

    reservation = await session.scalar(
        select(ProviderBudgetReservation)
        .where(ProviderBudgetReservation.id == reservation_id)
        .with_for_update()
    )
    if reservation is None:
        raise LookupError(f"unknown budget reservation: {reservation_id}")
    if reservation.status is BudgetReservationStatus.RESERVED:
        reservation.status = BudgetReservationStatus.RELEASED
        reservation.reconciled_at = func.now()


async def _usage_totals(
    session: AsyncSession,
    *,
    task_id: uuid.UUID | None = None,
    step_id: uuid.UUID | None = None,
    budget_epoch: int | None = None,
) -> tuple[int, int, Decimal, Decimal]:
    statement = select(
        func.coalesce(func.sum(UsageRecord.input_tokens), 0),
        func.coalesce(func.sum(UsageRecord.output_tokens), 0),
        func.coalesce(func.sum(UsageRecord.cost_units), 0),
        func.coalesce(func.sum(UsageRecord.quota_units), 0),
    ).join(
        ProviderBudgetReservation,
        ProviderBudgetReservation.id == UsageRecord.reservation_id,
    )
    if task_id is not None:
        statement = statement.where(UsageRecord.task_id == task_id)
    if step_id is not None:
        statement = statement.where(UsageRecord.step_id == step_id)
    if budget_epoch is not None:
        statement = statement.where(ProviderBudgetReservation.budget_epoch == budget_epoch)
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
    budget_epoch: int | None = None,
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
    if budget_epoch is not None:
        statement = statement.where(ProviderBudgetReservation.budget_epoch == budget_epoch)
    row = (await session.execute(statement)).one()
    return int(row[0]), int(row[1]), Decimal(row[2]), Decimal(row[3])


async def close_step_reservations(
    session: AsyncSession,
    *,
    step_id: uuid.UUID,
    outcome: str,
    release: bool,
    provider: str = ORPHAN_PROVIDER,
    model: str = ORPHAN_MODEL,
    accounting: AccountingContext | None = None,
) -> int:
    """Close every still-RESERVED reservation for a step.

    Used by lease recovery and cancellation where the originating lease is gone,
    so the fenced ``reconcile_usage``/``release_reservation`` paths cannot run.
    ``release=True`` is only safe when the handler never started (the step was
    still LEASED/QUEUED); otherwise the call is charged conservatively.
    """

    reservations = tuple(
        (
            await session.scalars(
                select(ProviderBudgetReservation)
                .where(
                    ProviderBudgetReservation.step_id == step_id,
                    ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED,
                )
                .with_for_update()
            )
        ).all()
    )
    closed = 0
    for reservation in reservations:
        existing = await session.scalar(
            select(UsageRecord.id).where(UsageRecord.reservation_id == reservation.id)
        )
        if existing is not None:
            continue
        if release:
            reservation.status = BudgetReservationStatus.RELEASED
            reservation.reconciled_at = func.now()
        else:
            if accounting is not None:
                _apply_accounting(reservation, accounting)
            session.add(
                _settle_reservation(
                    reservation,
                    provider=provider,
                    model=model,
                    usage=None,
                    provider_request_id=None,
                    outcome=outcome,
                    conservative=True,
                    late_receipt=True,
                )
            )
        closed += 1
    await session.flush()
    return closed


async def release_orphan_reservations(
    session: AsyncSession,
    *,
    older_than_seconds: int = 900,
    batch_size: int = 100,
) -> int:
    """Bounded sweep closing reservations leaked by a dead worker.

    A reservation that is still RESERVED after its step stopped being actively
    leased can otherwise hold daily caps forever. Bounded by age and batch size;
    it never runs inside a request path and never overrides an active lease.
    """

    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=max(0, older_than_seconds))
    reservations = tuple(
        (
            await session.scalars(
                select(ProviderBudgetReservation)
                .where(
                    ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED,
                    ProviderBudgetReservation.created_at < cutoff,
                )
                .order_by(ProviderBudgetReservation.created_at, ProviderBudgetReservation.id)
                .with_for_update(skip_locked=True)
                .limit(batch_size)
            )
        ).all()
    )
    closed = 0
    for reservation in reservations:
        step = await session.scalar(
            select(Step).where(Step.id == reservation.step_id).with_for_update()
        )
        if (
            step is not None
            and step.status in (StepStatus.LEASED, StepStatus.RUNNING)
            and step.lease_expires_at is not None
            and step.lease_expires_at > now
        ):
            continue
        existing = await session.scalar(
            select(UsageRecord.id).where(UsageRecord.reservation_id == reservation.id)
        )
        if existing is not None:
            continue
        never_started = step is None or step.status in (
            StepStatus.PENDING,
            StepStatus.QUEUED,
            StepStatus.LEASED,
        )
        if never_started:
            reservation.status = BudgetReservationStatus.RELEASED
            reservation.reconciled_at = func.now()
        else:
            # The handler may have reached the provider before dying. Charge the
            # conservative floor and mark it unknown rather than dropping it.
            session.add(
                _settle_reservation(
                    reservation,
                    provider=ORPHAN_PROVIDER,
                    model=ORPHAN_MODEL,
                    usage=None,
                    provider_request_id=None,
                    outcome="orphan_recovered",
                    conservative=True,
                    late_receipt=True,
                )
            )
        closed += 1
    await session.flush()
    return closed


async def usage_totals_by_purpose(
    session: AsyncSession, *, task_id: uuid.UUID | None = None
) -> list[tuple[str | None, Decimal, int]]:
    """Purpose breakdown and invocation count (one projection of the rows)."""

    statement = select(
        UsageRecord.purpose,
        func.coalesce(func.sum(UsageRecord.cost_units), 0),
        func.count(),
    ).group_by(UsageRecord.purpose)
    if task_id is not None:
        statement = statement.where(UsageRecord.task_id == task_id)
    rows = (await session.execute(statement)).all()
    return [
        (str(row[0]) if row[0] is not None else None, Decimal(row[1]), int(row[2]))
        for row in rows
    ]


async def usage_retry_subtotal(
    session: AsyncSession, *, task_id: uuid.UUID | None = None
) -> tuple[Decimal, int]:
    """Retry/repair subtotal: another projection of the same rows, never an addend."""

    statement = select(
        func.coalesce(func.sum(UsageRecord.cost_units), 0),
        func.count(),
    ).where(
        UsageRecord.attempt_kind.is_not(None),
        UsageRecord.attempt_kind != AttemptKind.INITIAL.value,
    )
    if task_id is not None:
        statement = statement.where(UsageRecord.task_id == task_id)
    row = (await session.execute(statement)).one()
    return Decimal(row[0]), int(row[1])
