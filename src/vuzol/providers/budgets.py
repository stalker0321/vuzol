"""Atomic provider budget reservation and idempotent usage reconciliation."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_UP, Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.revision import content_revision
from vuzol.config.settings import HardLimits
from vuzol.providers.control import ceiling_is_admission_only
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import (
    Event,
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


EXPLICIT_MONEY_CAP_EXCEEDED_SUMMARY = (
    "Явный денежный потолок исчерпан. Сделанная работа сохранена. "
    "Увеличение потолка продолжает остаток и не стирает историю."
)
EXPLICIT_MONEY_CAP_UNKNOWN_SUMMARY = (
    "Явный денежный потолок нельзя подтвердить: есть неизвестный расход. "
    "Сделанная работа сохранена. Увеличение потолка само по себе не продолжит задачу. "  # noqa: RUF001
    "Нужна сверка с поздней квитанцией или отдельное решение владельца, "  # noqa: RUF001
    "что эта неопределённость допустима. Неизвестный расход не обнуляется."
)
ADMISSION_ONLY_CONSENT_SUMMARY = (
    "Строгий денежный потолок нельзя гарантировать для этой сессии: адаптер "
    "может отказать до запуска и остановить процесс, но не ограничивает уже "
    "запущенную сессию. Нужно явное согласие владельца на эту ограниченную "
    "гарантию либо контролируемый вызов."
)
# Kept for callers that still import the exceeded wording.
EXPLICIT_MONEY_CAP_SUMMARY = EXPLICIT_MONEY_CAP_EXCEEDED_SUMMARY


async def _note_budget_event(
    session: AsyncSession,
    task_id: uuid.UUID,
    *,
    event_type: str,
    dimension: str,
    detail: str,
) -> None:
    """Record one budget fact per task and dimension.

    The budget advisory lock serializes callers, so a second overrun of the
    same dimension does not enqueue another notification.
    """

    existing = await session.scalar(
        select(Event.id).where(
            Event.entity_type == "task",
            Event.entity_id == task_id,
            Event.event_type == event_type,
            Event.payload["dimension"].as_string() == dimension,
        )
    )
    if existing is not None:
        return
    session.add(
        Event(
            entity_type="task",
            entity_id=task_id,
            event_type=event_type,
            actor_type="budget",
            payload={"dimension": dimension, "detail": detail},
        )
    )


async def _enforce_or_note(
    session: AsyncSession,
    *,
    task_id: uuid.UUID | None,
    exceeded: bool,
    hard: bool,
    dimension: str,
    detail: str,
) -> None:
    if not exceeded:
        return
    if hard or task_id is None:
        raise BudgetExceeded(detail)
    await _note_budget_event(
        session,
        task_id,
        event_type="budget.soft_overrun",
        dimension=dimension,
        detail=detail,
    )


async def note_admission_only_ceiling(
    session: AsyncSession, task_id: uuid.UUID, profile_id: str
) -> None:
    """Say, once, that a running CLI/tool session can outspend its reservation."""

    await _note_budget_event(
        session,
        task_id,
        event_type="budget.ceiling_unenforceable",
        dimension=f"launch:{profile_id}",
        detail=(
            "explicit money cap is checked only before launch; "
            "the adapter cannot cap the running session"
        ),
    )


async def _explicit_money_family(session: AsyncSession, task_id: uuid.UUID) -> set[uuid.UUID]:
    """Root task plus descendants. A child does not receive a fresh ceiling."""

    root = task_id
    seen = {task_id}
    current = task_id
    for _ in range(20):
        parent_id = await session.scalar(select(Task.parent_task_id).where(Task.id == current))
        if parent_id is None or parent_id in seen:
            break
        seen.add(parent_id)
        root = parent_id
        current = parent_id
    family = {root}
    frontier = [root]
    for _ in range(20):
        if not frontier:
            break
        children = (
            await session.scalars(select(Task.id).where(Task.parent_task_id.in_(frontier)))
        ).all()
        frontier = [child for child in children if child not in family]
        family.update(frontier)
    return family


async def _task_family_root(session: AsyncSession, task_id: uuid.UUID) -> uuid.UUID:
    root = task_id
    seen = {task_id}
    current = task_id
    for _ in range(20):
        parent_id = await session.scalar(select(Task.parent_task_id).where(Task.id == current))
        if parent_id is None or parent_id in seen:
            break
        seen.add(parent_id)
        root = parent_id
        current = parent_id
    return root


async def _accepted_unknown_usage_ids(session: AsyncSession, family: set[uuid.UUID]) -> set[str]:
    """Usage ids an owner has explicitly accepted as still unknown."""

    rows = (
        await session.scalars(
            select(Event).where(
                Event.entity_type == "task",
                Event.entity_id.in_(family),
                Event.event_type == "budget.cost_uncertainty_accepted",
            )
        )
    ).all()
    covered: set[str] = set()
    for event in rows:
        payload = event.payload if isinstance(event.payload, dict) else {}
        raw_ids = payload.get("usage_ids")
        if isinstance(raw_ids, list):
            covered.update(str(item) for item in raw_ids)
    return covered


async def _has_admission_only_consent(session: AsyncSession, task_id: uuid.UUID) -> bool:
    root = await _task_family_root(session, task_id)
    existing = await session.scalar(
        select(Event.id).where(
            Event.entity_type == "task",
            Event.entity_id == root,
            Event.event_type == "budget.admission_only_consent",
        )
    )
    return existing is not None


async def _require_admission_only_consent(
    session: AsyncSession,
    *,
    task_id: uuid.UUID | None,
    limits: HardLimits,
    admission_only: bool,
) -> None:
    if not admission_only or task_id is None or limits.explicit_money_cap_units is None:
        return
    if await _has_admission_only_consent(session, task_id):
        return
    raise BudgetExceeded(
        "explicit money cap cannot cover an admission-only launch without owner consent"
    )


async def record_admission_only_consent(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    principal_user_id: int,
    reason: str,
) -> uuid.UUID:
    """Store the owner's acceptance that a strict cap cannot bound a CLI/tool session."""

    if principal_user_id == 0 or not reason.strip():
        raise ValueError("admission-only consent requires an owner and a reason")
    root = await _task_family_root(session, task_id)
    existing = await session.scalar(
        select(Event.id).where(
            Event.entity_type == "task",
            Event.entity_id == root,
            Event.event_type == "budget.admission_only_consent",
        )
    )
    if existing is not None:
        return existing
    row = Event(
        entity_type="task",
        entity_id=root,
        event_type="budget.admission_only_consent",
        actor_type="user",
        actor_id=str(principal_user_id),
        payload={
            "principal_user_id": principal_user_id,
            "reason": reason.strip(),
            "scope": "task_family",
            "root_task_id": str(root),
            "statement": ADMISSION_ONLY_CONSENT_SUMMARY,
        },
    )
    session.add(row)
    await session.flush()
    return row.id


async def reconcile_unknown_usage(
    session: AsyncSession,
    *,
    usage_id: uuid.UUID,
    cost_units: Decimal,
    principal_user_id: int,
) -> None:
    """Replace an unknown floor with a measured receipt. Does not invent zero."""

    if principal_user_id == 0:
        raise ValueError("reconciliation requires an owner")
    if cost_units <= 0:
        raise ValueError("a measured receipt is not zero")
    row = await session.get(UsageRecord, usage_id, with_for_update=True)
    if row is None or row.cost_known or row.task_id is None:
        raise ValueError("unknown usage receipt is missing")
    previous = row.cost_units
    row.cost_units = cost_units
    row.cost_known = True
    row.late_receipt = True
    session.add(
        Event(
            entity_type="task",
            entity_id=row.task_id,
            event_type="budget.cost_reconciled",
            actor_type="user",
            actor_id=str(principal_user_id),
            payload={
                "usage_id": str(usage_id),
                "previous_cost_units": None if previous is None else str(previous),
                "cost_units": str(cost_units),
                "principal_user_id": principal_user_id,
            },
        )
    )
    await session.flush()


async def accept_cost_uncertainty(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    principal_user_id: int,
    reason: str,
) -> None:
    """Owner accepts the current unknown receipts. Later unknown rows stay blocked.

    The stored floor is kept. ``cost_known`` stays false. Epoch changes do not
    cover a receipt that was not listed here.
    """

    if principal_user_id == 0 or not reason.strip():
        raise ValueError("uncertainty acceptance requires an owner and a reason")
    family = await _explicit_money_family(session, task_id)
    unknown_rows = (
        await session.scalars(
            select(UsageRecord).where(
                UsageRecord.task_id.in_(family),
                UsageRecord.cost_known.is_(False),
            )
        )
    ).all()
    if not unknown_rows:
        raise ValueError("no unknown cost to accept")
    root = await _task_family_root(session, task_id)
    session.add(
        Event(
            entity_type="task",
            entity_id=root,
            event_type="budget.cost_uncertainty_accepted",
            actor_type="user",
            actor_id=str(principal_user_id),
            payload={
                "principal_user_id": principal_user_id,
                "reason": reason.strip(),
                "scope": "task_family",
                "usage_ids": [str(row.id) for row in unknown_rows],
                "floors": [
                    None if row.cost_units is None else str(row.cost_units) for row in unknown_rows
                ],
            },
        )
    )
    await session.flush()


async def _enforce_explicit_money_cap(
    session: AsyncSession,
    *,
    task_id: uuid.UUID | None,
    estimate: ReservationEstimate,
    limits: HardLimits,
) -> None:
    """Lifetime owner ceiling. Epoch, retry, and review allowance do not reset it.

    Unknown cost is a floor, not a ceiling, so a strict cap cannot be certified
    while any family receipt is unknown.
    """

    if task_id is None or limits.explicit_money_cap_units is None:
        return
    family = await _explicit_money_family(session, task_id)
    unknown_rows = (
        await session.scalars(
            select(UsageRecord).where(
                UsageRecord.task_id.in_(family),
                UsageRecord.cost_known.is_(False),
            )
        )
    ).all()
    covered = await _accepted_unknown_usage_ids(session, family)
    floors: list[Decimal] = []
    for row in unknown_rows:
        if str(row.id) not in covered or row.cost_units is None:
            raise BudgetExceeded("explicit money cap cannot be certified while cost is unknown")
        floors.append(Decimal(row.cost_units))
    unknown_floor = sum(floors, Decimal("0"))
    usage_cost = await session.scalar(
        select(func.coalesce(func.sum(UsageRecord.cost_units), 0)).where(
            UsageRecord.task_id.in_(family),
            UsageRecord.cost_known.is_(True),
        )
    )
    reserved_cost = await session.scalar(
        select(func.coalesce(func.sum(ProviderBudgetReservation.reserved_cost_units), 0)).where(
            ProviderBudgetReservation.task_id.in_(family),
            ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED,
        )
    )
    spent = Decimal(usage_cost or 0) + Decimal(reserved_cost or 0) + unknown_floor
    if spent + estimate.cost_units > Decimal(str(limits.explicit_money_cap_units)):
        raise BudgetExceeded("explicit money cap exceeded")


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
    "planner": "planning",
    "execute_code": "coding",
    "execute_agent": "coding",
    "research_execute": "research",
    "synthesize": "research",
    "ensure_capabilities": "setup",
    "ensure_dependencies": "setup",
    # D3: every routed step type is classified explicitly; the "coding"
    # default below covers only legacy/unknown types, never new steps.
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


async def resolve_package_id(session: AsyncSession, task_id: uuid.UUID) -> uuid.UUID | None:
    """Owning package of a materialized task, if any (D3 counters/fences)."""

    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task_id)
    )
    return link.work_package_id if link is not None else None


async def _lifetime_task_count(session: AsyncSession, scopes: set[uuid.UUID]) -> int:
    """Tasks plus attempt rows for a lifetime owner (REDO-3 unit).

    Count each task once across goal and package receipts, plus its WorkAttempt
    rows. Epochs, retries and revisions never shrink the history.
    """

    usage_tasks = (
        await session.scalars(
            select(UsageRecord.task_id).where(
                UsageRecord.horizon_id.in_(scopes),
                UsageRecord.task_id.is_not(None),
            )
        )
    ).all()
    reserved_tasks = (
        await session.scalars(
            select(ProviderBudgetReservation.task_id).where(
                ProviderBudgetReservation.horizon_id.in_(scopes),
                ProviderBudgetReservation.task_id.is_not(None),
            )
        )
    ).all()
    tasks = {task_id for task_id in (*usage_tasks, *reserved_tasks)}
    attempts = 0
    if tasks:
        attempts = (
            await session.scalar(
                select(func.count()).select_from(WorkAttempt).where(WorkAttempt.task_id.in_(tasks))
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
                func.coalesce(func.sum(ProviderBudgetReservation.reserved_input_tokens), 0),
                func.coalesce(func.sum(ProviderBudgetReservation.reserved_output_tokens), 0),
                func.coalesce(func.sum(ProviderBudgetReservation.reserved_cost_units), 0),
                func.coalesce(func.sum(ProviderBudgetReservation.reserved_quota_units), 0),
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
            select(func.count()).select_from(UsageRecord).where(UsageRecord.task_id == task_id)
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


async def remaining_output_allowance(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    step_id: uuid.UUID,
    budget_epoch: int,
    requested: int,
    limits: HardLimits,
) -> int:
    """Fit the next invocation to the remaining hard token envelope.

    A retry must not request the entire step ceiling on top of its measured
    first attempt. Outstanding reservations count too. The normal reserve path
    still enforces every money/quota/input limit; this grants no extra budget.
    """
    output = min(requested, limits.provider_call_output_tokens)
    if limits.usage_total_enforcement != "hard":
        return output
    await session.execute(select(func.pg_advisory_xact_lock(BUDGET_LOCK_KEY)))
    task_usage = await _usage_totals(session, task_id=task_id, budget_epoch=budget_epoch)
    task_reserved = await _reserved_totals(session, task_id=task_id, budget_epoch=budget_epoch)
    step_usage = await _usage_totals(session, step_id=step_id, budget_epoch=budget_epoch)
    step_reserved = await _reserved_totals(session, step_id=step_id, budget_epoch=budget_epoch)
    return max(
        0,
        min(
            output,
            limits.task_output_tokens - task_usage[1] - task_reserved[1],
            limits.step_output_tokens - step_usage[1] - step_reserved[1],
        ),
    )


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
    admission_only: bool = False,
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
    usage_hard = limits.usage_total_enforcement == "hard"
    cost_hard = limits.estimated_cost_enforcement == "hard"
    await _enforce_or_note(
        session,
        task_id=task_id,
        exceeded=(
            step_usage[0] + step_reserved[0] + estimate.input_tokens > limits.step_input_tokens
        ),
        hard=usage_hard,
        dimension="step_input_tokens",
        detail="step input-token limit exceeded",
    )
    await _enforce_or_note(
        session,
        task_id=task_id,
        exceeded=(
            step_usage[1] + step_reserved[1] + estimate.output_tokens > limits.step_output_tokens
        ),
        hard=usage_hard,
        dimension="step_output_tokens",
        detail="step output-token limit exceeded",
    )
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
            task_usage[0] + task_reserved[0] + estimate.input_tokens - limits.task_input_tokens,
            0,
        )
        over_out = max(
            task_usage[1] + task_reserved[1] + estimate.output_tokens - limits.task_output_tokens,
            0,
        )
        over_cost = max(
            task_usage[2]
            + task_reserved[2]
            + estimate.cost_units
            - Decimal(str(limits.task_cost_units)),
            Decimal("0"),
        )
        allowed_in, allowed_out, allowed_cost = (
            min(estimate.input_tokens, over_in, headroom_in),
            min(estimate.output_tokens, over_out, headroom_out),
            min(estimate.cost_units, over_cost, headroom_cost),
        )
    await _enforce_or_note(
        session,
        task_id=task_id,
        exceeded=(
            task_usage[0] + task_reserved[0] + estimate.input_tokens - allowed_in
            > limits.task_input_tokens
        ),
        hard=usage_hard,
        dimension="task_input_tokens",
        detail="task input-token limit exceeded",
    )
    await _enforce_or_note(
        session,
        task_id=task_id,
        exceeded=(
            task_usage[1] + task_reserved[1] + estimate.output_tokens - allowed_out
            > limits.task_output_tokens
        ),
        hard=usage_hard,
        dimension="task_output_tokens",
        detail="task output-token limit exceeded",
    )
    await _enforce_or_note(
        session,
        task_id=task_id,
        exceeded=(
            step_usage[2] + step_reserved[2] + estimate.cost_units
            > Decimal(str(limits.step_cost_units))
        ),
        hard=cost_hard,
        dimension="step_cost_units",
        detail="step cost limit exceeded",
    )
    await _enforce_or_note(
        session,
        task_id=task_id,
        exceeded=(
            task_usage[2] + task_reserved[2] + estimate.cost_units - allowed_cost
            > Decimal(str(limits.task_cost_units))
        ),
        hard=cost_hard,
        dimension="task_cost_units",
        detail="task cost limit exceeded",
    )
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
                select(func.count()).select_from(WorkAttempt).where(WorkAttempt.task_id == task_id)
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
    await _require_admission_only_consent(
        session, task_id=task_id, limits=limits, admission_only=admission_only
    )
    await _enforce_explicit_money_cap(session, task_id=task_id, estimate=estimate, limits=limits)

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
    """Enforce lead work limits across current and historical package scopes.

    The persisted horizon_id column remains the accounting owner key. Existing
    package receipts are included, without resetting costs on revision or retry.
    """
    from vuzol.leadership.limits import parse_work_limits
    from vuzol.leadership.persistence import ManagedGoal

    package = await session.get(WorkPackage, horizon_id)
    goal_id = package.managed_goal_id if package is not None else horizon_id
    goal = await session.get(ManagedGoal, goal_id) if goal_id is not None else None
    if goal is None:
        return
    budget = parse_work_limits(goal.constraints.get("work_limits", {}))
    if budget.max_cost is None and budget.max_attempts is None:
        return
    scopes = {
        goal.id,
        *await session.scalars(
            select(WorkPackage.id).where(WorkPackage.managed_goal_id == goal.id)
        ),
    }
    spent_cost = Decimal(0)
    for scope in scopes:
        totals = await _lifetime_totals(session, scope)
        spent_cost += totals[2]
    if budget.max_cost is not None and spent_cost + estimate.cost_units > Decimal(
        str(budget.max_cost)
    ):
        raise BudgetExceeded("lifetime owner cost budget exhausted")
    if (
        budget.max_attempts is not None
        and await _lifetime_task_count(session, scopes) >= budget.max_attempts
    ):
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
    budget_epoch = 0
    if task_id is not None:
        task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
        if task is None:
            raise LookupError(f"unknown budget task: {task_id}")
        # Admission and the stored row must name the same epoch. Epoch 0 is
        # only the bucket for an invocation that is not bound to a task.
        budget_epoch = task.budget_epoch
        task_usage = await _usage_totals(session, task_id=task_id, budget_epoch=budget_epoch)
        task_reserved = await _reserved_totals(session, task_id=task_id, budget_epoch=budget_epoch)
        usage_hard = limits.usage_total_enforcement == "hard"
        cost_hard = limits.estimated_cost_enforcement == "hard"
        await _enforce_or_note(
            session,
            task_id=task_id,
            exceeded=(
                task_usage[0] + task_reserved[0] + estimate.input_tokens > limits.task_input_tokens
            ),
            hard=usage_hard,
            dimension="task_input_tokens",
            detail="task input-token limit exceeded",
        )
        await _enforce_or_note(
            session,
            task_id=task_id,
            exceeded=(
                task_usage[1] + task_reserved[1] + estimate.output_tokens
                > limits.task_output_tokens
            ),
            hard=usage_hard,
            dimension="task_output_tokens",
            detail="task output-token limit exceeded",
        )
        await _enforce_or_note(
            session,
            task_id=task_id,
            exceeded=(
                task_usage[2] + task_reserved[2] + estimate.cost_units
                > Decimal(str(limits.task_cost_units))
            ),
            hard=cost_hard,
            dimension="task_cost_units",
            detail="task cost limit exceeded",
        )
    if accounting.horizon_id is not None:
        await _enforce_lifetime_budget(
            session,
            horizon_id=accounting.horizon_id,
            estimate=estimate,
        )
    await _require_admission_only_consent(
        session,
        task_id=task_id,
        limits=limits,
        admission_only=ceiling_is_admission_only(profile),
    )
    await _enforce_explicit_money_cap(session, task_id=task_id, estimate=estimate, limits=limits)
    reservation = ProviderBudgetReservation(
        task_id=task_id,
        run_id=run_id,
        step_id=None,
        invocation_id=invocation_id,
        profile_id=profile.id,
        budget_epoch=budget_epoch,
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

    accounted = account_usage(profile, usage) if usage is not None else None
    if reservation.status is not BudgetReservationStatus.RESERVED:
        existing = await session.scalar(
            select(UsageRecord)
            .where(UsageRecord.reservation_id == reservation.id)
            .with_for_update()
        )
        if existing is None:
            raise ValueError(f"invocation reservation is not open: {reservation.id}")
        if existing.cost_known or accounted is None:
            return existing
        # The orphan sweep may win the race with a delayed actual receipt.
        # Replace its conservative floor in-place, never add a second charge.
        previous_cost = existing.cost_units
        for field in (
            "input_tokens",
            "output_tokens",
            "cached_tokens",
            "quota_units",
            "cost_units",
        ):
            measured = getattr(accounted, field)
            if measured is not None:
                setattr(existing, field, measured)
                if field != "cached_tokens":
                    setattr(reservation, f"reconciled_{field}", measured)
        if accounted.cost_units is not None:
            existing.cost_known = True
            reservation.status = BudgetReservationStatus.RECONCILED
        existing.provider, existing.model = profile.provider, profile.model
        existing.duration_ms = accounted.duration_ms
        if provider_request_id is not None:
            existing.provider_request_id = provider_request_id
            reservation.provider_request_id = provider_request_id
        existing.outcome, existing.late_receipt = outcome, True
        existing.invocation_id = reservation.invocation_id
        session.add(
            Event(
                entity_type="provider_invocation",
                entity_id=reservation.invocation_id or reservation.id,
                event_type="budget.late_provider_receipt",
                actor_type="system",
                actor_id="provider",
                payload={
                    "reservation_id": str(reservation.id),
                    "previous_cost_units": str(previous_cost),
                    "cost_units": str(existing.cost_units),
                },
            )
        )
        await session.flush()
        return existing
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


async def release_unsent_invocation(
    session: AsyncSession, reservation: ProviderBudgetReservation
) -> None:
    """An authoritative no-send receipt may correct an orphan estimate, never measured spend."""
    existing = await session.scalar(
        select(UsageRecord).where(UsageRecord.reservation_id == reservation.id).with_for_update()
    )
    if existing is not None:
        if existing.cost_known and existing.outcome != "not_sent":
            raise ValueError("no-send receipt conflicts with measured provider usage")
        previous = existing.cost_units
        existing.input_tokens = existing.output_tokens = existing.cached_tokens = 0
        existing.cost_units = existing.quota_units = Decimal(0)
        existing.cost_known, existing.late_receipt = True, True
        existing.outcome = "not_sent"
        existing.invocation_id = reservation.invocation_id
        session.add(
            Event(
                entity_type="provider_invocation",
                entity_id=reservation.invocation_id or reservation.id,
                event_type="budget.authoritative_no_send",
                actor_type="system",
                actor_id="provider",
                payload={"reservation_id": str(reservation.id), "previous_estimate": str(previous)},
            )
        )
    reservation.status = BudgetReservationStatus.RELEASED
    reservation.reconciled_input_tokens = reservation.reconciled_output_tokens = 0
    reservation.reconciled_cost_units = reservation.reconciled_quota_units = Decimal(0)
    reservation.reconciled_at = func.now()
    await session.flush()


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
        invocation_id=reservation.invocation_id,
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


async def release_reservation_unfenced(session: AsyncSession, *, reservation_id: uuid.UUID) -> None:
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
        if reservation.invocation_id is not None:
            from vuzol.leadership.persistence import (
                LeadershipInference,
                LeadershipTurn,
                ManagedGoal,
            )

            phase = await session.get(LeadershipInference, reservation.invocation_id)
            if phase is not None:
                turn = await session.get(LeadershipTurn, phase.turn_id)
                goal = None if turn is None else await session.get(ManagedGoal, turn.goal_id)
                if (
                    goal is not None
                    and turn is not None
                    and goal.lease_epoch == turn.lease_epoch
                    and goal.lease_until is not None
                    and goal.lease_until > now
                    and turn.status == "claimed"
                ):
                    continue
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
        never_started = reservation.invocation_id is None and (
            step is None
            or step.status
            in (
                StepStatus.PENDING,
                StepStatus.QUEUED,
                StepStatus.LEASED,
            )
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


