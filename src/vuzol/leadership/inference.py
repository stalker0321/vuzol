"""Step-less provider calls using the existing budget reservation and usage ledger."""

from __future__ import annotations

import asyncio

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.settings import HardLimits
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.providers.budgets import (
    BudgetExceeded,
    accounting_for_profile,
    estimate_reservation,
    reserve_invocation_budget,
    settle_invocation_budget,
)
from vuzol.providers.domain import ProviderRequest, ProviderResult
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import ProviderAdapter
from vuzol.storage.models import ProviderBudgetReservation
from vuzol.storage.types import BudgetReservationStatus
from vuzol.workflows.ports import CancellationContext


async def invoke(
    factory: async_sessionmaker[AsyncSession],
    profile: ProviderProfileConfig,
    adapter: ProviderAdapter,
    request: ProviderRequest,
    *,
    purpose: str,
    limits: HardLimits,
) -> ProviderResult:
    estimate = estimate_reservation(
        profile, input_tokens=request.max_input_tokens, output_tokens=request.max_output_tokens
    )
    try:
        async with factory.begin() as session:
            # A durable invocation without a decision is an accounting/recovery case,
            # not permission to send the same paid request again.
            from vuzol.providers.budgets import BUDGET_LOCK_KEY

            await session.execute(select(func.pg_advisory_xact_lock(BUDGET_LOCK_KEY)))
            existing = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.invocation_id == request.task_id
                )
            )
            if existing is not None:
                raise LeadModelUnavailable("lead_inference_reconciliation")
            active = await session.scalar(
                select(func.count())
                .select_from(ProviderBudgetReservation)
                .where(
                    ProviderBudgetReservation.profile_id == profile.id,
                    ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED,
                )
            )
            if int(active or 0) >= profile.concurrency_limit:
                raise BudgetExceeded("profile concurrency limit exceeded")
            reservation = await reserve_invocation_budget(
                session,
                invocation_id=request.task_id,
                profile=profile,
                estimate=estimate,
                limits=limits,
                accounting=accounting_for_profile(profile, purpose=purpose),
            )
            reservation_id = reservation.id
    except BudgetExceeded as error:
        raise LeadModelUnavailable("lead_budget_exceeded") from error
    request = request.model_copy(
        update={
            "reserved_cost_units": estimate.cost_units,
            "reserved_quota_units": estimate.quota_units,
        }
    )
    result = None
    try:
        async with asyncio.timeout(request.timeout_seconds):
            result = await adapter.execute(request, profile, CancellationContext())
        return result
    except ProviderFailure as error:
        result = error.receipt_result()
        raise LeadModelUnavailable("lead_model_unavailable") from error
    except Exception as error:
        raise LeadModelUnavailable("lead_model_unavailable") from error
    finally:
        async with factory.begin() as session:
            stored = await session.get(
                ProviderBudgetReservation, reservation_id, with_for_update=True
            )
            assert stored is not None
            await settle_invocation_budget(
                session,
                reservation=stored,
                profile=profile,
                usage=None if result is None else result.usage,
                provider_request_id=None if result is None else result.provider_request_id,
                outcome="unknown" if result is None else result.status.value,
                conservative=result is None,
            )
