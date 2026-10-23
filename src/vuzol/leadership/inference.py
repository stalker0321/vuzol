"""Step-less provider calls using the existing budget reservation and usage ledger."""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import suppress

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import LaunchMode, ProviderProfileConfig
from vuzol.config.revision import content_revision
from vuzol.config.settings import HardLimits
from vuzol.leadership.invocation_context import (
    TurnInvocationContext,
    current_turn,
    invocation_current,
)
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.persistence import LeadershipInference
from vuzol.observability import get_logger
from vuzol.providers.budgets import (
    BudgetExceeded,
    accounting_for_profile,
    estimate_reservation,
    release_unsent_invocation,
    reserve_invocation_budget,
    settle_invocation_budget,
)
from vuzol.providers.domain import (
    ProviderErrorCategory,
    ProviderFailureDetails,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import ProviderAdapter
from vuzol.storage.models import ProviderBudgetReservation
from vuzol.storage.types import BudgetReservationStatus
from vuzol.workflows.ports import CancellationContext

_logger = get_logger(__name__)


def _failure_reason(failure: ProviderFailureDetails | None, purpose: str) -> str:
    if purpose == "planning":
        return "planner_" + (failure.category.value if failure else "unavailable")
    review = purpose == "project_goal_review"
    if failure is not None and failure.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT:
        return "goal_review_invalid" if review else "lead_response_invalid"
    return "goal_review_unavailable" if review else "lead_model_unavailable"


async def invoke(
    factory: async_sessionmaker[AsyncSession],
    profile: ProviderProfileConfig,
    adapter: ProviderAdapter,
    request: ProviderRequest,
    *,
    purpose: str,
    limits: HardLimits,
) -> ProviderResult:
    context = current_turn.get()
    # Runtime authority is kept out of the model's prompt. The CLI adapter
    # hands the reference to the supervised transport, which verifies it.
    if context is not None and profile.launch_mode is LaunchMode.CLI:
        request = request.model_copy(
            update={
                "sandbox_reference": f"lead-turn:{context.turn_id}",
                "lease_generation": context.claim.lease_epoch,
            }
        )
    input_payload = request.model_dump(
        mode="json",
        exclude={
            "lease_generation",
            "reserved_cost_units",
            "reserved_quota_units",
        },
    )
    input_hash = hashlib.sha256(
        json.dumps(
            input_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    phase = {"project_goal_review": "goal_review", "planning": "planner"}.get(purpose, "lead")
    estimate = estimate_reservation(
        profile, input_tokens=request.max_input_tokens, output_tokens=request.max_output_tokens
    )
    try:
        async with factory.begin() as session:
            if context is not None:
                if not await invocation_current(session, context):
                    raise LeadModelUnavailable("lead_model_unavailable")
                prior = await session.scalar(
                    select(LeadershipInference).where(
                        LeadershipInference.turn_id == context.turn_id,
                        LeadershipInference.phase == phase,
                    )
                )
                if prior is not None:
                    if (
                        prior.id != request.task_id
                        or prior.input_hash != input_hash
                        or prior.profile_id != profile.id
                        or prior.profile_revision != content_revision(profile)
                    ):
                        raise LeadModelUnavailable("lead_inference_reconciliation")
                    if prior.state in {"received", "not_sent"} and prior.result is not None:
                        replay = ProviderResult.model_validate(prior.result)
                        if replay.status is not ProviderResultStatus.SUCCEEDED:
                            raise LeadModelUnavailable(_failure_reason(replay.failure, purpose))
                        return replay
                    if prior.state == "not_sent":
                        raise LeadModelUnavailable("lead_model_unavailable")
                    raise LeadModelUnavailable("lead_inference_reconciliation")
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
            if context is not None:
                session.add(
                    LeadershipInference(
                        id=request.task_id,
                        turn_id=context.turn_id,
                        phase=phase,
                        profile_id=profile.id,
                        profile_revision=content_revision(profile),
                        input_hash=input_hash,
                        request=input_payload,
                        reservation_id=reservation_id,
                        state="sending",
                    )
                )
    except BudgetExceeded as error:
        raise LeadModelUnavailable(
            "planner_budget_exceeded" if phase == "planner" else "lead_budget_exceeded"
        ) from error
    request = request.model_copy(
        update={
            "reserved_cost_units": estimate.cost_units,
            "reserved_quota_units": estimate.quota_units,
        }
    )
    result = None
    not_sent = False
    try:
        async with asyncio.timeout(request.timeout_seconds):
            result = await _send_guarded(factory, adapter, request, profile, context)
        return result
    except ProviderFailure as error:
        not_sent = not error.request_sent
        result = error.receipt_result()
        _logger.warning(
            "Project model invocation failed",
            extra={
                "event": "leadership.provider_failed",
                "invocation_id": str(request.task_id),
                "phase": phase,
                "profile_id": profile.id,
                "failure": error.details().model_dump(mode="json"),
            },
        )
        raise LeadModelUnavailable(_failure_reason(error.details(), purpose)) from error
    except Exception as error:
        _logger.warning(
            "Project model invocation ended without a receipt",
            extra={
                "event": "leadership.provider_receipt_missing",
                "invocation_id": str(request.task_id),
                "phase": phase,
                "profile_id": profile.id,
                "category": "timeout" if isinstance(error, TimeoutError) else "unknown",
                # Without the type the operator cannot tell a bad pin from a crash.
                "error_type": type(error).__name__,
            },
            exc_info=error,
        )
        raise LeadModelUnavailable(_failure_reason(None, purpose)) from error
    finally:
        async with factory.begin() as session:
            stored = await session.get(
                ProviderBudgetReservation, reservation_id, with_for_update=True
            )
            assert stored is not None
            if not_sent:
                await release_unsent_invocation(session, stored)
            elif stored.status is BudgetReservationStatus.RESERVED:
                await settle_invocation_budget(
                    session,
                    reservation=stored,
                    profile=profile,
                    usage=None if result is None else result.usage,
                    provider_request_id=None if result is None else result.provider_request_id,
                    outcome="unknown" if result is None else result.status.value,
                    conservative=result is None,
                )
            elif result is not None:
                await settle_invocation_budget(
                    session,
                    reservation=stored,
                    profile=profile,
                    usage=result.usage,
                    provider_request_id=result.provider_request_id,
                    outcome=result.status.value,
                )
            if context is not None:
                phase_row = await session.get(
                    LeadershipInference, request.task_id, with_for_update=True
                )
                assert phase_row is not None
                phase_row.state = (
                    "not_sent" if not_sent else "unknown" if result is None else "received"
                )
                phase_row.result = None if result is None else result.model_dump(mode="json")


async def _send_guarded(
    factory: async_sessionmaker[AsyncSession],
    adapter: ProviderAdapter,
    request: ProviderRequest,
    profile: ProviderProfileConfig,
    context: TurnInvocationContext | None,
) -> ProviderResult:
    cancellation = CancellationContext()
    if context is None:
        return await adapter.execute(request, profile, cancellation)

    async def admissible() -> bool:
        async with asyncio.timeout(5), factory.begin() as session:
            return await invocation_current(session, context)

    try:
        permitted = await admissible()
    except Exception as error:
        raise ProviderFailure(
            ProviderErrorCategory.PROVIDER_UNAVAILABLE,
            retryable=True,
            request_sent=False,
            safe_summary="lead authority could not be checked before provider send",
        ) from error
    if not permitted:
        raise ProviderFailure(
            ProviderErrorCategory.CANCELLED,
            retryable=False,
            request_sent=False,
            safe_summary="lead authority was revoked before provider send",
        )

    async def watch() -> None:
        while True:
            await asyncio.sleep(0.25)
            if not await admissible():
                return

    execution = asyncio.create_task(adapter.execute(request, profile, cancellation))
    watcher = asyncio.create_task(watch())
    try:
        done, _ = await asyncio.wait((execution, watcher), return_when=asyncio.FIRST_COMPLETED)
        if execution in done:
            # A simultaneous revocation may forbid applying the answer, but
            # must not erase an already available provider/usage receipt.
            return await execution
        if watcher in done:
            # A failed database check is also a lost authority, not permission
            # to continue until the provider happens to finish.
            with suppress(Exception):
                watcher.result()
            raise LeadModelUnavailable("lead_model_unavailable")
        return await execution
    finally:
        cancellation.request()
        for task in (watcher, execution):
            if not task.done():
                task.cancel()
        await asyncio.gather(watcher, execution, return_exceptions=True)
