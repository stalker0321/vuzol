"""J1 decision binding + chain PostgreSQL integration (transaction/competition)."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tests.integration.providers._test_routing_helpers import (
    BudgetExceeded,
    Decimal,
    bundle,
    estimate_reservation,
    profile,
    seed_provider_step,
    storage,
)
from vuzol.context.decision_binding import (
    Coverage,
    DecisionBinding,
    DecisionOutput,
    DecisionSnapshot,
    DecisionStatus,
)
from vuzol.interpretation.decision_chain import (
    LATE_DECISION_EVENT,
    DecisionChainFrozen,
    apply_decision,
    execute_decision_step,
    record_late_decision,
)
from vuzol.ops.retention import pin_artifacts_for_audit
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.models import (
    Artifact,
    Event,
    ProviderBudgetReservation,
    Task,
    UsageRecord,
)
from vuzol.storage.types import TaskStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@dataclass(frozen=True, slots=True)
class _Result:
    usage: NormalizedUsage | None
    provider_request_id: str | None


def _binding() -> DecisionBinding:
    return DecisionBinding(
        decision_kind="intake",
        request_hash="a" * 64,
        schema_hash="b" * 64,
        prompt_hash="c" * 64,
        packet_hash="d" * 64,
        coverage=Coverage.COMPLETE,
    )


async def test_budget_refusal_never_calls_provider(postgres_dsn: str, tmp_path: Path) -> None:
    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    limits = settings.limits.model_copy(update={"task_cost_units": 0.001})
    api = profile("api")
    estimate = estimate_reservation(api, input_tokens=1, output_tokens=1)
    calls: list[str] = []

    async def provider() -> _Result:
        calls.append("called")
        return _Result(usage=None, provider_request_id=None)

    try:
        task_id, run_id, _step = await seed_provider_step(factory)
        with pytest.raises(BudgetExceeded):
            await execute_decision_step(
                factory,
                profile=api,
                limits=limits,
                decision_kind="intake",
                estimate=estimate,
                call=provider,
                task_id=task_id,
                run_id=run_id,
            )
        assert calls == []
    finally:
        await engine.dispose()


async def test_concurrent_decision_calls_share_one_limit(postgres_dsn: str, tmp_path: Path) -> None:
    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    limits = settings.limits.model_copy(update={"task_cost_units": 0.015})
    api = profile("api")
    estimate = estimate_reservation(api, input_tokens=1, output_tokens=1)
    calls: list[str] = []

    async def provider() -> _Result:
        calls.append("called")
        return _Result(
            usage=NormalizedUsage(input_tokens=1, output_tokens=1, duration_ms=1),
            provider_request_id="req-1",
        )

    try:
        task_id, run_id, _step = await seed_provider_step(factory)

        async def run() -> _Result:
            return await execute_decision_step(
                factory,
                profile=api,
                limits=limits,
                decision_kind="intake",
                estimate=estimate,
                call=provider,
                task_id=task_id,
                run_id=run_id,
            )

        results = await asyncio.gather(run(), run(), return_exceptions=True)
        successes = [item for item in results if not isinstance(item, BaseException)]
        failures = [item for item in results if isinstance(item, BaseException)]
        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], BudgetExceeded)
        assert len(calls) == 1
    finally:
        await engine.dispose()


async def test_provider_crash_is_not_recorded_as_zero_cost(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    settings, _registries = bundle(tmp_path, profile("api"))
    api = profile("api")
    estimate = estimate_reservation(api, input_tokens=1, output_tokens=1)
    invocation_id = uuid.uuid4()

    async def provider() -> _Result:
        raise RuntimeError("provider crashed")

    try:
        task_id, run_id, _step = await seed_provider_step(factory)
        with pytest.raises(RuntimeError):
            await execute_decision_step(
                factory,
                profile=api,
                limits=settings.limits,
                decision_kind="intake",
                estimate=estimate,
                call=provider,
                task_id=task_id,
                run_id=run_id,
                invocation_id=invocation_id,
            )
        async with factory() as session:
            reservation = await session.scalar(
                select(ProviderBudgetReservation).where(
                    ProviderBudgetReservation.invocation_id == invocation_id
                )
            )
            assert reservation is not None
            record = await session.scalar(
                select(UsageRecord).where(UsageRecord.reservation_id == reservation.id)
            )
            assert record is not None
            assert record.cost_known is False
            assert record.cost_units == estimate.cost_units
            assert record.cost_units > Decimal("0")
            assert record.outcome == "crashed"
    finally:
        await engine.dispose()


async def test_late_decision_and_kill_switch_never_change_state(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        task_id, _run_id, _step = await seed_provider_step(factory)
        applied: list[str] = []

        async def apply() -> None:
            applied.append("applied")

        with pytest.raises(DecisionChainFrozen):
            await apply_decision(
                binding=_binding(),
                snapshot=DecisionSnapshot(refs={}, candidate_ids=frozenset()),
                apply=apply,
                kill_switch=True,
            )
        async with factory.begin() as session:
            await record_late_decision(
                session,
                binding=_binding(),
                output=DecisionOutput(
                    decision_kind="intake",
                    status=DecisionStatus.ABSTAIN,
                    reason="late_result",
                ),
                correlation_id="j1-late",
            )
        assert applied == []
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task is not None and task.status is TaskStatus.EXECUTING
            event = await session.scalar(
                select(Event).where(Event.event_type == LATE_DECISION_EVENT)
            )
            assert event is not None and event.payload["late"] is True
    finally:
        await engine.dispose()


async def test_pin_artifacts_for_audit_only_extends(postgres_dsn: str, tmp_path: Path) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        task_id, _run_id, _step = await seed_provider_step(factory)
        now = datetime.now(UTC)
        old = now - timedelta(days=1)
        async with factory.begin() as session:
            artifact = Artifact(
                task_id=task_id,
                artifact_type="decision-input",
                content_uri="artifact://decision/input",
                size_bytes=1,
                content_hash="a" * 64,
                media_type="application/json",
                sensitivity="private",
                visibility="task",
                retention_until=old,
                metadata_json={},
            )
            session.add(artifact)
            await session.flush()
            artifact_id = artifact.id
        async with factory.begin() as session:
            assert (
                await pin_artifacts_for_audit(
                    session, artifact_ids=(artifact_id,), retention_days=30, now=now
                )
                == 1
            )
        async with factory() as session:
            row = await session.get(Artifact, artifact_id)
            assert row is not None and row.retention_until > old
        async with factory.begin() as session:
            assert (
                await pin_artifacts_for_audit(
                    session, artifact_ids=(artifact_id,), retention_days=30, now=now
                )
                == 0
            )
    finally:
        await engine.dispose()
