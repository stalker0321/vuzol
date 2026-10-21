"""Real composition/ledger with fake provider I/O; no paid model calls."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_lead_review_regressions import control
from tests.integration.leadership.test_lead_surface import _profile, _settings, _Transport
from tests.integration.leadership.test_managed_goal_flow import (
    PROJECT,
    _enable,
    _factory,
    _runtime,
    _session,
)
from tests.integration.leadership.test_stage2_contracts import expire_claim

from vuzol.config.registries import ProfileRegistry
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.persistence import LeadershipInference, LeadershipTurn, ManagedGoal
from vuzol.projects.model_matrix import set_assignment
from vuzol.providers.budgets import (
    release_orphan_reservations,
    release_unsent_invocation,
    settle_invocation_budget,
)
from vuzol.providers.domain import NormalizedUsage, ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.storage.leasing import claim_outbox_item
from vuzol.storage.models import ProviderBudgetReservation, UsageRecord
from vuzol.storage.types import BudgetReservationStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _claim(factory, worker, owner):
    async with factory.begin() as session:
        token = await claim_outbox_item(
            session,
            owner=owner,
            lease_seconds=120,
            allowed_destinations=frozenset({"project_lead"}),
        )
    prepared = await worker._prepare_turn(token)
    assert prepared is not None and prepared[1] == "claimed"
    return prepared[0]


def _worker(factory, settings, transport, *, owner, profiles=None):
    return build_project_lead_worker(
        factory,
        settings=settings,
        profiles=profiles or ProfileRegistry((_profile(),)),
        owner=owner,
        adapter=transport,
    )


async def test_restart_replays_received_result_without_resending(postgres_dsn, monkeypatch):
    settings = _settings(monkeypatch, "lead-local")
    transport = _Transport({"commands": []})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        first = _worker(factory, settings, transport, owner="before")
        claim = await _claim(factory, first, "before")
        decision = await first._infer(claim)
        assert decision.profile_id == "lead-local"
        # Crash after the provider receipt/usage committed, before saving commands.
        await expire_claim(factory)
        second = _worker(factory, settings, transport, owner="after")
        assert await second.process_one()
        assert len(transport.requests) == 1
        async with factory() as session:
            phase = await session.scalar(select(LeadershipInference))
            goal = await session.scalar(select(ManagedGoal))
            assert phase.state == "received"
            assert goal.waiting_reason == "model_idle"
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1


async def test_restart_preserves_failure_reason_and_diagnostics_without_resending(
    postgres_dsn, monkeypatch
):
    from vuzol.leadership.model import LeadModelUnavailable
    from vuzol.providers.domain import OutputDiagnostic

    calls = []

    class Invalid(_Transport):
        async def execute(self, request, profile, cancellation):
            calls.append(request)
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                True,
                True,
                safe_summary="untrusted-secret-must-not-be-persisted",
                execution_completed=True,
                usage=NormalizedUsage(input_tokens=4435, output_tokens=2460, duration_ms=1),
                diagnostic=OutputDiagnostic(
                    stage="json_parse", content_sha256="a" * 64, content_bytes=8
                ),
            )

    settings = _settings(monkeypatch, "lead-local")
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        first = _worker(factory, settings, Invalid({}), owner="before")
        claim = await _claim(factory, first, "before")
        with pytest.raises(LeadModelUnavailable, match="lead_response_invalid"):
            await first._infer(claim)
        async with factory() as session:
            phase = await session.scalar(select(LeadershipInference))
            saved = phase.result
            assert saved["failure"]["diagnostic"]["stage"] == "json_parse"
            assert "untrusted-secret" not in str(saved)
        await expire_claim(factory)
        second = _worker(factory, settings, Invalid({}), owner="after")
        assert await second.process_one()
        assert len(calls) == 1
        async with factory() as session:
            phase = await session.scalar(select(LeadershipInference))
            goal = await session.scalar(select(ManagedGoal))
            assert phase.result == saved and phase.state == "received"
            assert goal.waiting_reason == "lead_response_invalid"
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1


async def test_switch_during_turn_keeps_receipt_pin_then_new_turn_uses_new_choice(
    postgres_dsn,
    monkeypatch,
    tmp_path,
):
    settings = _settings(monkeypatch, "lead-local")
    other = _profile().model_copy(update={"id": "other-model", "model": "unrelated-model"})
    profiles = ProfileRegistry((_profile(), other))
    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=True)
    bundle = runtime.registries.model_copy(update={"profiles": profiles})
    selected = []

    class Observe(_Transport):
        async def execute(self, request, profile, cancellation):
            selected.append(profile.id)
            return await super().execute(request, profile, cancellation)

    transport = Observe({"commands": []})
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        first = _worker(factory, settings, transport, owner="before", profiles=profiles)
        claim = await _claim(factory, first, "before")
        await first._infer(claim)
        async with factory.begin() as session:
            await set_assignment(
                session,
                project_id=PROJECT,
                slot="lead",
                profile_id=other.id,
                expected_revision=1,
                user_id=42,
                registries=bundle,
            )
        await expire_claim(factory)
        second = _worker(factory, settings, transport, owner="after", profiles=profiles)
        await second.process_one()
        assert selected == ["lead-local"]  # Old receipt, no new inference.
        assert (await control(factory, goal_id, "pause_goal")).status == "applied"
        assert (await control(factory, goal_id, "resume_goal")).status == "applied"
        await second.process_one()
        assert selected == ["lead-local", "other-model"]
        async with factory() as session:
            pins = list(await session.scalars(select(LeadershipTurn.model_assignments)))
            assert {pin["lead"]["profile_id"] for pin in pins} == {"lead-local", "other-model"}


@pytest.mark.parametrize("command", ["pause_goal", "revoke_delegation"])
async def test_authority_loss_cancels_actual_adapter_and_never_applies(
    postgres_dsn,
    monkeypatch,
    command,
):
    settings = _settings(monkeypatch, "lead-local")
    started, stopped = asyncio.Event(), asyncio.Event()

    class InFlight(_Transport):
        async def execute(self, request, profile, cancellation):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                assert cancellation.requested
                stopped.set()

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        worker = _worker(factory, settings, InFlight({}), owner="in-flight")
        work = asyncio.create_task(worker.process_one())
        try:
            await asyncio.wait_for(started.wait(), 5)
            assert (await control(factory, goal_id, command)).status == "applied"
            await asyncio.wait_for(work, 5)
            assert stopped.is_set()
            async with factory() as session:
                goal = await session.get(ManagedGoal, goal_id)
                phase = await session.scalar(select(LeadershipInference))
                assert goal.lifecycle != "active"
                assert goal.proposed_plan is None
                assert phase.state == "unknown"
        finally:
            if not work.done():
                work.cancel()
            await asyncio.gather(work, return_exceptions=True)


async def test_unknown_send_is_not_repeated_or_released_as_never_started(
    postgres_dsn,
    monkeypatch,
):
    settings = _settings(monkeypatch, "lead-local")
    transport = _Transport({"commands": []})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        first = _worker(factory, settings, transport, owner="before")
        claim = await _claim(factory, first, "before")
        await first._infer(claim)
        # Fault injection: the provider answered, but its response transaction
        # was lost. Only the durable send intent and reserved budget survived.
        async with factory.begin() as session:
            phase = await session.scalar(select(LeadershipInference))
            phase.state, phase.result = "sending", None
            usage = await session.scalar(select(UsageRecord))
            await session.delete(usage)
            reservation = await session.get(ProviderBudgetReservation, phase.reservation_id)
            reservation.status = BudgetReservationStatus.RESERVED
            reservation.reconciled_at = None
            reservation.created_at = datetime.now(UTC) - timedelta(hours=1)
        async with factory.begin() as session:
            assert await release_orphan_reservations(session) == 0  # Active turn.
        await expire_claim(factory)
        async with factory.begin() as session:
            assert await release_orphan_reservations(session) == 1
            reservation = await session.scalar(select(ProviderBudgetReservation))
            assert reservation.status is not BudgetReservationStatus.RELEASED
            usage = await session.scalar(select(UsageRecord))
            assert usage.cost_known is False and usage.cost_units > 0
            assert usage.invocation_id == reservation.invocation_id
        second = _worker(factory, settings, transport, owner="after")
        await second.process_one()
        assert len(transport.requests) == 1
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert goal.waiting_reason == "lead_inference_reconciliation"


async def test_authoritative_no_send_releases_budget_without_a_paid_receipt(
    postgres_dsn, monkeypatch
):
    settings = _settings(monkeypatch, "lead-local")

    class Busy(_Transport):
        async def execute(self, request, profile, cancellation):
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                request_sent=False,
                retryable=True,
                safe_summary="account busy",
            )

    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await _worker(factory, settings, Busy({}), owner="busy").process_one()
        async with factory() as session:
            phase = await session.scalar(select(LeadershipInference))
            reservation = await session.scalar(select(ProviderBudgetReservation))
            assert phase.state == "not_sent" and phase.result is None
            assert reservation.status is BudgetReservationStatus.RELEASED
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 0


async def test_late_measured_receipt_replaces_unknown_floor_in_place(postgres_dsn, monkeypatch):
    settings = _settings(monkeypatch, "lead-local")
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await _worker(
            factory, settings, _Transport({"commands": []}), owner="receipt"
        ).process_one()
        async with factory.begin() as session:
            reservation = await session.scalar(select(ProviderBudgetReservation))
            row = await session.scalar(select(UsageRecord))
            identity = row.id
            assert row.cost_known is False
            measured = NormalizedUsage(
                input_tokens=123,
                output_tokens=45,
                cost_units=Decimal("0.123456"),
                duration_ms=30,
            )
            for _ in range(2):
                actual = await settle_invocation_budget(
                    session,
                    reservation=reservation,
                    profile=_profile(),
                    usage=measured,
                    provider_request_id="late-provider-receipt",
                    outcome="succeeded",
                )
                assert actual.id == identity
                assert actual.cost_known is True and actual.late_receipt is True
                assert actual.cost_units == measured.cost_units
                assert actual.input_tokens == 123 and actual.output_tokens == 45
                assert actual.invocation_id == reservation.invocation_id
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1


@pytest.mark.parametrize("unsent", [False, True])
async def test_late_partial_or_unsent_receipt_corrects_unknown_usage(
    postgres_dsn, monkeypatch, unsent
):
    settings = _settings(monkeypatch, "lead-local")
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await _worker(
            factory, settings, _Transport({"commands": []}), owner="partial-receipt"
        ).process_one()
        async with factory.begin() as session:
            reservation = await session.scalar(select(ProviderBudgetReservation))
            row = await session.scalar(select(UsageRecord))
            identity, floor = row.id, row.cost_units
            assert row.cost_known is False
            if unsent:
                # Simulate an orphan estimate corrected by an authoritative
                # admission failure from the still-draining original caller.
                row.outcome = "unknown"
                await release_unsent_invocation(session, reservation)
                assert reservation.status is BudgetReservationStatus.RELEASED
                assert row.cost_known is True and row.cost_units == 0
                assert row.input_tokens == row.output_tokens == 0
                assert row.outcome == "not_sent"
            else:
                actual = await settle_invocation_budget(
                    session,
                    reservation=reservation,
                    profile=_profile(),
                    usage=NormalizedUsage(input_tokens=123, output_tokens=45, duration_ms=3),
                    provider_request_id="partial-receipt",
                    outcome="succeeded",
                )
                assert actual.id == identity and actual.cost_known is False
                assert actual.cost_units == floor
                assert actual.input_tokens == 123 and actual.output_tokens == 45
                assert actual.late_receipt is True
            assert row.invocation_id == reservation.invocation_id
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1


async def test_crash_between_commands_keeps_pins_and_replays_remaining_commands(
    postgres_dsn,
    monkeypatch,
):
    from tests.integration.leadership.test_managed_goal_flow import _start_decision

    from vuzol.leadership.model import decision_payload
    from vuzol.storage.models import WorkPackage

    settings = _settings(monkeypatch, "lead-local")
    transport = _Transport({"commands": decision_payload(_start_decision())["commands"]})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        first = _worker(factory, settings, transport, owner="same-owner")
        original = first._applied_receipt
        count = 0

        async def crash_before_second(principal, key):
            nonlocal count
            count += 1
            if count == 2:
                raise RuntimeError("crash-between-commands")
            return await original(principal, key)

        monkeypatch.setattr(first, "_applied_receipt", crash_before_second)
        with pytest.raises(RuntimeError, match="crash-between-commands"):
            await first.process_one()
        async with factory() as session:
            turn = await session.scalar(select(LeadershipTurn))
            goal = await session.scalar(select(ManagedGoal))
            assert turn.status == "responded" and turn.model_assignments is not None
            assert goal.version > turn.goal_version
        second = _worker(factory, settings, transport, owner="same-owner")
        await second.process_one()
        assert len(transport.requests) == 1
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(WorkPackage)) == 1
            turn = await session.scalar(select(LeadershipTurn))
            assert turn.status == "applied"
