"""Research fallback uses real inference receipts, with an in-memory accounting store."""

import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from tests.unit.leadership.test_jobs import _running
from tests.unit.providers.test_pi_adapter import profile as pi_profile

from vuzol.config.models import ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import assignment_revision
from vuzol.config.settings import Settings
from vuzol.leadership import inference
from vuzol.leadership.composition import _TurnModels
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import TurnInvocationContext, current_turn
from vuzol.leadership.job_runner import ResearchJobRunner
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.persistence import LeadershipInference, LeadJob
from vuzol.projects.model_matrix import ModelAssignment
from vuzol.providers.domain import (
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.storage.models import ProviderBudgetReservation
from vuzol.storage.types import BudgetReservationStatus


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Store:
    def __init__(self):
        self.receipts = {}
        self.reservations = {}

    @asynccontextmanager
    async def begin(self):
        yield self

    __call__ = begin

    async def get(self, model, identity, **kwargs):
        return (self.receipts if model is LeadershipInference else self.reservations).get(identity)

    async def scalar(self, query):
        entity = query.column_descriptions[0]["entity"]
        params = query.compile().params
        if entity is LeadershipInference:
            return next(
                (
                    row
                    for row in self.receipts.values()
                    if row.turn_id == params["turn_id_1"]
                    and row.phase == params["phase_1"]
                    and row.round == params["round_1"]
                ),
                None,
            )
        if "count(" in str(query):
            return sum(
                r.status is BudgetReservationStatus.RESERVED for r in self.reservations.values()
            )
        assert entity is ProviderBudgetReservation
        return self.reservations.get(params["invocation_id_1"])

    async def execute(self, query):
        pass

    def add(self, row):
        self.receipts[row.id] = row


@pytest.fixture
def chain(monkeypatch):
    base = pi_profile().model_copy(
        update={
            "id": "codex-a",
            "provider": "codex",
            "model": "gpt-6.1-sol",
            "model_reasoning_effort": "medium",
            "runtime_identity": "vuzol-codex-a",
            "state_directory": pi_profile().state_directory.parent / "codex-test",
            "roles": frozenset({ProviderRole.PLANNER}),
        }
    )
    fallback = pi_profile().model_copy(
        update={"id": "pi-opencode-go-c", "model": "deepseek-v4.1-flash"}
    )
    choices = [
        {"profile_id": base.id, "model": "gpt-6.1-terra", "reasoning_effort": "high"},
        {"profile_id": fallback.id},
    ]
    store = Store()

    async def reserve(session, *, invocation_id, **kwargs):
        row = SimpleNamespace(id=invocation_id, status=BudgetReservationStatus.RESERVED)
        store.reservations[invocation_id] = row
        return row

    async def settle(session, *, reservation, **kwargs):
        reservation.status = BudgetReservationStatus.RECONCILED

    async def release(session, reservation):
        reservation.status = BudgetReservationStatus.RELEASED

    monkeypatch.setattr(inference, "reserve_invocation_budget", reserve)
    monkeypatch.setattr(inference, "settle_invocation_budget", settle)
    monkeypatch.setattr(inference, "release_unsent_invocation", release)
    monkeypatch.setattr(inference, "invocation_current", AsyncMock(return_value=True))
    adapters = {base.id: AsyncMock(), fallback.id: AsyncMock()}
    answer = ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        structured_output={"report": "Ответ: src/a.py:1"},
        usage=NormalizedUsage(duration_ms=2, input_tokens=10),
        adapter_version="test",
    )
    for adapter in adapters.values():
        adapter.execute.return_value = answer
    model = _TurnModels(
        store,
        Settings(_env_file=None, project_lead={"research_models": choices}),
        ProfileRegistry((base, fallback)),
        None,
        SimpleNamespace(get=adapters.__getitem__),
        None,
        None,
        None,
    )
    pin = ModelAssignment(
        profile_id=base.id,
        model=base.model,
        reasoning_effort=base.model_reasoning_effort,
        content_revision=assignment_revision(base),
    )
    model._pins = AsyncMock(return_value={"planner_strong": pin})
    _, goal, job, claim = _running()
    context = TurnInvocationContext(
        TurnClaim(goal.id, job.id, claim.owner, claim.generation, claim.generation),
        uuid.uuid5(job.id, "attempt:1"),
        "p",
        1,
        1,
        1,
        job=True,
    )
    token = current_turn.set(context)
    yield SimpleNamespace(
        model=model,
        store=store,
        adapters=adapters,
        base=base,
        fallback=fallback,
        job=job,
        claim=claim,
        pin=pin,
        answer=answer,
    )
    current_turn.reset(token)


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["unsent", "completed", "invalid", "truncated"])
async def test_definitive_failure_falls_back_and_replays_without_paying_again(chain, kind):
    first = chain.adapters[chain.base.id]
    if kind in {"unsent", "completed"}:
        first.execute.side_effect = ProviderFailure(
            ProviderErrorCategory.RATE_LIMITED,
            retryable=True,
            request_sent=kind != "unsent",
            execution_completed=kind == "completed",
        )
        reason = "rate_limited"
    else:
        first.execute.return_value = chain.answer.model_copy(
            update=(
                {"structured_output": {"report": ""}}
                if kind == "invalid"
                else {"finish_reason": "length"}
            )
        )
        reason = "invalid_structured_output" if kind == "invalid" else "output_truncated"
    report, usage = await chain.model.research(chain.job)
    assert report == chain.answer.structured_output["report"]
    assert usage["profile_id"] == chain.fallback.id and usage["model"] == chain.fallback.model
    assert usage["failed_attempts"] == [
        {"profile_id": chain.base.id, "model": "gpt-6.1-terra", "reason": reason}
    ]
    assert first.execute.call_args.args[1].model == "gpt-6.1-terra"
    assert first.execute.call_args.args[1].model_reasoning_effort == "high"
    for index, profile in enumerate((chain.base, chain.fallback)):
        request = chain.adapters[profile.id].execute.call_args.args[0]
        assert request.task_id == uuid.uuid5(chain.job.id, f"research:{index}:{profile.id}")
    assert len(chain.store.reservations) == 2
    assert {row.round for row in chain.store.receipts.values()} == {0, 1}
    # A new lease/turn must find the job's old receipts, including failures without usage.
    current_turn.set(replace(current_turn.get(), turn_id=uuid.uuid4()))
    assert await chain.model.research(chain.job) == (report, usage)
    assert all(adapter.execute.await_count == 1 for adapter in chain.adapters.values())
    planner, _ = chain.model._resolve(chain.pin, "planner_strong")
    assert planner.model == "gpt-6.1-sol" and planner.model_reasoning_effort == "medium"


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["timeout", "sent_failure", "receipt_without_completion"])
async def test_unknown_outcome_fails_job_without_fallback_even_on_replay(chain, kind):
    first = chain.adapters[chain.base.id]
    first.execute.side_effect = (
        TimeoutError()
        if kind == "timeout"
        else ProviderFailure(
            ProviderErrorCategory.TIMEOUT,
            retryable=True,
            request_sent=True,
            usage=NormalizedUsage(duration_ms=3) if kind == "receipt_without_completion" else None,
        )
    )
    runner = ResearchJobRunner(chain.store, owner="runner", model=chain.model, enabled=lambda: True)
    # _research loads the durable job before invoking the actual model.
    get = chain.store.get

    # Keep inference receipt/reservation lookups on the accounting store.
    async def lookup(model, identity, **kwargs):
        return chain.job if model is LeadJob else await get(model, identity, **kwargs)

    chain.store.get = AsyncMock(side_effect=lookup)
    report, reason, usage = await runner._research(chain.claim)
    assert report is None and reason is not None
    assert usage["failed_attempts"][0]["profile_id"] == chain.base.id
    chain.adapters[chain.fallback.id].execute.assert_not_awaited()
    current_turn.set(replace(current_turn.get(), turn_id=uuid.uuid4()))
    with pytest.raises(LeadModelUnavailable):
        await chain.model.research(chain.job)
    assert first.execute.await_count == 1
    chain.adapters[chain.fallback.id].execute.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("bad", ["missing", "disabled", "no_read", "invalid_override"])
async def test_invalid_chain_fails_before_any_send(chain, bad):
    if bad == "missing":
        chain.model._profiles = ProfileRegistry((chain.base,))
    elif bad == "invalid_override":
        chain.model._settings.project_lead.research_models[0] = (
            chain.model._settings.project_lead.research_models[0].model_copy(
                update={"model": "--unsafe"}
            )
        )
    else:
        invalid = chain.fallback.model_copy(
            update={"enabled": False} if bad == "disabled" else {"capabilities": frozenset()}
        )
        chain.model._profiles = ProfileRegistry((chain.base, invalid))
    with pytest.raises(LeadModelUnavailable, match="research_model_unavailable"):
        await chain.model.research(chain.job)
    assert all(adapter.execute.await_count == 0 for adapter in chain.adapters.values())


@pytest.mark.anyio
async def test_empty_choices_keep_the_planner_pin_and_legacy_invocation(chain):
    chain.model._settings = Settings(_env_file=None)
    _, usage = await chain.model.research(chain.job)
    request, profile, _ = chain.adapters[chain.base.id].execute.call_args.args
    assert request.task_id == uuid.uuid5(chain.job.id, "research")
    assert profile is chain.base and profile.model == "gpt-6.1-sol"
    assert "executor_model_override" not in request.task_draft
    assert usage == chain.answer.usage.model_dump(mode="json")


@pytest.mark.anyio
async def test_first_answer_shortcircuits_the_chain_without_a_planner_pin(chain):
    chain.model._pins.side_effect = AssertionError("configured research needs no planner pin")
    _, usage = await chain.model.research(chain.job)
    assert usage["model"] == "gpt-6.1-terra" and usage["reasoning_effort"] == "high"
    assert usage["failed_attempts"] == []
    chain.adapters[chain.fallback.id].execute.assert_not_awaited()


@pytest.mark.anyio
async def test_shared_profile_concurrency_limit_stops_before_sending_or_fallback(chain):
    chain.store.reservations[uuid.uuid4()] = SimpleNamespace(
        status=BudgetReservationStatus.RESERVED
    )
    with pytest.raises(LeadModelUnavailable, match="planner_budget_exceeded") as raised:
        await chain.model.research(chain.job)
    assert len(raised.value.usage["failed_attempts"]) == 1
    assert all(adapter.execute.await_count == 0 for adapter in chain.adapters.values())
