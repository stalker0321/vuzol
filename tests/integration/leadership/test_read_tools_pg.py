"""Real claims, receipts and ledger; scripted provider I/O only."""

import json
from decimal import Decimal

import pytest
from jsonschema import Draft202012Validator
from sqlalchemy import func, select, text
from tests.integration.leadership.test_lead_review_regressions import control
from tests.integration.leadership.test_lead_surface import _profile, _settings, _Transport
from tests.integration.leadership.test_managed_goal_flow import _enable, _factory, _session
from tests.integration.leadership.test_model_assignment_receipts import _claim, _worker
from tests.integration.leadership.test_stage2_contracts import expire_claim

from vuzol.config.registries import ProfileRegistry
from vuzol.leadership.commands import LeadCommandName
from vuzol.leadership.persistence import (
    GoalDelegation,
    LeadershipCommandReceipt,
    LeadershipInference,
    LeadershipTurn,
    ManagedGoal,
)
from vuzol.leadership.service import read_projection
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.models import ProviderBudgetReservation, UsageRecord

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def read(name="read_status", **args):
    return {"name": name, "args": args}


class Scripted(_Transport):
    def __init__(self, *answers):
        super().__init__({})
        self.answers = list(answers)

    async def execute(self, request, profile, cancellation):
        self.payload = self.answers.pop(0)
        Draft202012Validator(request.output_json_schema).validate(self.payload)
        result = await super().execute(request, profile, cancellation)
        return result.model_copy(
            update={
                "usage": NormalizedUsage(
                    input_tokens=12, output_tokens=4, duration_ms=1, cost_units=Decimal(".1")
                )
            }
        )


def worker(factory, settings, adapter, owner="reads"):
    profiles = ProfileRegistry((_profile().model_copy(update={"context_limit": 100000}),))
    return _worker(factory, settings, adapter, owner=owner, profiles=profiles)


def audits(request):
    return [audit for item in request.context for audit in json.loads(item.content)["tool_results"]]


async def test_direct_decision_is_round_zero(postgres_dsn, monkeypatch):
    settings = _settings(monkeypatch, "lead-local")
    adapter = Scripted({"reply": "", "commands": []})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        assert await worker(factory, settings, adapter).process_one()
        async with factory() as session:
            row = (await session.scalars(select(LeadershipInference))).one()
            assert row.round == 0 and row.state == "received"
            assert row.id == adapter.requests[0].task_id
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1


async def test_reads_share_projections_and_have_separate_receipts(postgres_dsn, monkeypatch):
    settings = _settings(monkeypatch, "lead-local")
    adapter = Scripted(
        {"tool_calls": [read(), read("read_result"), read("recall", query="unmatched")]},
        {"reply": "", "commands": []},
    )
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        lead = worker(factory, settings, adapter)
        claim = await _claim(factory, lead, "reads")
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            grant = await session.get(GoalDelegation, goal.delegation_id)
            before = goal.version
            expected = [
                await read_projection(session, name, goal, grant)
                for name in (LeadCommandName.GET_PROJECT_STATE, LeadCommandName.GET_RESULT)
            ]
        decision = await lead._infer(claim)
        assert decision.usage == {"cost_known": True, "cost_amount": 0.2}
        assert [a["result"] for a in audits(adapter.requests[1])[:2]] == expected
        assert audits(adapter.requests[1])[2]["result"] == {"hits": [], "message": "no matches"}
        async with factory() as session:
            rows = list(
                await session.scalars(
                    select(LeadershipInference).order_by(LeadershipInference.round)
                )
            )
            assert [r.round for r in rows] == [0, 1]
            assert all(r.state == "received" for r in rows)
            assert len(rows[0].tool_audit) == 3
            assert rows[0].tool_audit[2]["name"] == "recall"
            assert rows[0].tool_audit[2]["hit_count"] == 0
            assert rows[0].tool_audit[2]["result_bytes"] > 0
            assert rows[1].tool_audit == []
            assert rows[1].request["context"][0]["source"] == "untrusted_lead_tool_result"
            assert rows[0].reservation_id != rows[1].reservation_id
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 2
            assert (
                await session.scalar(select(func.count()).select_from(ProviderBudgetReservation))
                == 2
            )
            assert (await session.get(ManagedGoal, goal_id)).version == before
            assert not await session.scalar(
                select(LeadershipCommandReceipt).where(
                    LeadershipCommandReceipt.actor_kind == "agent"
                )
            )
        # Saving the decision exposes aggregate usage on the turn itself.
        assert await lead._save_decision(goal_id, claim.wakeup_id, decision, claim=claim)
        async with factory() as session:
            turn = await session.scalar(select(LeadershipTurn))
            assert turn.usage == {"cost_known": True, "cost_amount": 0.2}


async def test_recall_finds_what_the_owner_saved_in_the_inbox(postgres_dsn, monkeypatch):
    """Owner 2026-10-31: a tool forwarded to the private inbox is found by project leads."""
    from vuzol.storage.models import InboxItem

    settings = _settings(monkeypatch, "lead-local")
    adapter = Scripted(
        {"tool_calls": [read("recall", query="агентские инструменты")]},
        {"reply": "", "commands": []},
    )
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            for owner, title in ((goal.owner_user_id, "agent-kit"), (goal.owner_user_id + 1, "x")):
                session.add(
                    InboxItem(
                        owner_user_id=owner,
                        chat_id=owner,
                        ordinal=0,
                        kind="repo",
                        title=title,
                        summary="Агентские инструменты для лида",
                        tags=["агенты"],
                        fields={"verdict": "Годный, MIT"},
                        evidence="",
                        answers=[],
                    )
                )
        lead = worker(factory, settings, adapter)
        claim = await _claim(factory, lead, "reads")
        await lead._infer(claim)
        hits = audits(adapter.requests[1])[0]["result"]["hits"]
        assert [hit["kind"] for hit in hits] == ["owner_inbox_repo"]
        assert hits[0]["text"].startswith("agent-kit") and "Годный, MIT" in hits[0]["text"]


@pytest.mark.parametrize("state", ["received", "sending", "unknown"])
async def test_restart_preserves_followup_request_without_rereading_or_resending(
    postgres_dsn, monkeypatch, state
):
    settings = _settings(monkeypatch, "lead-local")
    adapter = Scripted({"tool_calls": [read()]}, {"reply": "", "commands": []})
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        first = worker(factory, settings, adapter, "before")
        await first._infer(await _claim(factory, first, "before"))
        async with factory.begin() as session:
            row = await session.scalar(
                select(LeadershipInference).where(LeadershipInference.round == 1)
            )
            saved = row.request
            if state != "received":
                row.state, row.result = state, None
        await expire_claim(factory)

        async def cannot_read(*args, **kwargs):
            raise AssertionError("Replay must use the saved request, not reread mutable data")

        monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", cannot_read)
        second = worker(factory, settings, adapter, "after")
        assert await second.process_one()
        assert len(adapter.requests) == 2
        async with factory() as session:
            row = await session.scalar(
                select(LeadershipInference).where(LeadershipInference.round == 1)
            )
            assert row.request == saved
            goal = await session.scalar(select(ManagedGoal))
            assert goal.waiting_reason == (
                "model_idle" if state == "received" else "lead_inference_reconciliation"
            )
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 2


@pytest.mark.parametrize("boundary", ["before_read", "after_read"])
async def test_revoked_authority_stops_tools_or_reinvoke_without_commands(
    postgres_dsn, monkeypatch, boundary
):
    from vuzol.leadership import read_tools

    settings = _settings(monkeypatch, "lead-local")
    adapter = Scripted(
        {"tool_calls": [read()]},
        {"reply": "", "commands": [{"name": "pause_goal", "payload": {}, "reason": ""}]},
    )
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        original = read_tools.execute_read

        async def revoke(factory, snapshot, call):
            if boundary == "before_read":
                assert (await control(factory, goal_id, "revoke_delegation")).status == "applied"
            result = await original(factory, snapshot, call)
            assert (await control(factory, goal_id, "revoke_delegation")).status == "applied"
            return result

        monkeypatch.setattr(read_tools, "execute_read", revoke)
        await worker(factory, settings, adapter).process_one()
        assert len(adapter.requests) == 1
        async with factory() as session:
            inference = await session.scalar(select(LeadershipInference))
            assert len(inference.tool_audit) == 1
            entry = inference.tool_audit[0]
            assert entry["name"] == "read_status"
            if boundary == "before_read":
                assert entry["error"] == "read_authority_unavailable"
                assert entry["result_bytes"] == 0
            else:
                assert entry["result_bytes"] > 0
            assert not await session.scalar(
                select(LeadershipCommandReceipt).where(
                    LeadershipCommandReceipt.actor_kind == "agent"
                )
            )


async def test_tool_session_is_postgresql_read_only(postgres_dsn, monkeypatch):
    settings = _settings(monkeypatch, "lead-local")
    adapter = Scripted({"tool_calls": [read()]}, {"reply": "", "commands": []})
    original = read_projection

    async def observe(session, *args):
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"
        return await original(session, *args)

    monkeypatch.setattr("vuzol.leadership.service.read_projection", observe)
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        lead = worker(factory, settings, adapter)
        assert (await lead._infer(await _claim(factory, lead, "reads"))).commands == ()
