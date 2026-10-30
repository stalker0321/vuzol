"""Scripted provider reads must finish with one bounded, accounted decision."""

import json
import uuid
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from jsonschema import Draft202012Validator

from vuzol.config.models import LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.config.settings import HardLimits
from vuzol.leadership.model import LeadModelUnavailable, LeadSnapshot
from vuzol.leadership.provider_model import ProviderLeadModel
from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus


@pytest.fixture
def anyio_backend():
    return "asyncio"


def snapshot():
    return LeadSnapshot(
        goal_id=uuid.uuid4(),
        project_id="demo",
        lifecycle="active",
        version=1,
        fence_epoch=1,
        scope_revision=1,
        criteria=(),
        satisfied_criteria=(),
        integration_branch="branch",
        current_package_id=None,
        current_package_status=None,
        integration_head_commit=None,
        request="task",
        scope_text="task",
        waiting_reason=None,
        invocation_id=uuid.uuid4(),
    )


def profile():
    return ProviderProfileConfig(
        id="lead",
        provider="openai-compatible",
        model="test",
        launch_mode=LaunchMode.API,
        context_limit=100000,
        output_limit=2000,
        concurrency_limit=1,
        roles=frozenset({ProviderRole.LEAD}),
        sandbox_required=False,
        capabilities=frozenset(),
        cost_class="cheap",
        supported_task_types=frozenset({"project_lead"}),
        api_base_url="https://example.com/v1",
        credential_reference="env:TEST_TOKEN",
    )


def read(name="read_status", **args):
    return {"name": name, "args": args}


FINAL = {"commands": [], "reply": "Готово"}


class Scripted:
    def __init__(self, *payloads, costs=None):
        self.payloads = list(payloads)
        self.costs = costs or [Decimal("0.2")] * len(payloads)
        self.requests = []
        self.rounds = []

    async def invoke(self, factory, profile, adapter, request, *, purpose, limits, round=0):
        self.rounds.append(round)
        self.requests.append(request)
        payload = self.payloads.pop(0)
        # Exercise the same response validation boundary as production adapters.
        Draft202012Validator(request.output_json_schema).validate(payload)
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            structured_output=payload,
            usage=NormalizedUsage(
                input_tokens=2, output_tokens=3, duration_ms=4, cost_units=self.costs.pop(0)
            ),
            adapter_version="scripted",
        )


def model(monkeypatch, script, **limits):
    monkeypatch.setattr("vuzol.leadership.provider_model.invoke", script.invoke)
    return ProviderLeadModel(AsyncMock(), profile(), AsyncMock(), limits=HardLimits(**limits))


def results(request):
    return [audit for item in request.context for audit in json.loads(item.content)["tool_results"]]


@pytest.mark.anyio
async def test_direct_decision_stays_one_invocation(monkeypatch):
    script = Scripted(FINAL)
    decision = await model(monkeypatch, script).decide(snapshot())
    assert decision.reply == FINAL["reply"]
    assert decision.usage == {"cost_known": True, "cost_amount": 0.2}
    assert script.rounds == [0]
    assert not script.requests[0].context


@pytest.mark.anyio
async def test_prompt_exposes_goal_budget_and_explains_exhaustion_in_russian(monkeypatch):
    script = Scripted(FINAL)
    state = replace(snapshot(), budget={"spent": 1.2, "reserved": 0.2, "cap": 1.5})
    await model(monkeypatch, script).decide(state)
    prompt = script.requests[0].original_input
    assert 'budget: {"spent": 1.2, "reserved": 0.2, "cap": 1.5}' in prompt
    assert "nearly exhausted" in prompt
    assert "spent of the cap" in prompt
    assert "/lead limits cost" in prompt
    assert "close the goal" in prompt
    assert "Do not retry refused work" in prompt


@pytest.mark.anyio
@pytest.mark.parametrize(
    "costs,expected", [([Decimal(".2"), Decimal(".3")], 0.5), ([None, Decimal(".3")], None)]
)
async def test_tool_results_reach_next_round_with_audit_and_aggregate_usage(
    monkeypatch, costs, expected
):
    script = Scripted({"tool_calls": [read("recall", query="choice")]}, FINAL, costs=costs)
    executor = AsyncMock(return_value={"hits": [{"kind": "decision_entry", "text": "stored"}]})
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", executor, raising=False)
    source = snapshot()
    decision = await model(monkeypatch, script).decide(source)
    assert script.rounds == [0, 1]
    assert script.requests[0].task_id == source.invocation_id
    assert script.requests[1].task_id == uuid.uuid5(source.invocation_id, "lead:1")
    item = script.requests[1].context[0]
    assert "untrusted" in item.source
    audit = results(script.requests[1])[0]
    assert audit["name"] == "recall" and audit["args"]["query"] == "choice"
    assert audit["hit_count"] == 1 and audit["result"]["hits"][0]["text"] == "stored"
    assert audit["result_bytes"] > 0
    assert decision.usage == (
        {"cost_known": False} if expected is None else {"cost_known": True, "cost_amount": expected}
    )


@pytest.mark.anyio
async def test_round_exhaustion_forces_decide_now_schema(monkeypatch):
    script = Scripted(*[{"tool_calls": [read()]}] * 3, FINAL)
    executor = AsyncMock(return_value={"lifecycle": "active"})
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", executor, raising=False)
    await model(monkeypatch, script).decide(snapshot())
    assert script.rounds == [0, 1, 2, 3]
    assert executor.await_count == 3
    assert not Draft202012Validator(script.requests[-1].output_json_schema).is_valid(
        {"tool_calls": [read()]}
    )
    assert "decide now" in script.requests[-1].original_input.lower()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "limits,answer,calls,executed",
    [
        ({"lead_tool_calls_per_round": 2}, {"ok": True}, [read()] * 3, 2),
        ({"lead_tool_result_bytes": 128}, {"text": 'я\\"' * 5000}, [read()], 1),
        ({"lead_tool_total_bytes": 128}, {"text": "я" * 100}, [read()] * 2, 1),
    ],
)
async def test_calls_and_utf8_byte_exhaustion_force_final(
    monkeypatch, limits, answer, calls, executed
):
    script = Scripted({"tool_calls": calls}, FINAL)
    executor = AsyncMock(return_value=answer)
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", executor, raising=False)
    await model(monkeypatch, script, **limits).decide(snapshot())
    assert executor.await_count == executed
    audits = results(script.requests[-1])
    assert len(audits) == len(calls)  # Every attempted call is audited, including rejected calls.
    for audit in audits:
        assert audit["result_bytes"] <= limits.get("lead_tool_result_bytes", 8192)
    assert sum(a["result_bytes"] for a in audits) <= limits.get("lead_tool_total_bytes", 24576)
    assert not Draft202012Validator(script.requests[-1].output_json_schema).is_valid(
        {"tool_calls": [read()]}
    )


@pytest.mark.anyio
async def test_malformed_call_returns_tool_error_and_counts_as_a_round(monkeypatch):
    script = Scripted({"tool_calls": [{"name": "recall", "args": {"query": 3}}]}, FINAL)
    executor = AsyncMock()
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", executor, raising=False)
    await model(monkeypatch, script, lead_tool_rounds=1).decide(snapshot())
    executor.assert_not_called()
    audit = results(script.requests[-1])[0]
    assert audit["error"] == "invalid_tool_call" and audit["hit_count"] == 0
    assert "decide now" in script.requests[-1].original_input.lower()


@pytest.mark.anyio
async def test_owner_reply_is_required_only_for_the_final_decision(monkeypatch):
    script = Scripted({"tool_calls": [read()]}, FINAL)
    monkeypatch.setattr(
        "vuzol.leadership.read_tools.execute_read", AsyncMock(return_value={}), raising=False
    )
    decision = await model(monkeypatch, script).decide(
        replace(snapshot(), owner_event={"text": "past work"})
    )
    assert decision.reply == FINAL["reply"]


@pytest.mark.anyio
async def test_tool_and_final_envelopes_cannot_be_mixed(monkeypatch):
    # A nonvalidating adapter cannot bypass the application's own envelope check.
    async def invoke(*args, **kwargs):
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            structured_output={**FINAL, "tool_calls": [read()]},
            usage=NormalizedUsage(input_tokens=1, output_tokens=1, duration_ms=1),
            adapter_version="fake",
        )

    monkeypatch.setattr("vuzol.leadership.provider_model.invoke", invoke)
    with pytest.raises(LeadModelUnavailable, match="lead_response_invalid"):
        await ProviderLeadModel(AsyncMock(), profile(), AsyncMock()).decide(snapshot())


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["received", "sending", "unknown"])
async def test_invoke_reconciles_the_requested_round_without_resending(monkeypatch, state):
    import hashlib
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from vuzol.config.revision import content_revision
    from vuzol.leadership.inference import invoke
    from vuzol.leadership.invocation_context import current_turn
    from vuzol.leadership.provider_model import _request

    selected = profile()
    request = _request(snapshot(), uuid.uuid4())
    payload = request.model_dump(
        mode="json", exclude={"lease_generation", "reserved_cost_units", "reserved_quota_units"}
    )
    receipt = ProviderResult(
        status=ProviderResultStatus.SUCCEEDED,
        structured_output=FINAL,
        usage=NormalizedUsage(input_tokens=1, output_tokens=1, duration_ms=1),
        adapter_version="fake",
    )
    prior = SimpleNamespace(
        id=request.task_id,
        input_hash=hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        profile_id=selected.id,
        profile_revision=content_revision(selected),
        state=state,
        result=receipt.model_dump(mode="json") if state == "received" else None,
    )
    session = AsyncMock()
    session.scalar.return_value = prior

    class Factory:
        @asynccontextmanager
        async def begin(self):
            yield session

    adapter = AsyncMock()
    monkeypatch.setattr(
        "vuzol.leadership.inference.invocation_current", AsyncMock(return_value=True)
    )
    token = current_turn.set(SimpleNamespace(turn_id=uuid.uuid4()))
    try:
        if state == "received":
            assert (
                await invoke(
                    Factory(),
                    selected,
                    adapter,
                    request,
                    purpose="project_lead",
                    limits=HardLimits(),
                    round=1,
                )
                == receipt
            )
        else:
            with pytest.raises(LeadModelUnavailable, match="lead_inference_reconciliation"):
                await invoke(
                    Factory(),
                    selected,
                    adapter,
                    request,
                    purpose="project_lead",
                    limits=HardLimits(),
                    round=1,
                )
        sql = session.scalar.call_args.args[0].compile()
        assert "leadership_inferences.round" in str(sql)
        assert 1 in sql.params.values()
        adapter.execute.assert_not_called()
    finally:
        current_turn.reset(token)


@pytest.mark.anyio
async def test_crash_replays_saved_round_one_and_never_rereads_mutable_data(monkeypatch):
    import hashlib
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from vuzol.config.revision import content_revision
    from vuzol.leadership.inference import invoke
    from vuzol.leadership.invocation_context import current_turn

    source = snapshot()
    responses = [{"tool_calls": [read("recall", query="choice")]}, FINAL]
    script = Scripted(*responses)
    monkeypatch.setattr(
        "vuzol.leadership.read_tools.execute_read",
        AsyncMock(return_value={"hits": [{"text": "old stored data"}]}),
    )
    await model(monkeypatch, script).decide(source)
    rows = {}
    for index, request in enumerate(script.requests):
        payload = request.model_dump(
            mode="json", exclude={"lease_generation", "reserved_cost_units", "reserved_quota_units"}
        )
        rows[index] = SimpleNamespace(
            id=request.task_id,
            request=payload,
            input_hash=hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            profile_id=profile().id,
            profile_revision=content_revision(profile()),
            state="received",
            result=ProviderResult(
                status=ProviderResultStatus.SUCCEEDED,
                structured_output=responses[index],
                usage=NormalizedUsage(
                    input_tokens=2, output_tokens=3, duration_ms=1, cost_units=Decimal(".2")
                ),
                adapter_version="fake",
            ).model_dump(mode="json"),
        )
    session = AsyncMock()

    async def select_row(statement):
        params = statement.compile().params
        return rows[params["round_1"]]

    session.scalar.side_effect = select_row

    class Factory:
        @asynccontextmanager
        async def __call__(self):
            yield session

        begin = __call__

    adapter = AsyncMock()
    monkeypatch.setattr("vuzol.leadership.provider_model.invoke", invoke)
    monkeypatch.setattr(
        "vuzol.leadership.inference.invocation_current", AsyncMock(return_value=True)
    )
    reread = AsyncMock(side_effect=AssertionError("must reuse saved read result"))
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", reread)
    token = current_turn.set(SimpleNamespace(turn_id=uuid.uuid4()))
    try:
        decision = await ProviderLeadModel(Factory(), profile(), adapter).decide(
            replace(source, request="snapshot changed")
        )
        assert decision.reply == FINAL["reply"]
        assert decision.usage == {"cost_known": True, "cost_amount": 0.4}
        adapter.execute.assert_not_called()
        reread.assert_not_called()
    finally:
        current_turn.reset(token)


@pytest.mark.anyio
async def test_byte_limit_is_cumulative_across_rounds(monkeypatch):
    script = Scripted(*[{"tool_calls": [read()]}] * 2, FINAL)
    executor = AsyncMock(return_value={"text": "x" * 140})
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", executor)
    await model(monkeypatch, script, lead_tool_total_bytes=300).decide(snapshot())
    entries = results(script.requests[-1])
    assert len(entries) == 2 and entries[-1]["truncated"]
    assert sum(a["result_bytes"] for a in entries) <= 300
    assert "decide now" in script.requests[-1].original_input.lower()


@pytest.mark.anyio
async def test_excess_call_batch_stays_replayable_and_audits_every_attempt(monkeypatch):
    from vuzol.providers.domain import ProviderRequest

    script = Scripted({"tool_calls": [read()] * 60}, FINAL)
    executor = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", executor)
    await model(monkeypatch, script).decide(snapshot())
    assert executor.await_count == 4
    request = script.requests[-1]
    # Persisted requests must validate on restart, including the provider context limit.
    ProviderRequest.model_validate(request.model_dump())
    entries = results(request)
    assert len(entries) == 60
    assert sum(a.get("error") == "tool_call_limit" for a in entries) == 56


@pytest.mark.anyio
async def test_oversized_snapshot_keeps_the_existing_context_limit_failure(monkeypatch):
    script = Scripted(FINAL)
    with pytest.raises(LeadModelUnavailable, match="lead_context_limit"):
        await model(monkeypatch, script).decide(replace(snapshot(), request="x" * 50000))
    assert not script.requests


@pytest.mark.anyio
async def test_malformed_oversized_arguments_return_a_bounded_error(monkeypatch):
    script = Scripted({"tool_calls": [read("recall", query="x" * 30000)]}, FINAL)
    executor = AsyncMock()
    monkeypatch.setattr("vuzol.leadership.read_tools.execute_read", executor)
    await model(monkeypatch, script).decide(snapshot())
    executor.assert_not_called()
    entry = results(script.requests[-1])[0]
    assert entry["error"] == "invalid_tool_call"
    assert entry["args"]["truncated"] is True
    assert len(script.requests[-1].context[0].content) < 4000


@pytest.mark.anyio
async def test_completed_read_is_audited_before_a_fenced_reinvoke(monkeypatch):
    from vuzol.leadership import read_tools

    script = Scripted({"tool_calls": [read("recall", query="choice")]})
    lead = model(monkeypatch, script)
    audit = AsyncMock()
    monkeypatch.setattr(read_tools, "record_tool_audit", audit, raising=False)
    monkeypatch.setattr(
        read_tools, "execute_read", AsyncMock(return_value={"hits": [{"text": "fact"}]})
    )
    initial = script.invoke

    async def invoke(*args, **kwargs):
        if kwargs["round"] == 1:
            # Revocation prevents creation of the following inference row.
            raise LeadModelUnavailable("lead_model_unavailable")
        return await initial(*args, **kwargs)

    monkeypatch.setattr("vuzol.leadership.provider_model.invoke", invoke)
    with pytest.raises(LeadModelUnavailable):
        await lead.decide(snapshot())
    audit.assert_awaited_once()
    entry = audit.call_args.args[-1]
    assert entry["name"] == "recall" and entry["args"] == {"query": "choice"}
    assert entry["hit_count"] == 1 and entry["result_bytes"] > 0


@pytest.mark.anyio
async def test_denied_read_keeps_an_audit_without_a_second_provider_call(monkeypatch):
    from vuzol.leadership import read_tools

    script = Scripted({"tool_calls": [read()]})
    lead = model(monkeypatch, script)
    audit = AsyncMock()
    monkeypatch.setattr(read_tools, "record_tool_audit", audit, raising=False)
    monkeypatch.setattr(
        read_tools,
        "execute_read",
        AsyncMock(side_effect=LeadModelUnavailable("lead_model_unavailable")),
    )
    with pytest.raises(LeadModelUnavailable):
        await lead.decide(snapshot())
    assert script.rounds == [0]
    audit.assert_awaited_once()
    assert audit.call_args.args[-1]["error"] == "read_authority_unavailable"


START = {
    "plan_revision_id": str(uuid.uuid4()),
    "plan_hash": "a" * 64,
    "scope_revision": 1,
    "ordinal": 3,
}


def startable(**overrides):
    control = {"start_spec": START, "next_item": {"ordinal": 3}, "tasks": []}
    control.update(overrides)
    return replace(
        snapshot(),
        owner_event={"text": "Запусти проверку"},
        result_context={"task_control": control},
    )


@pytest.mark.anyio
async def test_reply_without_command_is_nudged_once_when_next_item_can_start(monkeypatch):
    promised = {"reply": "Запускаю проверку", "commands": []}
    started = {
        "reply": "Запускаю проверку",
        "commands": [{"name": "start_task", "reason": "owner", "payload": {"spec": START}}],
    }
    script = Scripted(promised, started)
    decision = await model(monkeypatch, script).decide(startable())
    assert script.rounds == [0, 1]
    assert "previous answer to the owner had a reply but no commands" in (
        script.requests[1].original_input
    )
    assert "tool_calls" not in json.dumps(script.requests[1].output_json_schema)
    assert [command.name for command in decision.commands] == ["start_task"]
    assert decision.usage == {"cost_known": True, "cost_amount": 0.4}


@pytest.mark.anyio
async def test_nudged_answer_without_command_is_final(monkeypatch):
    answer = {"reply": "Ответила на вопрос", "commands": []}
    script = Scripted(answer, answer)
    decision = await model(monkeypatch, script).decide(startable())
    assert script.rounds == [0, 1]
    assert decision.commands == () and decision.reply == "Ответила на вопрос"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "state",
    [
        "no_owner_event",
        "waiting",
        "running_task",
        "no_next_item",
    ],
)
async def test_no_nudge_when_nothing_can_start(monkeypatch, state):
    source = startable()
    if state == "no_owner_event":
        source = replace(source, owner_event=None)
    elif state == "waiting":
        source = replace(source, lifecycle="waiting")
    elif state == "running_task":
        source = startable(tasks=[{"status": "executing"}])
    else:
        source = startable(next_item=None)
    script = Scripted(FINAL)
    await model(monkeypatch, script).decide(source)
    assert script.rounds == [0]
