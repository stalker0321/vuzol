"""The lead read protocol is exclusive, strict at execution, and byte bounded."""

import json
import uuid

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from vuzol.config.settings import HardLimits
from vuzol.leadership.provider_model import decision_schema, parse_lead_response
from vuzol.leadership.read_tools import parse_tool_call


def test_schema_accepts_exclusive_decision_or_reads_and_preserves_legacy_parser():
    schema = decision_schema(tools_allowed=True)
    validator = Draft202012Validator(schema)
    final = {"reply": "", "commands": []}
    reads = {"tool_calls": [{"name": "read_status", "args": {}}]}
    assert validator.is_valid(final)
    assert validator.is_valid(reads)
    assert not validator.is_valid({**final, **reads})
    assert not validator.is_valid({**reads, "extra": True})
    assert not validator.is_valid({"tool_calls": []})
    assert not Draft202012Validator(decision_schema(tools_allowed=False)).is_valid(reads)
    legacy = {"reply": "", "commands": [{"name": "read_status", "payload": {}, "reason": ""}]}
    assert not validator.is_valid(legacy)
    assert parse_lead_response(legacy).commands[0].name == "read_status"
    assert "recall" in json.dumps(schema)


@pytest.mark.parametrize(
    "call",
    [
        None,
        {"name": "delete", "args": {}},
        {"name": "read_status", "args": {"goal_id": str(uuid.uuid4())}},
        {"name": "read_result", "args": {}, "extra": True},
        {"name": "recall", "args": {}},
        {"name": "recall", "args": {"query": " "}},
        {"name": "recall", "args": {"query": "x" * 201}},
        {"name": "recall", "args": {"query": "x", "project": "another"}},
        {"name": "recall", "args": {"query": "x", "limit": 9}},
        {"name": "recall", "args": {"query": "x", "limit": True}},
        {"name": "recall", "args": {"query": "x", "limit": "2"}},
    ],
)
def test_malformed_calls_reach_the_server_for_a_tool_error(call):
    # Adapters validate this envelope before the server can report a tool error.
    assert Draft202012Validator(decision_schema(tools_allowed=True)).is_valid(
        {"tool_calls": [call]}
    )
    with pytest.raises(ValidationError):
        parse_tool_call(call)


def test_recall_defaults_and_hard_limits():
    call = parse_tool_call({"name": "recall", "args": {"query": " previous approach "}})
    assert call.args.model_dump() == {"query": "previous approach", "project": "this", "limit": 8}
    limits = HardLimits()
    assert limits.lead_tool_rounds == 3
    assert limits.lead_tool_calls_per_round == 4
    assert limits.lead_tool_result_bytes == 8192
    assert limits.lead_tool_total_bytes == 24576


def test_read_job_is_a_strict_scoped_read():
    job_id = str(uuid.uuid4())
    call = parse_tool_call({"name": "read_job", "args": {"job_id": job_id}})
    assert call.args.model_dump() == {"job_id": job_id}
    for args in ({}, {"job_id": "bad"}, {"job_id": job_id, "goal_id": str(uuid.uuid4())}):
        with pytest.raises(ValidationError):
            parse_tool_call({"name": "read_job", "args": args})
    assert "read_job" in json.dumps(decision_schema(tools_allowed=True))
