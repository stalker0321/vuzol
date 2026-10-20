"""Bounded context retains references and signals omissions instead of inventing data."""

import json

from vuzol.leadership.feedback import MAX_CONTEXT_BYTES, _plan, _put


def test_projection_is_allowlisted_and_limits_plan_items():
    plan = _plan(
        {
            "title": "bounded",
            "owner_user_id": "PRIVATE_OWNER",
            "items": [
                {
                    "summary": "x" * 10_000,
                    "completion_criteria": ["test"] * 100,
                    "credential": "PRIVATE_CREDENTIAL",
                }
            ]
            * 100,
        }
    )
    assert plan["omitted_items"] == 96
    assert len(plan["items"]) == 4
    assert len(plan["items"][0]["summary"]) < 600
    assert len(plan["items"][0]["completion_criteria"]) == 4
    assert "PRIVATE" not in json.dumps(plan)


def test_global_context_budget_is_in_utf8_bytes_and_keeps_valid_json():
    context = {"binding": {"scope_revision": 1}, "truncated": False}
    for i in range(40):
        _put(context, f"section_{i}", {"summary": "я" * 1000})
    encoded = json.dumps(context, ensure_ascii=False)
    assert len(encoded.encode()) <= MAX_CONTEXT_BYTES
    assert context["truncated"] is True
    assert json.loads(encoded)["binding"]["scope_revision"] == 1
