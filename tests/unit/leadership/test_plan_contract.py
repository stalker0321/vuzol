"""One concrete contract survives serialization and dependency ordering."""

import uuid

import pytest

from vuzol.discussion.domain import DomainError, canonical_plan_body, canonical_plan_hash
from vuzol.discussion.service import _plan_from_revision_body
from vuzol.leadership.service import plan_draft_from_payload


def item(local_id="first", **changes):
    return {
        "local_id": local_id,
        "summary": "Add a checked marker",
        "goal": "Record completion of the import",
        "allowed_scope": "src/importer.py, tests/test_importer.py",
        "expected_outcome": "Import writes one marker after success",
        "completion_criteria": ["pytest tests/test_importer.py passes"],
        "dependencies": [],
        "out_of_scope": ["deployment"],
        "trusted_checks": ["pytest tests/test_importer.py"],
        "suggested_risk": "medium",
        "estimated_complexity": "large",
        "needs_approval": True,
        "derived": False,
        "source_turn_ref": "turn-1",
        "source_spec_revision": "source-1",
        **changes,
    }


def test_all_contract_fields_round_trip():
    plan = plan_draft_from_payload({"plan": {"title": "Import", "items": [item()]}})
    body = canonical_plan_body(plan, (uuid.uuid4(),))
    restored = _plan_from_revision_body(body)
    assert canonical_plan_body(restored, (restored.items[0].item_id,)) == body
    for field, value in item().items():
        assert body["items"][0][field] == value


@pytest.mark.parametrize(
    "items",
    [
        [item(dependencies=["absent"])],
        [item(dependencies=["second"]), item("second", dependencies=["first"])],
        [item(dependencies=["first"])],
        [item(local_id=None)],
        [item(goal=None)],
        [item(allowed_scope=[])],
        [item(needs_approval="false")],
        [{k: v for k, v in item().items() if k != "dependencies"}],
    ],
)
def test_invalid_contract_fails_closed(items):
    with pytest.raises(DomainError) as error:
        plan_draft_from_payload({"plan": {"title": "Import", "items": items}})
    assert error.value.code == "invalid_plan"


def test_forward_dependencies_become_execution_order():
    plan = plan_draft_from_payload(
        {
            "plan": {
                "title": "Import",
                "items": [
                    item("last", dependencies=["second"]),
                    item("second", dependencies=["first"]),
                    item(),
                ],
            }
        }
    )
    assert [i.local_id for i in plan.items] == ["first", "second", "last"]


def test_scope_and_repository_are_hashed():
    body = {"scope_revision": 1, "source_repo_commit": "a" * 40, "items": [item()]}
    digest = canonical_plan_hash(body)
    assert canonical_plan_hash({**body, "scope_revision": 2}) != digest
    assert canonical_plan_hash({**body, "source_repo_commit": "b" * 40}) != digest


def test_lead_schema_requests_planner_and_cannot_supply_plan_or_batch_commands():
    from vuzol.leadership.model import LeadModelUnavailable
    from vuzol.leadership.provider_model import parse_lead_response

    request = {
        "name": "request_plan",
        "payload": {"goal": "Import", "constraints": ["src/importer.py"]},
    }
    assert parse_lead_response({"commands": [request]}).commands[0].name == "request_plan"
    for commands in (
        [request, request],
        [request, {"name": "read_status", "payload": {}}],
        [{"name": "propose_batch", "payload": {"plan": {"title": "Import", "items": [item()]}}}],
        [{"name": "request_plan", "payload": {**request["payload"], "plan": {}}}],
    ):
        with pytest.raises(LeadModelUnavailable, match="lead_response_invalid"):
            parse_lead_response({"commands": commands})
