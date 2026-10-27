"""Trusted audit metadata must not invalidate pre-provenance receipts."""

from dataclasses import replace

from vuzol.leadership.commands import LeadCommand, payload_hash


def test_enable_goal_retry_preserves_legacy_semantic_hash():
    command = LeadCommand(
        name="enable_managed_goal",
        project_id="example",
        idempotency_key="stable-key",
        payload={"request": "same work", "scope": "README only"},
    )
    received = replace(
        command,
        payload={
            **command.payload,
            "request_origin": {
                "channel": "mcp",
                "display_name": "ChatGPT",
                "client_id": "first-client",
            },
        },
    )
    renamed = replace(
        command,
        payload={
            **command.payload,
            "request_origin": {
                "channel": "mcp",
                "display_name": "Renamed client",
                "client_id": "second-client",
            },
        },
    )
    assert payload_hash(command) == payload_hash(received) == payload_hash(renamed)
    changed = replace(received, payload={**received.payload, "request": "different work"})
    assert payload_hash(changed) != payload_hash(command)


def test_amendment_alias_and_saved_reply():
    import uuid

    from vuzol.leadership.commands import LeadCommandName, parse_command_name
    from vuzol.leadership.model import freeze_decision
    from vuzol.leadership.provider_model import parse_lead_response

    assert parse_command_name("request_revision") is LeadCommandName.AMEND_GOAL
    command = LeadCommand(
        name="request_revision",
        project_id="demo",
        idempotency_key="amend-once",
        payload={"scope": "README"},
    )
    assert payload_hash(command) == payload_hash(replace(command, name="amend_goal"))
    decision = parse_lead_response(
        {
            "reply": "I will finish the current task first.",
            "commands": [
                {
                    "name": "amend_goal",
                    "reason": "Owner requested a change",
                    "payload": {
                        "scope": "README",
                        "criteria": [{"id": "readme", "description": "Updated"}],
                        "source_event_id": str(uuid.uuid4()),
                        "inflight_policy": "finish_current",
                    },
                }
            ],
        }
    )
    frozen = freeze_decision(
        decision,
        goal_id=uuid.uuid4(),
        wakeup_id=uuid.uuid4(),
        input_version=3,
        input_fence=4,
        input_scope=1,
        input_lease_epoch=2,
    )
    assert frozen["commands"][0]["expected_version"] == 3


def test_amendment_ends_the_decision_and_cannot_supply_authority():
    import uuid

    import pytest

    from vuzol.leadership.model import LeadModelUnavailable
    from vuzol.leadership.provider_model import parse_lead_response

    amendment = {
        "name": "amend_goal",
        "reason": "Change",
        "payload": {
            "scope": "README",
            "criteria": [{"id": "readme", "description": "Updated"}],
            "source_event_id": str(uuid.uuid4()),
            "inflight_policy": "finish_current",
        },
    }
    with pytest.raises(LeadModelUnavailable):
        parse_lead_response({"commands": [amendment, {"name": "start_batch", "payload": {}}]})
    with pytest.raises(LeadModelUnavailable):
        parse_lead_response(
            {
                "commands": [
                    {
                        **amendment,
                        "payload": {
                            **amendment["payload"],
                            "allowed_actions": ["deploy"],
                        },
                    }
                ]
            }
        )
