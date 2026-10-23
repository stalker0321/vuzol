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
