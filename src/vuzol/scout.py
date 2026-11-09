"""Validation of persisted scout packet bytes."""

import json

SCOUT_PACKET_SCHEMA = "scout-packet.v1"
SCOUT_STATUSES = frozenset({"complete", "partial", "failed"})


def validate_scout_packet_bytes(content: bytes) -> tuple[str, ...]:
    """Fail-closed typed consumer for scout packets (W5 pair validation)."""

    try:
        payload = json.loads(content.decode("utf-8"))
    except Exception:
        return ("scout_packet_not_json",)
    if not isinstance(payload, dict):
        return ("scout_packet_not_object",)
    if payload.get("schema") != SCOUT_PACKET_SCHEMA:
        return ("scout_packet_schema_mismatch",)
    if payload.get("status") not in SCOUT_STATUSES:
        return ("scout_packet_status_invalid",)
    facts = payload.get("facts")
    if not isinstance(facts, list):
        return ("scout_packet_facts_invalid",)
    for fact in facts:
        if (
            not isinstance(fact, dict)
            or not isinstance(fact.get("probe"), str)
            or not isinstance(fact.get("source_hash"), str)
        ):
            return ("scout_packet_fact_malformed",)
    for field in ("packet_id", "request_hash", "observed_revision", "observed_at", "scope"):
        if not isinstance(payload.get(field), str) or not payload.get(field):
            return (f"scout_packet_{field}_missing",)
    return ()
