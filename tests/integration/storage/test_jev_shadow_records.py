import asyncio
import hashlib
import uuid

import pytest

from vuzol.experiments.decision import (
    ReasonCode,
    TriageChoice,
    TriageDecision,
    abstain_decision,
    interpret_output,
)
from vuzol.experiments.shadow import EVENT_TYPE, load_shadow_records, record_shadow_decision

from .helpers import storage


def _decision() -> TriageDecision:
    ref = f"artifact:test-report:sha256:{hashlib.sha256(b'evidence').hexdigest()}"
    decision, _ = interpret_output(
        {
            "schema": "decision.v1",
            "decision_kind": "repair_triage",
            "state_revision": 3,
            "choice": "repair",
            "evidence_refs": [ref],
            "reason_code": "known_local_failure",
            "abstain": False,
            "input_fingerprint": "cc" * 32,
        }
    )
    return decision


@pytest.mark.postgresql
def test_shadow_records_roundtrip_in_event_ledger(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            async with factory.begin() as session:
                event_id = await record_shadow_decision(
                    session,
                    _decision(),
                    correlation_id="shadow-exp-1",
                    rules_action="repair",
                )
                assert isinstance(event_id, uuid.UUID)
            async with factory() as session:
                loaded = await load_shadow_records(session, "shadow-exp-1")
            assert len(loaded) == 1
            assert loaded[0]["decision"]["choice"] == TriageChoice.REPAIR.value
            assert loaded[0]["rules_action"] == "repair"
            assert loaded[0]["agrees_with_rules"] is True
            assert loaded[0]["decision_sha256"]
            async with factory() as session:
                assert await load_shadow_records(session, "other-exp") == ()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_abstain_shadow_record_routes_to_attention(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            async with factory.begin() as session:
                decision = abstain_decision(
                    state_revision=9,
                    input_fingerprint="dd" * 32,
                    reason_code=ReasonCode.OOD_INPUT,
                )
                await record_shadow_decision(
                    session, decision, correlation_id="shadow-exp-2", rules_action="attention"
                )
            async with factory() as session:
                (loaded,) = await load_shadow_records(session, "shadow-exp-2")
            assert loaded["decision"]["abstain"] is True
            assert loaded["decision"]["choice"] == "attention"
            assert loaded["rules_action"] == "attention"
            assert EVENT_TYPE == "jev.shadow_recorded"
        finally:
            await engine.dispose()

    asyncio.run(scenario())
