"""J3 INTAKE shadow harness tests (no live provider, fake session)."""

from __future__ import annotations

from typing import Any

import pytest

from vuzol.experiments.intake_shadow import (
    IntakeDecisionInvalid,
    IntakeEffect,
    parse_intake_output,
)

_HASH = "a" * 64


def _valid_output(**changes: object) -> dict[str, Any]:
    values: dict[str, Any] = {
        "schema": "decision.v3",
        "decision_kind": "intake",
        "state_revision": 0,
        "effect": "execute_request",
        "relation": "new",
        "target_id": None,
        "support_refs": ["turn:abc"],
        "coverage": "complete",
        "abstain": False,
        "reason_code": "clear_match",
        "input_fingerprint": _HASH,
    }
    values.update(changes)
    return values


def test_parser_rejects_unknown_keys_and_forged_refs() -> None:
    with pytest.raises(IntakeDecisionInvalid):
        parse_intake_output(
            _valid_output(extra="x"),
            allowed_effects=tuple(IntakeEffect),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )
    with pytest.raises(IntakeDecisionInvalid):
        parse_intake_output(
            _valid_output(target_id="task:ghost"),
            allowed_effects=tuple(IntakeEffect),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )
    with pytest.raises(IntakeDecisionInvalid):
        parse_intake_output(
            _valid_output(effect="takeover"),
            allowed_effects=tuple(IntakeEffect),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )
    with pytest.raises(IntakeDecisionInvalid):
        parse_intake_output(
            _valid_output(abstain=True, target_id="turn:abc"),
            allowed_effects=tuple(IntakeEffect),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )


def test_abstain_rejects_effect_outside_allowed() -> None:
    with pytest.raises(IntakeDecisionInvalid):
        parse_intake_output(
            _valid_output(
                abstain=True,
                effect="execute_request",
                target_id=None,
                reason_code="injection_detected",
            ),
            allowed_effects=(IntakeEffect.RESPOND,),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )


@pytest.mark.parametrize("effect", [e.value for e in IntakeEffect if e is not IntakeEffect.MIXED])
def test_null_relation_remains_invalid_for_existing_effects(effect: str) -> None:
    with pytest.raises(IntakeDecisionInvalid, match="unsupported relation"):
        parse_intake_output(
            _valid_output(effect=effect, relation=None),
            allowed_effects=tuple(IntakeEffect),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )


def test_mixed_does_not_allow_missing_relation_or_bypass_allowed_effects() -> None:
    missing = _valid_output(effect="mixed")
    del missing["relation"]
    with pytest.raises(IntakeDecisionInvalid, match="unsupported relation"):
        parse_intake_output(
            missing,
            allowed_effects=tuple(IntakeEffect),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )
    with pytest.raises(IntakeDecisionInvalid, match="effect not allowed"):
        parse_intake_output(
            _valid_output(effect="mixed", relation=None),
            allowed_effects=(IntakeEffect.EXECUTE_REQUEST,),
            allowed_refs=frozenset({"turn:abc"}),
            prompt_digest=_HASH,
        )

    decision = parse_intake_output(
        _valid_output(),
        allowed_effects=tuple(IntakeEffect),
        allowed_refs=frozenset({"turn:abc"}),
        prompt_digest=_HASH,
    )
    assert decision.effect is IntakeEffect.EXECUTE_REQUEST
    assert decision.decision_hash == decision.decision_hash


@pytest.mark.parametrize("relation", [None, "new", "continue", "none"])
def test_mixed_relation_and_canonical_receipt(relation: str | None) -> None:
    decision = parse_intake_output(
        _valid_output(effect="mixed", relation=relation),
        allowed_effects=tuple(IntakeEffect),
        allowed_refs=frozenset({"turn:abc"}),
        prompt_digest=_HASH,
    )
    assert decision.effect is IntakeEffect.MIXED
    assert decision.canonical()["relation"] == relation
    assert len(decision.decision_hash) == 64
