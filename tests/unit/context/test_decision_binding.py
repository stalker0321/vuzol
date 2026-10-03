"""J1 decision binding: packet/binding split, dynamic validation, snapshots."""

from __future__ import annotations

import pytest

from vuzol.context.decision_binding import (
    Coverage,
    DecisionBinding,
    DecisionBindingError,
    DecisionPacket,
    DecisionRef,
    DecisionSnapshot,
    RefKind,
    SnapshotRef,
    assert_applicable,
    assert_binding_matches,
    build_binding,
    parse_decision_output,
    sha256_json,
    sha256_text,
)

_HASH_A = "a" * 64
_HASH_B = "b" * 64


def _ref(
    ref_id: str,
    *,
    kind: RefKind = RefKind.CANDIDATE,
    revision: str | None = "r1",
    content_hash: str = _HASH_A,
) -> DecisionRef:
    return DecisionRef(kind=kind, id=ref_id, revision=revision, content_hash=content_hash)


def _packet() -> DecisionPacket:
    return DecisionPacket(
        decision_kind="intake",
        subject=_ref("turn:current", kind=RefKind.SOURCE_TURN),
        refs=(_ref("spec:one", kind=RefKind.SPEC_REVISION),),
        candidates=(_ref("candidate:auth"), _ref("candidate:ui")),
        coverage=Coverage.COMPLETE,
        allowed_options=("execute_request", "respond"),
    )


def _binding() -> DecisionBinding:
    return build_binding(
        _packet(),
        request_payload={"kind": "intake", "text": "fix the auth bug"},
        schema_text="schema-body",
        prompt_text="prompt-body",
        source_turn_ref="turn:current",
    )


def _output(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "schema_version": "decision-output.v1",
        "decision_kind": "intake",
        "status": "abstain",
        "target_ref": None,
        "support_refs": [],
        "reason": "compound_intent",
    }
    values.update(changes)
    return values


def _refs(*, consumed: str | None = None) -> dict[str, SnapshotRef]:
    packet = _packet()
    assert packet.subject is not None
    refs = {
        ref.id: SnapshotRef(
            ref_id=ref.id,
            revision=ref.revision,
            content_hash=ref.content_hash,
            consumed=ref.id == consumed,
        )
        for ref in (*packet.refs, *packet.candidates)
    }
    refs[packet.subject.id] = SnapshotRef(
        ref_id=packet.subject.id, revision="r1", content_hash=_HASH_A
    )
    return refs


def _candidate_ids() -> frozenset[str]:
    return frozenset(ref.id for ref in _packet().candidates)


def _snapshot(*, consumed: str | None = None, kill_switch: bool = False) -> DecisionSnapshot:
    return DecisionSnapshot(
        refs=_refs(consumed=consumed), candidate_ids=_candidate_ids(), kill_switch=kill_switch
    )


def test_binding_binds_exact_hashes_and_matches() -> None:
    binding = _binding()
    assert binding.request_hash == sha256_json({"kind": "intake", "text": "fix the auth bug"})
    assert binding.schema_hash == sha256_text("schema-body")
    assert binding.prompt_hash == sha256_text("prompt-body")
    assert binding.source_turn_ref == "turn:current"
    assert_binding_matches(
        binding,
        _packet(),
        request_payload={"kind": "intake", "text": "fix the auth bug"},
        schema_text="schema-body",
        prompt_text="prompt-body",
    )


def test_binding_drift_rejected() -> None:
    binding = _binding()
    with pytest.raises(DecisionBindingError) as error:
        assert_binding_matches(
            binding,
            _packet(),
            request_payload={"kind": "intake", "text": "fix the auth bug"},
            schema_text="schema-body",
            prompt_text="different-prompt",
        )
    assert error.value.category == "binding_drift"


def test_forged_ref_id_rejected_at_build() -> None:
    bad = DecisionPacket(
        decision_kind="intake",
        candidates=(_ref("Bad Ref!"),),
    )
    with pytest.raises(DecisionBindingError) as error:
        build_binding(bad, request_payload={}, schema_text="s", prompt_text="p")
    assert error.value.category == "forged_ref"


def test_unknown_output_keys_fail_closed() -> None:
    with pytest.raises(DecisionBindingError) as error:
        parse_decision_output(_output(evil="x"), binding=_binding())
    assert error.value.category == "unknown_keys"


def test_decided_requires_allowed_target_and_support() -> None:
    binding = _binding()
    forged = _output(
        status="decided",
        target_ref="candidate:ghost",
        support_refs=["spec:one"],
        reason=None,
    )
    with pytest.raises(DecisionBindingError) as error:
        parse_decision_output(forged, binding=binding)
    assert error.value.category == "forged_ref"

    no_support = _output(
        status="decided", target_ref="candidate:auth", support_refs=[], reason=None
    )
    with pytest.raises(DecisionBindingError) as error:
        parse_decision_output(no_support, binding=binding)
    assert error.value.category == "missing_evidence"


def test_decided_valid_output_and_abstain_rules() -> None:
    binding = _binding()
    parsed = parse_decision_output(
        _output(
            status="decided",
            target_ref="candidate:auth",
            support_refs=["spec:one", "candidate:auth"],
            reason=None,
        ),
        binding=binding,
    )
    assert parsed.status.value == "decided"
    assert parsed.target_ref == "candidate:auth"

    with pytest.raises(DecisionBindingError):
        parse_decision_output(_output(status="abstain", reason=None), binding=binding)
    with pytest.raises(DecisionBindingError):
        parse_decision_output(
            _output(status="abstain", target_ref="candidate:auth", reason="x"), binding=binding
        )


def test_snapshot_accepts_current_state() -> None:
    assert_applicable(_binding(), _snapshot())


def test_snapshot_rejects_stale_forged_consumed_candidate_and_kill_switch() -> None:
    binding = _binding()
    stale_refs = _refs()
    stale_refs["candidate:auth"] = SnapshotRef(
        ref_id="candidate:auth", revision="r1", content_hash=_HASH_B
    )
    with pytest.raises(DecisionBindingError) as error:
        assert_applicable(
            binding,
            DecisionSnapshot(refs=stale_refs, candidate_ids=_candidate_ids()),
        )
    assert error.value.category == "stale_ref"

    forged_refs = _refs()
    del forged_refs["candidate:ui"]
    with pytest.raises(DecisionBindingError) as error:
        assert_applicable(
            binding,
            DecisionSnapshot(refs=forged_refs, candidate_ids=_candidate_ids()),
        )
    assert error.value.category == "forged_ref"

    with pytest.raises(DecisionBindingError) as error:
        assert_applicable(binding, _snapshot(consumed="candidate:auth"))
    assert error.value.category == "consumed_ref"

    with pytest.raises(DecisionBindingError) as error:
        assert_applicable(
            binding,
            DecisionSnapshot(refs=_refs(), candidate_ids=frozenset({"candidate:auth"})),
        )
    assert error.value.category == "stale_candidate_set"

    with pytest.raises(DecisionBindingError) as error:
        assert_applicable(binding, _snapshot(kill_switch=True))
    assert error.value.category == "frozen"
