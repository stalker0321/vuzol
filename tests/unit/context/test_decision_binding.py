"""J1 decision binding: packet/binding split, dynamic validation, snapshots."""

from __future__ import annotations

from vuzol.context.decision_binding import (
    Coverage,
    DecisionPacket,
    DecisionRef,
    RefKind,
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


def _candidate_ids() -> frozenset[str]:
    return frozenset(ref.id for ref in _packet().candidates)
