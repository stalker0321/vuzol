"""Decision context binding: model-visible packet vs runtime-only binding (J1).

The semantic producer must decide against a *snapshot* of context, not against
whatever the database happens to hold at apply time. This module keeps that
snapshot in two shapes:

- :class:`DecisionPacket` — the model-visible context: opaque refs plus allowed
  options. It carries no vendor names and no authority.
- :class:`DecisionBinding` — runtime-only provenance: the exact hashes of the
  request/schema/prompt and the refs the decision was made against. It is never
  sent to the model and never grants execution by itself.

Dynamic validation runs *after* JSON Schema parsing: unknown keys fail closed,
refs must belong to the binding, decided/abstain combinations are checked, and
apply-time snapshot checks reject stale, forged or consumed refs. The existing
``decision.v1``/``decision.v2`` readers are untouched (this is a new namespace).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

DECISION_PACKET_SCHEMA = "decision-packet.v1"
DECISION_BINDING_SCHEMA = "decision-binding.v1"
DECISION_OUTPUT_SCHEMA = "decision-output.v1"

_HASH = r"^[0-9a-f]{64}$"
_REF_ID = re.compile(r"^[a-z][a-z0-9_.:-]{0,80}$")


class DecisionBindingError(ValueError):
    """A decision packet/binding/output is unsafe; fail closed."""

    def __init__(self, category: str, message: str | None = None) -> None:
        self.category = category
        super().__init__(message or category)


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Coverage(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class RefKind(StrEnum):
    SOURCE_TURN = "source_turn"
    SPEC_REVISION = "spec_revision"
    PLAN_REVISION = "plan_revision"
    ATTEMPT = "attempt"
    CANDIDATE = "candidate"
    EVIDENCE = "evidence"


class DecisionRef(FrozenModel):
    """Opaque reference to a scoped source at an exact revision/hash."""

    kind: RefKind
    id: str = Field(min_length=1, max_length=100)
    revision: str | None = Field(default=None, max_length=100)
    content_hash: str = Field(pattern=_HASH)

    def validate_id(self) -> None:
        if not _REF_ID.match(self.id):
            raise DecisionBindingError("forged_ref", f"malformed ref id: {self.id[:80]}")


class DecisionPacket(FrozenModel):
    """Model-visible context. Built from the runtime binding, never the reverse."""

    schema_version: str = DECISION_PACKET_SCHEMA
    decision_kind: str = Field(min_length=1, max_length=40)
    subject: DecisionRef | None = None
    refs: tuple[DecisionRef, ...] = ()
    candidates: tuple[DecisionRef, ...] = ()
    coverage: Coverage = Coverage.UNKNOWN
    allowed_options: tuple[str, ...] = ()

    @property
    def ref_ids(self) -> frozenset[str]:
        return frozenset(ref.id for ref in (*self.refs, *self.candidates))


class DecisionBinding(FrozenModel):
    """Runtime-only provenance for one decision snapshot. Never model-visible."""

    schema_version: str = DECISION_BINDING_SCHEMA
    decision_kind: str = Field(min_length=1, max_length=40)
    request_hash: str = Field(pattern=_HASH)
    schema_hash: str = Field(pattern=_HASH)
    prompt_hash: str = Field(pattern=_HASH)
    packet_hash: str = Field(pattern=_HASH)
    source_turn_ref: str | None = Field(default=None, max_length=200)
    refs: tuple[DecisionRef, ...] = ()
    candidates: tuple[DecisionRef, ...] = ()
    coverage: Coverage = Coverage.UNKNOWN

    @property
    def ref_ids(self) -> frozenset[str]:
        return frozenset(ref.id for ref in (*self.refs, *self.candidates))

    @property
    def candidate_ids(self) -> frozenset[str]:
        return frozenset(ref.id for ref in self.candidates)


class DecisionStatus(StrEnum):
    DECIDED = "decided"
    ABSTAIN = "abstain"


class DecisionOutput(FrozenModel):
    """Validated model decision. Advisory: applying it is a separate step."""

    schema_version: str = DECISION_OUTPUT_SCHEMA
    decision_kind: str
    status: DecisionStatus
    target_ref: str | None = None
    support_refs: tuple[str, ...] = ()
    reason: str | None = None


_OUTPUT_KEYS = frozenset(
    {
        "schema_version",
        "decision_kind",
        "status",
        "target_ref",
        "support_refs",
        "reason",
    }
)


def _all_refs(packet: DecisionPacket) -> tuple[DecisionRef, ...]:
    """All refs that must stay current for a decision to apply: subject + refs."""

    subject = (packet.subject,) if packet.subject is not None else ()
    return subject + packet.refs


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return sha256_text(encoded)


def build_binding(
    packet: DecisionPacket,
    *,
    request_payload: Mapping[str, Any],
    schema_text: str,
    prompt_text: str,
    source_turn_ref: str | None = None,
) -> DecisionBinding:
    """Bind a packet to the exact request/schema/prompt it was built from."""

    for ref in (*_all_refs(packet), *packet.candidates):
        ref.validate_id()
    return DecisionBinding(
        decision_kind=packet.decision_kind,
        request_hash=sha256_json(request_payload),
        schema_hash=sha256_text(schema_text),
        prompt_hash=sha256_text(prompt_text),
        packet_hash=sha256_json(packet.model_dump(mode="json")),
        source_turn_ref=source_turn_ref,
        refs=_all_refs(packet),
        candidates=packet.candidates,
        coverage=packet.coverage,
    )


def assert_binding_matches(
    binding: DecisionBinding,
    packet: DecisionPacket,
    *,
    request_payload: Mapping[str, Any],
    schema_text: str,
    prompt_text: str,
) -> None:
    """Reject a packet/request/schema/prompt that drifted from the binding."""

    expected = DecisionBinding(
        decision_kind=packet.decision_kind,
        request_hash=sha256_json(request_payload),
        schema_hash=sha256_text(schema_text),
        prompt_hash=sha256_text(prompt_text),
        packet_hash=sha256_json(packet.model_dump(mode="json")),
        source_turn_ref=binding.source_turn_ref,
        refs=_all_refs(packet),
        candidates=packet.candidates,
        coverage=packet.coverage,
    )
    if binding != expected:
        raise DecisionBindingError("binding_drift", "decision binding no longer matches input")


def parse_decision_output(
    raw: Mapping[str, Any],
    *,
    binding: DecisionBinding,
) -> DecisionOutput:
    """Dynamic validation after JSON Schema: strict, fail-closed."""

    if not isinstance(raw, Mapping):
        raise DecisionBindingError("invalid_output", "decision output must be a mapping")
    unknown = set(raw.keys()) - _OUTPUT_KEYS
    if unknown:
        raise DecisionBindingError(
            "unknown_keys", f"unknown decision output keys: {sorted(unknown)[:3]}"
        )
    missing = _OUTPUT_KEYS - {"reason"} - set(raw.keys())
    if missing:
        raise DecisionBindingError(
            "missing_keys", f"missing decision output keys: {sorted(missing)[:3]}"
        )
    if raw.get("schema_version") != DECISION_OUTPUT_SCHEMA:
        raise DecisionBindingError("invalid_output", "unsupported decision output schema")
    if raw.get("decision_kind") != binding.decision_kind:
        raise DecisionBindingError("kind_mismatch", "decision kind does not match binding")
    raw_status = raw.get("status")
    if not isinstance(raw_status, str):
        raise DecisionBindingError("invalid_output", "decision status must be a string")
    try:
        status = DecisionStatus(raw_status)
    except ValueError:
        raise DecisionBindingError("invalid_output", f"unsupported status: {raw_status}") from None

    target_ref = raw.get("target_ref")
    support_refs = raw.get("support_refs", ())
    if not isinstance(support_refs, (list, tuple)) or not all(
        isinstance(item, str) for item in support_refs
    ):
        raise DecisionBindingError("invalid_output", "support_refs must be a list of strings")
    # Support refs must belong to the binding for both statuses: an abstain with
    # a forged support ref is still a forged ref, never evidence.
    if any(ref not in binding.ref_ids for ref in support_refs):
        raise DecisionBindingError("forged_ref", "support ref is not in the binding")
    if status is DecisionStatus.DECIDED:
        if not isinstance(target_ref, str) or target_ref not in binding.ref_ids:
            raise DecisionBindingError("forged_ref", "decided target is not in the binding")
        if not support_refs:
            raise DecisionBindingError("missing_evidence", "decided output needs support refs")
        if raw.get("reason") not in (None, ""):
            raise DecisionBindingError("invalid_output", "decided output must not carry a reason")
        return DecisionOutput(
            decision_kind=binding.decision_kind,
            status=status,
            target_ref=target_ref,
            support_refs=tuple(support_refs),
        )
    reason = raw.get("reason")
    if target_ref not in (None, "") or not isinstance(reason, str) or not reason:
        raise DecisionBindingError("invalid_output", "abstain needs a reason and no target")
    return DecisionOutput(
        decision_kind=binding.decision_kind,
        status=status,
        support_refs=tuple(support_refs),
        reason=reason,
    )


@dataclass(frozen=True, slots=True)
class SnapshotRef:
    """Current runtime state of one ref the snapshot was taken against."""

    ref_id: str
    revision: str | None
    content_hash: str
    consumed: bool = False


@dataclass(frozen=True, slots=True)
class DecisionSnapshot:
    """Apply-time state: current refs, candidate set and kill switch."""

    refs: Mapping[str, SnapshotRef]
    candidate_ids: frozenset[str]
    kill_switch: bool = False


def assert_applicable(
    binding: DecisionBinding,
    snapshot: DecisionSnapshot,
) -> None:
    """Snapshot checks in the point where the existing transition applies."""

    if snapshot.kill_switch:
        raise DecisionBindingError("frozen", "kill switch is active")
    if binding.candidate_ids != snapshot.candidate_ids:
        raise DecisionBindingError("stale_candidate_set", "candidate set changed since binding")
    for ref in (*binding.refs, *binding.candidates):
        current = snapshot.refs.get(ref.id)
        if current is None:
            raise DecisionBindingError("forged_ref", f"unknown ref at apply time: {ref.id}")
        if current.consumed:
            raise DecisionBindingError("consumed_ref", f"ref already consumed: {ref.id}")
        expected_revision = ref.revision
        if current.content_hash != ref.content_hash or (
            expected_revision is not None and current.revision != expected_revision
        ):
            raise DecisionBindingError("stale_ref", f"ref drifted since binding: {ref.id}")
