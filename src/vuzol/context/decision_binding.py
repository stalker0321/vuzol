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

import re
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

DECISION_PACKET_SCHEMA = "decision-packet.v1"
DECISION_BINDING_SCHEMA = "decision-binding.v1"

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


