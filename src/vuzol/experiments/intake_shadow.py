"""Shadow INTAKE / TARGET_RESOLUTION harness (J3).

Runs exactly one INTAKE classification for a semantically unresolved turn and
at most one TARGET_RESOLUTION pass when new target facts appear. Everything is
advisory: the harness writes ``Event`` records only and never touches Task,
Run, WorkPackage or approval state. Explicit task commands skip the classifier
entirely (zero provider calls). Every run reports the full provider call count,
not just the Jev price.

Schema: ``decision.v3`` (new version; ``decision.v1``/``decision.v2`` readers are
untouched).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from vuzol.context.decision_binding import Coverage

INTAKE_SCHEMA = "decision.v3"
INTAKE_KIND = "intake"


_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class IntakeEffect(StrEnum):
    RESPOND = "respond"
    PLAN = "plan"
    EXECUTE_REQUEST = "execute_request"
    CONTROL_REQUEST = "control_request"
    STATUS = "status"
    CAPTURE = "capture"
    MIXED = "mixed"


class IntakeRelation(StrEnum):
    NEW = "new"
    CONTINUE = "continue"
    AMEND = "amend"
    CORRECT = "correct"
    REVIEW_RESULT = "review_result"
    NONE = "none"


class IntakeReasonCode(StrEnum):
    CLEAR_MATCH = "clear_match"
    COMPOUND_INTENT = "compound_intent"
    AMBIGUOUS_TARGET = "ambiguous_target"
    UNLISTED_GAP = "unlisted_gap"
    NO_SUPPORTED_TARGET = "no_supported_target"
    QUOTED_OR_NEGATED = "quoted_or_negated"
    INJECTION_DETECTED = "injection_detected"
    OOD_INPUT = "ood_input"
    STALE_STATE = "stale_state"
    INVALID_OUTPUT = "invalid_output"
    PROVIDER_FAILURE = "provider_failure"
    SOURCE_CHANGED = "source_changed"
    MISSING_EVIDENCE = "missing_evidence"
    EXPLICIT_COMMAND = "explicit_command"


class IntakeDecisionInvalid(ValueError):
    """Shadow output failed validation: advisory abstain, no transition."""


@dataclass(frozen=True, slots=True)
class IntakeDecision:
    decision_kind: str
    state_revision: int
    effect: IntakeEffect
    relation: IntakeRelation | None
    target_id: str | None
    support_refs: tuple[str, ...]
    coverage: Coverage
    abstain: bool
    reason_code: IntakeReasonCode
    input_fingerprint: str
    prompt_hash: str
    repaired_once: bool = False

    def canonical(self) -> dict[str, Any]:
        return {
            "schema": INTAKE_SCHEMA,
            "decision_kind": self.decision_kind,
            "state_revision": self.state_revision,
            "effect": self.effect.value,
            "relation": self.relation.value if self.relation is not None else None,
            "target_id": self.target_id,
            "support_refs": list(self.support_refs),
            "coverage": self.coverage.value,
            "abstain": self.abstain,
            "reason_code": self.reason_code.value,
            "input_fingerprint": self.input_fingerprint,
            "prompt_hash": self.prompt_hash,
        }

    @property
    def decision_hash(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


def _known_output_keys() -> frozenset[str]:
    return frozenset(
        {
            "schema",
            "decision_kind",
            "state_revision",
            "effect",
            "relation",
            "target_id",
            "support_refs",
            "coverage",
            "abstain",
            "reason_code",
            "input_fingerprint",
        }
    )


def parse_intake_output(
    data: Mapping[str, Any],
    *,
    allowed_effects: tuple[IntakeEffect, ...],
    allowed_refs: frozenset[str],
    prompt_digest: str,
) -> IntakeDecision:
    """Strict validation after JSON Schema: unknown keys and forged refs fail closed."""

    if not isinstance(data, Mapping):
        raise IntakeDecisionInvalid("decision output must be a mapping")
    unknown = set(data.keys()) - _known_output_keys()
    if unknown:
        raise IntakeDecisionInvalid(f"unknown decision fields: {sorted(unknown)[:3]}")
    if data.get("schema") != INTAKE_SCHEMA:
        raise IntakeDecisionInvalid("decision schema must be decision.v3")
    if data.get("decision_kind") != INTAKE_KIND:
        raise IntakeDecisionInvalid("decision kind must be intake")
    state_revision = data.get("state_revision")
    if not isinstance(state_revision, int) or state_revision < 0:
        raise IntakeDecisionInvalid("state_revision must be a non-negative integer")
    raw_effect = data.get("effect")
    if not isinstance(raw_effect, str):
        raise IntakeDecisionInvalid(f"unsupported effect: {raw_effect!r}")
    try:
        effect = IntakeEffect(raw_effect)
    except ValueError:
        raise IntakeDecisionInvalid(f"unsupported effect: {raw_effect!r}") from None
    raw_relation = data.get("relation")
    if raw_relation is None and "relation" in data and effect is IntakeEffect.MIXED:
        relation = None
    else:
        if not isinstance(raw_relation, str):
            raise IntakeDecisionInvalid(f"unsupported relation: {raw_relation!r}")
        try:
            relation = IntakeRelation(raw_relation)
        except ValueError:
            raise IntakeDecisionInvalid(f"unsupported relation: {raw_relation!r}") from None
    target_id = data.get("target_id")
    if target_id is not None and (not isinstance(target_id, str) or target_id not in allowed_refs):
        raise IntakeDecisionInvalid("target_id is not an allowed supplied reference")
    support_refs = data.get("support_refs")
    if not isinstance(support_refs, (list, tuple)) or not all(
        isinstance(ref, str) for ref in support_refs
    ):
        raise IntakeDecisionInvalid("support_refs must be a list of strings")
    if any(ref not in allowed_refs for ref in support_refs):
        raise IntakeDecisionInvalid("support_ref is not an allowed supplied reference")
    raw_coverage = data.get("coverage")
    if not isinstance(raw_coverage, str):
        raise IntakeDecisionInvalid(f"unsupported coverage: {raw_coverage!r}")
    try:
        coverage = Coverage(raw_coverage)
    except ValueError:
        raise IntakeDecisionInvalid(f"unsupported coverage: {raw_coverage!r}") from None
    abstain = data.get("abstain")
    if not isinstance(abstain, bool):
        raise IntakeDecisionInvalid("abstain must be a boolean")
    raw_reason = data.get("reason_code")
    if not isinstance(raw_reason, str):
        raise IntakeDecisionInvalid(f"unsupported reason code: {raw_reason!r}")
    try:
        reason_code = IntakeReasonCode(raw_reason)
    except ValueError:
        raise IntakeDecisionInvalid(f"unsupported reason code: {raw_reason!r}") from None
    fingerprint = data.get("input_fingerprint")
    if not isinstance(fingerprint, str) or not _SHA256.match(fingerprint):
        raise IntakeDecisionInvalid("input_fingerprint must be a sha256 hex digest")
    # Effect membership is checked for both statuses: an abstain carrying an
    # effect outside the allowed set is still a forged effect.
    if effect not in allowed_effects:
        raise IntakeDecisionInvalid(f"effect not allowed: {effect.value}")
    if abstain:
        if reason_code is IntakeReasonCode.CLEAR_MATCH:
            raise IntakeDecisionInvalid("abstain requires a non-clear reason code")
        if target_id is not None:
            raise IntakeDecisionInvalid("abstain carries no target")
    elif not support_refs:
        raise IntakeDecisionInvalid("decided output needs support refs")
    return IntakeDecision(
        decision_kind=INTAKE_KIND,
        state_revision=state_revision,
        effect=effect,
        relation=relation,
        target_id=target_id,
        support_refs=tuple(support_refs),
        coverage=coverage,
        abstain=abstain,
        reason_code=reason_code,
        input_fingerprint=fingerprint,
        prompt_hash=prompt_digest,
    )


