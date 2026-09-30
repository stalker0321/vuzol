"""Shadow target-selection decisions (D4 W7 rollout).

Second decision kind, parallel to ``repair_triage`` — never a modification of
it. The model chooses one opaque candidate ID from a deterministically
selected set, or abstains. Same downstream runtime, same evidence/revision
contract, shared budget, bounded single repair, no auto-promotion.

Kind: ``target_selection``. Schema: ``decision.v2`` (new version, not an
extension of ``decision.v1``).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

DECISION_SCHEMA = "decision.v2"
DECISION_KIND = "target_selection"

_CANDIDATE_ID_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_EVIDENCE_REF_PATTERN = re.compile(r"^artifact:[a-z0-9_-]{1,60}:sha256:[0-9a-f]{64}$")
_HEX64_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class TargetChoice(StrEnum):
    SELECT = "select"
    ATTENTION = "attention"


class TargetReasonCode(StrEnum):
    CLEAR_MATCH = "clear_match"
    WRONG_TARGET_RISK = "wrong_target_risk"
    AMBIGUOUS_CANDIDATES = "ambiguous_candidates"
    CORRECTION_APPLIED = "correction_applied"
    OOD_INPUT = "ood_input"
    STALE_STATE = "stale_state"
    INVALID_OUTPUT = "invalid_output"
    MISSING_EVIDENCE = "missing_evidence"


class TargetDecisionInvalid(ValueError):
    """Shadow output failed validation: no transition."""


class TargetDecisionStale(ValueError):
    """Shadow output targets an old state revision: no transition."""


@dataclass(frozen=True, slots=True)
class TargetDecision:
    decision_kind: str
    state_revision: int
    choice: TargetChoice
    candidate_id: str | None
    evidence_refs: tuple[str, ...]
    reason_code: TargetReasonCode
    abstain: bool
    input_fingerprint: str
    repaired_once: bool = False

    def canonical(self) -> dict[str, Any]:
        return {
            "schema": DECISION_SCHEMA,
            "decision_kind": self.decision_kind,
            "state_revision": self.state_revision,
            "choice": self.choice.value,
            "candidate_id": self.candidate_id,
            "evidence_refs": list(self.evidence_refs),
            "reason_code": self.reason_code.value,
            "abstain": self.abstain,
            "input_fingerprint": self.input_fingerprint,
        }

    @property
    def decision_hash(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


def _parse_strict(data: Mapping[str, Any]) -> TargetDecision:
    if not isinstance(data, Mapping):
        raise TargetDecisionInvalid("decision output must be a mapping")
    known = {
        "schema",
        "decision_kind",
        "state_revision",
        "choice",
        "candidate_id",
        "evidence_refs",
        "reason_code",
        "abstain",
        "input_fingerprint",
    }
    unknown = set(data.keys()) - known
    if unknown:
        raise TargetDecisionInvalid(f"unknown decision fields: {sorted(unknown)[:3]}")
    if data.get("schema") != DECISION_SCHEMA:
        raise TargetDecisionInvalid("decision schema must be decision.v2")
    if data.get("decision_kind") != DECISION_KIND:
        raise TargetDecisionInvalid("decision kind must be target_selection")
    state_revision = data.get("state_revision")
    if not isinstance(state_revision, int) or state_revision < 0:
        raise TargetDecisionInvalid("state_revision must be a non-negative integer")
    raw_choice = data.get("choice")
    if not isinstance(raw_choice, str):
        raise TargetDecisionInvalid(f"unsupported choice: {raw_choice!r}")
    try:
        choice = TargetChoice(raw_choice)
    except ValueError:
        raise TargetDecisionInvalid(f"unsupported choice: {raw_choice!r}") from None
    candidate_id = data.get("candidate_id")
    if isinstance(candidate_id, str) and _CANDIDATE_ID_PATTERN.fullmatch(candidate_id) is None:
        raise TargetDecisionInvalid("candidate_id must be a bounded slug")
    if candidate_id is not None and not isinstance(candidate_id, str):
        raise TargetDecisionInvalid("candidate_id must be a bounded slug")
    raw_reason = data.get("reason_code")
    if not isinstance(raw_reason, str):
        raise TargetDecisionInvalid(f"unsupported reason code: {raw_reason!r}")
    try:
        reason_code = TargetReasonCode(raw_reason)
    except ValueError:
        raise TargetDecisionInvalid(f"unsupported reason code: {raw_reason!r}") from None
    evidence_refs = data.get("evidence_refs")
    if not isinstance(evidence_refs, (list, tuple)) or not all(
        isinstance(ref, str) for ref in evidence_refs
    ):
        raise TargetDecisionInvalid("evidence_refs must be a list of strings")
    for ref in evidence_refs:
        if not _EVIDENCE_REF_PATTERN.match(ref):
            raise TargetDecisionInvalid(f"malformed evidence ref: {ref[:80]}")
    abstain = data.get("abstain")
    if not isinstance(abstain, bool):
        raise TargetDecisionInvalid("abstain must be a boolean")
    fingerprint = data.get("input_fingerprint")
    if not isinstance(fingerprint, str) or not _HEX64_PATTERN.match(fingerprint):
        raise TargetDecisionInvalid("input_fingerprint must be a sha256 hex digest")
    if abstain and choice is not TargetChoice.ATTENTION:
        raise TargetDecisionInvalid("abstain must route to attention")
    if abstain and candidate_id is not None:
        raise TargetDecisionInvalid("abstain carries no candidate")
    if not abstain and (not evidence_refs or candidate_id is None):
        raise TargetDecisionInvalid("select requires evidence refs and candidate_id")
    return TargetDecision(
        decision_kind=DECISION_KIND,
        state_revision=state_revision,
        choice=choice,
        candidate_id=candidate_id,
        evidence_refs=tuple(evidence_refs),
        reason_code=reason_code,
        abstain=abstain,
        input_fingerprint=fingerprint,
    )


def _normalize_once(data: Mapping[str, Any]) -> dict[str, Any]:
    known = {
        "schema",
        "decision_kind",
        "state_revision",
        "choice",
        "candidate_id",
        "evidence_refs",
        "reason_code",
        "abstain",
        "input_fingerprint",
    }
    cleaned: dict[str, Any] = {}
    for key, value in data.items():
        if key not in known:
            continue
        cleaned[key] = value.strip() if isinstance(value, str) else value
    refs = cleaned.get("evidence_refs")
    if isinstance(refs, list):
        cleaned["evidence_refs"] = [
            ref.strip() for ref in refs if isinstance(ref, str) and ref.strip()
        ]
    return cleaned


def interpret_output(data: Mapping[str, Any]) -> tuple[TargetDecision, bool]:
    """Validate with at most one schema-repair, else raise (caller abstains)."""

    try:
        return _parse_strict(data), False
    except TargetDecisionInvalid:
        pass
    try:
        decision = _parse_strict(_normalize_once(data))
    except TargetDecisionInvalid as error:
        raise TargetDecisionInvalid(f"unrepairable shadow output: {error}") from error
    return (
        TargetDecision(
            decision_kind=decision.decision_kind,
            state_revision=decision.state_revision,
            choice=decision.choice,
            candidate_id=decision.candidate_id,
            evidence_refs=decision.evidence_refs,
            reason_code=decision.reason_code,
            abstain=decision.abstain,
            input_fingerprint=decision.input_fingerprint,
            repaired_once=True,
        ),
        True,
    )


def abstain_decision(
    *, state_revision: int, input_fingerprint: str, reason_code: TargetReasonCode
) -> TargetDecision:
    if not _HEX64_PATTERN.match(input_fingerprint):
        raise TargetDecisionInvalid("input_fingerprint must be a sha256 hex digest")
    return TargetDecision(
        decision_kind=DECISION_KIND,
        state_revision=state_revision,
        choice=TargetChoice.ATTENTION,
        candidate_id=None,
        evidence_refs=(),
        reason_code=reason_code,
        abstain=True,
        input_fingerprint=input_fingerprint,
    )


def check_fresh(decision: TargetDecision, expected_state_revision: int) -> None:
    if decision.state_revision != expected_state_revision:
        raise TargetDecisionStale(
            f"decision targets revision {decision.state_revision}, "
            f"expected {expected_state_revision}"
        )
