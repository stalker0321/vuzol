"""Shadow repair-triage decisions (WP11, report §9).

Pure module: one decision class ``repair_triage`` with finite choices plus
abstain. A shadow choice never executes anything by itself — executability is
decided by :func:`authorize_execution`, which fails closed on invalid, stale,
unsupported or evidence-less output. Abstain routes to the existing policy
path (``RecoveryAction.ATTENTION``). Field inventory is closed: choice,
evidence refs, reason code, revision, fingerprint — nothing else.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from vuzol.workflows.recovery_policy import RecoveryAction

DECISION_SCHEMA = "decision.v1"
DECISION_KIND = "repair_triage"
DECISION_SCHEMA_REVISION = "decision.v1"


class TriageChoice(StrEnum):
    RETRY = "retry"
    REPAIR = "repair"
    WAIT = "wait"
    ATTENTION = "attention"


EXECUTABLE_CHOICES = frozenset({TriageChoice.RETRY, TriageChoice.REPAIR, TriageChoice.WAIT})


class ReasonCode(StrEnum):
    KNOWN_LOCAL_FAILURE = "known_local_failure"
    TRANSIENT_RETRYABLE = "transient_retryable"
    BACKPRESSURE_WAIT = "backpressure_wait"
    UNKNOWN_FAILURE = "unknown_failure"
    OOD_INPUT = "ood_input"
    STALE_STATE = "stale_state"
    INVALID_OUTPUT = "invalid_output"
    MISSING_EVIDENCE = "missing_evidence"
    OSCILLATION_GUARD = "oscillation_guard"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    CAP_EXHAUSTED = "cap_exhausted"


_EVIDENCE_REF_PATTERN = re.compile(r"^artifact:[a-z0-9_-]{1,60}:sha256:[0-9a-f]{64}$")
_HEX64_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class DecisionInvalid(ValueError):
    """Shadow output failed validation: no transition."""


class DecisionStale(ValueError):
    """Shadow output targets an old state revision: no transition."""


class DecisionNotExecutable(ValueError):
    """Validated decision cannot execute (stale/unsupported/missing evidence)."""


@dataclass(frozen=True, slots=True)
class TriageDecision:
    decision_kind: str
    state_revision: int
    choice: TriageChoice
    evidence_refs: tuple[str, ...]
    reason_code: ReasonCode
    abstain: bool
    input_fingerprint: str
    repaired_once: bool = False

    def canonical(self) -> dict[str, Any]:
        return {
            "schema": DECISION_SCHEMA,
            "decision_kind": self.decision_kind,
            "state_revision": self.state_revision,
            "choice": self.choice.value,
            "evidence_refs": list(self.evidence_refs),
            "reason_code": self.reason_code.value,
            "abstain": self.abstain,
            "input_fingerprint": self.input_fingerprint,
        }

    @property
    def decision_hash(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


def _check_ref_format(ref: str) -> None:
    if not _EVIDENCE_REF_PATTERN.match(ref):
        raise DecisionInvalid(f"malformed evidence ref: {ref[:80]}")


def _parse_strict(data: Mapping[str, Any]) -> TriageDecision:
    if not isinstance(data, Mapping):
        raise DecisionInvalid("decision output must be a mapping")
    known = {
        "schema",
        "decision_kind",
        "state_revision",
        "choice",
        "evidence_refs",
        "reason_code",
        "abstain",
        "input_fingerprint",
    }
    unknown = set(data.keys()) - known
    if unknown:
        raise DecisionInvalid(f"unknown decision fields: {sorted(unknown)[:3]}")
    if data.get("schema") != DECISION_SCHEMA:
        raise DecisionInvalid("decision schema must be decision.v1")
    if data.get("decision_kind") != DECISION_KIND:
        raise DecisionInvalid("decision kind must be repair_triage")
    state_revision = data.get("state_revision")
    if not isinstance(state_revision, int) or state_revision < 0:
        raise DecisionInvalid("state_revision must be a non-negative integer")
    raw_choice = data.get("choice")
    if not isinstance(raw_choice, str):
        raise DecisionInvalid(f"unsupported choice: {raw_choice!r}")
    try:
        choice = TriageChoice(raw_choice)
    except ValueError:
        raise DecisionInvalid(f"unsupported choice: {raw_choice!r}") from None
    raw_reason = data.get("reason_code")
    if not isinstance(raw_reason, str):
        raise DecisionInvalid(f"unsupported reason code: {raw_reason!r}")
    try:
        reason_code = ReasonCode(raw_reason)
    except ValueError:
        raise DecisionInvalid(f"unsupported reason code: {raw_reason!r}") from None
    evidence_refs = data.get("evidence_refs")
    if not isinstance(evidence_refs, (list, tuple)) or not all(
        isinstance(ref, str) for ref in evidence_refs
    ):
        raise DecisionInvalid("evidence_refs must be a list of strings")
    for ref in evidence_refs:
        _check_ref_format(ref)
    abstain = data.get("abstain")
    if not isinstance(abstain, bool):
        raise DecisionInvalid("abstain must be a boolean")
    fingerprint = data.get("input_fingerprint")
    if not isinstance(fingerprint, str) or not _HEX64_PATTERN.match(fingerprint):
        raise DecisionInvalid("input_fingerprint must be a sha256 hex digest")
    if abstain and choice is not TriageChoice.ATTENTION:
        raise DecisionInvalid("abstain must route to attention")
    if not abstain and not evidence_refs:
        raise DecisionInvalid("non-abstain choice requires evidence refs")
    return TriageDecision(
        decision_kind=DECISION_KIND,
        state_revision=state_revision,
        choice=choice,
        evidence_refs=tuple(evidence_refs),
        reason_code=reason_code,
        abstain=abstain,
        input_fingerprint=fingerprint,
    )


def _normalize_once(data: Mapping[str, Any]) -> dict[str, Any]:
    """Single allowed schema-repair pass: trim strings, drop unknown keys."""

    known = {
        "schema",
        "decision_kind",
        "state_revision",
        "choice",
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


def interpret_output(data: Mapping[str, Any]) -> tuple[TriageDecision, bool]:
    """Validate shadow output with at most one schema-repair, else abstain.

    Returns (decision, repaired_once). A second failure becomes an abstain
    decision to the existing policy path — never a transition, never a retry
    of the model.
    """

    try:
        return _parse_strict(data), False
    except DecisionInvalid:
        pass
    try:
        decision = _parse_strict(_normalize_once(data))
    except DecisionInvalid as error:
        raise DecisionInvalid(f"unrepairable shadow output: {error}") from error
    return (
        TriageDecision(
            decision_kind=decision.decision_kind,
            state_revision=decision.state_revision,
            choice=decision.choice,
            evidence_refs=decision.evidence_refs,
            reason_code=decision.reason_code,
            abstain=decision.abstain,
            input_fingerprint=decision.input_fingerprint,
            repaired_once=True,
        ),
        True,
    )


def abstain_decision(
    *, state_revision: int, input_fingerprint: str, reason_code: ReasonCode
) -> TriageDecision:
    """Build the fail-closed abstain decision (existing policy path)."""

    if not _HEX64_PATTERN.match(input_fingerprint):
        raise DecisionInvalid("input_fingerprint must be a sha256 hex digest")
    return TriageDecision(
        decision_kind=DECISION_KIND,
        state_revision=state_revision,
        choice=TriageChoice.ATTENTION,
        evidence_refs=(),
        reason_code=reason_code,
        abstain=True,
        input_fingerprint=input_fingerprint,
    )


def check_fresh(decision: TriageDecision, expected_state_revision: int) -> None:
    if decision.state_revision != expected_state_revision:
        raise DecisionStale(
            f"decision targets revision {decision.state_revision}, "
            f"expected {expected_state_revision}"
        )


def verify_evidence_link(ref: str, content: bytes) -> bool:
    """Re-verify an evidence ref against retained bytes (hash must match)."""

    digest = ref.rsplit(":", 1)[-1]
    return hashlib.sha256(content).hexdigest() == digest


def authorize_execution(
    decision: TriageDecision,
    *,
    expected_state_revision: int,
    evidence: Mapping[str, bytes],
) -> RecoveryAction:
    """Map a validated shadow decision to the existing policy action.

    Fails closed (raises, no transition) on stale revision, unsupported
    choice, or missing/unverifiable evidence. Abstain returns ATTENTION —
    the existing policy path — without executing any model-proposed action.
    """

    check_fresh(decision, expected_state_revision)
    if decision.abstain:
        return RecoveryAction.ATTENTION
    if decision.choice not in EXECUTABLE_CHOICES and decision.choice is not TriageChoice.ATTENTION:
        raise DecisionNotExecutable(f"unsupported choice: {decision.choice.value}")
    for ref in decision.evidence_refs:
        content = evidence.get(ref)
        if content is None:
            raise DecisionNotExecutable(f"missing evidence: {ref[:60]}")
        if not verify_evidence_link(ref, content):
            raise DecisionNotExecutable(f"evidence link broken: {ref[:60]}")
    return RecoveryAction(decision.choice.value)


@dataclass(frozen=True, slots=True)
class WhitelistGate:
    """Production whitelist for decision classes. Default off.

    Enabling requires explicit gate evidence (e.g. a preregistration/report
    hash); allowlist membership is the only route to production.
    """

    enabled_kinds: frozenset[str] = frozenset()

    def allows(self, decision_kind: str) -> bool:
        return decision_kind in self.enabled_kinds

    def enable(self, decision_kind: str, *, gate_evidence: str) -> WhitelistGate:
        if not gate_evidence.strip():
            raise ValueError("whitelist enable requires gate evidence")
        return WhitelistGate(enabled_kinds=self.enabled_kinds | {decision_kind})
