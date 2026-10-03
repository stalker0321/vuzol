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
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.context.assembler import AssembledContext
from vuzol.context.decision_binding import Coverage
from vuzol.interpretation.prompt_loader import PromptKind, compose_prompt, prompt_hash
from vuzol.storage.models import Event

INTAKE_SCHEMA = "decision.v3"
INTAKE_KIND = "intake"
TARGET_RESOLUTION_KIND = "target_resolution"

ADVISORY_EVENT_TYPE = "jev.shadow_recorded"
ADVISORY_ENTITY_TYPE = "jev_shadow_decision"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_INJECTION_MARKERS = (
    "ignore previous",
    "ignore all previous",
    "ignore the above",
    "system prompt",
    "you are now",
    "assistant:",
    "<system>",
    "disregard your",
)


class IntakeEffect(StrEnum):
    RESPOND = "respond"
    PLAN = "plan"
    EXECUTE_REQUEST = "execute_request"
    CONTROL_REQUEST = "control_request"
    STATUS = "status"
    CAPTURE = "capture"


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
    relation: IntakeRelation
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
            "relation": self.relation.value,
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


@dataclass(frozen=True, slots=True)
class ShadowRun:
    decision_kind: str
    explicit: bool
    provider_calls: int
    decision: IntakeDecision | None
    route_hint: str | None
    reason_code: IntakeReasonCode
    event_id: uuid.UUID | None
    prompt_hash: str | None
    context_hash: str
    target_resolution_ran: bool = False
    production_transitions: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


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
    if abstain:
        if reason_code is IntakeReasonCode.CLEAR_MATCH:
            raise IntakeDecisionInvalid("abstain requires a non-clear reason code")
        if target_id is not None:
            raise IntakeDecisionInvalid("abstain carries no target")
    else:
        if effect not in allowed_effects:
            raise IntakeDecisionInvalid(f"effect not allowed: {effect.value}")
        if not support_refs:
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


def intake_abstain_decision(
    *,
    state_revision: int,
    input_fingerprint: str,
    prompt_digest: str,
    reason_code: IntakeReasonCode,
    coverage: Coverage = Coverage.UNKNOWN,
) -> IntakeDecision:
    return IntakeDecision(
        decision_kind=INTAKE_KIND,
        state_revision=state_revision,
        effect=IntakeEffect.RESPOND,
        relation=IntakeRelation.NONE,
        target_id=None,
        support_refs=(),
        coverage=coverage,
        abstain=True,
        reason_code=reason_code,
        input_fingerprint=input_fingerprint,
        prompt_hash=prompt_digest,
    )


def route_hint(decision: IntakeDecision) -> str | None:
    """Translate a label into an advisory route hint; it grants nothing."""

    if decision.abstain:
        return None
    return decision.effect.value


def detect_injection(text: str | None) -> bool:
    if not text:
        return False
    normalized = text.casefold()
    return any(marker in normalized for marker in _INJECTION_MARKERS)


def context_fingerprint(context: AssembledContext, prompt_digest: str) -> str:
    encoded = json.dumps(
        {
            "context": context.model_dump(mode="json"),
            "prompt_hash": prompt_digest,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


async def record_advisory_decision(
    session: AsyncSession,
    *,
    correlation_id: str,
    decision_kind: str,
    decision_hash: str,
    reason_code: IntakeReasonCode,
    payload: Mapping[str, Any],
    route_hint_value: str | None = None,
    prompt_digest: str | None = None,
    source_turn_ref: str | None = None,
) -> uuid.UUID:
    """Persist one advisory event. It never advances execution state."""

    event = Event(
        entity_type=ADVISORY_ENTITY_TYPE,
        entity_id=uuid.uuid5(uuid.NAMESPACE_URL, f"{correlation_id}:{decision_hash}"),
        event_type=ADVISORY_EVENT_TYPE,
        actor_type="jev_shadow_recorder",
        actor_id="jev-shadow",
        correlation_id=correlation_id,
        payload={
            "decision_kind": decision_kind,
            "decision_sha256": decision_hash,
            "reason_code": reason_code.value,
            "route_hint": route_hint_value,
            "prompt_hash": prompt_digest,
            "source_turn_ref": source_turn_ref,
            "advisory": True,
            "production_transition": False,
            "decision": dict(payload),
        },
    )
    session.add(event)
    await session.flush()
    return event.id


async def run_intake_shadow(
    session: AsyncSession,
    *,
    context: AssembledContext,
    correlation_id: str,
    provider_call: Callable[[str], Awaitable[Mapping[str, Any]]],
    allowed_effects: tuple[IntakeEffect, ...] = tuple(IntakeEffect),
    allowed_refs: frozenset[str] = frozenset(),
    expected_state_revision: int = 0,
    explicit_command: bool = False,
    injection_text: str | None = None,
    source_turn_ref: str | None = None,
    expected_context_hash: str | None = None,
    run_target_resolution: Callable[[str], Awaitable[Mapping[str, Any]]] | None = None,
) -> ShadowRun:
    """One INTAKE pass (plus at most one TARGET_RESOLUTION) in shadow.

    The provider call is never invoked for an explicit command, a detected
    injection, an out-of-domain snapshot or a source change. Provider failures
    and schema errors are recorded as visible advisory outcomes with the prompt
    hash and fingerprint preserved (provenance survives fallback).
    """

    prompt_digest = prompt_hash(PromptKind.INTAKE)
    context_digest = context_fingerprint(context, prompt_digest)
    calls = 0

    async def record(
        *,
        decision: IntakeDecision,
        reason_code: IntakeReasonCode,
        extra_calls: int = 0,
        target_resolution_ran: bool = False,
    ) -> ShadowRun:
        event_id = await record_advisory_decision(
            session,
            correlation_id=correlation_id,
            decision_kind=INTAKE_KIND,
            decision_hash=decision.decision_hash,
            reason_code=reason_code,
            payload=decision.canonical(),
            route_hint_value=route_hint(decision),
            prompt_digest=decision.prompt_hash,
            source_turn_ref=source_turn_ref,
        )
        return ShadowRun(
            decision_kind=INTAKE_KIND,
            explicit=explicit_command,
            provider_calls=calls + extra_calls,
            decision=decision,
            route_hint=route_hint(decision),
            reason_code=reason_code,
            event_id=event_id,
            prompt_hash=decision.prompt_hash,
            context_hash=context_digest,
            target_resolution_ran=target_resolution_ran,
        )

    if explicit_command:
        decision = intake_abstain_decision(
            state_revision=expected_state_revision,
            input_fingerprint=context_digest,
            prompt_digest=prompt_digest,
            reason_code=IntakeReasonCode.EXPLICIT_COMMAND,
            coverage=context.coverage,
        )
        return await record(decision=decision, reason_code=IntakeReasonCode.EXPLICIT_COMMAND)

    if detect_injection(injection_text):
        decision = intake_abstain_decision(
            state_revision=expected_state_revision,
            input_fingerprint=context_digest,
            prompt_digest=prompt_digest,
            reason_code=IntakeReasonCode.INJECTION_DETECTED,
            coverage=context.coverage,
        )
        return await record(decision=decision, reason_code=IntakeReasonCode.INJECTION_DETECTED)

    if expected_context_hash is not None and expected_context_hash != context_digest:
        decision = intake_abstain_decision(
            state_revision=expected_state_revision,
            input_fingerprint=context_digest,
            prompt_digest=prompt_digest,
            reason_code=IntakeReasonCode.SOURCE_CHANGED,
            coverage=context.coverage,
        )
        return await record(decision=decision, reason_code=IntakeReasonCode.SOURCE_CHANGED)

    if (
        context.coverage is Coverage.UNKNOWN
        and not context.candidates
        and not context.recent.entries
    ):
        decision = intake_abstain_decision(
            state_revision=expected_state_revision,
            input_fingerprint=context_digest,
            prompt_digest=prompt_digest,
            reason_code=IntakeReasonCode.OOD_INPUT,
            coverage=context.coverage,
        )
        return await record(decision=decision, reason_code=IntakeReasonCode.OOD_INPUT)

    prompt = compose_prompt(PromptKind.INTAKE)
    calls += 1
    try:
        raw = await provider_call(prompt)
        decision = parse_intake_output(
            raw,
            allowed_effects=allowed_effects,
            allowed_refs=allowed_refs,
            prompt_digest=prompt_digest,
        )
    except IntakeDecisionInvalid:
        decision = intake_abstain_decision(
            state_revision=expected_state_revision,
            input_fingerprint=context_digest,
            prompt_digest=prompt_digest,
            reason_code=IntakeReasonCode.INVALID_OUTPUT,
            coverage=context.coverage,
        )
        return await record(decision=decision, reason_code=IntakeReasonCode.INVALID_OUTPUT)

    reason_code = decision.reason_code if decision.abstain else IntakeReasonCode.CLEAR_MATCH
    target_resolution_ran = False
    extra_calls = 0
    if (
        run_target_resolution is not None
        and not decision.abstain
        and decision.effect in {IntakeEffect.EXECUTE_REQUEST, IntakeEffect.CONTROL_REQUEST}
        and decision.target_id is None
        and (bool(context.candidates) or context.coverage is Coverage.PARTIAL)
    ):
        extra_calls += 1
        target_resolution_ran = True
        target_prompt = compose_prompt(PromptKind.TARGET_RESOLUTION)
        await run_target_resolution(target_prompt)
    return await record(
        decision=decision,
        reason_code=reason_code,
        extra_calls=extra_calls,
        target_resolution_ran=target_resolution_ran,
    )
