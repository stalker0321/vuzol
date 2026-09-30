"""Claim/source report for research-result.v1 (WP06, pure, side-effect free).

Every factual claim links to a retrieved source/position; unsupported claims
are marked, never silent. Without retrieval there is no verified research:
a report with zero sources is invalid, and a supported claim without a
citation is a validation error, not a warning.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass, field

RESEARCH_RESULT_SCHEMA = "research-result"
RESEARCH_RESULT_SCHEMA_VERSION = "research-result.v1"

# Legacy provider text wrapper (D0 split): the provider-step payload that used
# to reuse the source-report name. It carries no sources/claims and must never
# be presented as verified research.
RESEARCH_PROVIDER_RESULT_SCHEMA = "research-provider-result"
RESEARCH_PROVIDER_RESULT_SCHEMA_VERSION = "research-provider-result.v1"

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_RETRIEVERS = frozenset({"approved-http", "local-docs-fixture"})
_FRESHNESS = frozenset({"fresh", "stale"})
_SUPPORT = frozenset({"supported", "unsupported", "conflicting"})


@dataclass(frozen=True, slots=True)
class Source:
    source_id: str
    uri: str
    retriever: str
    retrieved_at: str
    content: str
    scope: str
    freshness_class: str = "fresh"
    quote: str | None = None

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.content.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Claim:
    claim_id: str
    statement: str
    support: str
    citations: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class ResearchReport:
    research_id: str
    question: str
    sources: tuple[Source, ...]
    claims: tuple[Claim, ...]
    created_at: str


def build_report(
    *,
    research_id: str,
    question: str,
    sources: tuple[Source, ...],
    claims: tuple[Claim, ...],
    created_at: str,
) -> ResearchReport:
    """Assemble a report; structural validation is a separate explicit step."""

    return ResearchReport(
        research_id=research_id,
        question=question,
        sources=sources,
        claims=claims,
        created_at=created_at,
    )


def validate_report(report: ResearchReport) -> tuple[str, ...]:
    """Fail-closed validation; returns stable error codes (empty means valid)."""

    errors: list[str] = []
    if report.research_id.strip() == "" or not _is_uuid(report.research_id):
        errors.append("research_id_invalid")
    if not report.question.strip():
        errors.append("question_missing")
    if not report.sources:
        errors.append("no_retrieval_no_verified_research")
    if not report.claims:
        errors.append("claims_missing")
    known_sources = {source.source_id for source in report.sources}
    for source in report.sources:
        if not source.source_id.strip():
            errors.append("source_id_missing")
        if not source.uri.strip():
            errors.append("source_uri_missing")
        if source.retriever not in _RETRIEVERS:
            errors.append("source_retriever_unapproved")
        if not source.retrieved_at.strip():
            errors.append("source_retrieved_at_missing")
        if not _HEX64.match(source.content_hash):
            errors.append("source_content_hash_invalid")
        if not source.scope.strip():
            errors.append("source_scope_missing")
        if source.freshness_class not in _FRESHNESS:
            errors.append("source_freshness_invalid")
    seen_claims = set()
    for claim in report.claims:
        if not claim.claim_id.strip() or claim.claim_id in seen_claims:
            errors.append("claim_id_invalid_or_duplicate")
        seen_claims.add(claim.claim_id)
        if not claim.statement.strip():
            errors.append("claim_statement_missing")
        if claim.support not in _SUPPORT:
            errors.append("claim_support_invalid")
        if claim.support == "supported" and not claim.citations:
            errors.append("supported_claim_without_citation")
        if claim.support == "conflicting" and len(claim.citations) < 2:
            errors.append("conflicting_claim_needs_two_citations")
        for source_id, position in claim.citations:
            if source_id not in known_sources:
                errors.append("citation_source_unknown")
            if not position.strip():
                errors.append("citation_position_missing")
    return tuple(errors)


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


_SOURCE_REPORT_BYTES_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["schema", "research_id", "question", "sources", "claims", "created_at"],
    "properties": {
        "schema": {"const": RESEARCH_RESULT_SCHEMA_VERSION},
        "research_id": {"type": "string", "minLength": 1},
        "question": {"type": "string", "minLength": 1},
        "sources": {"type": "array", "minItems": 1},
        "claims": {"type": "array", "minItems": 1},
        "created_at": {"type": "string", "minLength": 1},
    },
}


def validate_source_report_bytes(content: bytes) -> tuple[str, ...]:
    """Fail-closed typed consumer for source-report bytes (D0).

    Checks the raw bytes, never the ``binding.schema_version`` string. Legacy
    provider-text payloads (``text``/``structured_output`` without
    ``sources``/``claims``) are rejected, as is any shape mismatch — before
    any provider spend. Returns stable error codes (empty means valid).
    """

    import json

    from jsonschema import Draft202012Validator
    from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

    try:
        payload = json.loads(content.decode("utf-8"))
    except Exception:
        return ("source_report_not_json",)
    if not isinstance(payload, dict):
        return ("source_report_not_object",)
    # Legacy text wrapper must never pass as a source report, even when the
    # binding row claims the source-report version.
    if "sources" not in payload or "claims" not in payload:
        return ("legacy_provider_result_not_source_report",)
    if (
        "text" in payload
        and isinstance(payload.get("schema"), str)
        and not isinstance(payload.get("sources"), list)
    ):
        # A text wrapper relabelled as research-result.v1 is still legacy.
        return ("legacy_provider_result_not_source_report",)
    try:
        Draft202012Validator(_SOURCE_REPORT_BYTES_SCHEMA).validate(payload)
    except JsonSchemaValidationError as error:
        return (f"source_report_schema_mismatch:{error.validator}",)
    # Deep structural check via the typed validator (citations, hashes, etc.).
    try:
        sources = tuple(
            Source(
                source_id=str(item.get("source_id", "")),
                uri=str(item.get("uri", "")),
                retriever=str(item.get("retriever", "")),
                retrieved_at=str(item.get("retrieved_at", "")),
                content=str(item.get("content", item.get("uri", ""))),
                scope=str(item.get("scope", "")),
                freshness_class=str(item.get("freshness_class", "fresh")),
                quote=(str(item["quote"]) if item.get("quote") is not None else None),
            )
            for item in payload["sources"]
            if isinstance(item, dict)
        )
        claims = tuple(
            Claim(
                claim_id=str(item.get("claim_id", "")),
                statement=str(item.get("statement", "")),
                support=str(item.get("support", "")),
                citations=tuple(
                    (str(sid), str(pos))
                    for sid, pos in (item.get("citations") or [])
                    if isinstance(sid, str) and isinstance(pos, str)
                )
                if isinstance(item.get("citations"), list)
                else (),
            )
            for item in payload["claims"]
            if isinstance(item, dict)
        )
        report = ResearchReport(
            research_id=str(payload["research_id"]),
            question=str(payload["question"]),
            sources=sources,
            claims=claims,
            created_at=str(payload["created_at"]),
        )
    except Exception:
        return ("source_report_shape_invalid",)
    errors = validate_report(report)
    return errors


@dataclass
class MutableReport:
    """Test/fixture helper only; production code uses build_report."""

    question: str = ""
    sources: list[Source] = field(default_factory=list)
    claims: list[Claim] = field(default_factory=list)
