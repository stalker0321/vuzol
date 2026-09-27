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


@dataclass
class MutableReport:
    """Test/fixture helper only; production code uses build_report."""

    question: str = ""
    sources: list[Source] = field(default_factory=list)
    claims: list[Claim] = field(default_factory=list)
