"""Source-backed research assembly (D3 W1).

Connects a ``research_execute`` model result to the retrieval adapter:
model-proposed source URIs are fetched (offline fixtures in CI/tests, the
approved-HTTP adapter only when explicitly configured — live trials are
forbidden), lifted into ``Source`` rows, bound to claims and persisted as a
real ``research-result.v1`` report. Anything short of full success falls
back to the legacy provider-text path (no verified label); there are no
partial verified reports.

Hash chain: ``RetrievedSource.content_hash`` is sha256 of the RAW bytes.
Report ``Source.content_hash`` is sha256 of the decoded text. Assembly
requires strict UTF-8 so both hashes coincide; otherwise it refuses outright
(W1 acceptance: raw sha == report hash, or explicit refusal).

Citations are structural: ``offset:<start>-<end>`` byte ranges into the
cited source's raw bytes, validated against the raw length. Schema validity
(``validate_report``) is enforced; claim entailment is NOT checked and is
never presented as verified (see RESEARCH_PROVENANCE).
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from vuzol.research.report import (
    RESEARCH_RESULT_SCHEMA_VERSION,
    Claim,
    Source,
)
from vuzol.research.retrieval import RetrievalError, RetrievedSource
from vuzol.research.synthesize import (
    SynthesisError,
    sources_from_retrieved,
)
from vuzol.research.synthesize import (
    bind_report as _bind_report,
)

# D3: source-backed research bindings expire on the oldest retrieval, not on
# repack time. Producers set this; the resolver anchors on it (lead Q7).
SOURCE_FRESHNESS_MAX_AGE_SECONDS = 7 * 86400

CITATION_RE = re.compile(r"^offset:(\d+)-(\d+)$")


class SourceFetcher(Protocol):
    """Retrieval seam: fetch one URI at a logical time (offline in CI/tests)."""

    def __call__(self, uri: str, *, now: str) -> RetrievedSource: ...


class SourceBackedError(RuntimeError):
    """Source-backed assembly refused; caller falls back to legacy text."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class RawSourceBlob:
    uri: str
    content: bytes

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.content).hexdigest()


def parse_citation_position(position: str, *, raw_length: int) -> tuple[int, int]:
    """Parse ``offset:start-end`` and bound it to the raw byte length."""

    match = CITATION_RE.fullmatch(position.strip())
    if match is None:
        raise SourceBackedError(
            "citation_position_not_structural",
            f"citation must be offset:start-end, got {position!r}",
        )
    start, end = int(match.group(1)), int(match.group(2))
    if not (0 <= start < end <= raw_length):
        raise SourceBackedError(
            "citation_range_out_of_bounds",
            f"citation {position!r} outside raw length {raw_length}",
        )
    return start, end


def report_to_json(
    *,
    research_id: str,
    question: str,
    sources: tuple[Source, ...],
    claims: tuple[Claim, ...],
    created_at: str,
) -> bytes:
    """Canonical ``research-result.v1`` bytes (must pass the D0 consumer)."""

    document = {
        "schema": RESEARCH_RESULT_SCHEMA_VERSION,
        "research_id": research_id,
        "question": question,
        "sources": [
            {
                "source_id": source.source_id,
                "uri": source.uri,
                "retriever": source.retriever,
                "retrieved_at": source.retrieved_at,
                "content": source.content,
                "content_hash": source.content_hash,
                "scope": source.scope,
                "freshness_class": source.freshness_class,
                **({"quote": source.quote} if source.quote is not None else {}),
            }
            for source in sources
        ],
        "claims": [
            {
                "claim_id": claim.claim_id,
                "statement": claim.statement,
                "support": claim.support,
                "citations": [list(pair) for pair in claim.citations],
            }
            for claim in claims
        ],
        "created_at": created_at,
    }
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _model_contract(structured: object) -> dict[str, Any]:
    """Extract the model-proposed research contract (fail-closed)."""

    if not isinstance(structured, dict):
        raise SourceBackedError("research_contract_missing")
    contract = structured.get("research")
    if not isinstance(contract, dict):
        raise SourceBackedError("research_contract_missing")
    sources = contract.get("sources")
    claims = contract.get("claims")
    if not isinstance(sources, list) or not sources:
        raise SourceBackedError("research_sources_missing")
    if not isinstance(claims, list) or not claims:
        raise SourceBackedError("research_claims_missing")
    question = contract.get("question", "")
    if not isinstance(question, str) or not question.strip():
        raise SourceBackedError("research_question_missing")
    return {"question": question.strip(), "sources": sources, "claims": claims}


def assemble_source_report(
    *,
    structured_output: object,
    task_question: str,
    scope: str,
    created_at: str,
    fetch: SourceFetcher,
    retriever: str,
) -> tuple[bytes, tuple[RawSourceBlob, ...], str]:
    """Fetch, validate and serialize a source-backed report.

    Returns ``(report_bytes, raw_blobs, retrieved_anchor)`` where the anchor
    is the oldest source retrieval time (D3 freshness anchor, lead Q7).
    Raises ``SourceBackedError`` on anything short of full success.
    """

    contract = _model_contract(structured_output)
    raw_by_uri: dict[str, RetrievedSource] = {}
    for entry in contract["sources"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("uri"), str):
            raise SourceBackedError("research_source_malformed")
        uri = entry["uri"]
        if uri in raw_by_uri:
            continue
        try:
            raw_by_uri[uri] = fetch(uri, now=created_at)
        except RetrievalError as error:
            raise SourceBackedError("research_retrieval_failed", str(error)) from error
    texts: dict[str, str] = {}
    for uri, item in raw_by_uri.items():
        try:
            texts[uri] = item.content.decode("utf-8", "strict")
        except UnicodeDecodeError as error:
            raise SourceBackedError("research_source_not_utf8", uri) from error
        if hashlib.sha256(texts[uri].encode()).hexdigest() != item.content_hash:
            raise SourceBackedError("research_hash_chain_broken", uri)
    ordered_uris = list(raw_by_uri.keys())
    source_ids = {uri: f"s{index + 1}" for index, uri in enumerate(ordered_uris)}
    claims: list[Claim] = []
    for entry in contract["claims"]:
        if not isinstance(entry, dict):
            raise SourceBackedError("research_claim_malformed")
        citations: list[tuple[str, str]] = []
        raw_citations = entry.get("citations")
        if not isinstance(raw_citations, list):
            raise SourceBackedError("research_citations_malformed")
        for pair in raw_citations:
            if (
                not isinstance(pair, (list, tuple))
                or len(pair) != 2
                or not isinstance(pair[0], str)
                or not isinstance(pair[1], str)
            ):
                raise SourceBackedError("research_citation_malformed")
            uri, position = pair
            if uri not in raw_by_uri:
                raise SourceBackedError("research_citation_source_unknown", uri)
            parse_citation_position(position, raw_length=len(raw_by_uri[uri].content))
            citations.append((source_ids[uri], position))
        claims.append(
            Claim(
                claim_id=str(entry.get("claim_id", "")),
                statement=str(entry.get("statement", "")),
                support=str(entry.get("support", "")),
                citations=tuple(citations),
            )
        )
    retrieved = tuple(raw_by_uri[uri] for uri in ordered_uris)
    sources = sources_from_retrieved(retrieved, retriever=retriever, scope=scope)
    try:
        report = _bind_report(
            research_id=str(uuid.uuid4()),
            question=contract["question"] or task_question,
            sources=sources,
            claims=tuple(claims),
            created_at=created_at,
        )
    except SynthesisError as error:
        raise SourceBackedError("research_report_invalid", str(error)) from error
    blobs = tuple(
        RawSourceBlob(uri=uri, content=raw_by_uri[uri].content) for uri in ordered_uris
    )
    anchor = min(item.retrieved_at for item in retrieved)
    return (
        report_to_json(
            research_id=report.research_id,
            question=report.question,
            sources=report.sources,
            claims=report.claims,
            created_at=report.created_at,
        ),
        blobs,
        anchor,
    )
