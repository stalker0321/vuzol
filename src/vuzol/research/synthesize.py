"""Synthesis binding over retrieved research (WP06, pure).

Turns retrieved sources plus draft claims into a validated ResearchReport and
renders the explicit synthesis context block consumed downstream. Invalid
reports never produce a context block (fail-closed).
"""

from __future__ import annotations

from vuzol.research.report import Claim, ResearchReport, Source, build_report, validate_report
from vuzol.research.retrieval import RetrievedSource


class SynthesisError(RuntimeError):
    """Stable, fail-closed synthesis rejection."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


def sources_from_retrieved(
    retrieved: tuple[RetrievedSource, ...],
    *,
    retriever: str,
    scope: str,
    freshness: dict[str, str] | None = None,
    quotes: dict[str, str] | None = None,
) -> tuple[Source, ...]:
    """Lift transport-level sources into report-level sources (WP02 scope/freshness)."""

    freshness = freshness or {}
    quotes = quotes or {}
    out: list[Source] = []
    for index, item in enumerate(retrieved):
        source_id = f"s{index + 1}"
        out.append(
            Source(
                source_id=source_id,
                uri=item.final_uri,
                retriever=retriever,
                retrieved_at=item.retrieved_at,
                content=item.content.decode("utf-8", errors="replace"),
                scope=scope,
                freshness_class=freshness.get(source_id, "fresh"),
                quote=quotes.get(source_id),
            )
        )
    return tuple(out)


def bind_report(
    *,
    research_id: str,
    question: str,
    sources: tuple[Source, ...],
    claims: tuple[Claim, ...],
    created_at: str,
) -> ResearchReport:
    """Build and fail-closed validate; raises SynthesisError on any violation."""

    report = build_report(
        research_id=research_id,
        question=question,
        sources=sources,
        claims=claims,
        created_at=created_at,
    )
    errors = validate_report(report)
    if errors:
        raise SynthesisError(errors[0])
    return report


def build_synthesis_context(report: ResearchReport) -> str:
    """Render the explicit context block; only valid reports reach here."""

    errors = validate_report(report)
    if errors:
        raise SynthesisError(errors[0])
    lines = [f"Research question: {report.question}", ""]
    markers = {
        "supported": "[verified]",
        "unsupported": "[unsupported]",
        "conflicting": "[conflicting]",
    }
    for claim in report.claims:
        marker = markers[claim.support]
        lines.append(f"- {marker} {claim.claim_id}: {claim.statement}")
        for source_id, position in claim.citations:
            source = next(s for s in report.sources if s.source_id == source_id)
            stale = " (stale)" if source.freshness_class == "stale" else ""
            lines.append(f"    -> {source_id} @ {position}{stale} [{source.content_hash[:8]}]")
        if claim.support == "unsupported":
            lines.append("    -> no retrieved source; do not present as verified")
    lines.append("")
    lines.append(f"report: {report.research_id} ({len(report.sources)} sources)")
    return "\n".join(lines)
