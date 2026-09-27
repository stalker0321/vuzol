"""Unit tests for the research synthesis binding (WP06)."""

import uuid

import pytest

from vuzol.research.report import Claim, Source
from vuzol.research.retrieval import RetrievedSource
from vuzol.research.synthesize import (
    SynthesisError,
    bind_report,
    build_synthesis_context,
    sources_from_retrieved,
)


def _retrieved() -> tuple[RetrievedSource, ...]:
    return (
        RetrievedSource(
            uri="fixture://a.md",
            final_uri="fixture://a.md",
            status_code=200,
            content=b"frozen bytes",
            retrieved_at="2026-09-27T10:00:00Z",
        ),
    )


def _source() -> Source:
    return sources_from_retrieved(_retrieved(), retriever="local-docs-fixture", scope="vuzol")[0]


def test_sources_carry_hash_scope_and_retriever() -> None:
    source = _source()
    assert len(source.content_hash) == 64
    assert source.scope == "vuzol"
    assert source.retriever == "local-docs-fixture"


def test_context_block_contains_citations_and_markers() -> None:
    report = bind_report(
        research_id=str(uuid.uuid4()),
        question="Which adapter backs CI?",
        sources=(_source(),),
        claims=(
            Claim(
                claim_id="c1",
                statement="CI is fixture-based.",
                support="supported",
                citations=(("s1", "para 2"),),
            ),
            Claim(claim_id="c2", statement="Latency unknown.", support="unsupported"),
        ),
        created_at="2026-09-27T10:10:00Z",
    )
    block = build_synthesis_context(report)
    assert "[verified] c1" in block
    assert "s1 @ para 2" in block
    assert "[unsupported] c2" in block
    assert "do not present as verified" in block


def test_stale_source_visible_in_block() -> None:
    source = Source(
        source_id="s1",
        uri="fixture://a.md",
        retriever="local-docs-fixture",
        retrieved_at="2026-09-01T10:00:00Z",
        content="old bytes",
        scope="vuzol",
        freshness_class="stale",
    )
    report = bind_report(
        research_id=str(uuid.uuid4()),
        question="Q?",
        sources=(source,),
        claims=(
            Claim(
                claim_id="c1", statement="S.", support="supported", citations=(("s1", "para 1"),)
            ),
        ),
        created_at="2026-09-27T10:10:00Z",
    )
    assert "(stale)" in build_synthesis_context(report)


def test_invalid_report_never_produces_context() -> None:
    from vuzol.research.report import build_report

    report = build_report(
        research_id=str(uuid.uuid4()),
        question="Q?",
        sources=(_source(),),
        claims=(),
        created_at="2026-09-27T10:10:00Z",
    )
    with pytest.raises(SynthesisError, match="claims_missing"):
        build_synthesis_context(report)
    with pytest.raises(SynthesisError, match="claims_missing"):
        bind_report(
            research_id=str(uuid.uuid4()),
            question="Q?",
            sources=(_source(),),
            claims=(),
            created_at="2026-09-27T10:10:00Z",
        )
