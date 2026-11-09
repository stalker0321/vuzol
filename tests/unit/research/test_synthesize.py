"""Unit tests for the research synthesis binding (WP06)."""

import uuid

import pytest

from vuzol.research.report import Source
from vuzol.research.retrieval import RetrievedSource
from vuzol.research.synthesize import SynthesisError, bind_report, sources_from_retrieved


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


def test_bind_report_rejects_missing_claims() -> None:
    with pytest.raises(SynthesisError, match="claims_missing"):
        bind_report(
            research_id=str(uuid.uuid4()),
            question="Q?",
            sources=(_source(),),
            claims=(),
            created_at="2026-09-27T10:10:00Z",
        )
