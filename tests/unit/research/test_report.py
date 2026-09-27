"""Unit tests for the research-result.v1 claim/source report (WP06)."""

import uuid

from vuzol.research.report import Claim, Source, build_report, validate_report


def _source(source_id: str = "s1") -> Source:
    return Source(
        source_id=source_id,
        uri=f"fixture://research/{source_id}.md",
        retriever="local-docs-fixture",
        retrieved_at="2026-09-27T10:00:00Z",
        content="frozen fixture bytes",
        scope="vuzol",
    )


def _report(**overrides: object) -> object:
    kwargs: dict[str, object] = {
        "research_id": str(uuid.uuid4()),
        "question": "Which adapter backs CI retrieval?",
        "sources": (_source(),),
        "claims": (
            Claim(
                claim_id="c1",
                statement="CI retrieval is fixture-based.",
                support="supported",
                citations=(("s1", "para 2"),),
            ),
        ),
        "created_at": "2026-09-27T10:10:00Z",
    }
    kwargs.update(overrides)
    return build_report(**kwargs)  # type: ignore[arg-type]


def test_valid_report_has_no_errors() -> None:
    assert validate_report(_report()) == ()  # type: ignore[arg-type]


def test_unsupported_claim_is_marked_not_silent() -> None:
    report = _report(
        claims=(Claim(claim_id="c9", statement="Latency is unknown.", support="unsupported"),)
    )
    assert validate_report(report) == ()  # type: ignore[arg-type]


def test_no_retrieval_means_no_verified_research() -> None:
    report = _report(sources=())
    assert "no_retrieval_no_verified_research" in validate_report(report)  # type: ignore[arg-type]


def test_supported_claim_without_citation_fails_closed() -> None:
    report = _report(claims=(Claim(claim_id="c1", statement="CI is fast.", support="supported"),))
    assert "supported_claim_without_citation" in validate_report(report)  # type: ignore[arg-type]


def test_citation_to_unknown_source_fails_closed() -> None:
    report = _report(
        claims=(
            Claim(
                claim_id="c1",
                statement="CI is fixture-based.",
                support="supported",
                citations=(("ghost", "para 1"),),
            ),
        )
    )
    assert "citation_source_unknown" in validate_report(report)  # type: ignore[arg-type]
