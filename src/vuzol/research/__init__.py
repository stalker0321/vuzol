"""Bounded research result module (WP06): claim/source report over research-result.v1."""

from vuzol.research.report import (
    RESEARCH_PROVIDER_RESULT_SCHEMA,
    RESEARCH_PROVIDER_RESULT_SCHEMA_VERSION,
    RESEARCH_RESULT_SCHEMA,
    RESEARCH_RESULT_SCHEMA_VERSION,
    Claim,
    Source,
    build_report,
    validate_report,
    validate_source_report_bytes,
)
from vuzol.research.source_backed import (
    SOURCE_FRESHNESS_MAX_AGE_SECONDS,
    RawSourceBlob,
    SourceBackedError,
    assemble_source_report,
    parse_citation_position,
    report_to_json,
)

__all__ = [
    "RESEARCH_PROVIDER_RESULT_SCHEMA",
    "RESEARCH_PROVIDER_RESULT_SCHEMA_VERSION",
    "RESEARCH_RESULT_SCHEMA",
    "RESEARCH_RESULT_SCHEMA_VERSION",
    "SOURCE_FRESHNESS_MAX_AGE_SECONDS",
    "Claim",
    "RawSourceBlob",
    "Source",
    "SourceBackedError",
    "assemble_source_report",
    "build_report",
    "parse_citation_position",
    "report_to_json",
    "validate_report",
    "validate_source_report_bytes",
]
