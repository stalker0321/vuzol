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

__all__ = [
    "RESEARCH_PROVIDER_RESULT_SCHEMA",
    "RESEARCH_PROVIDER_RESULT_SCHEMA_VERSION",
    "RESEARCH_RESULT_SCHEMA",
    "RESEARCH_RESULT_SCHEMA_VERSION",
    "Claim",
    "Source",
    "build_report",
    "validate_report",
    "validate_source_report_bytes",
]
