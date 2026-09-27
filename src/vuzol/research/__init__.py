"""Bounded research result module (WP06): claim/source report over research-result.v1."""

from vuzol.research.report import (
    RESEARCH_RESULT_SCHEMA,
    RESEARCH_RESULT_SCHEMA_VERSION,
    Claim,
    Source,
    build_report,
    validate_report,
)

__all__ = [
    "RESEARCH_RESULT_SCHEMA",
    "RESEARCH_RESULT_SCHEMA_VERSION",
    "Claim",
    "Source",
    "build_report",
    "validate_report",
]
