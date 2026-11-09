"""Coverage vocabulary for interpretation context."""

from enum import StrEnum


class Coverage(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNKNOWN = "unknown"
