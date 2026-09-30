"""Pure derived-memory values: identity, status rules, templates (D5).

Templates-first, no embeddings, no generative expansion. The async writer
(`memory_writer.py`) persists these; retrieval filters active statuses only.
Hypotheses can never become verified facts in code.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime

from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory import ensure_memory_safe
from vuzol.storage.types import MemoryUnitStatus

EXTRACTOR_VERSION = "memory-extractor.v1"
MEMORY_DESTINATION = "memory_extract"
MEMORY_EXTRACT_OPERATION = "extract_memory_unit"

UNIT_TYPES = frozenset({"decision_template", "outcome_template", "observation", "lesson"})

RECALLABLE_STATUSES = frozenset({MemoryUnitStatus.OBSERVATION, MemoryUnitStatus.VERIFIED})

MAX_TEXT_CHARS = 4_000
MAX_RECALL_LIMIT = 50

_JOB_KEY_VERSION = "memory-job.v1"


def extraction_scope(*, project_id: str | None, session_id: uuid.UUID | None) -> str:
    """Stable scope fragment for job and unit identity."""

    return f"{project_id or '-'}:{session_id or '-'}:"


def job_idempotency_key(
    *, trigger_event_id: uuid.UUID, scope: str, operation: str = MEMORY_EXTRACT_OPERATION
) -> str:
    """Outbox dedup key: (trigger_event_id, extractor_version, scope)."""

    return f"{_JOB_KEY_VERSION}:{operation}:{EXTRACTOR_VERSION}:{trigger_event_id}:{scope}"


def extraction_identity(
    *,
    trigger_event_id: uuid.UUID,
    scope: str,
    unit_type: str,
    unit_key: str,
) -> str:
    """Unit-level unique extraction identity (DELTA §D5 migrations)."""

    if unit_type not in UNIT_TYPES:
        raise DomainError("invalid_memory_unit", f"unknown unit type: {unit_type}")
    normalized_key = unit_key.strip()
    if not normalized_key or len(normalized_key) > 128:
        raise DomainError("invalid_memory_unit", "unit key must contain 1..128 characters")
    digest = hashlib.sha256(
        f"{EXTRACTOR_VERSION}:{trigger_event_id}:{scope}:{unit_type}:{normalized_key}".encode()
    ).hexdigest()
    return f"memory:{digest}"


def check_unit_type(unit_type: str) -> str:
    if unit_type not in UNIT_TYPES:
        raise DomainError("invalid_memory_unit", f"unknown unit type: {unit_type}")
    return unit_type


def check_status_transition(*, source: MemoryUnitStatus, target: MemoryUnitStatus) -> None:
    """Fail closed on hypothesis promotion and tombstone resurrection."""

    if source is MemoryUnitStatus.HYPOTHESIS and target is MemoryUnitStatus.VERIFIED:
        raise DomainError("invalid_memory_transition", "hypotheses never become verified facts")
    if source is MemoryUnitStatus.TOMBSTONED:
        raise DomainError("invalid_memory_transition", "tombstoned units stay tombstoned")
    # Tombstoning is a privacy operation, orthogonal to the lifecycle: any
    # live status may be tombstoned; tombstoned rows stay terminal.
    allowed: dict[MemoryUnitStatus, frozenset[MemoryUnitStatus]] = {
        MemoryUnitStatus.HYPOTHESIS: frozenset(
            {MemoryUnitStatus.OBSERVATION, MemoryUnitStatus.TOMBSTONED}
        ),
        MemoryUnitStatus.OBSERVATION: frozenset(
            {
                MemoryUnitStatus.OBSERVATION,
                MemoryUnitStatus.SUPERSEDED,
                MemoryUnitStatus.RETRACTED,
                MemoryUnitStatus.TOMBSTONED,
            }
        ),
        MemoryUnitStatus.VERIFIED: frozenset(
            {
                MemoryUnitStatus.SUPERSEDED,
                MemoryUnitStatus.RETRACTED,
                MemoryUnitStatus.TOMBSTONED,
            }
        ),
        MemoryUnitStatus.SUPERSEDED: frozenset(),
        MemoryUnitStatus.RETRACTED: frozenset(),
        MemoryUnitStatus.TOMBSTONED: frozenset(),
    }
    if target not in allowed[source] and target is not source:
        raise DomainError(
            "invalid_memory_transition", f"{source.value} cannot become {target.value}"
        )


def should_supersede(
    *,
    current_effective_at: datetime,
    incoming_effective_at: datetime,
) -> bool:
    """Delayed-writer rule: source revisions decide, never job completion order.

    The incoming unit wins only on a strictly newer effective timestamp; ties
    keep the first writer (deterministic, order-independent).
    """

    return incoming_effective_at > current_effective_at


def clamp_recall_limit(limit: int) -> int:
    if limit < 1:
        raise DomainError("invalid_memory_recall", "recall limit must be positive")
    return min(limit, MAX_RECALL_LIMIT)


def decision_template(*, key: str, statement: str, accepted_by_user_id: int) -> str:
    """Deterministic verified template for an explicit accepted decision."""

    return ensure_memory_safe(
        f"Decision {key.strip()}: {statement.strip()} "
        f"(accepted by user {accepted_by_user_id})"
    )[:MAX_TEXT_CHARS]


def outcome_template(
    *,
    package_id: uuid.UUID,
    revision_number: int,
    accepted_by_user_id: int,
    evidence_hash: str | None,
) -> str:
    """Deterministic verified template for a goal-acceptance outcome."""

    evidence = f" evidence {evidence_hash[:16]}" if evidence_hash else " waiver acceptance"
    return ensure_memory_safe(
        f"Accepted package {package_id} revision {revision_number}"
        f" by user {accepted_by_user_id}:{evidence}."
    )[:MAX_TEXT_CHARS]


def observation_text(*, body: str) -> str:
    return ensure_memory_safe(body)[:MAX_TEXT_CHARS]


def lesson_text(*, body: str, evidence_present: bool) -> str:
    """Failed-attempt lessons only from confirmed evidence with context."""

    if not evidence_present:
        raise DomainError("invalid_memory_lesson", "lessons require confirmed evidence")
    return ensure_memory_safe(body)[:MAX_TEXT_CHARS]


@dataclass(frozen=True, slots=True)
class RecallQuery:
    project_id: str | None = None
    unit_types: frozenset[str] = frozenset()
    query: str | None = None
    limit: int = 10

    def __post_init__(self) -> None:
        unknown = set(self.unit_types) - UNIT_TYPES
        if unknown:
            raise DomainError("invalid_memory_recall", f"unknown unit types: {sorted(unknown)}")
        if self.limit < 1:
            raise DomainError("invalid_memory_recall", "recall limit must be positive")
        if self.query is not None and not self.query.strip():
            raise DomainError("invalid_memory_recall", "recall query must not be blank")
