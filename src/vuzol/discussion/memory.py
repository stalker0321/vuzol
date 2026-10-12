"""Pure bounded-memory values and deterministic packing rules."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from vuzol.discussion.context_budget import CHAT_DOCUMENT_CHARS, CHAT_TEXT_CHARS, ChatContextError
from vuzol.discussion.domain import DomainError
from vuzol.storage.types import ConversationSummaryGenerator, ConversationTurnRole

_DECISION_KEY = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_SENSITIVE_MEMORY = (
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)(?:api[_-]?key|password|secret|token)\s*[:=]\s*\S{8,}"),
    re.compile(r"(?i)[a-z][a-z0-9+.-]*://[^\s/:]+:[^\s/@]+@"),
    re.compile(r"(?:^|\s)/(?:etc|home|opt|root|srv|var)/\S+"),
)


class DiscussionEvent(StrEnum):
    TURN_APPENDED = "conversation_turn.appended"
    SUMMARY_UPDATED = "conversation_summary.updated"
    DECISION_ACCEPTED = "decision.accepted"
    DECISION_SUPERSEDED = "decision.superseded"
    DECISION_RETRACTED = "decision.retracted"


class ExplicitDecisionSource(StrEnum):
    BUTTON = "button"
    EXPLICIT_COMMAND = "explicit_command"
    USER_CONFIRM = "user_confirm"


@dataclass(frozen=True, slots=True)
class MemoryLimits:
    max_raw_turns: int = 20
    max_raw_chars: int = 24_000
    max_summary_chars: int = 2_000
    max_decisions: int = 50

    def __post_init__(self) -> None:
        for name, value in (
            ("max_raw_turns", self.max_raw_turns),
            ("max_raw_chars", self.max_raw_chars),
            ("max_summary_chars", self.max_summary_chars),
            ("max_decisions", self.max_decisions),
        ):
            if value < 1:
                raise DomainError("invalid_memory_limits", f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class MemoryTurn:
    id: uuid.UUID
    ordinal: int
    role: ConversationTurnRole
    content: str
    created_at: datetime
    # Set only from the durable prepared-document snapshot, never inferred from
    # a user-controlled marker in ordinary text. Canonical content stays intact.
    caption: str | None = None
    document_context: str | None = None
    document_text_chars: int = 0

    @property
    def chat_text(self) -> str:
        return (self.caption or "") if self.document_context is not None else self.content


@dataclass(frozen=True, slots=True)
class MemorySummary:
    revision: int
    body: str
    covered_through_turn_ordinal: int
    generator: ConversationSummaryGenerator
    content_hash: str


@dataclass(frozen=True, slots=True)
class DecisionView:
    id: uuid.UUID
    key: str
    statement: str
    source_turn_id: uuid.UUID | None
    accepted_by_user_id: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RecalledMemory:
    id: uuid.UUID
    text: str
    unit_type: str
    status: str


@dataclass(frozen=True, slots=True)
class MemoryPack:
    session_id: uuid.UUID
    summary: MemorySummary | None
    decisions: tuple[DecisionView, ...]
    turns: tuple[MemoryTurn, ...]
    raw_turns_truncated: bool
    summary_truncated: bool
    decisions_truncated: bool
    recalled: tuple[RecalledMemory, ...] = ()
    uncovered_history_complete: bool = False


def ensure_memory_safe(text: str) -> str:
    normalized = text.strip()
    if not normalized:
        raise DomainError("invalid_memory_text", "memory text must not be empty")
    if any(pattern.search(normalized) for pattern in _SENSITIVE_MEMORY):
        raise DomainError(
            "sensitive_memory", "summary and decision memory must not contain secrets"
        )
    return normalized


def build_chat_memory_pack(
    *,
    session_id: uuid.UUID,
    turns: tuple[MemoryTurn, ...],
    summary: MemorySummary | None,
    decisions: tuple[DecisionView, ...],
    limits: MemoryLimits,
) -> MemoryPack:
    """Only summary-covered turns are disposable; no slicing or arbitrary last-N cutoff."""
    boundary = summary.covered_through_turn_ordinal if summary is not None else 1
    selected = list(turns)

    def oversized() -> bool:
        return (
            sum(len(t.chat_text) for t in selected) > CHAT_TEXT_CHARS
            or sum(t.document_text_chars for t in selected) > CHAT_DOCUMENT_CHARS
        )

    while oversized() and selected and selected[0].ordinal < boundary:
        selected.pop(0)
    if oversized():
        raise ChatContextError("chat_context_too_large")
    if summary is not None and len(summary.body) > limits.max_summary_chars:
        raise ChatContextError("chat_context_too_large")
    if len(decisions) > limits.max_decisions:
        raise ChatContextError("chat_context_too_large")
    return MemoryPack(
        session_id=session_id,
        summary=summary,
        decisions=decisions,
        turns=tuple(selected),
        raw_turns_truncated=len(selected) != len(turns),
        summary_truncated=False,
        decisions_truncated=False,
        uncovered_history_complete=True,
    )


def validate_decision(key: str, statement: str) -> tuple[str, str]:
    normalized_key = key.strip()
    if len(normalized_key) > 64 or _DECISION_KEY.fullmatch(normalized_key) is None:
        raise DomainError("invalid_decision", "decision key must be a lowercase slug")
    normalized_statement = ensure_memory_safe(statement)
    if len(normalized_statement) > 500:
        raise DomainError("invalid_decision", "decision statement exceeds 500 characters")
    return normalized_key, normalized_statement


def build_memory_pack(
    *,
    session_id: uuid.UUID,
    turns: tuple[MemoryTurn, ...],
    summary: MemorySummary | None,
    decisions: tuple[DecisionView, ...],
    limits: MemoryLimits,
) -> MemoryPack:
    """Bound L0/L1/L2 deterministically; raw turns remain chronological."""
    selected = list(turns[-limits.max_raw_turns :])
    raw_truncated = len(selected) != len(turns)
    while len(selected) > 1 and sum(len(turn.content) for turn in selected) > limits.max_raw_chars:
        selected.pop(0)
        raw_truncated = True
    if selected and len(selected[0].content) > limits.max_raw_chars:
        newest = selected[0]
        selected[0] = MemoryTurn(
            id=newest.id,
            ordinal=newest.ordinal,
            role=newest.role,
            content=newest.content[: limits.max_raw_chars],
            created_at=newest.created_at,
        )
        raw_truncated = True

    bounded_summary = summary
    summary_truncated = False
    if summary is not None and len(summary.body) > limits.max_summary_chars:
        bounded_summary = MemorySummary(
            revision=summary.revision,
            body=summary.body[: limits.max_summary_chars],
            covered_through_turn_ordinal=summary.covered_through_turn_ordinal,
            generator=summary.generator,
            content_hash=summary.content_hash,
        )
        summary_truncated = True

    return MemoryPack(
        session_id=session_id,
        summary=bounded_summary,
        decisions=decisions[: limits.max_decisions],
        turns=tuple(selected),
        raw_turns_truncated=raw_truncated,
        summary_truncated=summary_truncated,
        decisions_truncated=len(decisions) > limits.max_decisions,
    )
