import uuid
from datetime import UTC, datetime

import pytest

from vuzol.discussion import DomainError, MemoryLimits
from vuzol.discussion.memory import (
    DecisionView,
    MemorySummary,
    MemoryTurn,
    build_memory_pack,
    ensure_memory_safe,
    validate_decision,
)
from vuzol.storage.types import ConversationSummaryGenerator, ConversationTurnRole


def turn(ordinal: int, content: str) -> MemoryTurn:
    return MemoryTurn(
        id=uuid.uuid4(),
        ordinal=ordinal,
        role=ConversationTurnRole.USER,
        content=content,
        created_at=datetime.now(UTC),
    )


def decision(key: str) -> DecisionView:
    return DecisionView(
        id=uuid.uuid4(),
        key=key,
        statement=f"Decision {key}",
        source_turn_id=None,
        accepted_by_user_id=42,
        created_at=datetime.now(UTC),
    )


def test_memory_pack_drops_oldest_turns_and_truncates_single_newest() -> None:
    session_id = uuid.uuid4()
    packed = build_memory_pack(
        session_id=session_id,
        turns=(turn(1, "aaaa"), turn(2, "bbbb"), turn(3, "c" * 12)),
        summary=None,
        decisions=(decision("first"), decision("second")),
        limits=MemoryLimits(
            max_raw_turns=2,
            max_raw_chars=5,
            max_summary_chars=10,
            max_decisions=1,
        ),
    )

    assert packed.session_id == session_id
    assert [(item.ordinal, item.content) for item in packed.turns] == [(3, "ccccc")]
    assert [item.key for item in packed.decisions] == ["first"]
    assert packed.raw_turns_truncated
    assert packed.decisions_truncated


def test_memory_pack_truncates_summary_without_changing_hash_or_revision() -> None:
    summary = MemorySummary(
        revision=3,
        body="0123456789",
        covered_through_turn_ordinal=8,
        generator=ConversationSummaryGenerator.MODEL,
        content_hash="a" * 64,
    )
    packed = build_memory_pack(
        session_id=uuid.uuid4(),
        turns=(),
        summary=summary,
        decisions=(),
        limits=MemoryLimits(max_summary_chars=4),
    )

    assert packed.summary is not None
    assert packed.summary.body == "0123"
    assert packed.summary.revision == 3 and packed.summary.content_hash == "a" * 64
    assert packed.summary_truncated


def test_memory_validation_rejects_bad_limits_secrets_and_decisions() -> None:
    with pytest.raises(DomainError) as invalid_limits:
        MemoryLimits(max_raw_turns=0)
    assert invalid_limits.value.code == "invalid_memory_limits"

    for unsafe in (
        "token=abcdefghijk",
        "".join(("postgresql://", "user:", "password", "@example/db")),
        "read /etc/secret-file",
        "".join(("-----BEGIN ", "PRIVATE KEY-----")),
    ):
        with pytest.raises(DomainError) as sensitive:
            ensure_memory_safe(unsafe)
        assert sensitive.value.code == "sensitive_memory"

    assert validate_decision("mvp-scope", "Keep the MVP small") == (
        "mvp-scope",
        "Keep the MVP small",
    )
    with pytest.raises(DomainError) as invalid_key:
        validate_decision("MVP scope", "Keep it small")
    assert invalid_key.value.code == "invalid_decision"
    with pytest.raises(DomainError) as empty:
        ensure_memory_safe("  ")
    assert empty.value.code == "invalid_memory_text"
    with pytest.raises(DomainError) as too_long:
        validate_decision("long", "x" * 501)
    assert too_long.value.code == "invalid_decision"
