import uuid
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from vuzol.discussion import DomainError, MemoryLimits
from vuzol.discussion.context_budget import ChatContextError
from vuzol.discussion.memory import (
    DecisionView,
    MemorySummary,
    MemoryTurn,
    build_chat_memory_pack,
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


def test_chat_document_budget_does_not_evict_uncovered_critique():
    critique = turn(1, "Моя критика. Пока не запускай. " + "к" * 3800)
    files = replace(
        turn(2, "canonical raw files"),
        caption="Вот два референса",
        document_context="r" * 50_000,
        document_text_chars=50_000,
    )
    followup = turn(3, "Мы продолжаем обсуждение, задачи ещё нет.")
    packed = build_chat_memory_pack(
        session_id=uuid.uuid4(),
        turns=(critique, files, followup),
        summary=None,
        decisions=(),
        limits=MemoryLimits(),
    )
    assert packed.turns == (critique, files, followup) and not packed.raw_turns_truncated


def test_chat_packing_never_drops_uncovered_text_or_uses_last_n():
    turns = tuple(turn(i + 1, "короткая реплика") for i in range(50))
    pack = build_chat_memory_pack(
        session_id=uuid.uuid4(), turns=turns, summary=None, decisions=(), limits=MemoryLimits()
    )
    assert pack.turns == turns
    with pytest.raises(ChatContextError):
        build_chat_memory_pack(
            session_id=uuid.uuid4(),
            turns=(turn(1, "x" * 40_001),),
            summary=None,
            decisions=(),
            limits=MemoryLimits(),
        )


def test_chat_compaction_only_removes_summary_covered_turns():
    summary = MemorySummary(
        1, "Initial proposal summarized", 2, ConversationSummaryGenerator.MODEL, "a" * 64
    )
    old = turn(1, "x" * 38_000)
    correction = turn(2, "Не меняй архитектуру. " + "я" * 4000)  # noqa: RUF001
    packed = build_chat_memory_pack(
        session_id=uuid.uuid4(),
        turns=(old, correction),
        summary=summary,
        decisions=(),
        limits=MemoryLimits(),
    )
    assert packed.turns == (correction,) and packed.raw_turns_truncated
    with pytest.raises(ChatContextError):
        build_chat_memory_pack(
            session_id=uuid.uuid4(),
            turns=(correction, turn(3, "x" * 38_000)),
            summary=summary,
            decisions=(),
            limits=MemoryLimits(),
        )
