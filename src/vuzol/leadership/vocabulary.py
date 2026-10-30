"""Owner work slang for speech-to-text, added and removed through the lead."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import TranscriptionTerm
from vuzol.telegram.projections import enqueue_chat_notice

MAX_TERMS = 150
TERM_CHARS = 100


def _clean(value: str) -> str:
    term = " ".join(value.split())
    if not term or len(term) > TERM_CHARS or "<" in term or ">" in term:
        raise ValueError("term must be 1-100 characters without < or >")
    return term


class AddVocabularyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    terms: list[str] = Field(min_length=1, max_length=10)

    @field_validator("terms")
    @classmethod
    def clean_terms(cls, value: list[str]) -> list[str]:
        return [_clean(term) for term in value]


class RemoveVocabularyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    term: str

    @field_validator("term")
    @classmethod
    def clean_term(cls, value: str) -> str:
        return _clean(value)


async def active_terms(session: AsyncSession) -> tuple[str, ...]:
    rows = await session.scalars(
        select(TranscriptionTerm.term)
        .where(TranscriptionTerm.removed_at.is_(None))
        .order_by(TranscriptionTerm.created_at, TranscriptionTerm.id)
    )
    return tuple(rows)


async def apply_vocabulary_command(
    session: AsyncSession, goal: ManagedGoal, command: LeadCommand
) -> dict[str, object]:
    # One writer at a time keeps the size check and the unique index in step.
    await session.execute(select(func.pg_advisory_xact_lock(0x766F6361)))
    existing = {term.lower(): term for term in await active_terms(session)}
    if command.name == "add_vocabulary":
        try:
            added = AddVocabularyPayload.model_validate(command.payload)
        except ValueError as error:
            raise DomainError("invalid_vocabulary_payload") from error
        new: list[str] = []
        for term in added.terms:
            if term.lower() not in existing and term.lower() not in {t.lower() for t in new}:
                new.append(term)
        if len(existing) + len(new) > MAX_TERMS:
            raise DomainError("vocabulary_full", "remove terms before adding more")
        for term in new:
            session.add(
                TranscriptionTerm(term=term, added_by_user_id=goal.owner_user_id, goal_id=goal.id)
            )
        await session.flush()
        shown = new or added.terms
        verb = "Добавил в словарь" if new else "Уже в словаре"
        notice = f"📖 {verb}: " + ", ".join(f"«{term}»" for term in shown)
        result: dict[str, object] = {"added": new, "already_present": len(added.terms) - len(new)}
    else:
        try:
            removed = RemoveVocabularyPayload.model_validate(command.payload)
        except ValueError as error:
            raise DomainError("invalid_vocabulary_payload") from error
        row = await session.scalar(
            select(TranscriptionTerm).where(
                TranscriptionTerm.removed_at.is_(None),
                func.lower(TranscriptionTerm.term) == removed.term.lower(),
            )
        )
        if row is None:
            raise DomainError(
                "vocabulary_term_missing", f"«{removed.term}» is not in the vocabulary"
            )
        row.removed_at = datetime.now(UTC)
        notice = f"🗑 Убрал из словаря: «{row.term}»"
        result = {"removed": row.term}
    await enqueue_chat_notice(
        session,
        chat_id=goal.source_chat_id,
        thread_id=goal.source_thread_id,
        key=f"vocabulary:{command.idempotency_key}",
        text=notice,
    )
    return result
