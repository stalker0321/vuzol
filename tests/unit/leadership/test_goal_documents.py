"""Owner 2026-10-31: a long document must not break the lead; workers get it in full."""

from vuzol.leadership.goal_documents import EXCERPT_CHARS, INLINE_CHARS, lead_turn


def prepared(text: str) -> dict:
    return {
        "caption": "Поправь главу",
        "turn": "Поправь главу\n\nAttached document text ...\n" + text,
        "documents": [
            {"filename": "glava.docx", "media_type": "x", "sha256": "a" * 64, "text": text}
        ],
    }


def test_short_document_stays_inline():
    turn, kept = lead_turn(prepared("коротко"))
    assert turn.endswith("коротко") and kept == []


def test_long_document_gives_the_lead_an_excerpt_and_keeps_the_text():
    text = "абв " * INLINE_CHARS
    turn, kept = lead_turn(prepared(text))
    assert turn.startswith("Поправь главу")
    assert "glava.docx (" in turn and "Workers of this goal's tasks receive the full" in turn
    assert len(turn) < EXCERPT_CHARS + 1_000
    assert kept[0]["text"] == text
