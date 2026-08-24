"""Tests for the classifier summary sanitising in discussion reply text."""

from vuzol.interpretation.discussion import DiscussionInterpretation
from vuzol.interpretation.service import _discussion_reply_text
from vuzol.storage.types import InteractionMode


def _interpretation(*, summary: str, question: str | None = None) -> DiscussionInterpretation:
    return DiscussionInterpretation(
        interaction_mode=InteractionMode.DISCUSSION,
        confidence=0.9,
        user_visible_summary=summary,
        clarification_question=question,
    )


def test_clean_summary_is_returned_unchanged() -> None:
    result = _interpretation(summary="Классифицировал как вопрос по плану.")

    assert _discussion_reply_text(result) == "Классифицировал как вопрос по плану."


def test_envelope_wrapped_summary_is_unwrapped() -> None:
    result = _interpretation(summary='{"summary": "Внутренняя выжимка классификатора."}')

    assert _discussion_reply_text(result) == '{"summary": "Внутренняя выжимка классификатора."}'


def test_reply_envelope_in_summary_is_unwrapped() -> None:
    result = _interpretation(
        summary='Промежуточная болтовня. {"reply": "Чистый ответ для пользователя."}'
    )

    assert _discussion_reply_text(result) == "Чистый ответ для пользователя."


def test_clarification_question_is_unwrapped_too() -> None:
    result = _interpretation(
        summary="Нужно уточнение.",
        question='Какое окружение? {"reply": "Уточни окружение: staging или prod?"}',
    )

    assert (
        _discussion_reply_text(result) == "Нужно уточнение.\n\nУточни окружение: staging или prod?"  # noqa: RUF001
    )
