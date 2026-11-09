# ruff: noqa: RUF001 - owner-facing Russian text
from vuzol.leadership.service import evidence_question

CRITERIA = [
    {"id": "requested-outcome", "description": "Добавь в README раздел про лицензию."},
    {"id": "tests", "description": "Тесты проходят."},
]


def test_one_missing_criterion_is_quoted_by_its_description() -> None:
    question = evidence_question(CRITERIA, ["requested-outcome"])
    assert question == (
        "Не нашёл подтверждения, что выполнено: «Добавь в README раздел про лицензию.». "
        "Что делаем дальше?"
    )
    assert "requested-outcome" not in question


def test_several_are_listed_and_an_unknown_id_still_shows() -> None:
    question = evidence_question(CRITERIA, ["tests", "ghost"])
    assert question.splitlines() == [
        "Не нашёл подтверждения, что выполнено:",
        "• Тесты проходят.",
        "• ghost",
        "Что делаем дальше?",
    ]
    assert evidence_question(None, ["x"]).count("«x»") == 1
