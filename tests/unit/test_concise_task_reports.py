from vuzol.telegram.projections import _concise_completion_report


def test_completion_report_drops_routine_assurances_and_unrequested_non_actions() -> None:
    text = (
        "Код первой версии сайта сохранён. Grok-ревью, применение и публикация не выполнялись. "
        "Пересоздавать задачу, переписку или документы не нужно; контекст и расходы сохранены."
    )
    assert _concise_completion_report(text) == "Код первой версии сайта сохранён."
    assert _concise_completion_report(
        "Добавлена проверка, чтобы платежи не выполнялись без согласия."
    ) == ("Добавлена проверка, чтобы платежи не выполнялись без согласия.")
