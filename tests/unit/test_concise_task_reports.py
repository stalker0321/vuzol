from vuzol.telegram.projections import _concise_completion_report, task_progress_report


def test_report_has_only_done_stop_cause() -> None:
    assert task_progress_report(
        completed="Код сайта сохранён.", stopped="Проверка.", reason="Нет Node.js."
    ) == "\n".join(
        ("Сделано: Код сайта сохранён.", "Остановка: Проверка.", "Причина: Нет Node.js.")
    )
    assert task_progress_report(completed="Код сайта сохранён.") == "Сделано: Код сайта сохранён."


def test_completion_report_drops_routine_assurances_and_unrequested_non_actions() -> None:
    text = (
        "Код первой версии сайта сохранён. Grok-ревью, применение и публикация не выполнялись. "
        "Пересоздавать задачу, переписку или документы не нужно; контекст и расходы сохранены."
    )
    assert _concise_completion_report(text) == "Код первой версии сайта сохранён."
    assert _concise_completion_report(
        "Добавлена проверка, чтобы платежи не выполнялись без согласия."
    ) == ("Добавлена проверка, чтобы платежи не выполнялись без согласия.")
