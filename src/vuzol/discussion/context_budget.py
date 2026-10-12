"""Application admission partitions, not claims about a model context window."""

CHAT_TEXT_CHARS = 40_000
CHAT_DOCUMENT_CHARS = 50_000
CHAT_INPUT_CHARS = CHAT_TEXT_CHARS + CHAT_DOCUMENT_CHARS


class ChatContextError(ValueError):
    def __init__(self, category: str = "chat_context_too_large") -> None:
        super().__init__(category)
        self.category = category


def context_failure_text(category: str = "chat_context_too_large") -> str:
    if category == "chat_context_source_mismatch":
        return (
            "Не удалось подтвердить полный контекст документов. История сохранена; "  # noqa: RUF001
            "ответ по неполному набору не запрашивался, ничего не запущено."
        )
    return (
        "Полный запрос не помещается в бюджет контекста: 40 000 символов для "
        "переписки и служебного контекста, отдельно 50 000 для текстов документов. "
        "История сохранена; непокрытые резюме сообщения и документы не обрезались. "
        "Ничего не запущено. Укажи более узкий вопрос или меньший набор референсов."
    )
