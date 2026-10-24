from vuzol.leadership.text import owner_sentence


def test_first_sentence_without_identifier_gaps():
    text = "Сверить diff относительно " + "a" * 40 + ". Остальное — подробности."
    assert owner_sentence(text) == "Сверить diff относительно."


def test_preferred_sentence_answers_the_card_line():
    scope = "Одна небольшая задача в пилоте. Изменять разрешено только README.md. Тесты не трогать."
    assert owner_sentence(scope, prefer=(r"измен",)) == "Изменять разрешено только README.md."
    assert owner_sentence(scope, prefer=(r"нет такого",)) == "Одна небольшая задача в пилоте."
    assert owner_sentence("", prefer=(r"измен",)) == ""
