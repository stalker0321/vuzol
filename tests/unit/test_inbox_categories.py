"""A card chooses one catalog entry or proposes one, never both."""

import pytest
from pydantic import ValidationError

from vuzol.inbox.schemas import Card


@pytest.mark.parametrize(
    "selection",
    [
        {},
        {
            "category": "book",
            "new_category": {
                "slug": "podcast",
                "name": "Подкасты",
                "emoji": "🎙",
                "description": "Аудиопрограммы.",
                "why_none_fits": "Речь.",
            },
        },
    ],
)
def test_card_requires_exactly_one_category_choice(selection):
    with pytest.raises(ValidationError):
        Card(title="Test", **selection)


def test_catalog_labels_are_escaped_in_card_html():
    from vuzol.inbox.render import card_html
    from vuzol.storage.models import InboxCategory, InboxItem

    item = InboxItem(
        kind="podcast",
        title="Выпуск",
        category=InboxCategory(name="Подкасты <тест>", emoji="🎙", description="Речь & музыка."),
    )
    html = card_html(item, "UTC", new_category=True)
    assert "🎙 Подкасты &lt;тест&gt;" in html
    assert "🆕 новая категория: Подкасты &lt;тест&gt; — Речь &amp; музыка." in html
