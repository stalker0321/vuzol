"""Offline visual fixtures from the production renderer, never Telegram mutations.

PYTHONPATH=src python tests/preview_lead_cards.py /tmp/lead-cards [--png]
HTML is dependency-free. Optional PNG rendering uses an already-installed
ImageMagick/Pango; it is a layout preview, not a live Telegram-client screenshot.
"""
# ruff: noqa: RUF001

from __future__ import annotations

import argparse
import html
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path
from typing import ClassVar

from vuzol.leadership.card import render_goal_card
from vuzol.telegram.formatting import telegram_markdown_parts


def examples() -> dict[str, dict[str, object]]:
    common: dict[str, object] = {
        "request": "Нормализация меток: normalize_label(), тесты и README",
        "goal_id": "12345678-1234-1234-1234-123456789012",
        "package_title": "Нормализация и тесты",
        "covered": [],
        "missing": ["normalization", "tests"],
    }
    return {
        "working": {
            **common,
            "lifecycle": "active",
            "current_package_status": "running",
        },
        "ready": {
            **common,
            "lifecycle": "completed",
            "current_package_status": "completed",
            "completion_confirmed": True,
            "covered": ["normalization", "tests"],
            "missing": [],
            "result_commit": "3ff96ebff8b1000000000000000000000000000000",
        },
        "paused": {
            **common,
            "lifecycle": "paused",
            "waiting_reason": "owner_pause",
            "current_package_status": "paused",
            "result_commit": "3ff96ebff8b1000000000000000000000000000000",
        },
        "question": {
            **common,
            "lifecycle": "waiting",
            "waiting_reason": "owner",
            "question": "Сохранять регистр букв или приводить все метки к нижнему регистру?",
            "question_version": 7,
        },
        "error": {
            **common,
            "lifecycle": "waiting",
            "waiting_reason": "lead_response_invalid",
            "question": "Проверить выбранную модель и разрешить новый ход руководителя?",
            "question_version": 9,
        },
    }


class PangoPreview(HTMLParser):
    """Adapt only Telegram's text styling to an offline renderer, not to production."""

    _tags: ClassVar[dict[str, str]] = {
        "b": "b",
        "i": "i",
        "code": 'span font_family="monospace"',
        "pre": "span",
    }

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.parts.append(f"<{self._tags[tag]}>")

    def handle_endtag(self, tag: str) -> None:
        self.parts.append(f"</{self._tags[tag].split()[0]}>")

    def handle_data(self, data: str) -> None:
        # ImageMagick decodes entities once before handing the markup to Pango.
        self.parts.append(html.escape(html.escape(data)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--png", action="store_true")
    args = parser.parse_args()
    convert = shutil.which("convert") if args.png else None
    if args.png and convert is None:
        parser.error("--png requires an existing ImageMagick/Pango installation")
    args.output.mkdir(parents=True, exist_ok=True)
    cards: list[str] = []
    for name, view in examples().items():
        parts = telegram_markdown_parts(render_goal_card(view))
        for number, (markup, _) in enumerate(parts, 1):
            cards.append(
                f"<section><small>{name} · {number}/{len(parts)}</small>"
                f"<article>{markup}</article></section>"
            )
            if convert:
                preview = PangoPreview()
                preview.feed(markup)
                preview.close()
                subprocess.run(
                    [
                        convert,
                        "-background",
                        "#182533",
                        "-fill",
                        "#edf2f7",
                        "-font",
                        "DejaVu-Sans",
                        "-pointsize",
                        "17",
                        "-size",
                        "540x",
                        "pango:" + "".join(preview.parts),
                        "-bordercolor",
                        "#182533",
                        "-border",
                        "24",
                        str(args.output / f"{name}-{number}.png"),
                    ],
                    check=True,
                    timeout=30,
                )
    page = (
        '<!doctype html><html lang="ru"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        "<title>Вузол · карточки руководителя</title><style>"
        "body{background:#101b26;color:#edf2f7;font:17px/1.5 system-ui;margin:24px}"
        "main{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,580px));gap:24px}"
        "article{white-space:pre-wrap;overflow-wrap:anywhere;background:#182533;"
        "padding:24px;border-radius:16px;line-height:1.45}"
        "small{color:#a8bacb}i{color:#bbcbd9}code{font-size:.9em}"
        "</style><h1>Карточки руководителя</h1>"
        "<p>Локальные примеры с синтетическими данными и настоящим Telegram HTML-рендерером. "
        "Это не скриншоты живого Telegram; сообщения не отправлялись.</p>"
        "<main>" + "".join(cards) + "</main></html>"
    )
    (args.output / "index.html").write_text(page, encoding="utf-8")
    print(args.output / "index.html")


if __name__ == "__main__":
    main()
