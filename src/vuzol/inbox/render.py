"""Inbox records as short Telegram HTML."""

from __future__ import annotations

from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo

from vuzol.storage.models import InboxItem

KIND_LABELS = {
    "event": ("🎫", "мероприятие"),
    "book": ("📚", "книга"),
    "film": ("🎬", "фильм"),
    "series": ("📺", "сериал"),
    "music": ("🎵", "музыка"),
    "repo": ("🧰", "репозиторий"),
    "tool": ("🛠", "инструмент"),
    "article": ("📰", "статья"),
    "post": ("💬", "пост"),
    "course": ("🎓", "курс"),
    "place": ("📍", "место"),
    "product": ("🛒", "вещь"),
    "person": ("👤", "человек"),
    "idea": ("💡", "идея"),
    "note": ("📝", "заметка"),
    "other": ("📎", "запись"),
}
_DAYS = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")
_MONTHS = (
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)
# Fields shown on the card's second line, in this order.
_SHOWN = ("author", "director", "year", "location", "venue", "city", "price", "license", "stars")


def when(moment: datetime, tz: str) -> str:
    local = moment.astimezone(ZoneInfo(tz))
    text = f"{_DAYS[local.weekday()]}, {local.day} {_MONTHS[local.month - 1]}"
    if local.year != datetime.now(ZoneInfo(tz)).year:
        text += f" {local.year}"
    if (local.hour, local.minute) != (0, 0):
        text += f", {local:%H:%M}"
    return text


def card_html(item: InboxItem, tz: str, *, header: str = "📥 Сохранил") -> str:
    icon, label = KIND_LABELS.get(item.kind, KIND_LABELS["other"])
    lines = [f"{header} · {icon} {label}", f"<b>{escape(item.title)}</b>"]
    facts: list[str] = []
    if item.event_start is not None:
        facts.append("📅 " + when(item.event_start, tz))
    fields = {key.lower(): value for key, value in (item.fields or {}).items()}
    facts += [escape(str(fields[key])) for key in _SHOWN if fields.get(key)]
    if facts:
        lines.append(" · ".join(facts[:4]))
    if item.summary:
        lines.append(escape(item.summary))
    verdict = fields.get("verdict")
    if verdict:
        lines.append("💬 " + escape(verdict))
    url = fields.get("url")
    if url and url.startswith(("http://", "https://")):
        lines.append(escape(url))
    if item.tags:
        lines.append(" ".join("#" + escape(tag.replace(" ", "_")) for tag in item.tags[:6]))
    return "\n".join(lines)


def answer_html(text: str) -> str:
    return escape(text)
