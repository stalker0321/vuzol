"""Safe CommonMark rendering for Telegram's supported HTML subset."""

from __future__ import annotations

import html
from html.parser import HTMLParser
from typing import cast
from urllib.parse import urlparse

from markdown_it import MarkdownIt
from markdown_it.token import Token

_MARKDOWN = MarkdownIt("commonmark")


def telegram_markdown_parts(markdown: str, *, limit: int = 4096) -> tuple[tuple[str, str], ...]:
    """Losslessly split rendered text, keeping tags/entities valid in every part.

    Count UTF-16 units conservatively, never raw HTML length. Each tuple holds
    formatted HTML and an escaped plain-text fallback for that exact part.
    """
    if limit < 2:
        raise ValueError("message limit must accommodate a Unicode code point")
    splitter = _TelegramHtmlSplitter(limit)
    splitter.feed(telegram_markdown_html(markdown))
    splitter.close()
    return splitter.finish()


class _TelegramHtmlSplitter(HTMLParser):
    """Only consumes the safe, balanced subset emitted by our renderer."""

    def __init__(self, limit: int) -> None:
        super().__init__(convert_charrefs=False)
        self.limit = limit
        self.parts: list[tuple[str, str]] = []
        self.tags: list[tuple[str, str]] = []
        self.markup: list[str] = []
        self.plain: list[str] = []
        self.units = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        opening = self.get_starttag_text()
        assert opening is not None
        self.tags.append((tag, opening))
        self.markup.append(opening)

    def handle_endtag(self, tag: str) -> None:
        assert self.tags and self.tags[-1][0] == tag
        self.tags.pop()
        self.markup.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        offset = 0
        while offset < len(data):
            available = self.limit - self.units
            end = offset
            units = 0
            while end < len(data):
                width = 2 if ord(data[end]) > 0xFFFF else 1
                if units + width > available:
                    break
                units += width
                end += 1
            if end == offset:
                self._flush()
                continue
            if end < len(data):
                # Prefer a line/word boundary, retaining the delimiter itself.
                boundary = max(data.rfind("\n", offset, end), data.rfind(" ", offset, end))
                if boundary >= offset + (end - offset) // 2:
                    end = boundary + 1
            text = data[offset:end]
            self.markup.append(html.escape(text, quote=False))
            self.plain.append(text)
            self.units += len(text.encode("utf-16-le")) // 2
            offset = end
            if offset < len(data):
                self._flush()

    def handle_entityref(self, name: str) -> None:
        self._entity(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self._entity(f"&#{name};")

    def _entity(self, escaped: str) -> None:
        decoded = html.unescape(escaped)
        width = len(decoded.encode("utf-16-le")) // 2
        if self.units + width > self.limit:
            self._flush()
        self.markup.append(escaped)
        self.plain.append(decoded)
        self.units += width

    def _flush(self) -> None:
        if not self.units:
            return
        closing = "".join(f"</{tag}>" for tag, _ in reversed(self.tags))
        self.parts.append(("".join(self.markup) + closing, html.escape("".join(self.plain))))
        self.markup = [opening for _, opening in self.tags]
        self.plain = []
        self.units = 0

    def finish(self) -> tuple[tuple[str, str], ...]:
        assert not self.tags
        self._flush()
        return tuple(self.parts) or (("", ""),)


def telegram_markdown_html(markdown: str) -> str:
    """Render untrusted CommonMark without passing arbitrary HTML to Telegram."""

    output: list[str] = []
    lists: list[list[object]] = []
    for token in _MARKDOWN.parse(markdown):
        kind = token.type
        if kind == "inline":
            output.append(_render_inline(token.children or []))
        elif kind == "heading_open":
            output.append("<b>")
        elif kind == "heading_close":
            output.append("</b>\n")
        elif kind == "paragraph_close" and not token.hidden:
            output.append("\n\n")
        elif kind == "bullet_list_open":
            lists.append(["bullet", 1])
        elif kind == "ordered_list_open":
            lists.append(["ordered", int(str(token.attrGet("start") or 1))])
        elif kind in {"bullet_list_close", "ordered_list_close"}:
            lists.pop()
            output.append("\n")
        elif kind == "list_item_open":
            indent = "  " * max(0, len(lists) - 1)
            if lists and lists[-1][0] == "ordered":
                number = cast(int, lists[-1][1])
                lists[-1][1] = number + 1
                output.append(f"{indent}{number}. ")
            else:
                output.append(f"{indent}• ")
        elif kind == "list_item_close":
            output.append("\n")
        elif kind == "blockquote_open":
            output.append("<blockquote>")
        elif kind == "blockquote_close":
            output.append("</blockquote>\n")
        elif kind in {"fence", "code_block"}:
            language = token.info.strip().split(maxsplit=1)[0] if token.info.strip() else ""
            language_attr = (
                f' class="language-{html.escape(language, quote=True)}"' if language else ""
            )
            escaped_code = html.escape(token.content, quote=False)
            output.append(f"<pre><code{language_attr}>{escaped_code}</code></pre>\n")
        elif kind == "hr":
            output.append("────────\n")
        elif kind in {"html_block"}:
            output.append(html.escape(token.content, quote=False))
    return "".join(output).strip()


def _render_inline(tokens: list[Token]) -> str:
    output: list[str] = []
    link_stack: list[bool] = []
    for token in tokens:
        kind = token.type
        if kind == "text":
            output.append(html.escape(token.content, quote=False))
        elif kind == "code_inline":
            output.append(f"<code>{html.escape(token.content, quote=False)}</code>")
        elif kind in {"softbreak", "hardbreak"}:
            output.append("\n")
        elif kind == "strong_open":
            output.append("<b>")
        elif kind == "strong_close":
            output.append("</b>")
        elif kind == "em_open":
            output.append("<i>")
        elif kind == "em_close":
            output.append("</i>")
        elif kind == "s_open":
            output.append("<s>")
        elif kind == "s_close":
            output.append("</s>")
        elif kind == "link_open":
            href = str(token.attrGet("href") or "")
            allowed = _safe_link(href)
            link_stack.append(allowed)
            if allowed:
                output.append(f'<a href="{html.escape(href, quote=True)}">')
        elif kind == "link_close":
            if link_stack.pop():
                output.append("</a>")
        elif kind in {"image", "html_inline"}:
            output.append(html.escape(token.content, quote=False))
    return "".join(output)


def _safe_link(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme.lower() in {"http", "https", "tg", "mailto"}
