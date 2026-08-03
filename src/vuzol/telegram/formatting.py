"""Safe CommonMark rendering for Telegram's supported HTML subset."""

from __future__ import annotations

import html
from typing import cast
from urllib.parse import urlparse

from markdown_it import MarkdownIt
from markdown_it.token import Token

_MARKDOWN = MarkdownIt("commonmark")


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
            output.append(
                f"<pre><code{language_attr}>{escaped_code}</code></pre>\n"
            )
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
