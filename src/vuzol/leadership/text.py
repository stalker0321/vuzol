"""Last server boundary for model text shown to the owner."""

import re
import textwrap

_IDENTIFIERS = re.compile(
    r"(?<![0-9a-f])(?:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}|"
    r"[0-9a-f]{64}|[0-9a-f]{40})(?![0-9a-f])",
    re.IGNORECASE,
)


def owner_text(value: object) -> str:
    return _IDENTIFIERS.sub("", str(value or ""))


def short_owner_text(value: object, width: int = 180) -> str:
    return textwrap.shorten(owner_text(value), width=width, placeholder="…")


def owner_sentences(value: object) -> list[str]:
    text = " ".join(owner_text(value).split())
    # Removing an identifier can leave "относительно ." behind; drop the gap.
    text = re.sub(r"\s+([,.;:)])", r"\1", text)
    return [part.rstrip(";") for part in re.split(r"(?<=[.!?;])\s+", text) if part.strip()]


def owner_sentence(value: object, width: int = 220, *, prefer: tuple[str, ...] = ()) -> str:
    """One complete sentence for a card line: the first matching a preferred
    pattern (in order), otherwise the first one."""
    sentences = owner_sentences(value)
    chosen = next(
        (s for pattern in prefer for s in sentences if re.search(pattern, s, re.I)),
        sentences[0] if sentences else "",
    )
    return textwrap.shorten(chosen, width=width, placeholder="…")


def is_plan_approval_question(question: str) -> bool:
    return bool(
        re.search(
            r"утверд|одобр|подтвер|прин[яи]|запус[кт]|запуст|приступ|нач(?:ать|ина)|"
            r"разреш|соглас|подходит|устраива|approv|confirm|accept|proceed|start",
            question,
            re.I,
        )
    )
