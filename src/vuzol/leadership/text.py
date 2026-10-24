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


def is_plan_approval_question(question: str) -> bool:
    return bool(
        re.search(
            r"утверд|одобр|подтвер|прин[яи]|запус[кт]|запуст|приступ|нач(?:ать|ина)|"
            r"разреш|соглас|подходит|устраива|approv|confirm|accept|proceed|start",
            question,
            re.I,
        )
    )
