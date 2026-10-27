"""Mechanical review signals over changed text."""

import re

from pydantic import Field

from vuzol.execution.domain import FrozenModel


class SuspiciousSignal(FrozenModel):
    path: str
    line: int = Field(ge=1)
    classification: str
    excerpt: str = Field(max_length=300)


_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    # A standalone ``exit 0`` is the normal end of many strict shell test runners.
    # Flag explicit failure suppression instead of successful process termination.
    ("forced_success", re.compile(r"\bor\s+True\b|\|\|\s*(?:true\b|exit\s+0\b)")),
    (
        "exception_swallowing",
        re.compile(r"except\s+(?:Exception|BaseException)[^:]*:\s*(?:#.*\n\s*)?pass\b"),
    ),
    ("arbitrary_skip", re.compile(r"pytest\.(?:skip|mark\.skip)|@pytest\.mark\.xfail")),
    # Temporary platform safeguard until P0/P1 invariant automation replaces it.
    (
        "coverage_weakening",
        re.compile(r"(?:fail-under|cov-fail-under)[^\n]*[=-]\s*(?:[0-8]?\d)(?:\D|$)"),
    ),
    ("ignore_added", re.compile(r"noqa|nosec|type:\s*ignore|pragma:\s*no cover")),
    (
        "broad_cleanup",
        re.compile(r"docker\s+(?:system|container|network)\s+prune|rm\s+-rf\s+[^\n]*\*"),
    ),
    ("shell_execution", re.compile(r"subprocess\.(?:run|Popen|call)\([^\n]*shell\s*=\s*True")),
    (
        "cleanup_error_assertion",
        re.compile(r"assert[^\n]+(?:not found|no such|deleted)[^\n]+\bor\b", re.I),
    ),
)


def scan_suspicious_patterns(changed_text: dict[str, str]) -> tuple[SuspiciousSignal, ...]:
    signals: list[SuspiciousSignal] = []
    for path, content in sorted(changed_text.items()):
        for classification, pattern in _PATTERNS:
            for match in pattern.finditer(content):
                line = content.count("\n", 0, match.start()) + 1
                excerpt = content.splitlines()[line - 1].strip()[:300]
                signals.append(
                    SuspiciousSignal(
                        path=path,
                        line=line,
                        classification=classification,
                        excerpt=excerpt,
                    )
                )
    return tuple(signals)
