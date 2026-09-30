"""Deterministic explicit-task detection (D4 W3).

Fast path alongside the model interpreter, not instead of it. Only explicit
user task commands arm ``EXPLICIT_TASK``:

- ``/task`` slash command (project topic);
- ``задача:`` / ``task:`` prefixed imperative lines;
- legacy direct-task create path (discussion disabled) and pre-model slash
  commands, which already bypass the slow LLM.

Everything else stays confirm-first.
"""

from __future__ import annotations

_EXPLICIT_PREFIXES = ("/task", "задача:", "task:")


def is_explicit_task_command(text: str | None) -> bool:
    """Return True only for explicit user task commands."""

    if not text:
        return False
    normalized = text.strip().casefold()
    return normalized.startswith(_EXPLICIT_PREFIXES)


def explicit_task_body(text: str) -> str:
    """Strip the explicit prefix, keeping the raw task body."""

    stripped = text.strip()
    lowered = stripped.casefold()
    for prefix in _EXPLICIT_PREFIXES:
        if lowered.startswith(prefix):
            return stripped[len(prefix) :].strip()
    return stripped
