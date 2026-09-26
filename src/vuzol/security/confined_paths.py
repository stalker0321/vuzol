"""Readability of executables under the real Landlock ruleset (E23).

An executable resolved with ``shutil.which`` may live outside the paths the
confined child can read (for example a per-user NVM install under ``$HOME``).
This module answers "would the confined process be able to read/execute this
path?" against the exact ruleset that will run it, without ever widening the
ruleset to ``$HOME``.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

from vuzol.security import landlock


def read_only_roots(extra_read_only: Iterable[str | Path] = ()) -> tuple[Path, ...]:
    """The real read roots: interpreter/system paths plus declared extras."""

    roots: list[Path] = []
    for path, _access in landlock.interpreter_read_only():
        roots.append(Path(path))
    for candidate in extra_read_only:
        if candidate:
            roots.append(Path(candidate))
    seen: list[Path] = []
    for root in roots:
        resolved = Path(os.path.realpath(root))
        if resolved not in seen:
            seen.append(resolved)
    return tuple(seen)


def executable_within_roots(executable: Path, roots: Iterable[Path]) -> bool:
    """True only if the real executable path is inside one of the read roots."""

    try:
        resolved = Path(os.path.realpath(executable))
    except OSError:
        return False
    if not resolved.is_file():
        return False
    for root in roots:
        try:
            real_root = Path(os.path.realpath(root))
        except OSError:
            continue
        if resolved == real_root or real_root in resolved.parents:
            return True
    return False


def confined_executable(
    executable: str | Path | None, extra_read_only: Iterable[str | Path] = ()
) -> Path | None:
    """Return the resolved path only if a confined child could read and exec it."""

    if executable is None:
        return None
    candidate = Path(executable)
    if not candidate.is_absolute():
        return None
    if not executable_within_roots(candidate, read_only_roots(extra_read_only)):
        return None
    return candidate
