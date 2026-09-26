"""E23: confined executables are checked against the real read roots."""

from __future__ import annotations

import os
from pathlib import Path

from vuzol.security import confined_paths, landlock


def _executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def test_executable_within_roots_accepts_only_declared_roots(tmp_path: Path) -> None:
    root = tmp_path / "approved"
    inside = _executable(root / "bin" / "node")
    outside = _executable(tmp_path / "nvm" / "bin" / "node")

    assert confined_paths.executable_within_roots(inside, (root,))
    assert not confined_paths.executable_within_roots(outside, (root,))


def test_home_is_not_added_to_read_only_roots(tmp_path: Path) -> None:
    home = Path(os.path.expanduser("~")).resolve()
    roots = confined_paths.read_only_roots((tmp_path,))
    assert tmp_path in roots
    interpreter_paths = {Path(path).resolve() for path, _access in landlock.interpreter_read_only()}
    if home not in interpreter_paths:
        assert home not in roots
        # An NVM-style binary under $HOME is not readable in the confined runtime.
        assert not confined_paths.executable_within_roots(
            home / ".nvm" / "versions" / "node" / "bin" / "node", roots
        )


def test_confined_executable_requires_absolute_and_contained(tmp_path: Path) -> None:
    root = tmp_path / "approved"
    inside = _executable(root / "node")
    assert confined_paths.confined_executable(inside, (root,)) == inside
    assert confined_paths.confined_executable("node", (root,)) is None
    assert confined_paths.confined_executable(None, (root,)) is None
