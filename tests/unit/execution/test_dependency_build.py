from pathlib import Path

import pytest

from vuzol.execution.dependency_build import (
    DependencyBuildError,
    _freeze_environment,
    _remove_python_interpreter_links,
)


def test_freeze_accepts_only_internal_relative_symlinks(tmp_path: Path) -> None:
    package = tmp_path / "node_modules" / "demo"
    binary = package / "bin" / "demo.js"
    binary.parent.mkdir(parents=True)
    binary.write_text("#!/usr/bin/env node\n")
    links = tmp_path / "node_modules" / ".bin"
    links.mkdir()
    (links / "demo").symlink_to("../demo/bin/demo.js")

    _freeze_environment(tmp_path, 1_000)

    assert (links / "demo").is_symlink()
    assert tmp_path.stat().st_mode & 0o222 == 0
    assert binary.stat().st_mode & 0o222 == 0


def test_freeze_rejects_external_or_dangling_symlink(tmp_path: Path) -> None:
    (tmp_path / "external").symlink_to("/etc/passwd")

    with pytest.raises(DependencyBuildError, match="external symbolic link"):
        _freeze_environment(tmp_path, 1_000)


def test_python_builder_drops_image_bound_interpreter_links(tmp_path: Path) -> None:
    bin_directory = tmp_path / "venv" / "bin"
    bin_directory.mkdir(parents=True)
    (bin_directory / "python").symlink_to("/opt/builder/python")
    (bin_directory / "python3").symlink_to("python")
    script = bin_directory / "tool"
    script.write_text("#!/bin/sh\n")

    _remove_python_interpreter_links(tmp_path)

    assert not (bin_directory / "python").exists()
    assert not (bin_directory / "python3").exists()
    assert script.exists()
