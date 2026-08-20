import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from vuzol.config import DependencyProvisioningSettings
from vuzol.config.models import SandboxNetworkMode, SandboxProfileConfig
from vuzol.execution.dependency_build import (
    DependencyBuildError,
    SandboxedDependencyBuilder,
    _freeze_environment,
    _remove_python_interpreter_links,
)
from vuzol.projects.custom_sources import CustomDependencySource
from vuzol.projects.dependencies import DependencyRequest


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
    for directory in (tmp_path, tmp_path / "node_modules", links, package, binary.parent):
        directory.chmod(0o700)
    binary.chmod(0o600)


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


def test_custom_python_source_enables_isolated_build_backend(tmp_path: Path) -> None:
    settings = MagicMock()
    settings.dependency_provisioning = DependencyProvisioningSettings(
        enabled=True, environment_root=tmp_path / "environments"
    )
    settings.execution.sandbox_seccomp_profile = tmp_path / "seccomp.json"
    settings.execution.sandbox_seccomp_profile_sha256 = "a" * 64
    settings.capability_provisioning.toolchain_root = tmp_path / "toolchains"
    builder = SandboxedDependencyBuilder(
        cast(Any, settings), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    source = CustomDependencySource(
        id=uuid.uuid4(),
        project_id="demo",
        ecosystem="python",
        package_name="demo",
        source_kind="git",
        source_url="https://github.com/acme/demo.git",
        source_pin="b" * 40,
    )
    request = DependencyRequest(
        ecosystem="python",
        manifest_name="pyproject.toml",
        manifest_sha256="c" * 64,
        direct_dependencies=("demo @ git+https://github.com/acme/demo.git@" + "b" * 40,),
        registry_provider="Python Packaging Authority",
        registry_hosts=("github.com", "pypi.org"),
        custom_sources=(source,),
    )
    step_request = cast(
        Any,
        SimpleNamespace(
            task_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            step_id=uuid.uuid4(),
            lease=SimpleNamespace(generation=1),
        ),
    )
    proxy = MagicMock()
    proxy.networks.internal_name = "vuzol-internal"
    proxy.proxy_url = "http://vuzol-proxy:8888"
    sandbox = SandboxProfileConfig(
        id="validation",
        image="validation@sha256:" + "d" * 64,
        enabled=True,
        network_mode=SandboxNetworkMode.NONE,
    )

    envelope = builder._envelope(
        step_request,
        worktree_id=uuid.uuid4(),
        build_root=tmp_path,
        request=request,
        sandbox=sandbox,
        proxy_lease=proxy,
    )

    assert "--no-build" not in envelope.argv
    assert envelope.sandbox.network_disabled is False
    assert envelope.sandbox.proxy_network == "vuzol-internal"
