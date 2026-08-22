import asyncio
import hashlib
import json
import os
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config import DependencyProvisioningSettings
from vuzol.config.models import SandboxNetworkMode, SandboxProfileConfig
from vuzol.execution.dependency_build import (
    DependencyBuildError,
    SandboxedDependencyBuilder,
    _freeze_environment,
    _remove_python_interpreter_links,
    _sha256_file,
)
from vuzol.execution.domain import MountMode, ProcessEnvelope
from vuzol.projects.custom_sources import CustomDependencySource
from vuzol.projects.dependencies import DependencyEnvironment, DependencyRequest
from vuzol.providers.ports import CodexProcessResult
from vuzol.workflows.ports import CancellationContext

_MANIFEST = b'[project]\nname = "demo"\n'
_LOCKFILE = b"uv-lock-approved\n"


class _FakeBuildRuntime:
    def __init__(
        self,
        *,
        exit_code: int = 0,
        stderr: str = "",
        lockfile: bytes = _LOCKFILE,
    ) -> None:
        self.envelopes: list[ProcessEnvelope] = []
        self.exit_code = exit_code
        self.stderr = stderr
        self.lockfile = lockfile

    async def run(self, envelope: ProcessEnvelope, _cancellation: object) -> CodexProcessResult:
        self.envelopes.append(envelope)
        build_root = next(
            mount.source for mount in envelope.sandbox.mounts if str(mount.target) == "/build"
        )
        if envelope.argv[0] == "/usr/local/bin/uv":
            interpreter_bin = build_root / "venv" / "bin"
            interpreter_bin.mkdir(parents=True)
            (interpreter_bin / "python3").symlink_to("/opt/builder/python3")
            (build_root / "uv.lock").write_bytes(self.lockfile)
        return CodexProcessResult(self.exit_code, "", self.stderr, 5)


def _sandbox_profile(*, enabled: bool = True) -> SandboxProfileConfig:
    return SandboxProfileConfig(
        id="validation",
        image="example/sandbox@sha256:" + "d" * 64,
        network_mode=SandboxNetworkMode.NONE,
        timeout_seconds=3_600,
        enabled=enabled,
    )


def _settings(tmp_path: Path) -> MagicMock:
    settings = MagicMock()
    settings.dependency_provisioning = DependencyProvisioningSettings(
        enabled=True,
        environment_root=tmp_path / "environments",
        maximum_environment_bytes=1_000_000,
    )
    settings.execution.sandbox_seccomp_profile = tmp_path / "seccomp.json"
    settings.execution.sandbox_seccomp_profile_sha256 = "a" * 64
    settings.capability_provisioning.toolchain_root = tmp_path / "toolchains"
    return settings


def _registries(sandbox: SandboxProfileConfig) -> MagicMock:
    registries = MagicMock()
    registries.projects.get.return_value = SimpleNamespace(
        validation_sandbox_profile=None, sandbox_profile=sandbox.id
    )
    registries.sandboxes.get.return_value = sandbox
    return registries


def _request(**overrides: object) -> DependencyRequest:
    values: dict[str, object] = {
        "ecosystem": "python",
        "manifest_name": "pyproject.toml",
        "manifest_sha256": hashlib.sha256(_MANIFEST).hexdigest(),
        "direct_dependencies": ("requests",),
        "registry_provider": "Python Packaging Authority",
        "registry_hosts": ("pypi.org",),
    }
    values.update(overrides)
    return DependencyRequest(**values)  # type: ignore[arg-type]


def _worktree(tmp_path: Path, request: DependencyRequest, *, manifest: bytes | None = None) -> Path:
    worktree = tmp_path / "worktree"
    worktree.mkdir(exist_ok=True)
    (worktree / request.manifest_name).write_bytes(manifest if manifest is not None else _MANIFEST)
    if request.input_lockfile_name is not None:
        (worktree / request.input_lockfile_name).write_bytes(_LOCKFILE)
    return worktree


def _wired_proxy_and_access() -> tuple[MagicMock, MagicMock]:
    proxy = MagicMock()
    lease = MagicMock()
    lease.networks.internal_name = "vuzol-proxy-internal"
    lease.proxy_url = "http://vuzol-proxy:8888"
    proxy.create = AsyncMock(return_value=lease)
    proxy.cleanup = AsyncMock()
    access = MagicMock()
    grant_result = MagicMock()
    grant_result.revoke = AsyncMock()
    access.grant = AsyncMock(return_value=grant_result)
    return proxy, access


def _builder(
    tmp_path: Path,
) -> tuple[SandboxedDependencyBuilder, _FakeBuildRuntime, MagicMock, MagicMock]:
    runtime = _FakeBuildRuntime()
    proxy, access = _wired_proxy_and_access()
    builder = SandboxedDependencyBuilder(
        cast(Any, _settings(tmp_path)),
        cast(Any, _registries(_sandbox_profile())),
        cast(Any, runtime),
        cast(Any, proxy),
        cast(Any, access),
    )
    return builder, runtime, proxy, access


def _step_request() -> Any:
    return SimpleNamespace(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        lease=SimpleNamespace(generation=1),
    )


def _build(
    builder: SandboxedDependencyBuilder,
    step_request: Any,
    worktree: Path,
    request: DependencyRequest,
    *,
    project_id: str = "demo",
) -> DependencyEnvironment:
    return asyncio.run(
        builder.build(
            step_request,
            project_id=project_id,
            worktree_id=uuid.uuid4(),
            worktree=worktree,
            request=request,
            cancellation=CancellationContext(),
        )
    )


def test_build_success_installs_frozen_environment_and_cleans_up(tmp_path: Path) -> None:
    request = _request()
    worktree = _worktree(tmp_path, request)
    builder, runtime, proxy, access = _builder(tmp_path)
    step_request = _step_request()

    environment = _build(builder, step_request, worktree, request)

    expected_root = tmp_path / "environments" / "demo" / "python" / request.environment_key
    assert environment.root == expected_root
    assert environment.lockfile_name == "uv.lock"
    assert environment.lockfile_sha256 == hashlib.sha256(_LOCKFILE).hexdigest()
    assert expected_root.is_dir()
    assert expected_root.stat().st_mode & 0o222 == 0
    envelope = runtime.envelopes[0]
    assert envelope.argv[-1] == "--no-build"
    assert "--locked" not in envelope.argv
    assert envelope.sandbox.timeout_seconds == 1_800
    assert envelope.sandbox.environment["UV_PROJECT_ENVIRONMENT"] == "/build/venv"
    assert not (expected_root / ".git").exists()
    proxy.create.assert_awaited_once()
    create_args = proxy.create.await_args.args
    assert create_args[:4] == (
        step_request.task_id,
        step_request.run_id,
        step_request.step_id,
        1,
    )
    targets = create_args[4]
    assert len(targets) == 1
    assert targets[0].hostname == "pypi.org"
    assert targets[0].port == 443
    assert targets[0].purpose == "python package registry"
    proxy.cleanup.assert_awaited_once()
    access.grant.assert_awaited_once()
    assert access.grant.await_args.kwargs == {"sandbox_uid": 10_001, "sandbox_gid": 10_001}
    access.grant.return_value.revoke.assert_awaited_once()


def test_build_reuses_cached_environment_without_runtime_or_proxy(tmp_path: Path) -> None:
    request = _request()
    worktree = _worktree(tmp_path, request)
    builder, runtime, proxy, _access = _builder(tmp_path)
    step_request = _step_request()

    first = _build(builder, step_request, worktree, request)
    second = _build(builder, step_request, worktree, request)

    assert second.root == first.root
    assert len(runtime.envelopes) == 1
    proxy.create.assert_awaited_once()


def test_build_rejects_incomplete_environment_directory(tmp_path: Path) -> None:
    request = _request()
    worktree = _worktree(tmp_path, request)
    builder, _runtime, proxy, access = _builder(tmp_path)
    stale_target = tmp_path / "environments" / "demo" / "python" / request.environment_key
    stale_target.mkdir(parents=True)

    with pytest.raises(DependencyBuildError, match="incomplete dependency environment"):
        _build(builder, _step_request(), worktree, request)

    proxy.create.assert_not_called()
    access.grant.assert_not_called()
    assert list(stale_target.parent.iterdir()) == [stale_target]


def test_build_rejects_disabled_sandbox_profile(tmp_path: Path) -> None:
    request = _request()
    worktree = _worktree(tmp_path, request)
    settings = _settings(tmp_path)
    registries = _registries(_sandbox_profile(enabled=False))
    runtime = _FakeBuildRuntime()
    proxy, access = _wired_proxy_and_access()
    builder = SandboxedDependencyBuilder(
        cast(Any, settings),
        cast(Any, registries),
        cast(Any, runtime),
        cast(Any, proxy),
        cast(Any, access),
    )

    with pytest.raises(DependencyBuildError, match="sandbox is disabled"):
        _build(builder, _step_request(), worktree, request)

    proxy.create.assert_not_called()
    access.grant.assert_not_called()
    assert not any((tmp_path / "environments" / "demo" / "python").iterdir())


def test_build_rejects_manifest_changed_after_approval(tmp_path: Path) -> None:
    request = _request()
    worktree = _worktree(tmp_path, request, manifest=b'[project]\nname = "other"\n')
    builder, _runtime, proxy, access = _builder(tmp_path)

    with pytest.raises(DependencyBuildError, match="manifest changed after approval"):
        _build(builder, _step_request(), worktree, request)

    proxy.create.assert_not_called()
    access.grant.assert_not_called()
    assert not any((tmp_path / "environments" / "demo" / "python").iterdir())


def test_build_reports_builder_failure_and_still_revokes_access(tmp_path: Path) -> None:
    request = _request()
    worktree = _worktree(tmp_path, request)
    settings = _settings(tmp_path)
    registries = _registries(_sandbox_profile())
    runtime = _FakeBuildRuntime(exit_code=1, stderr="resolver boom\n")
    proxy, access = _wired_proxy_and_access()
    builder = SandboxedDependencyBuilder(
        cast(Any, settings),
        cast(Any, registries),
        cast(Any, runtime),
        cast(Any, proxy),
        cast(Any, access),
    )

    with pytest.raises(DependencyBuildError, match="exited 1: resolver boom"):
        _build(builder, _step_request(), worktree, request)

    proxy.cleanup.assert_awaited_once()
    access.grant.return_value.revoke.assert_awaited_once()
    assert not any((tmp_path / "environments" / "demo" / "python").iterdir())


def test_build_rejects_lockfile_modified_by_package_manager(tmp_path: Path) -> None:
    request = _request(
        input_lockfile_name="uv.lock",
        input_lockfile_sha256=hashlib.sha256(_LOCKFILE).hexdigest(),
    )
    worktree = _worktree(tmp_path, request)
    settings = _settings(tmp_path)
    registries = _registries(_sandbox_profile())
    runtime = _FakeBuildRuntime(lockfile=b"uv-lock-regenerated\n")
    proxy, access = _wired_proxy_and_access()
    builder = SandboxedDependencyBuilder(
        cast(Any, settings),
        cast(Any, registries),
        cast(Any, runtime),
        cast(Any, proxy),
        cast(Any, access),
    )

    with pytest.raises(DependencyBuildError, match="changed the approved lockfile"):
        _build(builder, _step_request(), worktree, request)

    assert "--locked" in runtime.envelopes[0].argv
    proxy.cleanup.assert_awaited_once()
    access.grant.return_value.revoke.assert_awaited_once()
    assert not any((tmp_path / "environments" / "demo" / "python").iterdir())


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


def _envelope_for(
    tmp_path: Path,
    request: DependencyRequest,
    sandbox: object,
    *,
    settings: MagicMock | None = None,
) -> ProcessEnvelope:
    builder = SandboxedDependencyBuilder(
        cast(Any, settings if settings is not None else _settings(tmp_path)),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )
    build_root = tmp_path / "build"
    build_root.mkdir(exist_ok=True)
    proxy = MagicMock()
    proxy.networks.internal_name = "vuzol-proxy-internal"
    proxy.proxy_url = "http://vuzol-proxy:8888"
    return builder._envelope(
        _step_request(),
        worktree_id=uuid.uuid4(),
        build_root=build_root,
        request=request,
        sandbox=sandbox,
        proxy_lease=proxy,
    )


def test_envelope_rejects_invalid_sandbox_seccomp_and_ecosystem(tmp_path: Path) -> None:
    with pytest.raises(DependencyBuildError, match="sandbox profile is invalid"):
        _envelope_for(tmp_path, _request(), SimpleNamespace())
    unconfigured = _settings(tmp_path)
    unconfigured.execution.sandbox_seccomp_profile = None
    unconfigured.execution.sandbox_seccomp_profile_sha256 = None
    with pytest.raises(DependencyBuildError, match="seccomp profile is not configured"):
        _envelope_for(tmp_path, _request(), _sandbox_profile(), settings=unconfigured)
    with pytest.raises(DependencyBuildError, match="ecosystem builder is not implemented"):
        _envelope_for(tmp_path, _request(ecosystem="rust"), _sandbox_profile())


def _install_node_toolchain(tmp_path: Path) -> Path:
    toolchains = tmp_path / "toolchains"
    receipt_home = toolchains / "node-runtime"
    (receipt_home / "bin").mkdir(parents=True)
    receipt = {
        "schema_version": "capability-toolchain-receipt.v1",
        "capability_key": "node-runtime",
        "version": "22.11.0",
        "archive_sha256": "e" * 64,
        "executables": {"npm": "bin/npm"},
        "environment": {},
    }
    receipt_file = receipt_home / ".vuzol-toolchain.json"
    receipt_file.write_text(json.dumps(receipt), encoding="utf-8")
    receipt_file.chmod(0o444)
    npm = receipt_home / "bin" / "npm"
    npm.write_text("#!/bin/sh\n")
    npm.chmod(0o555)
    return toolchains


def test_envelope_node_requires_managed_npm_toolchain(tmp_path: Path) -> None:
    (tmp_path / "toolchains").mkdir()
    request = _request(ecosystem="node", manifest_name="package.json")

    with pytest.raises(DependencyBuildError, match=r"Node\.js toolchain is not installed"):
        _envelope_for(tmp_path, request, _sandbox_profile())


def test_envelope_node_wires_toolchain_mount_path_and_verbs(tmp_path: Path) -> None:
    toolchains = _install_node_toolchain(tmp_path)
    request = _request(ecosystem="node", manifest_name="package.json")

    install = _envelope_for(tmp_path, request, _sandbox_profile())
    locked_request = _request(
        ecosystem="node",
        manifest_name="package.json",
        input_lockfile_name="package-lock.json",
        input_lockfile_sha256=hashlib.sha256(_LOCKFILE).hexdigest(),
    )
    ci = _envelope_for(tmp_path, locked_request, _sandbox_profile())

    for envelope, verb in ((install, "install"), (ci, "ci")):
        assert envelope.argv[:2] == ("/toolchains/node-runtime/bin/npm", verb)
        assert envelope.argv[2:] == ("--ignore-scripts", "--no-audit", "--no-fund")
    toolchain_mount = next(
        mount for mount in install.sandbox.mounts if str(mount.target) == "/toolchains"
    )
    assert toolchain_mount.source == toolchains.resolve()
    assert toolchain_mount.mode is MountMode.READ_ONLY
    assert toolchain_mount.purpose == "approved-capability-toolchains"
    assert install.sandbox.environment["PATH"].startswith("/toolchains/node-runtime/bin:")


def test_sha256_file_rejects_directories_and_symlinks(tmp_path: Path) -> None:
    directory = tmp_path / "inputs"
    directory.mkdir()
    with pytest.raises(DependencyBuildError, match="not a regular file"):
        _sha256_file(directory)
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    link = tmp_path / "link.bin"
    link.symlink_to(target)
    with pytest.raises(DependencyBuildError, match="not a regular file"):
        _sha256_file(link)


def test_freeze_rejects_unsafe_file_types(tmp_path: Path) -> None:
    os.mkfifo(tmp_path / "pipe")
    with pytest.raises(DependencyBuildError, match="unsafe file type"):
        _freeze_environment(tmp_path, 1_000)


def test_freeze_enforces_storage_budget(tmp_path: Path) -> None:
    (tmp_path / "blob.bin").write_bytes(b"x" * 128)
    with pytest.raises(DependencyBuildError, match="exceeds storage policy"):
        _freeze_environment(tmp_path, 64)


def test_freeze_rejects_relative_symlinks_escaping_root(tmp_path: Path) -> None:
    environment = tmp_path / "env"
    environment.mkdir()
    (environment / "dangling").symlink_to("../missing-target")
    with pytest.raises(DependencyBuildError, match="unsafe symbolic link"):
        _freeze_environment(environment, 1_000)

    environment2 = tmp_path / "env2"
    environment2.mkdir()
    (tmp_path / "outside.txt").write_text("secret\n")
    (environment2 / "escape").symlink_to("../outside.txt")
    with pytest.raises(DependencyBuildError, match="unsafe symbolic link"):
        _freeze_environment(environment2, 1_000)


def test_freeze_pins_exact_read_only_modes(tmp_path: Path) -> None:
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/sh\n")
    script.chmod(0o755)
    data = tmp_path / "data.txt"
    data.write_text("value\n")
    nested = tmp_path / "nested"
    nested.mkdir()

    _freeze_environment(tmp_path, 1_000)

    assert script.stat().st_mode & 0o777 == 0o555
    assert data.stat().st_mode & 0o777 == 0o444
    assert nested.stat().st_mode & 0o777 == 0o555
