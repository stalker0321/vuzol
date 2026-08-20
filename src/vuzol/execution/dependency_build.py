"""Controlled-egress sandbox builder for immutable project dependency environments."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import uuid
from pathlib import Path

from vuzol.config import DependencyProvisioningSettings, Settings
from vuzol.config.models import SandboxNetworkMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.access import WorktreeAccessManager
from vuzol.execution.domain import MountMode, ProcessEnvelope, SandboxMount, SandboxSpec
from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.paths import PathViolation, contained, trusted_root
from vuzol.execution.ports import SandboxRuntime
from vuzol.execution.proxy_service import ProxyServiceLease, ProxyServiceManager
from vuzol.projects.dependencies import (
    DEPENDENCY_RECEIPT,
    DependencyEnvironment,
    DependencyRequest,
    dependency_environment_path,
    load_dependency_environment,
)
from vuzol.projects.toolchains import toolchain_runtime
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class DependencyBuildError(RuntimeError):
    pass


class SandboxedDependencyBuilder:
    def __init__(
        self,
        settings: Settings,
        registries: ConfigurationBundle,
        runtime: SandboxRuntime,
        proxy: ProxyServiceManager,
        access: WorktreeAccessManager,
    ) -> None:
        self._settings = settings
        self._dependency_settings: DependencyProvisioningSettings = settings.dependency_provisioning
        self._registries = registries
        self._runtime = runtime
        self._proxy = proxy
        self._access = access

    async def build(
        self,
        step_request: StepExecutionRequest,
        *,
        project_id: str,
        worktree_id: uuid.UUID,
        worktree: Path,
        request: DependencyRequest,
        cancellation: CancellationContext,
    ) -> DependencyEnvironment:
        existing = load_dependency_environment(
            self._dependency_settings.environment_root, project_id, request
        )
        if existing is not None:
            return existing
        target = dependency_environment_path(
            self._dependency_settings.environment_root, project_id, request
        )
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        if target.exists():
            raise DependencyBuildError("incomplete dependency environment needs cleanup")
        temporary = contained(
            trusted_root(target.parent, create=False),
            target.parent / f".{request.environment_key}-{uuid.uuid4().hex}.tmp",
            must_exist=False,
        )
        temporary.mkdir(mode=0o700)
        try:
            _copy_approved_inputs(worktree, temporary, request)
            (temporary / ".git").write_text("dependency-build-sentinel\n", encoding="ascii")
            project = self._registries.projects.get(project_id)
            profile_id = project.validation_sandbox_profile or project.sandbox_profile
            sandbox = self._registries.sandboxes.get(profile_id)
            if not sandbox.enabled:
                raise DependencyBuildError("dependency build sandbox is disabled")
            access = await self._access.grant(
                temporary, sandbox_uid=sandbox.uid, sandbox_gid=sandbox.gid
            )
            proxy_lease: ProxyServiceLease | None = None
            try:
                targets = tuple(
                    AllowedConnectTarget(
                        hostname=host,
                        port=443,
                        purpose=f"{request.ecosystem} package registry",
                    )
                    for host in request.registry_hosts
                )
                proxy_lease = await self._proxy.create(
                    step_request.task_id,
                    step_request.run_id,
                    step_request.step_id,
                    step_request.lease.generation,
                    targets,
                )
                envelope = self._envelope(
                    step_request,
                    worktree_id=worktree_id,
                    build_root=temporary,
                    request=request,
                    sandbox=sandbox,
                    proxy_lease=proxy_lease,
                )
                result = await self._runtime.run(envelope, cancellation)
                if result.exit_code != 0:
                    detail = (result.stderr or result.stdout).strip()[-1_000:]
                    raise DependencyBuildError(
                        f"dependency builder exited {result.exit_code}: {detail}"
                    )
            finally:
                try:
                    if proxy_lease is not None:
                        await self._proxy.cleanup(proxy_lease)
                finally:
                    await access.revoke()
            (temporary / ".git").unlink()
            shutil.rmtree(temporary / ".cache", ignore_errors=True)
            lockfile_name = "uv.lock" if request.ecosystem == "python" else "package-lock.json"
            lockfile = contained(temporary, temporary / lockfile_name)
            lockfile_sha256 = _sha256_file(lockfile)
            if request.input_lockfile_sha256 is not None and (
                request.input_lockfile_name != lockfile_name
                or request.input_lockfile_sha256 != lockfile_sha256
            ):
                raise DependencyBuildError("trusted package manager changed the approved lockfile")
            if request.ecosystem == "python":
                _remove_python_interpreter_links(temporary)
            environment = DependencyEnvironment(
                request=request,
                root=target,
                lockfile_name=lockfile_name,
                lockfile_sha256=lockfile_sha256,
            )
            receipt = temporary / DEPENDENCY_RECEIPT
            receipt.write_text(
                json.dumps(environment.receipt(), sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            _freeze_environment(temporary, self._dependency_settings.maximum_environment_bytes)
            os.replace(temporary, target)
            installed = load_dependency_environment(
                self._dependency_settings.environment_root, project_id, request
            )
            if installed is None:
                raise DependencyBuildError("dependency environment failed its receipt probe")
            return installed
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

    def _envelope(
        self,
        step_request: StepExecutionRequest,
        *,
        worktree_id: uuid.UUID,
        build_root: Path,
        request: DependencyRequest,
        sandbox: object,
        proxy_lease: ProxyServiceLease,
    ) -> ProcessEnvelope:
        from vuzol.config.models import SandboxProfileConfig

        if not isinstance(sandbox, SandboxProfileConfig):
            raise DependencyBuildError("dependency sandbox profile is invalid")
        if sandbox.network_mode not in {
            SandboxNetworkMode.NONE,
            SandboxNetworkMode.HTTPS_PROXY,
        }:
            raise DependencyBuildError("dependency sandbox network policy is invalid")
        seccomp_profile = self._settings.execution.sandbox_seccomp_profile
        seccomp_digest = self._settings.execution.sandbox_seccomp_profile_sha256
        if seccomp_profile is None or seccomp_digest is None:
            raise DependencyBuildError("sandbox seccomp profile is not configured")
        mounts = [
            SandboxMount(
                source=build_root,
                target=Path("/build"),
                mode=MountMode.READ_WRITE,
                purpose="approved-dependency-build",
            )
        ]
        environment = {
            "HOME": "/tmp/home",  # noqa: S108
            "CI": "1",
            "PATH": "/opt/vuzol-validation/bin:/usr/local/bin:/usr/bin:/bin",
        }
        argv: tuple[str, ...]
        if request.ecosystem == "python":
            argv = (
                "/usr/local/bin/uv",
                "sync",
                "--project",
                "/build",
                "--no-install-project",
                "--no-dev",
                "--no-editable",
            )
            if not request.custom_sources:
                argv = (*argv, "--no-build")
            if request.input_lockfile_name is not None:
                argv = (*argv, "--locked")
            environment.update(
                {
                    "UV_PROJECT_ENVIRONMENT": "/build/venv",
                    "UV_CACHE_DIR": "/build/.cache",
                }
            )
        elif request.ecosystem == "node":
            runtime = toolchain_runtime(
                trusted_root(self._settings.capability_provisioning.toolchain_root, create=False),
                ("node-runtime",),
            )
            executables = dict(runtime.executables)
            npm = executables.get("npm")
            if npm is None:
                raise DependencyBuildError("managed Node.js toolchain is not installed")
            mounts.append(
                SandboxMount(
                    source=trusted_root(
                        self._settings.capability_provisioning.toolchain_root, create=False
                    ),
                    target=Path("/toolchains"),
                    mode=MountMode.READ_ONLY,
                    purpose="approved-capability-toolchains",
                )
            )
            environment.update(dict(runtime.environment))
            environment["PATH"] = ":".join(runtime.path_entries) + ":" + environment["PATH"]
            verb = "ci" if request.input_lockfile_name is not None else "install"
            argv = (npm, verb, "--ignore-scripts", "--no-audit", "--no-fund")
        else:
            raise DependencyBuildError("dependency ecosystem builder is not implemented")
        spec = SandboxSpec(
            image=sandbox.image,
            uid=sandbox.uid,
            gid=sandbox.gid,
            seccomp_profile=seccomp_profile,
            seccomp_profile_sha256=seccomp_digest,
            working_directory=Path("/build"),
            mounts=tuple(mounts),
            cpu_count=sandbox.cpu_count,
            memory_bytes=sandbox.memory_bytes,
            pids_limit=sandbox.pids_limit,
            tmpfs_bytes=sandbox.tmpfs_bytes,
            open_files_limit=sandbox.open_files_limit,
            output_bytes=sandbox.output_bytes,
            timeout_seconds=min(sandbox.timeout_seconds, 1_800),
            stop_grace_seconds=sandbox.stop_grace_seconds,
            network_disabled=False,
            proxy_network=proxy_lease.networks.internal_name,
            https_proxy_url=proxy_lease.proxy_url,
            environment=environment,
        )
        return ProcessEnvelope(
            task_id=step_request.task_id,
            run_id=step_request.run_id,
            step_id=step_request.step_id,
            worktree_id=worktree_id,
            profile_id="dependency_builder",
            provider_attempt=1,
            lease_generation=step_request.lease.generation,
            argv=argv,
            stdin="",
            sandbox=spec,
        )


def _copy_approved_inputs(worktree: Path, destination: Path, request: DependencyRequest) -> None:
    source = contained(worktree, worktree / request.manifest_name)
    if _sha256_file(source) != request.manifest_sha256:
        raise DependencyBuildError("dependency manifest changed after approval")
    shutil.copyfile(source, destination / request.manifest_name)
    if request.input_lockfile_name is not None:
        lockfile = contained(worktree, worktree / request.input_lockfile_name)
        if _sha256_file(lockfile) != request.input_lockfile_sha256:
            raise DependencyBuildError("dependency lockfile changed after approval")
        shutil.copyfile(lockfile, destination / request.input_lockfile_name)


def _sha256_file(path: Path) -> str:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise DependencyBuildError("dependency input is not a regular file")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _freeze_environment(root: Path, maximum_bytes: int) -> None:
    total = 0
    paths = sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True)
    for path in paths:
        metadata = path.lstat()
        if path.is_symlink():
            try:
                link_target = os.readlink(path)
                if Path(link_target).is_absolute():
                    raise DependencyBuildError(
                        "dependency builder produced an external symbolic link"
                    )
                contained(root, path.resolve(strict=True))
            except (OSError, PathViolation) as error:
                raise DependencyBuildError(
                    "dependency builder produced an unsafe symbolic link"
                ) from error
            continue
        if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
            raise DependencyBuildError("dependency builder produced an unsafe file type")
        if stat.S_ISREG(metadata.st_mode):
            total += metadata.st_size
            if total > maximum_bytes:
                raise DependencyBuildError("dependency environment exceeds storage policy")
            path.chmod(0o555 if metadata.st_mode & 0o111 else 0o444)
        else:
            path.chmod(0o555)
    root.chmod(0o555)


def _remove_python_interpreter_links(root: Path) -> None:
    """Drop venv links bound to the builder image; validation supplies Python."""

    bin_directory = contained(root, root / "venv" / "bin")
    for name in ("python", "python3", "python3.12", "python3.13"):
        candidate = bin_directory / name
        if candidate.is_symlink():
            candidate.unlink()
