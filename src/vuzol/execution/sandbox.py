"""Fail-closed rootless Docker runtime adapter."""

import asyncio
import contextlib
import hashlib
import os
import stat
import time
from pathlib import Path

from vuzol.execution.domain import MountMode, ProcessEnvelope
from vuzol.execution.paths import PathViolation
from vuzol.providers.ports import CodexProcessResult
from vuzol.workflows.ports import CancellationContext


class SandboxError(RuntimeError):
    """The external sandbox could not prove or preserve isolation."""


class RootlessDockerRuntime:
    def __init__(self, socket: Path) -> None:
        self._socket = socket

    async def preflight(self) -> None:
        if self._socket == Path("/var/run/docker.sock"):
            raise SandboxError("rootful Docker socket is prohibited")
        try:
            socket_stat = self._socket.stat()
        except OSError as error:
            raise SandboxError("rootless Docker socket is unavailable") from error
        if not stat.S_ISSOCK(socket_stat.st_mode):
            raise SandboxError("configured Docker endpoint is not a Unix socket")
        if socket_stat.st_uid != os.geteuid():
            raise SandboxError("rootless Docker socket is not owned by the executor identity")
        security = await self._docker("info", "--format", "{{json .SecurityOptions}}")
        if "rootless" not in security:
            raise SandboxError("Docker daemon does not report rootless mode")
        if "seccomp" not in security:
            raise SandboxError("Docker daemon does not report seccomp enforcement")
        cgroup_version = await self._docker("info", "--format", "{{.CgroupVersion}}")
        if cgroup_version.strip() != "2":
            raise SandboxError("Docker daemon does not report cgroup v2")
        warnings = await self._docker("info", "--format", "{{json .Warnings}}")
        unsupported = ("No memory limit support", "No cpu cfs quota support")
        if any(message in warnings for message in unsupported):
            raise SandboxError("Docker daemon does not enforce required cgroup limits")

    async def run(
        self, envelope: ProcessEnvelope, cancellation: CancellationContext
    ) -> CodexProcessResult:
        name = f"vuzol-{str(envelope.step_id)[:12]}-{envelope.lease_generation}"
        argv = docker_run_argv(self._socket, name, envelope)
        started = time.monotonic()
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent"},
        )
        assert process.stdin is not None
        process.stdin.write(envelope.stdin.encode())
        await process.stdin.drain()
        process.stdin.close()
        stdout_task = asyncio.create_task(
            _bounded_read(process.stdout, envelope.sandbox.output_bytes), name="sandbox-stdout"
        )
        stderr_task = asyncio.create_task(
            _bounded_read(process.stderr, envelope.sandbox.output_bytes), name="sandbox-stderr"
        )
        wait_task = asyncio.create_task(process.wait(), name="sandbox-process")
        cancel_task = asyncio.create_task(cancellation.wait(), name="sandbox-cancellation")
        try:
            done, _pending = await asyncio.wait(
                {wait_task, cancel_task},
                timeout=envelope.sandbox.timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if wait_task not in done:
                await self._stop(name, envelope.sandbox.stop_grace_seconds)
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
                if cancel_task in done:
                    raise SandboxError("sandbox execution cancelled after start")
                raise SandboxError("sandbox execution timed out after start")
            stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
            return CodexProcessResult(
                exit_code=wait_task.result(),
                stdout=stdout.decode("utf-8", "replace"),
                stderr=stderr.decode("utf-8", "replace"),
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        except asyncio.CancelledError:
            await self._stop(name, envelope.sandbox.stop_grace_seconds)
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise
        except ArtifactOutputLimit as error:
            await self._stop(name, envelope.sandbox.stop_grace_seconds)
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise SandboxError("sandbox output limit exceeded") from error
        finally:
            for task in (wait_task, cancel_task, stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                wait_task, cancel_task, stdout_task, stderr_task, return_exceptions=True
            )
            # Ensure docker client process is reaped even on unusual exits
            if process.returncode is None:
                await self._stop(name, envelope.sandbox.stop_grace_seconds)
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()

    async def _stop(self, name: str, grace_seconds: int) -> None:
        try:
            await self._docker("stop", "--time", str(grace_seconds), name)
        except SandboxError:
            await self._docker("kill", name)

    async def _docker(self, *args: str) -> str:
        process = await asyncio.create_subprocess_exec(
            "docker",
            "--host",
            f"unix://{self._socket}",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent"},
        )
        stdout, _stderr = await process.communicate()
        if process.returncode != 0:
            raise SandboxError(f"rootless Docker operation failed: {args[0]}")
        return stdout.decode("utf-8", "replace")


class ArtifactOutputLimit(RuntimeError):
    pass


def validate_seccomp_profile(path: Path, expected_sha256: str) -> Path:
    try:
        path_stat = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise SandboxError("sandbox seccomp profile is unavailable") from error
    if path.is_symlink() or resolved != path:
        raise SandboxError("sandbox seccomp profile must not use symlinks")
    if not stat.S_ISREG(path_stat.st_mode):
        raise SandboxError("sandbox seccomp profile is not a regular file")
    if path_stat.st_uid not in {0, os.geteuid()} or path_stat.st_mode & 0o022:
        raise SandboxError("sandbox seccomp profile ownership or mode is unsafe")
    try:
        actual_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise SandboxError("sandbox seccomp profile is unreadable") from error
    if actual_sha256 != expected_sha256:
        raise SandboxError("sandbox seccomp profile digest mismatch")
    return resolved


async def _bounded_read(stream: asyncio.StreamReader | None, maximum: int) -> bytes:
    if stream is None:
        return b""
    chunks: list[bytes] = []
    total = 0
    while chunk := await stream.read(65_536):
        total += len(chunk)
        if total > maximum:
            raise ArtifactOutputLimit
        chunks.append(chunk)
    return b"".join(chunks)


def docker_run_argv(socket: Path, name: str, envelope: ProcessEnvelope) -> tuple[str, ...]:
    spec = envelope.sandbox
    seccomp_profile = validate_seccomp_profile(spec.seccomp_profile, spec.seccomp_profile_sha256)
    arguments = [
        "docker",
        "--host",
        f"unix://{socket}",
        "run",
        "-i",
        "--rm",
        "--name",
        name,
        "--pull",
        "never",
        "--network",
        "none" if spec.network_disabled else str(spec.proxy_network),
        "--read-only",
        "--user",
        f"{spec.uid}:{spec.gid}",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--security-opt",
        f"seccomp={seccomp_profile}",
        "--memory",
        str(spec.memory_bytes),
        "--memory-swap",
        str(spec.memory_bytes),
        "--cpus",
        str(spec.cpu_count),
        "--pids-limit",
        str(spec.pids_limit),
        "--ulimit",
        f"nofile={spec.open_files_limit}:{spec.open_files_limit}",
        "--tmpfs",
        f"/tmp:rw,nosuid,nodev,noexec,size={spec.tmpfs_bytes}",  # noqa: S108
        "--workdir",
        str(spec.working_directory),
        "--label",
        "vuzol.managed=true",
        "--label",
        "vuzol.resource=sandbox-container",
        "--label",
        f"vuzol.task_id={envelope.task_id}",
        "--label",
        f"vuzol.run_id={envelope.run_id}",
        "--label",
        f"vuzol.step_id={envelope.step_id}",
        "--label",
        f"vuzol.lease_generation={envelope.lease_generation}",
    ]
    for mount in spec.mounts:
        try:
            source = mount.source.resolve(strict=True)
        except OSError as error:
            raise PathViolation("sandbox mount source is unavailable") from error
        if source == Path("/var/run/docker.sock") or source.name == "docker.sock":
            raise PathViolation("Docker socket mount is prohibited")
        mount_spec = f"type=bind,src={source},dst={mount.target}"
        if mount.mode is MountMode.READ_ONLY:
            mount_spec += ",readonly"
        arguments.extend(("--mount", mount_spec))
    for key, value in sorted(spec.environment.items()):
        arguments.extend(("--env", f"{key}={value}"))
    if spec.https_proxy_url is not None:
        proxy_environment = {
            "HTTPS_PROXY": spec.https_proxy_url,
            "HTTP_PROXY": spec.https_proxy_url,
            "https_proxy": spec.https_proxy_url,
            "http_proxy": spec.https_proxy_url,
            "ALL_PROXY": "",
            "NO_PROXY": "",
            "all_proxy": "",
            "no_proxy": "",
        }
        for key, value in sorted(proxy_environment.items()):
            arguments.extend(("--env", f"{key}={value}"))
    arguments.append(spec.image)
    arguments.extend(envelope.argv)
    return tuple(arguments)
