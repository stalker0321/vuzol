"""Fail-closed rootless Docker runtime adapter."""

import asyncio
import contextlib
import hashlib
import io
import os
import stat
import tarfile
import time
import uuid
from pathlib import Path

from vuzol.execution.domain import MountMode, ProcessEnvelope
from vuzol.execution.paths import PathViolation
from vuzol.providers.grok import (
    GROK_DIAGNOSTIC_FILE_MAX_BYTES,
    grok_session_id_from_stdout,
    staged_grok_diagnostic_paths,
)
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
                await self._stop_owned_container(name, envelope)
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
                if cancel_task in done:
                    raise SandboxError("sandbox execution cancelled after start")
                raise SandboxError("sandbox execution timed out after start")
            stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
            result = CodexProcessResult(
                exit_code=wait_task.result(),
                stdout=stdout.decode("utf-8", "replace"),
                stderr=stderr.decode("utf-8", "replace"),
                duration_ms=int((time.monotonic() - started) * 1000),
            )
            if envelope.argv[:1] == ("grok",):
                await self._stage_grok_diagnostics(name, envelope, result.stdout)
            return result
        except asyncio.CancelledError:
            await self._stop_owned_container(name, envelope)
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()
            raise
        except ArtifactOutputLimit as error:
            await self._stop_owned_container(name, envelope)
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
                await self._stop_owned_container(name, envelope)
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
            await self._remove_owned_container(name, envelope)

    async def _stage_grok_diagnostics(
        self, name: str, envelope: ProcessEnvelope, stdout: str
    ) -> None:
        session_id = grok_session_id_from_stdout(stdout)
        staging = _artifact_staging(envelope)
        if session_id is None or staging is None:
            return
        paths = staged_grok_diagnostic_paths(staging, session_id)
        if paths is None:
            return
        try:
            container_id = await self._owned_container_id(name, envelope)
            if container_id is None:
                return
            _prepare_diagnostic_destinations(staging, paths)
        except (OSError, SandboxError, ValueError):
            return
        sources = (
            (
                f"/grok-home/.grok/sessions/%2Fworkspace/{session_id}/events.jsonl",
                paths[0],
            ),
            (
                f"/grok-home/.grok/sessions/%2Fworkspace/{session_id}/updates.jsonl",
                paths[1],
            ),
        )
        for source, destination in sources:
            try:
                content = await self._copy_container_regular_file(container_id, source)
                if content is not None:
                    _atomic_write_diagnostic(staging, destination, content)
            except (OSError, SandboxError, tarfile.TarError, ValueError):
                continue

    async def _copy_container_regular_file(self, name: str, source: str) -> bytes | None:
        process = await asyncio.create_subprocess_exec(
            "docker",
            "--host",
            f"unix://{self._socket}",
            "cp",
            f"{name}:{source}",
            "-",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent"},
        )
        stdout_task = asyncio.create_task(
            _bounded_read(process.stdout, GROK_DIAGNOSTIC_FILE_MAX_BYTES + 1_048_576)
        )
        stderr_task = asyncio.create_task(_bounded_read(process.stderr, 65_536))
        try:
            stdout, _stderr = await asyncio.gather(stdout_task, stderr_task)
            return_code = await process.wait()
        except ArtifactOutputLimit:
            process.kill()
            await process.wait()
            return None
        finally:
            for task in (stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        if return_code != 0:
            return None
        return _single_regular_tar_file(
            stdout,
            expected_name=Path(source).name,
            maximum=GROK_DIAGNOSTIC_FILE_MAX_BYTES,
        )

    async def _stop_owned_container(self, name: str, envelope: ProcessEnvelope) -> None:
        container_id = await self._owned_container_id(name, envelope)
        if container_id is None:
            return
        try:
            await self._docker(
                "stop", "--time", str(envelope.sandbox.stop_grace_seconds), container_id
            )
        except SandboxError:
            await self._docker("kill", container_id)

    async def _remove_owned_container(self, name: str, envelope: ProcessEnvelope) -> None:
        container_id = await self._owned_container_id(name, envelope)
        if container_id is None:
            return
        await self._docker("rm", "--force", container_id)

    async def _owned_container_id(self, name: str, envelope: ProcessEnvelope) -> str | None:
        filters = (
            f"name=^/{name}$",
            "label=vuzol.managed=true",
            "label=vuzol.resource=sandbox-container",
            f"label=vuzol.task_id={envelope.task_id}",
            f"label=vuzol.run_id={envelope.run_id}",
            f"label=vuzol.step_id={envelope.step_id}",
            f"label=vuzol.lease_generation={envelope.lease_generation}",
        )
        arguments = [
            "docker",
            "--host",
            f"unix://{self._socket}",
            "ps",
            "--all",
            "--no-trunc",
            "--format",
            "{{.ID}}",
        ]
        for item in filters:
            arguments.extend(("--filter", item))
        process = await asyncio.create_subprocess_exec(
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent"},
        )
        stdout, _stderr = await process.communicate()
        if process.returncode != 0:
            raise SandboxError("managed sandbox container lookup failed")
        matches = stdout.decode("ascii", "strict").splitlines()
        if not matches:
            return None
        if len(matches) != 1 or not all(
            len(value) == 64 and all(character in "0123456789abcdef" for character in value)
            for value in matches
        ):
            raise SandboxError("managed sandbox container identity is malformed")
        return matches[0]

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


def _artifact_staging(envelope: ProcessEnvelope) -> Path | None:
    matches = [
        mount
        for mount in envelope.sandbox.mounts
        if mount.target == Path("/artifacts") and mount.mode is MountMode.READ_WRITE
    ]
    if len(matches) != 1:
        return None
    source = matches[0].source
    try:
        source_stat = source.lstat()
        resolved = source.resolve(strict=True)
    except OSError:
        return None
    if source.is_symlink() or resolved != source or not stat.S_ISDIR(source_stat.st_mode):
        return None
    return source


def _single_regular_tar_file(payload: bytes, *, expected_name: str, maximum: int) -> bytes | None:
    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as archive:
            members = archive.getmembers()
            if (
                len(members) != 1
                or members[0].name.removeprefix("./") != expected_name
                or "/" in members[0].name.removeprefix("./")
                or not members[0].isreg()
                or members[0].size > maximum
            ):
                return None
            stream = archive.extractfile(members[0])
            if stream is None:
                return None
            content = stream.read(maximum + 1)
    except (EOFError, tarfile.TarError):
        return None
    return content if len(content) <= maximum else None


def _atomic_write_diagnostic(staging: Path, destination: Path, content: bytes) -> None:
    if len(content) > GROK_DIAGNOSTIC_FILE_MAX_BYTES:
        raise ValueError("staged Grok diagnostic exceeds the bounded limit")
    paths = staged_grok_diagnostic_paths(staging, destination.parent.name)
    if paths is None or destination not in paths:
        raise ValueError("staged Grok diagnostic destination is invalid")
    _ensure_diagnostic_directories(destination)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4()}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
    )
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written == 0:
                raise OSError("staged Grok diagnostic write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, destination)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise


def _prepare_diagnostic_destinations(staging: Path, paths: tuple[Path, Path]) -> None:
    for path in paths:
        _ensure_diagnostic_directories(path)
        try:
            path_stat = path.lstat()
        except FileNotFoundError:
            continue
        if path.is_symlink() or not stat.S_ISREG(path_stat.st_mode):
            raise ValueError("staged Grok diagnostic destination is unsafe")
        path.unlink()


def _ensure_diagnostic_directories(destination: Path) -> None:
    for directory in (destination.parent.parent, destination.parent):
        if directory.exists():
            directory_stat = directory.lstat()
            if directory.is_symlink() or not stat.S_ISDIR(directory_stat.st_mode):
                raise ValueError("staged Grok diagnostic directory is unsafe")
        else:
            directory.mkdir(mode=0o700)


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
