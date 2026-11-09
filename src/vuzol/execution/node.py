"""Sandboxes on another machine's rootless Docker, reached through a forwarded socket.

Owner 2026-11-08: "все сидят тут, работают где надо". The control plane stays on
this host; a node is only Docker. Mount sources do not exist there, so each one is
copied into a node volume before the run and the volume replaces the bind mount.
The files belong to sandbox identities, so local Docker reads them and root in the
node's user namespace unpacks them, which keeps numeric owners. Writable mounts
come back only after normal exit, through a fresh local sibling directory.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from vuzol.execution.domain import MountMode, ProcessEnvelope, TurnProcessBinding
from vuzol.execution.journal import ExecutionJournal
from vuzol.execution.proxy_service import (
    POLICY_CONTAINER_PATH,
    ProxyServiceError,
    ProxyServiceManager,
    _ownership_labels,
    _sandbox_ownership_labels,
)
from vuzol.execution.sandbox import (
    RootlessDockerRuntime,
    SandboxError,
    docker_run_argv,
    drain_runtime_cleanup,
)
from vuzol.providers.ports import CodexProcessResult
from vuzol.workflows.ports import CancellationContext

STAGE_LABEL = "vuzol.node-stage"
STAGE_TIMEOUT_SECONDS = 120
COPY_BACK_TIMEOUT_SECONDS = 600
DEFAULT_MAX_COPY_BACK_BYTES = 2 * 1024**3
_ENV = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent"}


async def _docker(
    socket: Path,
    *args: str,
    stdin: bytes | None = None,
    timeout_seconds: int = STAGE_TIMEOUT_SECONDS,
) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "--host",
        f"unix://{socket}",
        *args,
        stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_ENV,
    )
    try:
        async with asyncio.timeout(timeout_seconds):
            stdout, _stderr = await process.communicate(stdin)
    except BaseException as error:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        if isinstance(error, TimeoutError):
            raise SandboxError(f"node Docker operation timed out: {args[0]}") from error
        raise
    if process.returncode != 0:
        raise SandboxError(f"node Docker operation failed: {args[0]}")
    return stdout.decode("utf-8", "replace")


def _helper(
    *,
    socket: Path,
    mounts: tuple[str, ...],
    image: str,
    capabilities: tuple[str, ...],
    entrypoint: str = "tar",
) -> list[str]:
    argv = ["docker", "--host", f"unix://{socket}", "run", "--rm", "-i", "--pull", "never"]
    argv += ["--network", "none", "--user", "0:0", "--read-only", "--cap-drop", "ALL"]
    for capability in capabilities:
        argv += ["--cap-add", capability]
    argv += ["--security-opt", "no-new-privileges:true", "--memory", "268435456"]
    for mount in mounts:
        argv += ["--mount", mount]
    return [*argv, "--entrypoint", entrypoint, image]


def _packing(source: Path) -> tuple[tuple[str], list[str], str | None]:
    source = source.resolve(strict=True)
    if source.is_dir():
        return (
            (f"type=bind,src={source},dst=/s,readonly",),
            ["--acls", "--numeric-owner", "-C", "/s", "-cf", "-", "."],
            None,
        )
    return (
        (f"type=bind,src={source},dst=/s/{source.name},readonly",),
        ["--acls", "--numeric-owner", "-C", "/s", "-cf", "-", source.name],
        source.name,
    )


class NodeVolumes:
    """Owned node volumes holding copies of local mount sources."""

    def __init__(self, local_socket: Path, node_socket: Path) -> None:
        self._local = local_socket
        self._node = node_socket

    async def create(self, volume: str, labels: dict[str, str]) -> None:
        arguments = ["volume", "create"]
        for key, value in sorted({**labels, STAGE_LABEL: "1"}.items()):
            arguments += ["--label", f"{key}={value}"]
        await _docker(self._node, *arguments, volume)

    async def copy(self, source: Path, volume: str, image: str) -> str | None:
        """Copy a local file or directory in; returns the subpath a file is mounted by."""
        read, pack, subpath = _packing(source)
        reader, writer = os.pipe()
        processes: list[asyncio.subprocess.Process] = []
        try:
            try:
                packer = await asyncio.create_subprocess_exec(
                    *_helper(
                        socket=self._local,
                        mounts=read,
                        image=image,
                        capabilities=("DAC_READ_SEARCH",),
                    ),
                    *pack,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=writer,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=_ENV,
                )
                processes.append(packer)
                unpacker = await asyncio.create_subprocess_exec(
                    *_helper(
                        socket=self._node,
                        mounts=(f"type=volume,src={volume},dst=/seed",),
                        image=image,
                        capabilities=("CHOWN", "FOWNER", "DAC_OVERRIDE"),
                    ),
                    "--acls",
                    "-xpf",
                    "-",
                    "-C",
                    "/seed",
                    "--numeric-owner",
                    stdin=reader,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=_ENV,
                )
                processes.append(unpacker)
            finally:
                os.close(reader)
                os.close(writer)
            async with asyncio.timeout(STAGE_TIMEOUT_SECONDS):
                statuses = await asyncio.gather(packer.wait(), unpacker.wait())
        except BaseException as error:

            async def cleanup() -> None:
                for process in processes:
                    with contextlib.suppress(ProcessLookupError):
                        process.kill()
                    await process.wait()

            await drain_runtime_cleanup(cleanup())
            if isinstance(error, TimeoutError):
                raise SandboxError("node mount staging timed out") from error
            raise
        if any(status != 0 for status in statuses):
            raise SandboxError("node mount staging failed")
        return subpath

    async def copy_back(
        self,
        source: Path,
        volume: str,
        image: str,
        *,
        protected: tuple[Path, ...],
        max_bytes: int,
        cancellation: CancellationContext,
    ) -> None:
        """Extract beside the source, then swap without an await between renames."""
        try:
            source = await asyncio.to_thread(source.resolve, strict=True)
            staging = Path(tempfile.mkdtemp(prefix=f".{source.name}-node-", dir=source.parent))
        except OSError as error:
            raise SandboxError("node copy-back preparation failed") from error
        backup = source.with_name(f".{source.name}-old-{uuid4().hex}")
        processes: list[asyncio.subprocess.Process] = []
        try:
            packer = await asyncio.create_subprocess_exec(
                *_helper(
                    socket=self._node,
                    mounts=(f"type=volume,src={volume},dst=/seed,readonly",),
                    image=image,
                    capabilities=("DAC_READ_SEARCH",),
                ),
                "--acls",
                "--numeric-owner",
                "-cf",
                "-",
                "-C",
                "/seed",
                ".",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=_ENV,
            )
            processes.append(packer)
            unpacker = await asyncio.create_subprocess_exec(
                *_helper(
                    socket=self._local,
                    mounts=(f"type=bind,src={staging},dst=/w",),
                    image=image,
                    capabilities=("CHOWN", "FOWNER", "DAC_OVERRIDE"),
                ),
                "--acls",
                "-xpf",
                "-",
                "--numeric-owner",
                "-C",
                "/w",
                *(f"--exclude=./{path}" for path in protected),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                env=_ENV,
            )
            processes.append(unpacker)
            assert packer.stdout is not None and unpacker.stdin is not None
            async with asyncio.timeout(COPY_BACK_TIMEOUT_SECONDS):
                size = 0
                while chunk := await packer.stdout.read(65536):
                    size += len(chunk)
                    if size > max_bytes:
                        raise SandboxError("node copy-back byte limit exceeded")
                    unpacker.stdin.write(chunk)
                    await unpacker.stdin.drain()
                unpacker.stdin.close()
                statuses = await asyncio.gather(packer.wait(), unpacker.wait())
                if any(status != 0 for status in statuses):
                    raise SandboxError("node copy-back failed")
            # The overlaid metadata belongs to its own mount, never the outer volume.
            if protected:
                await self._local_helper(
                    image,
                    (
                        f"type=bind,src={source},dst=/original,readonly",
                        f"type=bind,src={staging},dst=/w",
                    ),
                    'cd /original && cp -a --parents -- "$@" /w',
                    *(str(p) for p in protected),
                )
            if cancellation.requested:
                raise SandboxError("node copy-back cancelled")
            replacement = staging if source.is_dir() else staging / source.name
            source.rename(backup)
            try:
                replacement.rename(source)
            except BaseException:
                backup.rename(source)
                raise
        except (OSError, TimeoutError) as error:
            raise SandboxError("node copy-back failed or timed out") from error
        finally:

            async def cleanup() -> None:
                for process in processes:
                    if process.returncode is None:
                        with contextlib.suppress(ProcessLookupError):
                            process.kill()
                    await process.wait()
                for path in (staging, backup):
                    if path.is_dir():
                        await self._local_helper(
                            image, (f"type=bind,src={path},dst=/w",), "find /w -mindepth 1 -delete"
                        )
                        path.rmdir()
                    elif path.exists():
                        path.unlink()

            try:
                await drain_runtime_cleanup(cleanup())
            except OSError as error:
                raise SandboxError("node copy-back cleanup failed") from error

    async def _local_helper(
        self,
        image: str,
        mounts: tuple[str, ...],
        script: str,
        *args: str,
    ) -> None:
        argv = _helper(
            socket=self._local,
            mounts=mounts,
            image=image,
            capabilities=("CHOWN", "FOWNER", "DAC_OVERRIDE"),
            entrypoint="sh",
        )
        await _docker(
            self._local,
            *argv[3:],
            "-c",
            script,
            "sh",
            *args,
            timeout_seconds=COPY_BACK_TIMEOUT_SECONDS,
        )

    async def write(self, volume: str, name: str, content: bytes, image: str) -> None:
        """Put one world-readable file the executor itself owns into a volume."""
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o444
            tar.addfile(info, io.BytesIO(content))
        argv = _helper(
            socket=self._node,
            mounts=(f"type=volume,src={volume},dst=/seed",),
            image=image,
            capabilities=("CHOWN", "FOWNER", "DAC_OVERRIDE"),
        )
        await _docker(self._node, *argv[3:], "-xpf", "-", "-C", "/seed", stdin=archive.getvalue())

    async def remove_owned(self, labels: dict[str, str]) -> None:
        found = await _docker(
            self._node,
            "volume",
            "ls",
            "--quiet",
            "--filter",
            f"label={STAGE_LABEL}=1",
            *(f"--filter=label={key}={value}" for key, value in sorted(labels.items())),
        )
        for volume in found.split():
            await self.remove(volume, labels)

    async def remove(self, volume: str, labels: dict[str, str]) -> None:
        found = await _docker(
            self._node,
            "volume",
            "ls",
            "--quiet",
            "--filter",
            f"name={volume}",
            "--filter",
            f"label={STAGE_LABEL}=1",
            *(f"--filter=label={key}={value}" for key, value in sorted(labels.items())),
        )
        if volume in found.split():
            await _docker(self._node, "volume", "rm", "--force", volume)


class NodeDockerRuntime(RootlessDockerRuntime):
    """Runs a sandbox on a node, with mounts staged into owned node volumes."""

    def __init__(
        self,
        node_id: str,
        socket: Path,
        local_socket: Path,
        *,
        journal: ExecutionJournal | None = None,
        max_copy_back_bytes: int = DEFAULT_MAX_COPY_BACK_BYTES,
        writable_worktree_only: bool = False,
    ) -> None:
        super().__init__(socket, journal=journal)
        self.node_id = node_id
        self._max_copy_back_bytes = max_copy_back_bytes
        self._writable_worktree_only = writable_worktree_only
        self._volumes = NodeVolumes(local_socket, socket)
        self._staged: dict[str, dict[int, tuple[str, str | None]]] = {}

    async def run(
        self, envelope: ProcessEnvelope, cancellation: CancellationContext
    ) -> CodexProcessResult:
        name = envelope.container_name
        if self._writable_worktree_only and any(
            mount.mode is MountMode.READ_WRITE
            and (mount.target != Path("/workspace") or mount.purpose != "task-worktree")
            for mount in envelope.sandbox.mounts
        ):
            raise SandboxError("node coding permits only the task worktree to be writable")
        try:
            staged: dict[int, tuple[str, str | None]] = {}
            for index, mount in enumerate(envelope.sandbox.mounts):
                volume = f"{name}-m{index}"
                await self._volumes.create(volume, envelope.ownership_labels)
                staged[index] = (
                    volume,
                    await self._volumes.copy(mount.source, volume, envelope.sandbox.image),
                )
            self._staged[name] = staged
            result = await super().run(envelope, cancellation)
            if cancellation.requested:
                raise SandboxError("node copy-back cancelled")
            for index, mount in enumerate(envelope.sandbox.mounts):
                if mount.mode is MountMode.READ_WRITE:
                    protected = tuple(
                        other.target.relative_to(mount.target)
                        for other in envelope.sandbox.mounts
                        if mount.target in other.target.parents
                    )
                    await self._volumes.copy_back(
                        mount.source,
                        staged[index][0],
                        envelope.sandbox.image,
                        protected=protected,
                        max_bytes=self._max_copy_back_bytes,
                        cancellation=cancellation,
                    )
            return result
        finally:
            self._staged.pop(name, None)
            await self._remove_volumes(envelope)

    async def cleanup(self, envelope: ProcessEnvelope) -> None:
        await super().cleanup(envelope)
        await self._remove_volumes(envelope)

    def _run_argv(self, name: str, envelope: ProcessEnvelope) -> tuple[str, ...]:
        return docker_run_argv(self._socket, name, envelope, volumes=self._staged[name])

    async def _remove_volumes(self, envelope: ProcessEnvelope) -> None:
        for index in range(len(envelope.sandbox.mounts)):
            await self._volumes.remove(
                f"{envelope.container_name}-m{index}", envelope.ownership_labels
            )


class NodeProxyServiceManager(ProxyServiceManager):
    """The controlled proxy runs beside its sandbox, its policy copied into a node volume."""

    def __init__(
        self,
        socket: Path,
        local_socket: Path,
        runtime_root: Path,
        proxy_image: str,
        *,
        helper_image: str,
    ) -> None:
        super().__init__(socket, runtime_root, proxy_image)
        self._volumes = NodeVolumes(local_socket, socket)
        # Any image both hosts hold that has tar; the sandbox image does.
        self._helper_image = helper_image

    def _policy_mount(self, name: str, policy_path: Path) -> str:
        return (
            f"type=volume,src={name}-policy,dst={POLICY_CONTAINER_PATH},"
            f"volume-subpath={policy_path.name},readonly"
        )

    def _policy_mount_matches(self, mount: dict[str, Any], name: str, policy_path: Path) -> bool:
        return mount.get("Type") == "volume" and mount.get("Name") == f"{name}-policy"

    async def _stage_policy(self, name: str, policy_path: Path, labels: dict[str, str]) -> None:
        try:
            await self._volumes.create(f"{name}-policy", labels)
            await self._volumes.write(
                f"{name}-policy",
                policy_path.name,
                await asyncio.to_thread(policy_path.read_bytes),
                self._helper_image,
            )
        except SandboxError as error:
            raise ProxyServiceError("proxy policy staging on the node failed") from error

    async def _remove_owned_sandbox(
        self,
        task_id: UUID | None,
        run_id: UUID | None,
        step_id: UUID | None,
        lease_generation: int,
        *,
        turn_binding: TurnProcessBinding | None = None,
    ) -> None:
        await super()._remove_owned_sandbox(
            task_id,
            run_id,
            step_id,
            lease_generation,
            turn_binding=turn_binding,
        )
        try:
            await self._volumes.remove_owned(
                _sandbox_ownership_labels(
                    task_id,
                    run_id,
                    step_id,
                    lease_generation,
                    turn_binding=turn_binding,
                )
            )
        except SandboxError as error:
            raise ProxyServiceError("sandbox volume removal on the node failed") from error

    async def _remove_owned_container(
        self,
        name: str,
        task_id: UUID | None,
        run_id: UUID | None,
        step_id: UUID | None,
        lease_generation: int,
        *,
        turn_binding: TurnProcessBinding | None = None,
    ) -> None:
        await super()._remove_owned_container(
            name, task_id, run_id, step_id, lease_generation, turn_binding=turn_binding
        )
        try:
            await self._volumes.remove(
                f"{name}-policy",
                _ownership_labels(
                    task_id, run_id, step_id, lease_generation, turn_binding=turn_binding
                ),
            )
        except SandboxError as error:
            raise ProxyServiceError("proxy policy volume removal failed") from error
