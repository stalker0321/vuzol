"""Sandboxes on another machine's rootless Docker, reached through a forwarded socket.

Owner 2026-11-08: "все сидят тут, работают где надо". The control plane stays on
this host; a node is only Docker. Mount sources do not exist there, so each one is
copied into a node volume before the run and the volume replaces the bind mount.
The files belong to sandbox identities, so local Docker reads them and root in the
node's user namespace unpacks them, which keeps numeric owners. Nodes take
read-only mounts only: nothing has to come back.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import tarfile
from pathlib import Path
from typing import Any
from uuid import UUID

from vuzol.execution.domain import MountMode, ProcessEnvelope, TurnProcessBinding
from vuzol.execution.journal import ExecutionJournal
from vuzol.execution.proxy_service import (
    POLICY_CONTAINER_PATH,
    ProxyServiceError,
    ProxyServiceManager,
    _ownership_labels,
)
from vuzol.execution.sandbox import RootlessDockerRuntime, SandboxError, docker_run_argv
from vuzol.providers.ports import CodexProcessResult
from vuzol.workflows.ports import CancellationContext

STAGE_LABEL = "vuzol.node-stage"
STAGE_TIMEOUT_SECONDS = 120
_ENV = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent"}


async def _docker(socket: Path, *args: str, stdin: bytes | None = None) -> str:
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
        async with asyncio.timeout(STAGE_TIMEOUT_SECONDS):
            stdout, _stderr = await process.communicate(stdin)
    except TimeoutError as error:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        raise SandboxError(f"node Docker operation timed out: {args[0]}") from error
    if process.returncode != 0:
        raise SandboxError(f"node Docker operation failed: {args[0]}")
    return stdout.decode("utf-8", "replace")


def _helper(
    *, socket: Path, mounts: tuple[str, ...], image: str, capabilities: tuple[str, ...]
) -> list[str]:
    argv = ["docker", "--host", f"unix://{socket}", "run", "--rm", "-i", "--pull", "never"]
    argv += ["--network", "none", "--user", "0:0", "--read-only", "--cap-drop", "ALL"]
    for capability in capabilities:
        argv += ["--cap-add", capability]
    argv += ["--security-opt", "no-new-privileges:true", "--memory", "268435456"]
    for mount in mounts:
        argv += ["--mount", mount]
    return [*argv, "--entrypoint", "tar", image]


def _packing(source: Path) -> tuple[tuple[str], list[str], str | None]:
    source = source.resolve(strict=True)
    if source.is_dir():
        return (f"type=bind,src={source},dst=/s,readonly",), ["-C", "/s", "-cf", "-", "."], None
    return (
        (f"type=bind,src={source},dst=/s/{source.name},readonly",),
        ["-C", "/s", "-cf", "-", source.name],
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
            unpacker = await asyncio.create_subprocess_exec(
                *_helper(
                    socket=self._node,
                    mounts=(f"type=volume,src={volume},dst=/seed",),
                    image=image,
                    capabilities=("CHOWN", "FOWNER", "DAC_OVERRIDE"),
                ),
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
        finally:
            os.close(reader)
            os.close(writer)
        try:
            async with asyncio.timeout(STAGE_TIMEOUT_SECONDS):
                statuses = await asyncio.gather(packer.wait(), unpacker.wait())
        except TimeoutError as error:
            for process in (packer, unpacker):
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
            raise SandboxError("node mount staging timed out") from error
        if any(status != 0 for status in statuses):
            raise SandboxError("node mount staging failed")
        return subpath

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
    """Runs a sandbox on a node, its read-only mounts staged into node volumes."""

    def __init__(
        self,
        node_id: str,
        socket: Path,
        local_socket: Path,
        *,
        journal: ExecutionJournal | None = None,
    ) -> None:
        super().__init__(socket, journal=journal)
        self.node_id = node_id
        self._volumes = NodeVolumes(local_socket, socket)
        self._staged: dict[str, dict[int, tuple[str, str | None]]] = {}

    async def run(
        self, envelope: ProcessEnvelope, cancellation: CancellationContext
    ) -> CodexProcessResult:
        name = envelope.container_name
        if any(mount.mode is not MountMode.READ_ONLY for mount in envelope.sandbox.mounts):
            raise SandboxError("node sandboxes take read-only mounts only")
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
            return await super().run(envelope, cancellation)
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
