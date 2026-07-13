"""Transactional lifecycle for the per-execution controlled-egress proxy."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_networks import (
    ProxyNetworkLease,
    ProxyNetworkManager,
    _make_network_name,
)

PROXY_ALIAS = "vuzol-proxy"
PROXY_PORT = 8888
POLICY_CONTAINER_PATH = "/etc/vuzol-proxy/policy.json"
STATE_FILE = "execution.json"


class ProxyServiceError(RuntimeError):
    """The proxy service could not prove a safe lifecycle transition."""


@dataclass(frozen=True)
class ProxyServiceLease:
    container_name: str
    networks: ProxyNetworkLease
    task_id: UUID
    run_id: UUID
    step_id: UUID
    lease_generation: int
    policy_hash: str

    @property
    def proxy_url(self) -> str:
        return f"http://{PROXY_ALIAS}:{PROXY_PORT}"


class ProxyServiceManager:
    def __init__(
        self,
        socket: Path,
        runtime_root: Path,
        proxy_image: str,
        *,
        networks: ProxyNetworkManager | None = None,
    ) -> None:
        if not socket.is_absolute() or socket in {
            Path("/run/docker.sock"),
            Path("/var/run/docker.sock"),
        }:
            raise ProxyServiceError("an absolute rootless Docker socket is required")
        if not runtime_root.is_absolute():
            raise ProxyServiceError("proxy runtime root must be absolute")
        digest = (
            proxy_image.rsplit("@sha256:", 1)[1]
            if "@sha256:" in proxy_image
            else proxy_image.removeprefix("sha256:")
        )
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ProxyServiceError("proxy image must be pinned by digest")
        self._socket = socket
        self._runtime_root = runtime_root
        self._proxy_image = proxy_image
        self._networks = networks or ProxyNetworkManager(socket)

    async def create(
        self,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
        targets: tuple[AllowedConnectTarget, ...],
    ) -> ProxyServiceLease:
        if lease_generation < 1 or not targets:
            raise ProxyServiceError("proxy identity and targets must be non-empty")
        name = _make_proxy_name(task_id, run_id, step_id, lease_generation)
        policy = _render_policy(targets)
        policy_hash = hashlib.sha256(policy).hexdigest()
        runtime_dir = self._runtime_directory(task_id, run_id, step_id, lease_generation)
        policy_path = runtime_dir / "policy.json"
        networks: ProxyNetworkLease | None = None
        container_created = False
        try:
            self._write_private_policy(runtime_dir, policy_path, policy)
            self._write_state(
                runtime_dir,
                task_id,
                run_id,
                step_id,
                lease_generation,
                policy_hash,
            )
            networks = await self._networks.create(task_id, run_id, step_id, lease_generation)
            if await self._container_exists(name):
                raise ProxyServiceError(f"proxy container name collision for {name}")
            await self._docker(
                *self._create_argv(
                    name, networks, policy_path, task_id, run_id, step_id, lease_generation
                )
            )
            container_created = True
            await self._docker(
                "network",
                "connect",
                "--alias",
                PROXY_ALIAS,
                networks.internal_name,
                name,
            )
            await self._validate_container(
                name,
                networks,
                policy_path,
                task_id,
                run_id,
                step_id,
                lease_generation,
                running=False,
            )
            await self._docker("start", name)
            await self._wait_ready(name)
            await self._validate_container(
                name,
                networks,
                policy_path,
                task_id,
                run_id,
                step_id,
                lease_generation,
                running=True,
            )
            return ProxyServiceLease(
                container_name=name,
                networks=networks,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                lease_generation=lease_generation,
                policy_hash=policy_hash,
            )
        except BaseException as primary:
            try:
                if container_created:
                    await self._remove_owned_container(
                        name, task_id, run_id, step_id, lease_generation
                    )
                if networks is not None:
                    await self._networks.cleanup(networks)
                self._remove_private_policy(runtime_dir, policy_path)
            except BaseException:
                raise ProxyServiceError(
                    "proxy startup failed and rollback was incomplete"
                ) from primary
            raise primary

    async def cleanup(self, lease: ProxyServiceLease) -> None:
        expected_name = _make_proxy_name(
            lease.task_id,
            lease.run_id,
            lease.step_id,
            lease.lease_generation,
        )
        if lease.container_name != expected_name:
            raise ProxyServiceError("proxy lease container identity is inconsistent")
        runtime_dir = self._runtime_directory(
            lease.task_id, lease.run_id, lease.step_id, lease.lease_generation
        )
        policy_path = runtime_dir / "policy.json"
        await self._remove_owned_container(
            lease.container_name,
            lease.task_id,
            lease.run_id,
            lease.step_id,
            lease.lease_generation,
        )
        await self._networks.cleanup(lease.networks)
        self._remove_private_policy(runtime_dir, policy_path, expected_hash=lease.policy_hash)

    async def reconcile_startup(self) -> int:
        """Remove crash leftovers described by executor-owned durable manifests.

        A manifest is written before the first Docker mutation.  Recovery never
        discovers resources by a name prefix: names are recomputed from the
        fenced identity and every existing container/network must carry the
        complete expected ownership labels before it is removed.
        """
        if not self._runtime_root.exists():
            return 0
        self._validate_runtime_root()
        recovered = 0
        for directory in sorted(self._runtime_root.iterdir()):
            if not directory.is_dir() or directory.is_symlink():
                raise ProxyServiceError("ambiguous entry in proxy runtime root")
            state_path = directory / STATE_FILE
            state = self._read_state(state_path)
            identity = (
                UUID(state["task_id"]),
                UUID(state["run_id"]),
                UUID(state["step_id"]),
                state["lease_generation"],
            )
            if directory != self._runtime_directory(*identity):
                raise ProxyServiceError("proxy recovery manifest path is inconsistent")
            task_id, run_id, step_id, generation = identity
            await self._remove_owned_sandbox(task_id, run_id, step_id, generation)
            await self._remove_owned_container(
                _make_proxy_name(*identity), task_id, run_id, step_id, generation
            )
            networks = ProxyNetworkLease(
                internal_name=_make_network_name(*identity, "internal"),
                egress_name=_make_network_name(*identity, "egress"),
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                lease_generation=generation,
            )
            await self._networks.cleanup(networks)
            self._remove_private_policy(
                directory,
                directory / "policy.json",
                expected_hash=state["policy_hash"],
                state_path=state_path,
            )
            recovered += 1
        return recovered

    async def wait_until_dead(self, lease: ProxyServiceLease) -> None:
        expected_name = _make_proxy_name(
            lease.task_id, lease.run_id, lease.step_id, lease.lease_generation
        )
        if lease.container_name != expected_name:
            raise ProxyServiceError("proxy lease container identity is inconsistent")
        while True:
            if not await self._container_exists(lease.container_name):
                return
            data = await self._inspect_container(lease.container_name)
            config = _dict(data, "Config", lease.container_name)
            if config.get("Labels") != _ownership_labels(
                lease.task_id,
                lease.run_id,
                lease.step_id,
                lease.lease_generation,
            ):
                raise ProxyServiceError("proxy ownership changed while execution was active")
            state = _dict(data, "State", lease.container_name)
            if state.get("Running") is not True:
                return
            await asyncio.sleep(0.25)

    def _runtime_directory(
        self, task_id: UUID, run_id: UUID, step_id: UUID, lease_generation: int
    ) -> Path:
        identity = f"{task_id}:{run_id}:{step_id}:{lease_generation}:proxy-files".encode()
        return self._runtime_root / hashlib.sha256(identity).hexdigest()[:20]

    def _write_private_policy(self, directory: Path, path: Path, policy: bytes) -> None:
        self._runtime_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._validate_runtime_root()
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError as error:
            raise ProxyServiceError("proxy runtime directory collision") from error
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags, 0o444)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(policy)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            with contextlib.suppress(OSError):
                path.unlink()
            with contextlib.suppress(OSError):
                directory.rmdir()
            raise

    def _validate_runtime_root(self) -> None:
        root_stat = self._runtime_root.lstat()
        if (
            stat.S_ISLNK(root_stat.st_mode)
            or not stat.S_ISDIR(root_stat.st_mode)
            or root_stat.st_uid != os.geteuid()
            or stat.S_IMODE(root_stat.st_mode) != 0o700
        ):
            raise ProxyServiceError("proxy runtime root is not a private real directory")

    def _write_state(
        self,
        directory: Path,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
        policy_hash: str,
    ) -> None:
        body = (
            json.dumps(
                {
                    "version": 1,
                    "task_id": str(task_id),
                    "run_id": str(run_id),
                    "step_id": str(step_id),
                    "lease_generation": lease_generation,
                    "policy_hash": policy_hash,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
        path = directory / STATE_FILE
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())

    def _read_state(self, path: Path) -> dict[str, Any]:
        try:
            file_stat = path.lstat()
        except OSError as error:
            raise ProxyServiceError("proxy recovery manifest is unavailable") from error
        if (
            stat.S_ISLNK(file_stat.st_mode)
            or not stat.S_ISREG(file_stat.st_mode)
            or file_stat.st_uid != os.geteuid()
            or stat.S_IMODE(file_stat.st_mode) != 0o600
        ):
            raise ProxyServiceError("proxy recovery manifest is ambiguous")
        try:
            state = json.loads(path.read_bytes())
            expected = {
                "version",
                "task_id",
                "run_id",
                "step_id",
                "lease_generation",
                "policy_hash",
            }
            if (
                not isinstance(state, dict)
                or set(state) != expected
                or state["version"] != 1
                or not isinstance(state["lease_generation"], int)
                or state["lease_generation"] < 1
                or not isinstance(state["policy_hash"], str)
                or len(state["policy_hash"]) != 64
            ):
                raise ValueError
            UUID(state["task_id"])
            UUID(state["run_id"])
            UUID(state["step_id"])
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
            raise ProxyServiceError("proxy recovery manifest is malformed") from error
        return state

    def _remove_private_policy(
        self,
        directory: Path,
        path: Path,
        *,
        expected_hash: str | None = None,
        state_path: Path | None = None,
    ) -> None:
        if path.exists() or path.is_symlink():
            file_stat = path.lstat()
            if (
                stat.S_ISLNK(file_stat.st_mode)
                or not stat.S_ISREG(file_stat.st_mode)
                or file_stat.st_uid != os.geteuid()
                or stat.S_IMODE(file_stat.st_mode) != 0o444
            ):
                raise ProxyServiceError("refusing to remove an ambiguous proxy policy path")
            if expected_hash is not None:
                actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
                if actual_hash != expected_hash:
                    raise ProxyServiceError("refusing to remove a modified proxy policy")
            path.unlink()
        manifest = state_path or directory / STATE_FILE
        if manifest.exists() or manifest.is_symlink():
            self._read_state(manifest)
            manifest.unlink()
        if directory.exists() or directory.is_symlink():
            directory_stat = directory.lstat()
            if (
                stat.S_ISLNK(directory_stat.st_mode)
                or not stat.S_ISDIR(directory_stat.st_mode)
                or directory_stat.st_uid != os.geteuid()
                or stat.S_IMODE(directory_stat.st_mode) != 0o700
            ):
                raise ProxyServiceError("refusing to remove an ambiguous proxy runtime path")
            directory.rmdir()

    async def _remove_owned_sandbox(
        self, task_id: UUID, run_id: UUID, step_id: UUID, lease_generation: int
    ) -> None:
        name = f"vuzol-{str(step_id)[:12]}-{lease_generation}"
        if not await self._container_exists(name):
            return
        data = await self._inspect_container(name)
        config = _dict(data, "Config", name)
        if config.get("Labels") != _sandbox_ownership_labels(
            task_id, run_id, step_id, lease_generation
        ):
            raise ProxyServiceError(f"refusing to remove foreign sandbox container {name}")
        state = _dict(data, "State", name)
        if state.get("Running") is True:
            await self._docker("stop", "--time", "5", name)
        # Sandbox containers use --rm, so a successful stop normally removes
        # the container before recovery can issue an explicit rm.
        if await self._container_exists(name):
            await self._docker("rm", "-f", name)
        if await self._container_exists(name):
            raise ProxyServiceError(f"sandbox container {name} remains after recovery")

    def _create_argv(
        self,
        name: str,
        networks: ProxyNetworkLease,
        policy_path: Path,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
    ) -> tuple[str, ...]:
        labels = _ownership_labels(task_id, run_id, step_id, lease_generation)
        argv = [
            "create",
            "--name",
            name,
            "--pull",
            "never",
            "--network",
            networks.egress_name,
            "--read-only",
            "--user",
            "10002:10002",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--memory",
            "67108864",
            "--memory-swap",
            "67108864",
            "--cpus",
            "0.25",
            "--pids-limit",
            "32",
            "--ulimit",
            "nofile=1024:1024",
            "--mount",
            f"type=bind,src={policy_path},dst={POLICY_CONTAINER_PATH},readonly",
        ]
        for key in sorted(labels):
            argv.extend(("--label", f"{key}={labels[key]}"))
        argv.append(self._proxy_image)
        return tuple(argv)

    async def _validate_container(
        self,
        name: str,
        networks: ProxyNetworkLease,
        policy_path: Path,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
        *,
        running: bool,
    ) -> None:
        data = await self._inspect_container(name)
        config = _dict(data, "Config", name)
        host = _dict(data, "HostConfig", name)
        state = _dict(data, "State", name)
        network_settings = _dict(data, "NetworkSettings", name)
        attached = _dict(network_settings, "Networks", name)
        if set(attached) != {networks.internal_name, networks.egress_name}:
            raise ProxyServiceError("proxy container network attachments are invalid")
        aliases = attached[networks.internal_name].get("Aliases") or []
        if PROXY_ALIAS not in aliases:
            raise ProxyServiceError("proxy internal network alias is missing")
        if state.get("Running") is not running:
            raise ProxyServiceError("proxy container running state is invalid")
        if config.get("Image") != self._proxy_image or config.get("User") != "10002:10002":
            raise ProxyServiceError("proxy image or identity is invalid")
        labels = config.get("Labels")
        expected_labels = _ownership_labels(task_id, run_id, step_id, lease_generation)
        if not isinstance(labels, dict) or labels != expected_labels:
            raise ProxyServiceError("proxy ownership labels are invalid")
        if host.get("ReadonlyRootfs") is not True or host.get("CapDrop") != ["ALL"]:
            raise ProxyServiceError("proxy filesystem or capabilities are not hardened")
        if "no-new-privileges:true" not in (host.get("SecurityOpt") or []):
            raise ProxyServiceError("proxy no-new-privileges is missing")
        if (
            host.get("Memory") != 67_108_864
            or host.get("MemorySwap") != 67_108_864
            or host.get("NanoCpus") != 250_000_000
            or host.get("PidsLimit") != 32
        ):
            raise ProxyServiceError("proxy resource limits are invalid")
        if config.get("ExposedPorts") or host.get("PortBindings"):
            raise ProxyServiceError("proxy publishes a host port")
        mounts = data.get("Mounts")
        if not isinstance(mounts, list) or len(mounts) != 1:
            raise ProxyServiceError("proxy mounts are invalid")
        mount = mounts[0]
        if (
            not isinstance(mount, dict)
            or mount.get("Type") != "bind"
            or Path(str(mount.get("Source"))) != policy_path
            or mount.get("Destination") != POLICY_CONTAINER_PATH
            or mount.get("RW") is not False
        ):
            raise ProxyServiceError("proxy policy mount is invalid")

    async def _wait_ready(self, name: str) -> None:
        command = (
            "import socket; s=socket.create_connection(('127.0.0.1',8888),1); "
            "s.sendall(b'GET /healthz HTTP/1.1\\r\\nHost: proxy\\r\\n\\r\\n'); "
            "assert b'204 No Content' in s.recv(1024)"
        )
        for _attempt in range(50):
            try:
                await self._docker("exec", name, "python", "-I", "-c", command, timeout_seconds=2)
                return
            except ProxyServiceError:
                await asyncio.sleep(0.1)
        raise ProxyServiceError("proxy readiness timed out")

    async def _container_exists(self, name: str) -> bool:
        output = await self._docker(
            "ps", "-a", "--filter", f"name=^/{name}$", "--format", "{{.Names}}"
        )
        names = [line.strip() for line in output.splitlines() if line.strip()]
        if any(item != name for item in names) or len(names) > 1:
            raise ProxyServiceError("proxy container lookup is ambiguous")
        return names == [name]

    async def _inspect_container(self, name: str) -> dict[str, Any]:
        output = await self._docker("inspect", name, "--format", "{{json .}}")
        try:
            data = json.loads(output)
        except (json.JSONDecodeError, TypeError) as error:
            raise ProxyServiceError("proxy container inspect is malformed") from error
        if not isinstance(data, dict):
            raise ProxyServiceError("proxy container inspect is malformed")
        return data

    async def _remove_owned_container(
        self,
        name: str,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
    ) -> None:
        if not await self._container_exists(name):
            return
        data = await self._inspect_container(name)
        config = _dict(data, "Config", name)
        if config.get("Labels") != _ownership_labels(task_id, run_id, step_id, lease_generation):
            raise ProxyServiceError(f"refusing to remove foreign proxy container {name}")
        state = _dict(data, "State", name)
        if state.get("Running") is True:
            await self._docker("stop", "--time", "5", name)
        await self._docker("rm", name)
        if await self._container_exists(name):
            raise ProxyServiceError(f"proxy container {name} remains after removal")

    async def _docker(self, *args: str, timeout_seconds: float = 30) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "--host",
                f"unix://{self._socket}",
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": "/nonexistent"},
            )
            stdout, _stderr = await asyncio.wait_for(process.communicate(), timeout_seconds)
        except TimeoutError as error:
            with contextlib.suppress(ProcessLookupError, UnboundLocalError):
                process.kill()
                await process.wait()
            raise ProxyServiceError("rootless proxy Docker operation timed out") from error
        except OSError as error:
            raise ProxyServiceError("rootless proxy Docker operation failed") from error
        if process.returncode != 0:
            raise ProxyServiceError("rootless proxy Docker operation failed")
        return stdout.decode("utf-8", "replace")


def _render_policy(targets: tuple[AllowedConnectTarget, ...]) -> bytes:
    unique = sorted({(target.hostname, target.port) for target in targets})
    if not unique:
        raise ProxyServiceError("proxy policy cannot be empty")
    body = {
        "version": 1,
        "targets": [{"hostname": hostname, "port": port} for hostname, port in unique],
        "connect_timeout_seconds": 10,
        "idle_timeout_seconds": 120,
        "tunnel_timeout_seconds": 1800,
        "maximum_bytes_per_direction": 268_435_456,
    }
    return (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _make_proxy_name(task_id: UUID, run_id: UUID, step_id: UUID, lease_generation: int) -> str:
    if lease_generation < 1:
        raise ProxyServiceError("lease_generation must be positive")
    material = f"{task_id}:{run_id}:{step_id}:{lease_generation}:proxy".encode()
    return f"vuzol-{hashlib.sha256(material).hexdigest()[:12]}-proxy"


def _ownership_labels(
    task_id: UUID, run_id: UUID, step_id: UUID, lease_generation: int
) -> dict[str, str]:
    return {
        "vuzol.managed": "true",
        "vuzol.resource": "egress-proxy",
        "vuzol.task_id": str(task_id),
        "vuzol.run_id": str(run_id),
        "vuzol.step_id": str(step_id),
        "vuzol.lease_generation": str(lease_generation),
    }


def _sandbox_ownership_labels(
    task_id: UUID, run_id: UUID, step_id: UUID, lease_generation: int
) -> dict[str, str]:
    return {
        "vuzol.managed": "true",
        "vuzol.resource": "sandbox-container",
        "vuzol.task_id": str(task_id),
        "vuzol.run_id": str(run_id),
        "vuzol.step_id": str(step_id),
        "vuzol.lease_generation": str(lease_generation),
    }


def _dict(parent: dict[str, Any], key: str, name: str) -> dict[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ProxyServiceError(f"proxy container {name} has malformed {key}")
    return value
