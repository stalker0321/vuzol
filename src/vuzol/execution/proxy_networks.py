"""Per-task Docker network lifecycle for controlled HTTPS proxy egress.

This module creates, validates, and cleans up exactly two bridge networks
per execution lease:

- an internal (sandbox-facing) network with --internal
- a non-internal egress network for the proxy

Only the production implementation is responsible for Docker resource
ownership and safety. No containers are started here.

See STEP_08_PROXY_EGRESS_DESIGN.md for topology.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class ProxyNetworkLease:
    """Immutable identity for a pair of per-task proxy networks.

    internal_name: the sandbox-only network (Internal: true)
    egress_name: the proxy-to-egress network (Internal: false)
    """

    internal_name: str
    egress_name: str
    task_id: UUID
    run_id: UUID
    step_id: UUID
    lease_generation: int


class ProxyNetworkError(RuntimeError):
    """A Docker network operation for proxy egress failed closed."""


class ProxyNetworkManager:
    """Owns creation, validation, and exact cleanup of proxy networks.

    All operations are performed against the configured rootless Docker
    socket using explicit --host and a minimal environment.
    """

    def __init__(self, socket: Path) -> None:
        if not socket.is_absolute():
            raise ProxyNetworkError("Docker socket path must be absolute")
        if socket in (Path("/var/run/docker.sock"), Path("/run/docker.sock")):
            raise ProxyNetworkError("rootful Docker socket is prohibited")
        self._socket = socket

    async def create(
        self,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
    ) -> ProxyNetworkLease:
        """Create and validate both networks for the lease.

        Returns a lease only after both networks pass full inspection.
        Creation order: internal then egress.
        On partial failure only owned resources from this call are removed
        after ownership/attachment verification; pre-existing resources are
        never touched.
        """
        if lease_generation < 1:
            raise ProxyNetworkError("lease_generation must be >= 1")
        internal_name = _make_network_name(task_id, run_id, step_id, lease_generation, "internal")
        egress_name = _make_network_name(task_id, run_id, step_id, lease_generation, "egress")

        base_labels: dict[str, str] = {
            "vuzol.managed": "true",
            "vuzol.resource": "proxy-network",
            "vuzol.task_id": str(task_id),
            "vuzol.run_id": str(run_id),
            "vuzol.step_id": str(step_id),
            "vuzol.lease_generation": str(lease_generation),
        }

        # Strict rejection: any pre-existing name collides; no adoption.
        if await self._network_exists(internal_name):
            raise ProxyNetworkError(f"network name collision for internal network {internal_name}")
        if await self._network_exists(egress_name):
            raise ProxyNetworkError(f"network name collision for egress network {egress_name}")

        created: list[tuple[str, str]] = []
        try:
            # 1. create internal
            await self._create_bridge_network(internal_name, internal=True, base_labels=base_labels)
            created.append(("internal", internal_name))
            internal_data = await self._inspect_network(internal_name)
            internal_labels = {**base_labels, "vuzol.network_role": "internal"}
            self._validate_internal_network(internal_data, internal_name, internal_labels)

            # 2. create egress
            await self._create_bridge_network(egress_name, internal=False, base_labels=base_labels)
            created.append(("egress", egress_name))
            egress_data = await self._inspect_network(egress_name)
            egress_labels = {**base_labels, "vuzol.network_role": "egress"}
            self._validate_egress_network(egress_data, egress_name, egress_labels)

            return ProxyNetworkLease(
                internal_name=internal_name,
                egress_name=egress_name,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                lease_generation=lease_generation,
            )
        except Exception as primary_err:
            # Partial rollback only for networks created in this invocation.
            # Re-raise combined if rollback itself fails.
            for role, name in reversed(created):
                try:
                    await self._rollback_owned(
                        name,
                        task_id,
                        run_id,
                        step_id,
                        lease_generation,
                        role,
                    )
                except Exception:
                    combined = ProxyNetworkError(
                        f"proxy network {role} creation failed and rollback failed for {name}"
                    )
                    raise combined from primary_err
            raise

    async def cleanup(self, lease: ProxyNetworkLease) -> None:
        """Idempotent exact cleanup of the lease's networks.

        Removes in reverse creation order (egress then internal).
        Validates lease name integrity first (recomputes from identifiers).
        Tolerates exact absence. Inspects ownership and zero attachments before rm.
        Verifies exact disappearance after rm. Fails closed on any Docker error.
        If egress cleanup fails, does not proceed to internal.
        """
        exp_internal = _make_network_name(
            lease.task_id, lease.run_id, lease.step_id, lease.lease_generation, "internal"
        )
        exp_egress = _make_network_name(
            lease.task_id, lease.run_id, lease.step_id, lease.lease_generation, "egress"
        )
        if lease.internal_name != exp_internal or lease.egress_name != exp_egress:
            raise ProxyNetworkError("lease network names are inconsistent with identifiers")

        # egress first
        await self._cleanup_one(
            lease.egress_name,
            lease.task_id,
            lease.run_id,
            lease.step_id,
            lease.lease_generation,
            "egress",
        )
        # then internal
        await self._cleanup_one(
            lease.internal_name,
            lease.task_id,
            lease.run_id,
            lease.step_id,
            lease.lease_generation,
            "internal",
        )

    async def validate_owned(self, lease: ProxyNetworkLease) -> None:
        """Validate exact network identity and labels without mutating resources."""
        expected = (
            (lease.internal_name, "internal"),
            (lease.egress_name, "egress"),
        )
        for name, role in expected:
            recomputed = _make_network_name(
                lease.task_id,
                lease.run_id,
                lease.step_id,
                lease.lease_generation,
                role,
            )
            if name != recomputed:
                raise ProxyNetworkError("lease network names are inconsistent with identifiers")
            if not await self._network_exists(name):
                continue
            data = await self._inspect_network(name)
            if not self._matches_ownership(
                data,
                lease.task_id,
                lease.run_id,
                lease.step_id,
                lease.lease_generation,
                role,
            ):
                raise ProxyNetworkError(f"refusing foreign recovery network {name}")
            if role == "internal" and data.get("Internal") is not True:
                raise ProxyNetworkError(f"recovery internal network {name} is not Internal")
            if role == "egress" and data.get("Internal") is True:
                raise ProxyNetworkError(f"recovery egress network {name} is Internal")

    # --- internal helpers ---

    def _make_labels(self, base: dict[str, str], role: str) -> list[str]:
        """Return deterministic ordered --label flags. Role added once."""
        labels = {**base, "vuzol.network_role": role}
        out: list[str] = []
        for k in sorted(labels.keys()):
            out.extend(["--label", f"{k}={labels[k]}"])
        return out

    async def _create_bridge_network(
        self, name: str, *, internal: bool, base_labels: dict[str, str]
    ) -> None:
        """Pass ONLY subcommand args to _docker. _docker alone adds executable + --host."""
        role = "internal" if internal else "egress"
        label_args = self._make_labels(base_labels, role)
        subargs: list[str] = [
            "network",
            "create",
            "--driver",
            "bridge",
        ]
        subargs.extend(label_args)
        if internal:
            subargs.append("--internal")
        subargs.append(name)
        await self._docker(*subargs)

    async def _network_exists(self, name: str) -> bool:
        """Reliable existence via filtered ls.

        Command failure (e.g. daemon unavailable) raises.
        Empty result = absent.
        Exact name match = present.
        Multiple ambiguous = fail closed.
        """
        out = await self._docker(
            "network",
            "ls",
            "--filter",
            f"name={name}",
            "--format",
            "{{.Name}}",
        )
        lines = [line.strip() for line in out.splitlines() if line.strip()]
        exact = [nm for nm in lines if nm == name]
        if len(exact) > 1:
            raise ProxyNetworkError(f"ambiguous network list result for {name}")
        return len(exact) == 1

    async def _inspect_network(self, name: str) -> dict[str, Any]:
        out = await self._docker("network", "inspect", name, "--format", "{{json .}}")
        try:
            data = json.loads(out)
        except (json.JSONDecodeError, TypeError) as e:
            raise ProxyNetworkError(f"malformed network inspect for {name}") from e
        if not isinstance(data, dict):
            raise ProxyNetworkError(f"malformed network inspect for {name}")
        return data

    def _validate_common(
        self, data: dict[str, Any], expected_name: str, expected_labels: dict[str, str]
    ) -> None:
        if not isinstance(data, dict):
            raise ProxyNetworkError(f"malformed inspect JSON for {expected_name}")
        if data.get("Name") != expected_name:
            raise ProxyNetworkError(f"name mismatch: {data.get('Name')} != {expected_name}")
        if data.get("Driver") != "bridge":
            raise ProxyNetworkError(f"unexpected driver for {expected_name}")
        if data.get("Attachable") is not False:
            raise ProxyNetworkError(f"unexpected Attachable for {expected_name}")
        ingress = data.get("Ingress")
        if ingress is not None and ingress is not False:
            raise ProxyNetworkError(f"unexpected Ingress for {expected_name}")
        if data.get("EnableIPv6") is not False:
            raise ProxyNetworkError(f"unexpected EnableIPv6 for {expected_name}")
        labels = data.get("Labels")
        if not isinstance(labels, dict):
            raise ProxyNetworkError(f"malformed Labels for {expected_name}")
        for k, v in expected_labels.items():
            if labels.get(k) != v:
                raise ProxyNetworkError(f"missing or wrong label {k} on {expected_name}")
        # no unexpected vuzol.* ownership labels
        for k in list(labels.keys()):
            if k.startswith("vuzol.") and k not in expected_labels:
                raise ProxyNetworkError(f"unexpected vuzol label {k} on {expected_name}")
        # zero attached containers/endpoints
        containers = data.get("Containers")
        if containers is None:
            containers = {}
        if not isinstance(containers, dict):
            raise ProxyNetworkError(f"malformed Containers for {expected_name}")
        if containers:
            keys = list(containers.keys())
            raise ProxyNetworkError(f"unexpected containers on {expected_name}: {keys}")
        if expected_name in {"bridge", "host", "none"}:
            raise ProxyNetworkError(f"reserved network name {expected_name}")

    def _validate_internal_network(
        self, data: dict[str, Any], name: str, labels: dict[str, str]
    ) -> None:
        self._validate_common(data, name, labels)
        if data.get("Internal") is not True:
            raise ProxyNetworkError(f"internal network {name} is not Internal")

    def _validate_egress_network(
        self, data: dict[str, Any], name: str, labels: dict[str, str]
    ) -> None:
        self._validate_common(data, name, labels)
        if data.get("Internal") is True:
            raise ProxyNetworkError(f"egress network {name} must not be Internal")

    def _matches_ownership(
        self,
        data: dict[str, Any],
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
        role: str,
    ) -> bool:
        labels = data.get("Labels") or {}
        if not isinstance(labels, dict):
            return False
        return labels == {
            "vuzol.managed": "true",
            "vuzol.resource": "proxy-network",
            "vuzol.network_role": role,
            "vuzol.task_id": str(task_id),
            "vuzol.run_id": str(run_id),
            "vuzol.step_id": str(step_id),
            "vuzol.lease_generation": str(lease_generation),
        }

    async def _rollback_owned(
        self,
        name: str,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
        role: str,
    ) -> None:
        """Inspect, verify exact ownership+role+no attachments, rm, verify absent.
        Raises on any failure to ensure no owned partial remains.
        """
        if not await self._network_exists(name):
            return
        data = await self._inspect_network(name)
        if not self._matches_ownership(data, task_id, run_id, step_id, lease_generation, role):
            raise ProxyNetworkError(f"rollback refused to touch non-owned network {name}")
        containers = data.get("Containers") or {}
        if containers:
            raise ProxyNetworkError(f"rollback refused to remove network {name} with attachments")
        await self._docker("network", "rm", name)
        if await self._network_exists(name):
            raise ProxyNetworkError(f"rollback: network {name} still present after rm")

    async def _cleanup_one(
        self,
        name: str,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
        role: str,
    ) -> None:
        if not await self._network_exists(name):
            return
        data = await self._inspect_network(name)
        if not self._matches_ownership(data, task_id, run_id, step_id, lease_generation, role):
            raise ProxyNetworkError(f"refusing to remove foreign network {name}")
        containers = data.get("Containers") or {}
        if containers:
            raise ProxyNetworkError(f"refusing to remove network {name} with attachments")
        await self._docker("network", "rm", name)
        if await self._network_exists(name):
            raise ProxyNetworkError(f"network {name} still present after rm")

    async def _docker(self, *args: str) -> str:
        """Single boundary for executable, host, socket, minimal env, timeout, reaping.
        Callers MUST pass only subcommand args (e.g. 'network', 'create', ...).
        """
        socket_str = f"unix://{self._socket}"
        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                "--host",
                socket_str,
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={
                    "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                    "HOME": "/nonexistent",
                },
            )
        except Exception as e:
            raise ProxyNetworkError("rootless Docker network operation failed") from e

        try:
            stdout, _stderr = await asyncio.wait_for(process.communicate(), timeout=30.0)
        except TimeoutError:
            with contextlib.suppress(Exception):
                process.kill()
            await process.wait()
            raise ProxyNetworkError("rootless Docker network operation timed out") from None

        if process.returncode != 0:
            # sanitized: no raw stderr, no full command, no env, no socket in message
            raise ProxyNetworkError("rootless Docker network operation failed")
        return stdout.decode("utf-8", "replace")


def _make_network_name(
    task_id: UUID, run_id: UUID, step_id: UUID, lease_generation: int, role: str
) -> str:
    """Deterministic SHA-256 derived name from complete lease identity.

    Includes task_id, run_id, step_id, lease_generation, role.
    Distinguishes internal/egress. Collision resistant short id.
    No user text, paths, or arbitrary content.
    """
    if lease_generation < 1:
        raise ProxyNetworkError("lease_generation must be >= 1")
    if role not in ("internal", "egress"):
        raise ProxyNetworkError(f"invalid network role {role}")
    material = f"{task_id}:{run_id}:{step_id}:{lease_generation}:{role}".encode()
    digest = hashlib.sha256(material).hexdigest()[:12]
    return f"vuzol-{digest}-{role}"
