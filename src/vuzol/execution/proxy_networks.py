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
import json
import os
from dataclasses import dataclass
from pathlib import Path
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
        On partial failure the first network is removed (if owned) before
        propagating the error.
        """
        internal_name = _make_network_name(step_id, lease_generation, "internal")
        egress_name = _make_network_name(step_id, lease_generation, "egress")

        base_labels = {
            "vuzol.managed": "true",
            "vuzol.resource": "proxy-network",
            "vuzol.task_id": str(task_id),
            "vuzol.run_id": str(run_id),
            "vuzol.step_id": str(step_id),
            "vuzol.lease_generation": str(lease_generation),
        }

        internal_labels = {**base_labels, "vuzol.network_role": "internal"}
        egress_labels = {**base_labels, "vuzol.network_role": "egress"}

        # Check for pre-existing names (strict collision handling)
        for name, role in [(internal_name, "internal"), (egress_name, "egress")]:
            if await self._network_exists(name):
                data = await self._inspect_network(name)
                if not self._matches_ownership(data, task_id, run_id, step_id, lease_generation, role):
                    raise ProxyNetworkError(
                        f"network name collision for {role} network {name} with foreign ownership"
                    )
                # If it matches exactly and has no endpoints we can adopt (idempotent for same lease)
                if data.get("Containers"):
                    raise ProxyNetworkError(
                        f"existing {role} network {name} has unexpected attachments"
                    )
                # For safety in this slice we still validate structure below after create path

        # Create internal first
        try:
            await self._create_bridge_network(internal_name, internal=True, labels=internal_labels)
            internal_data = await self._inspect_network(internal_name)
            self._validate_internal_network(internal_data, internal_name, internal_labels)

            # Create egress
            try:
                await self._create_bridge_network(egress_name, internal=False, labels=egress_labels)
                egress_data = await self._inspect_network(egress_name)
                self._validate_egress_network(egress_data, egress_name, egress_labels)
            except Exception:
                # Rollback internal only if we still own it
                await self._safe_remove_if_owned(internal_name, task_id, run_id, step_id, lease_generation, "internal")
                raise

            return ProxyNetworkLease(
                internal_name=internal_name,
                egress_name=egress_name,
                task_id=task_id,
                run_id=run_id,
                step_id=step_id,
                lease_generation=lease_generation,
            )
        except Exception:
            # Best effort rollback if internal was created but we are here
            await self._safe_remove_if_owned(internal_name, task_id, run_id, step_id, lease_generation, "internal")
            raise

    async def cleanup(self, lease: ProxyNetworkLease) -> None:
        """Idempotent exact cleanup of the lease's networks.

        Removes in reverse creation order (egress then internal).
        Never removes a network that fails ownership or attachment checks.
        """
        for name, role in [
            (lease.egress_name, "egress"),
            (lease.internal_name, "internal"),
        ]:
            if not await self._network_exists(name):
                continue
            data = await self._inspect_network(name)
            if not self._matches_ownership(
                data, lease.task_id, lease.run_id, lease.step_id, lease.lease_generation, role
            ):
                raise ProxyNetworkError(f"refusing to remove foreign network {name}")
            containers = data.get("Containers") or {}
            if containers:
                raise ProxyNetworkError(f"refusing to remove network {name} with attachments")
            await self._docker("network", "rm", name)
            # Verify gone
            if await self._network_exists(name):
                raise ProxyNetworkError(f"network {name} still present after rm")

    # --- internal helpers ---

    def _make_labels(self, base: dict[str, str], role: str) -> list[str]:
        labels = {**base, "vuzol.network_role": role}
        out: list[str] = []
        for k, v in labels.items():
            out.extend(["--label", f"{k}={v}"])
        return out

    async def _create_bridge_network(
        self, name: str, *, internal: bool, labels: dict[str, str]
    ) -> None:
        args = [
            "docker",
            "--host",
            f"unix://{self._socket}",
            "network",
            "create",
            "--driver",
            "bridge",
        ]
        args.extend(self._make_labels(labels, "internal" if internal else "egress"))
        if internal:
            args.append("--internal")
        args.append(name)
        await self._docker(*args)

    async def _network_exists(self, name: str) -> bool:
        try:
            await self._docker("network", "inspect", name, "--format", "{{.Name}}")
            return True
        except ProxyNetworkError:
            return False

    async def _inspect_network(self, name: str) -> dict:
        out = await self._docker("network", "inspect", name, "--format", "{{json .}}")
        try:
            return json.loads(out)
        except json.JSONDecodeError as e:
            raise ProxyNetworkError(f"malformed network inspect for {name}") from e

    def _validate_common(self, data: dict, expected_name: str, expected_labels: dict[str, str]) -> None:
        if data.get("Name") != expected_name:
            raise ProxyNetworkError(f"name mismatch: {data.get('Name')} != {expected_name}")
        if data.get("Driver") != "bridge":
            raise ProxyNetworkError(f"unexpected driver for {expected_name}")
        if data.get("Attachable") is True:
            raise ProxyNetworkError(f"unexpected Attachable for {expected_name}")
        labels = data.get("Labels") or {}
        for k, v in expected_labels.items():
            if labels.get(k) != v:
                raise ProxyNetworkError(f"missing or wrong label {k} on {expected_name}")
        # containers/endpoints must be empty for fresh lease
        containers = data.get("Containers") or {}
        if containers:
            raise ProxyNetworkError(f"unexpected containers on {expected_name}: {list(containers.keys())}")

    def _validate_internal_network(self, data: dict, name: str, labels: dict[str, str]) -> None:
        self._validate_common(data, name, labels)
        if not data.get("Internal"):
            raise ProxyNetworkError(f"internal network {name} is not Internal")
        # IPAM etc left to Docker defaults; no host exposure expected

    def _validate_egress_network(self, data: dict, name: str, labels: dict[str, str]) -> None:
        self._validate_common(data, name, labels)
        if data.get("Internal"):
            raise ProxyNetworkError(f"egress network {name} must not be Internal")

    def _matches_ownership(
        self,
        data: dict,
        task_id: UUID,
        run_id: UUID,
        step_id: UUID,
        lease_generation: int,
        role: str,
    ) -> bool:
        labels = data.get("Labels") or {}
        return (
            labels.get("vuzol.managed") == "true"
            and labels.get("vuzol.resource") == "proxy-network"
            and labels.get("vuzol.network_role") == role
            and labels.get("vuzol.task_id") == str(task_id)
            and labels.get("vuzol.run_id") == str(run_id)
            and labels.get("vuzol.step_id") == str(step_id)
            and labels.get("vuzol.lease_generation") == str(lease_generation)
        )

    async def _safe_remove_if_owned(
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
        try:
            data = await self._inspect_network(name)
            if self._matches_ownership(data, task_id, run_id, step_id, lease_generation, role):
                containers = data.get("Containers") or {}
                if not containers:
                    await self._docker("network", "rm", name)
        except Exception:
            # best effort; do not mask original error
            pass

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
            # Fail closed without leaking raw command/env details
            raise ProxyNetworkError(f"rootless Docker network operation failed: {args[0] if args else 'unknown'}")
        return stdout.decode("utf-8", "replace")


def _make_network_name(step_id: UUID, lease_generation: int, role: str) -> str:
    """Deterministic short name.

    Uses first 12 chars of step_id (trusted) + generation + role.
    Must stay well under Docker name limits.
    """
    short = str(step_id)[:12]
    return f"vuzol-{short}-{lease_generation}-{role}"
