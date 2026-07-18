import asyncio
import json
import stat
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_networks import ProxyNetworkLease
from vuzol.execution.proxy_service import (
    POLICY_CONTAINER_PATH,
    PROXY_ALIAS,
    STATE_FILE,
    ProxyRecoveryManifest,
    ProxyServiceError,
    ProxyServiceLease,
    ProxyServiceManager,
    _dict,
    _make_proxy_name,
    _ownership_labels,
    _render_policy,
    _sandbox_ownership_labels,
)

IMAGE = f"vuzol-proxy@sha256:{'a' * 64}"
SOCKET = Path("/run/user/994/docker.sock")

__all__ = [
    "IMAGE",
    "POLICY_CONTAINER_PATH",
    "PROXY_ALIAS",
    "SOCKET",
    "STATE_FILE",
    "UUID",
    "AllowedConnectTarget",
    "Any",
    "FakeNetworks",
    "Path",
    "ProxyNetworkLease",
    "ProxyRecoveryManifest",
    "ProxyServiceError",
    "ProxyServiceLease",
    "ProxyServiceManager",
    "_dict",
    "_identity",
    "_inspect",
    "_make_proxy_name",
    "_manager",
    "_ownership_labels",
    "_render_policy",
    "_sandbox_ownership_labels",
    "_target",
    "asyncio",
    "hashlib_sha256",
    "json",
    "pytest",
    "stat",
    "uuid4",
]


class FakeNetworks:
    def __init__(self) -> None:
        self.created: list[tuple[UUID, UUID, UUID, int]] = []
        self.cleaned: list[ProxyNetworkLease] = []

    async def create(
        self, task_id: UUID, run_id: UUID, step_id: UUID, generation: int
    ) -> ProxyNetworkLease:
        self.created.append((task_id, run_id, step_id, generation))
        return ProxyNetworkLease(
            internal_name="vuzol-internal",
            egress_name="vuzol-egress",
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            lease_generation=generation,
        )

    async def cleanup(self, lease: ProxyNetworkLease) -> None:
        self.cleaned.append(lease)

    async def validate_owned(self, lease: ProxyNetworkLease) -> None:
        del lease


def _target(hostname: str = "api.openai.com") -> AllowedConnectTarget:
    return AllowedConnectTarget(hostname=hostname, port=443, purpose="runtime API")


def _manager(tmp_path: Path, networks: FakeNetworks) -> ProxyServiceManager:
    return ProxyServiceManager(
        SOCKET,
        tmp_path / "proxy-runtime",
        IMAGE,
        networks=networks,  # type: ignore[arg-type]
    )


def _identity() -> tuple[UUID, UUID, UUID, int]:
    return uuid4(), uuid4(), uuid4(), 3


def _inspect(
    name: str,
    policy_path: Path,
    identity: tuple[UUID, UUID, UUID, int],
    *,
    running: bool,
    labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    task_id, run_id, step_id, generation = identity
    return {
        "Name": f"/{name}",
        "Config": {
            "Image": IMAGE,
            "User": "10002:10002",
            "Labels": labels or _ownership_labels(task_id, run_id, step_id, generation),
            "ExposedPorts": None,
        },
        "HostConfig": {
            "ReadonlyRootfs": True,
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges:true"],
            "Memory": 67_108_864,
            "MemorySwap": 67_108_864,
            "NanoCpus": 250_000_000,
            "PidsLimit": 32,
            "PortBindings": {},
        },
        "State": {"Running": running},
        "NetworkSettings": {
            "Networks": {
                "vuzol-egress": {"Aliases": [name]},
                "vuzol-internal": {"Aliases": [name, PROXY_ALIAS]},
            }
        },
        "Mounts": [
            {
                "Type": "bind",
                "Source": str(policy_path),
                "Destination": POLICY_CONTAINER_PATH,
                "RW": False,
            }
        ],
    }


def hashlib_sha256(content: bytes) -> str:
    import hashlib

    return hashlib.sha256(content).hexdigest()
