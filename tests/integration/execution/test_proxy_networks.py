"""Real Docker integration test for per-task proxy networks.

Uses production ProxyNetworkManager with explicit rootless socket.
Direct docker CLI used only for independent postcondition checks and
deliberate collision setup (never for happy-path create/cleanup).
"""

import asyncio
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

from vuzol.execution.proxy_networks import ProxyNetworkError, ProxyNetworkManager

pytestmark = pytest.mark.docker


def _rootless_socket() -> Path:
    """Obtain migrated rootless socket without hard-coding any UID."""
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        return Path(xdg) / "docker.sock"
    return Path(f"/run/user/{os.getuid()}") / "docker.sock"


def _docker_cli(*args: str, timeout: int = 30) -> "subprocess.CompletedProcess[str]":
    """Explicit --host rootless socket; never rely on ambient DOCKER_HOST."""
    sock = _rootless_socket()
    return subprocess.run(
        ["docker", "--host", f"unix://{sock}", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _inspect_json(name: str) -> dict[str, Any]:
    """Return inspect JSON as dict."""
    r = _docker_cli("network", "inspect", name, "--format", "{{json .}}")
    assert r.returncode == 0, f"inspect failed for {name}: {r.stderr}"
    data: dict[str, Any] = json.loads(r.stdout)
    return data


def _exists_via_cli(name: str) -> bool:
    """Check existence via explicit CLI ls."""
    r = _docker_cli("network", "ls", "--filter", f"name={name}", "--format", "{{.Name}}")
    if r.returncode != 0:
        return False
    lines = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
    return any(ln == name for ln in lines)


def test_real_production_network_lifecycle() -> None:
    """Exercise production manager create + cleanup with real rootless daemon."""
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    step_id = uuid.uuid4()
    gen = 42

    sock = _rootless_socket()
    mgr = ProxyNetworkManager(sock)

    lease = None
    try:
        lease = asyncio.run(mgr.create(task_id, run_id, step_id, gen))

        # independent postcondition inspection (CLI only for verify)
        int_data = _inspect_json(lease.internal_name)
        eg_data = _inspect_json(lease.egress_name)

        # complete names
        assert lease.internal_name.startswith("vuzol-")
        assert lease.egress_name.startswith("vuzol-")
        assert lease.internal_name.endswith("-internal")
        assert lease.egress_name.endswith("-egress")

        # full label set
        for d, role in [(int_data, "internal"), (eg_data, "egress")]:
            labs = d["Labels"]  # type: ignore[index]
            assert labs["vuzol.managed"] == "true"  # type: ignore[index]
            assert labs["vuzol.resource"] == "proxy-network"  # type: ignore[index]
            assert labs["vuzol.network_role"] == role  # type: ignore[index]
            assert labs["vuzol.task_id"] == str(task_id)  # type: ignore[index]
            assert labs["vuzol.run_id"] == str(run_id)  # type: ignore[index]
            assert labs["vuzol.step_id"] == str(step_id)  # type: ignore[index]
            assert labs["vuzol.lease_generation"] == str(gen)  # type: ignore[index]

        # Driver, Internal, Attachable, EnableIPv6, zero endpoints
        assert int_data["Driver"] == "bridge"
        assert eg_data["Driver"] == "bridge"
        assert int_data.get("Internal") is True
        assert eg_data.get("Internal") is False
        assert int_data.get("Attachable") is False
        assert eg_data.get("Attachable") is False
        assert int_data.get("EnableIPv6") is False
        assert eg_data.get("EnableIPv6") is False
        assert not int_data.get("Containers")
        assert not eg_data.get("Containers")

        # not reserved names
        assert lease.internal_name not in ("bridge", "host", "none")
        assert lease.egress_name not in ("bridge", "host", "none")

    finally:
        if lease is not None:
            # production cleanup must succeed; failures fail the test
            asyncio.run(mgr.cleanup(lease))

        # independently prove exact absence
        if lease is not None:
            assert not _exists_via_cli(lease.internal_name)
            assert not _exists_via_cli(lease.egress_name)
            # also rc-checked inspect must fail
            ri = _docker_cli("network", "inspect", lease.internal_name, "--format", "{{.Name}}")
            assert ri.returncode != 0
            re = _docker_cli("network", "inspect", lease.egress_name, "--format", "{{.Name}}")
            assert re.returncode != 0


def test_real_collision_prevents_creation_and_leaves_foreign_untouched() -> None:
    """Foreign net with colliding name but mismatched labels.

    Production create fails closed; foreign untouched; no internal created.
    """
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    step_id = uuid.uuid4()
    gen = 77

    sock = _rootless_socket()
    mgr = ProxyNetworkManager(sock)

    # compute what production would use for egress (to collide on name)
    # we must import private only for name computation in test setup
    from vuzol.execution.proxy_networks import _make_network_name

    foreign_egress = _make_network_name(task_id, run_id, step_id, gen, "egress")
    foreign_internal = _make_network_name(task_id, run_id, step_id, gen, "internal")

    # ensure we start clean for our test names (no pre-existing)
    if _exists_via_cli(foreign_egress):
        _docker_cli("network", "rm", foreign_egress)
    if _exists_via_cli(foreign_internal):
        _docker_cli("network", "rm", foreign_internal)

    # setup foreign network with same name as expected egress but wrong labels (no task etc)
    create_res = _docker_cli(
        "network",
        "create",
        "--driver",
        "bridge",
        "--label",
        "vuzol.managed=true",
        "--label",
        "vuzol.resource=proxy-network",
        "--label",
        "vuzol.network_role=egress",
        # deliberately missing or wrong ownership labels
        foreign_egress,
    )
    assert create_res.returncode == 0, f"foreign setup failed: {create_res.stderr}"

    created_foreign = True
    try:
        # production create must fail closed
        with pytest.raises(ProxyNetworkError, match="collision"):
            asyncio.run(mgr.create(task_id, run_id, step_id, gen))

        # foreign remains untouched
        assert _exists_via_cli(foreign_egress)
        # no internal was created by production
        assert not _exists_via_cli(foreign_internal)

    finally:
        # remove only the test's foreign network
        if created_foreign and _exists_via_cli(foreign_egress):
            rm = _docker_cli("network", "rm", foreign_egress)
            # cleanup command failures must fail the test
            assert rm.returncode == 0, f"foreign cleanup rc={rm.returncode} stderr={rm.stderr}"
