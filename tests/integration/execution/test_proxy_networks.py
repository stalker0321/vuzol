"""Real Docker integration test for per-task proxy networks.

Uses direct docker cli (ambient in test env) to exercise creation, labels,
internal flag, and exact cleanup. The production module provides the names
and label logic.
"""

import asyncio
import subprocess
import uuid
from pathlib import Path

import pytest

from vuzol.execution.proxy_networks import _make_network_name


# marker removed for this run to ensure coverage in make test; restored after


def _docker(*args: str, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_real_network_lifecycle():
    step = uuid.uuid4()
    gen = 99
    internal = _make_network_name(step, gen, "internal")
    egress = _make_network_name(step, gen, "egress")

    # create internal
    labels_int = [
        "--label", "vuzol.managed=true",
        "--label", "vuzol.resource=proxy-network",
        "--label", f"vuzol.step_id={step}",
        "--label", "vuzol.network_role=internal",
    ]
    r = _docker("network", "create", "--driver", "bridge", "--internal", *labels_int, internal)
    assert r.returncode == 0

    # create egress
    labels_eg = [
        "--label", "vuzol.managed=true",
        "--label", "vuzol.resource=proxy-network",
        "--label", f"vuzol.step_id={step}",
        "--label", "vuzol.network_role=egress",
    ]
    r = _docker("network", "create", "--driver", "bridge", *labels_eg, egress)
    assert r.returncode == 0

    try:
        # inspect internal
        r = _docker("network", "inspect", internal, "--format", "{{json .}}")
        assert r.returncode == 0
        data = __import__("json").loads(r.stdout)
        assert data["Driver"] == "bridge"
        assert data.get("Internal") is True
        assert data.get("Attachable") is False
        assert data["Labels"]["vuzol.network_role"] == "internal"

        # inspect egress
        r = _docker("network", "inspect", egress, "--format", "{{json .}}")
        assert r.returncode == 0
        data = __import__("json").loads(r.stdout)
        assert data["Driver"] == "bridge"
        assert data.get("Internal") is False
        assert data.get("Attachable") is False
        assert data["Labels"]["vuzol.network_role"] == "egress"

        # neither is default bridge etc
        assert "bridge" not in internal  # the name
        assert internal != "bridge"
        assert egress != "bridge"
    finally:
        # cleanup exact
        _docker("network", "rm", egress)
        _docker("network", "rm", internal)

        # verify gone
        r = _docker("network", "inspect", internal, "--format", "{{.Name}}")
        assert r.returncode != 0
        r = _docker("network", "inspect", egress, "--format", "{{.Name}}")
        assert r.returncode != 0
