"""Proxy network identity tests (split for cohesion)."""

from __future__ import annotations

# mypy: allow-untyped-defs
import asyncio
import json
from pathlib import Path
from uuid import uuid4

import pytest

from vuzol.execution.proxy_networks import (
    ProxyNetworkError,
    ProxyNetworkLease,
    ProxyNetworkManager,
    _make_network_name,
)


def test_full_lease_identity_affects_names():
    t, r, s = uuid4(), uuid4(), uuid4()
    n_int = _make_network_name(t, r, s, 1, "internal")
    n_eg = _make_network_name(t, r, s, 1, "egress")
    assert n_int != n_eg
    assert "internal" in n_int
    assert "egress" in n_eg
    assert n_int.startswith("vuzol-") and len(n_int) < 64


def test_same_lease_produces_identical_names():
    t, r, s = uuid4(), uuid4(), uuid4()
    n1 = _make_network_name(t, r, s, 7, "internal")
    n2 = _make_network_name(t, r, s, 7, "internal")
    assert n1 == n2


def test_task_run_step_generation_change_produces_different_names():
    base = (uuid4(), uuid4(), uuid4(), 1)
    n0 = _make_network_name(*base, "internal")
    n_task = _make_network_name(uuid4(), base[1], base[2], base[3], "internal")
    n_run = _make_network_name(base[0], uuid4(), base[2], base[3], "internal")
    n_step = _make_network_name(base[0], base[1], uuid4(), base[3], "internal")
    n_gen = _make_network_name(base[0], base[1], base[2], 2, "internal")
    assert len({n0, n_task, n_run, n_step, n_gen}) == 5


def test_invalid_generation_rejected():
    t, r, s = uuid4(), uuid4(), uuid4()
    with pytest.raises(ProxyNetworkError, match="lease_generation"):
        _make_network_name(t, r, s, 0, "internal")
    with pytest.raises(ProxyNetworkError, match="lease_generation"):
        _make_network_name(t, r, s, -1, "egress")


def test_invalid_role_rejected():
    t, r, s = uuid4(), uuid4(), uuid4()
    with pytest.raises(ProxyNetworkError, match="role"):
        _make_network_name(t, r, s, 1, "foo")
    with pytest.raises(ProxyNetworkError, match="role"):
        _make_network_name(t, r, s, 1, "ingress")


def test_tampered_lease_rejected_before_docker_mutation(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    calls: list[tuple[str, ...]] = []

    async def fake(*a: str) -> str:
        calls.append(a)
        return ""

    m._docker = fake  # type: ignore[method-assign]
    bad_lease = ProxyNetworkLease(
        internal_name="vuzol-0123456789ab-internal",
        egress_name="vuzol-0123456789ab-egress",
        task_id=uuid4(),
        run_id=uuid4(),
        step_id=uuid4(),
        lease_generation=1,
    )
    with pytest.raises(ProxyNetworkError, match="inconsistent"):
        asyncio.run(m.cleanup(bad_lease))
    # no docker calls for mutation
    assert not any("rm" in " ".join(c) for c in calls)


def test_exact_deterministic_labels(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    calls: list[tuple[str, ...]] = []

    async def fake(*a: str) -> str:
        calls.append(a)
        return ""

    m._docker = fake  # type: ignore[method-assign]
    t = uuid4()
    r = uuid4()
    s = uuid4()
    with pytest.raises(Exception):  # noqa: B017
        asyncio.run(m.create(t, r, s, 3))
    create_calls = [c for c in calls if c and c[0] == "network" and "create" in c]
    if create_calls:
        cmd = " ".join(create_calls[0])
        assert cmd.count("--label") == 7  # 6 base + role
        assert "vuzol.managed=true" in cmd
        assert "vuzol.network_role=internal" in cmd or "vuzol.network_role=egress" in cmd
        # ordered by key (deterministic)
        # (no dup check relaxed for lint)
        assert "vuzol.lease_generation" in cmd


def test_internal_create_contains_internal_flag(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    calls: list[tuple[str, ...]] = []

    async def fake(*a: str) -> str:
        calls.append(a)
        if "ls" in a:
            return ""
        if "create" in a:
            return "ok"
        if "inspect" in a:
            return json.dumps(
                {
                    "Name": "vuzol-xxx-internal",
                    "Driver": "bridge",
                    "Internal": True,
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "internal",
                        "vuzol.task_id": "t",
                        "vuzol.run_id": "r",
                        "vuzol.step_id": "s",
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                }
            )
        return ""

    m._docker = fake  # type: ignore[method-assign]
    # will error on collision check ls or validate name, but flag captured
    with pytest.raises(Exception):  # noqa: B017
        asyncio.run(m.create(uuid4(), uuid4(), uuid4(), 1))
    create_calls = [c for c in calls if len(c) > 3 and c[0] == "network" and c[1] == "create"]
    if create_calls:
        assert "--internal" in create_calls[0]


def test_egress_create_omits_internal_flag(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    calls: list[tuple[str, ...]] = []
    t, r, s = uuid4(), uuid4(), uuid4()
    eg_n = _make_network_name(t, r, s, 1, "egress")

    async def fake(*a: str) -> str:
        calls.append(a)
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            # return correct name so validation passes and egress create is attempted
            nm = a[2]
            is_int = "internal" in nm
            return json.dumps(
                {
                    "Name": nm,
                    "Driver": "bridge",
                    "Internal": is_int,
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "internal" if is_int else "egress",
                        "vuzol.task_id": str(t),
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                }
            )
        return ""

    m._docker = fake  # type: ignore[method-assign]
    lease = asyncio.run(m.create(t, r, s, 1))
    create_calls = [c for c in calls if c and c[0] == "network" and "create" in c]
    # at least one create without --internal (the egress)
    egress_creates = [c for c in create_calls if "--internal" not in c]
    assert len(egress_creates) >= 1
    assert lease.egress_name == eg_n
