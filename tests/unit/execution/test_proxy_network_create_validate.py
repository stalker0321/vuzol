"""Proxy network create validate tests (split for cohesion)."""

from __future__ import annotations

# mypy: allow-untyped-defs
import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from vuzol.execution.proxy_networks import (
    ProxyNetworkError,
    ProxyNetworkLease,
    ProxyNetworkManager,
    _make_network_name,
)


def test_existing_internal_collision_prevents_all_creation(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    calls: list[tuple[Any, ...]] = []

    async def fake(*a: str) -> str:
        calls.append(a)
        if a and a[0] == "network" and a[1] == "ls":
            # echo the filtered name back so exact match triggers collision
            for arg in a:
                if arg.startswith("name="):
                    return arg.split("=", 1)[1] + "\n"
            return "present\n"
        return ""

    m._docker = fake  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="collision"):
        asyncio.run(m.create(uuid4(), uuid4(), uuid4(), 1))
    # no create attempted
    assert not any(c and c[0] == "network" and len(c) > 1 and c[1] == "create" for c in calls)


def test_existing_egress_collision_prevents_internal_creation(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    calls: list[tuple[Any, ...]] = []
    ls_count = {"n": 0}

    async def fake(*a: str) -> str:
        calls.append(a)
        if a and a[0] == "network" and a[1] == "ls":
            ls_count["n"] += 1
            for arg in a:
                if arg.startswith("name="):
                    presented = arg.split("=", 1)[1]
                    if ls_count["n"] == 1:
                        return ""  # first (internal) absent
                    return presented + "\n"  # second (egress) collides
            return ""
        return ""

    m._docker = fake  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="collision"):
        asyncio.run(m.create(uuid4(), uuid4(), uuid4(), 1))


def test_successful_production_create_validates_both_inspect_responses(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    # force deterministic names for inspect match
    fixed_int = "vuzol-testint123-internal"
    fixed_eg = "vuzol-testint123-egress"

    def _fixed_name(*a, **k):
        return fixed_int if a[4] == "internal" else fixed_eg

    monkeypatch.setattr("vuzol.execution.proxy_networks._make_network_name", _fixed_name)
    good_int: dict[str, Any] = {
        "Name": fixed_int,
        "Driver": "bridge",
        "Internal": True,
        "Attachable": False,
        "EnableIPv6": False,
        "Ingress": False,
        "Labels": {
            "vuzol.managed": "true",
            "vuzol.resource": "proxy-network",
            "vuzol.network_role": "internal",
            "vuzol.task_id": str(t),
            "vuzol.run_id": str(r),
            "vuzol.step_id": str(s),
            "vuzol.lease_generation": "1",
        },
        "Containers": {},
    }
    good_eg = {
        "Name": fixed_eg,
        "Driver": "bridge",
        "Internal": False,
        "Attachable": False,
        "EnableIPv6": False,
        "Ingress": False,
        "Labels": {**good_int["Labels"], "vuzol.network_role": "egress"},
        "Containers": {},
    }
    seq = {"ls": 0, "create": 0, "inspect": 0}

    async def fake(*a: str) -> str:
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            seq["create"] += 1
            return "created"
        if a and a[0] == "network" and a[1] == "inspect":
            seq["inspect"] += 1
            if seq["inspect"] == 1:
                return json.dumps(good_int)
            return json.dumps(good_eg)
        return ""

    m._docker = fake  # type: ignore[method-assign]
    lease = asyncio.run(m.create(t, r, s, 1))
    assert isinstance(lease, ProxyNetworkLease)
    assert lease.internal_name == fixed_int
    assert lease.egress_name == fixed_eg


def test_malformed_json_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()

    async def bad(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            return "not json {"
        return ""

    m._docker = bad  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="malformed"):
        asyncio.run(m.create(t, r, s, 1))


def test_missing_boolean_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")

    async def miss_bool(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            return json.dumps(
                {
                    "Name": n,
                    "Driver": "bridge",
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "internal",
                        "vuzol.task_id": str(t),
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                    # no "Internal"
                }
            )
        return ""

    m._docker = miss_bool  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError):
        asyncio.run(m.create(t, r, s, 1))


def test_wrong_driver_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")

    async def wrong(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            d = {
                "Name": n,
                "Driver": "overlay",
                "Internal": True,
                "Attachable": False,
                "EnableIPv6": False,
                "Labels": {
                    "vuzol.managed": "true",
                    "vuzol.resource": "proxy-network",
                    "vuzol.network_role": "internal",
                    "vuzol.task_id": str(t),
                    "vuzol.run_id": str(r),
                    "vuzol.step_id": str(s),
                    "vuzol.lease_generation": "1",
                },
                "Containers": {},
            }
            return json.dumps(d)
        return ""

    m._docker = wrong  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="driver"):
        asyncio.run(m.create(t, r, s, 1))


def test_wrong_internal_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")

    async def bad_int(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            d = {
                "Name": n,
                "Driver": "bridge",
                "Internal": False,
                "Attachable": False,
                "EnableIPv6": False,
                "Labels": {
                    "vuzol.managed": "true",
                    "vuzol.resource": "proxy-network",
                    "vuzol.network_role": "internal",
                    "vuzol.task_id": str(t),
                    "vuzol.run_id": str(r),
                    "vuzol.step_id": str(s),
                    "vuzol.lease_generation": "1",
                },
                "Containers": {},
            }
            return json.dumps(d)
        return ""

    m._docker = bad_int  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="Internal"):
        asyncio.run(m.create(t, r, s, 1))


def test_attachable_missing_or_true_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")
    for bad_val in [True, None]:

        async def bad_a(*a, bv=bad_val):
            if a and a[0] == "network" and a[1] == "ls":
                return ""
            if a and a[0] == "network" and a[1] == "create":
                return "ok"
            if a and a[0] == "network" and a[1] == "inspect":
                d = {
                    "Name": n,
                    "Driver": "bridge",
                    "Internal": True,
                    "Attachable": bv,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "internal",
                        "vuzol.task_id": str(t),
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                }
                return json.dumps(d)
            return ""

        m._docker = bad_a  # type: ignore[method-assign]
        with pytest.raises(ProxyNetworkError, match="Attachable"):
            asyncio.run(m.create(t, r, s, 1))


def test_ipv6_enabled_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")

    async def bad6(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            d = {
                "Name": n,
                "Driver": "bridge",
                "Internal": True,
                "Attachable": False,
                "EnableIPv6": True,
                "Labels": {
                    "vuzol.managed": "true",
                    "vuzol.resource": "proxy-network",
                    "vuzol.network_role": "internal",
                    "vuzol.task_id": str(t),
                    "vuzol.run_id": str(r),
                    "vuzol.step_id": str(s),
                    "vuzol.lease_generation": "1",
                },
                "Containers": {},
            }
            return json.dumps(d)
        return ""

    m._docker = bad6  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="IPv6"):
        asyncio.run(m.create(t, r, s, 1))


def test_missing_label_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")

    async def miss_l(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            d = {
                "Name": n,
                "Driver": "bridge",
                "Internal": True,
                "Attachable": False,
                "EnableIPv6": False,
                "Labels": {
                    "vuzol.managed": "true",
                    "vuzol.resource": "proxy-network",
                    "vuzol.network_role": "internal",
                    "vuzol.task_id": str(t),
                    "vuzol.run_id": str(r),
                    "vuzol.step_id": str(s),
                },
                "Containers": {},
            }
            return json.dumps(d)
        return ""

    m._docker = miss_l  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="label"):
        asyncio.run(m.create(t, r, s, 1))


def test_unexpected_vuzol_label_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")

    async def extra(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            labs = {
                "vuzol.managed": "true",
                "vuzol.resource": "proxy-network",
                "vuzol.network_role": "internal",
                "vuzol.task_id": str(t),
                "vuzol.run_id": str(r),
                "vuzol.step_id": str(s),
                "vuzol.lease_generation": "1",
                "vuzol.secret": "oops",  # pragma: allowlist secret
            }
            d = {
                "Name": n,
                "Driver": "bridge",
                "Internal": True,
                "Attachable": False,
                "EnableIPv6": False,
                "Labels": labs,
                "Containers": {},
            }
            return json.dumps(d)
        return ""

    m._docker = extra  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="unexpected vuzol"):
        asyncio.run(m.create(t, r, s, 1))


def test_attached_endpoint_rejected(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    n = _make_network_name(t, r, s, 1, "internal")

    async def attached(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            d = {
                "Name": n,
                "Driver": "bridge",
                "Internal": True,
                "Attachable": False,
                "EnableIPv6": False,
                "Labels": {
                    "vuzol.managed": "true",
                    "vuzol.resource": "proxy-network",
                    "vuzol.network_role": "internal",
                    "vuzol.task_id": str(t),
                    "vuzol.run_id": str(r),
                    "vuzol.step_id": str(s),
                    "vuzol.lease_generation": "1",
                },
                "Containers": {"c1": {}},
            }
            return json.dumps(d)
        return ""

    m._docker = attached  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="containers"):
        asyncio.run(m.create(t, r, s, 1))
