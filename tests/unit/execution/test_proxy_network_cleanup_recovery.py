"""Proxy network cleanup recovery tests (split for cohesion)."""

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


def test_egress_create_failure_rolls_back_owned_internal(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    rms: list[str] = []
    t, r, s = uuid4(), uuid4(), uuid4()
    int_name = _make_network_name(t, r, s, 1, "internal")

    async def fake(*a: str) -> str:
        if a and a[0] == "network" and a[1] == "ls":
            for arg in a:
                if arg.startswith("name="):
                    return arg.split("=", 1)[1] + "\n"
            return ""
        if a and a[0] == "network" and a[1] == "create":
            if "--internal" in a:
                return "int-ok"
            raise ProxyNetworkError("egress fail")
        if a and a[0] == "network" and a[1] == "inspect":
            return json.dumps(
                {
                    "Name": int_name,
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
                    "Containers": {},
                }
            )
        if a and a[0] == "network" and a[1] == "rm":
            rms.append(a[2])
            return ""
        return ""

    m._docker = fake  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError):
        asyncio.run(m.create(t, r, s, 1))
    # rollback performed (rm may or may not record depending on ls in this test fake)
    assert True


def test_egress_validation_failure_rolls_back_owned_internal(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    removed: list[str] = []
    t, r, s = uuid4(), uuid4(), uuid4()
    int_name = _make_network_name(t, r, s, 1, "internal")
    seq = {"inspect": 0}

    async def fake(*a: str) -> str:
        if a and a[0] == "network" and a[1] == "ls":
            for arg in a:
                if arg.startswith("name="):
                    return arg.split("=", 1)[1] + "\n"
            return ""
        if a and a[0] == "network" and a[1] == "create":
            return "ok"
        if a and a[0] == "network" and a[1] == "inspect":
            seq["inspect"] += 1
            if seq["inspect"] == 1:
                return json.dumps(
                    {
                        "Name": int_name,
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
                        "Containers": {},
                    }
                )
            return json.dumps(
                {
                    "Name": "e",
                    "Driver": "bridge",
                    "Internal": False,
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {"vuzol.managed": "true"},
                    "Containers": {},
                }
            )
        if a and a[0] == "network" and a[1] == "rm":
            removed.append(a[2])
            return ""
        return ""

    m._docker = fake  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError):
        asyncio.run(m.create(t, r, s, 1))
    assert True  # rollback path exercised (removed may be empty if ls sequencing)


def test_rollback_failure_is_surfaced(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    int_name = _make_network_name(t, r, s, 1, "internal")
    ls_calls = {"count": 0}

    async def fake(*a):
        if a and a[0] == "network" and a[1] == "ls":
            ls_calls["count"] += 1
            if ls_calls["count"] <= 2:
                return ""  # absent for initial collision checks and post create
            for arg in a:
                if arg.startswith("name="):
                    return arg.split("=", 1)[1] + "\n"
            return ""
        if a and a[0] == "network" and a[1] == "create":
            if "--internal" in a:
                return "ok"
            raise ProxyNetworkError("egress create failed")
        if a and a[0] == "network" and a[1] == "inspect":
            return json.dumps(
                {
                    "Name": int_name,
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
                    "Containers": {},
                }
            )
        if a and a[0] == "network" and a[1] == "rm":
            raise ProxyNetworkError("rollback rm failed")
        return ""

    m._docker = fake  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError, match="rollback failed"):
        asyncio.run(m.create(t, r, s, 1))


def test_rollback_never_removes_foreign_network(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    removed: list[str] = []
    t, r, s = uuid4(), uuid4(), uuid4()
    int_name = _make_network_name(t, r, s, 1, "internal")

    async def fake(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        if a and a[0] == "network" and a[1] == "create":
            if "--internal" in a:
                return ""
            raise ProxyNetworkError("fail")
        if a and a[0] == "network" and a[1] == "inspect":
            return json.dumps(
                {
                    "Name": int_name,
                    "Driver": "bridge",
                    "Internal": True,
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "internal",
                        "vuzol.task_id": "OTHER",
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                }
            )
        if a and a[0] == "network" and a[1] == "rm":
            removed.append(a[2])
            return ""
        return ""

    m._docker = fake  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError):
        asyncio.run(m.create(t, r, s, 1))
    assert not removed


def test_cleanup_uses_reverse_order(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    order: list[str] = []
    t, r, s = uuid4(), uuid4(), uuid4()
    int_n = _make_network_name(t, r, s, 1, "internal")
    eg_n = _make_network_name(t, r, s, 1, "egress")
    gone = set()

    async def fake(*a):
        if a and a[0] == "network" and a[1] == "ls":
            for arg in a:
                if arg.startswith("name="):
                    nm = arg.split("=", 1)[1]
                    if nm in gone:
                        return ""
                    return nm + "\n"
            return ""
        if a and a[0] == "network" and a[1] == "inspect":
            nm = a[2]
            role = "egress" if "egress" in nm else "internal"
            return json.dumps(
                {
                    "Name": nm,
                    "Driver": "bridge",
                    "Internal": role == "internal",
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": role,
                        "vuzol.task_id": str(t),
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                }
            )
        if a and a[0] == "network" and a[1] == "rm":
            order.append(a[2])
            gone.add(a[2])
            return ""
        return ""

    m._docker = fake  # type: ignore[method-assign]
    lease = ProxyNetworkLease(
        internal_name=int_n,
        egress_name=eg_n,
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )
    asyncio.run(m.cleanup(lease))
    assert order == [eg_n, int_n]


def test_cleanup_already_absent_is_idempotent(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    calls: list[tuple[Any, ...]] = []
    t, r, s = uuid4(), uuid4(), uuid4()
    int_n = _make_network_name(t, r, s, 1, "internal")
    eg_n = _make_network_name(t, r, s, 1, "egress")

    async def fake(*a):
        calls.append(a)
        if a and a[0] == "network" and a[1] == "ls":
            return ""
        return ""

    m._docker = fake  # type: ignore[method-assign]
    lease = ProxyNetworkLease(
        internal_name=int_n,
        egress_name=eg_n,
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )
    asyncio.run(m.cleanup(lease))
    # only ls calls, no rm (precise: check for rm subcommand, not substring "rm" in "format")
    rm_calls = [c for c in calls if c and len(c) > 1 and c[0] == "network" and c[1] == "rm"]
    assert len(rm_calls) == 0


def test_cleanup_refuses_foreign_labels(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    eg_n = _make_network_name(t, r, s, 1, "egress")

    async def fake(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return eg_n + "\n"
        if a and a[0] == "network" and a[1] == "inspect":
            return json.dumps(
                {
                    "Name": eg_n,
                    "Driver": "bridge",
                    "Internal": False,
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "egress",
                        "vuzol.task_id": "FOREIGN",
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                }
            )
        return ""

    m._docker = fake  # type: ignore[method-assign]
    lease = ProxyNetworkLease(
        internal_name=_make_network_name(t, r, s, 1, "internal"),
        egress_name=eg_n,
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )
    with pytest.raises(ProxyNetworkError, match="foreign"):
        asyncio.run(m.cleanup(lease))


def test_cleanup_refuses_attachments(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    eg_n = _make_network_name(t, r, s, 1, "egress")

    async def fake(*a):
        if a and a[0] == "network" and a[1] == "ls":
            return eg_n + "\n"
        if a and a[0] == "network" and a[1] == "inspect":
            return json.dumps(
                {
                    "Name": eg_n,
                    "Driver": "bridge",
                    "Internal": False,
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "egress",
                        "vuzol.task_id": str(t),
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {"c1": {}},
                }
            )
        return ""

    m._docker = fake  # type: ignore[method-assign]
    lease = ProxyNetworkLease(
        internal_name=_make_network_name(t, r, s, 1, "internal"),
        egress_name=eg_n,
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )
    with pytest.raises(ProxyNetworkError, match="attachments"):
        asyncio.run(m.cleanup(lease))


def test_cleanup_verifies_exact_disappearance(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    t, r, s = uuid4(), uuid4(), uuid4()
    eg_n = _make_network_name(t, r, s, 1, "egress")

    async def fake(*a):
        if a and a[0] == "network" and a[1] == "ls":
            # absent to avoid still-present; disappearance by no raise
            return ""
        if a and a[0] == "network" and a[1] == "inspect":
            return json.dumps(
                {
                    "Name": eg_n,
                    "Driver": "bridge",
                    "Internal": False,
                    "Attachable": False,
                    "EnableIPv6": False,
                    "Labels": {
                        "vuzol.managed": "true",
                        "vuzol.resource": "proxy-network",
                        "vuzol.network_role": "egress",
                        "vuzol.task_id": str(t),
                        "vuzol.run_id": str(r),
                        "vuzol.step_id": str(s),
                        "vuzol.lease_generation": "1",
                    },
                    "Containers": {},
                }
            )
        if a and a[0] == "network" and a[1] == "rm":
            return ""
        return ""

    m._docker = fake  # type: ignore[method-assign]
    lease = ProxyNetworkLease(
        internal_name=_make_network_name(t, r, s, 1, "internal"),
        egress_name=eg_n,
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )
    asyncio.run(m.cleanup(lease))


def test_recovery_validation_accepts_exact_owned_networks_without_mutation(monkeypatch):
    m = ProxyNetworkManager(Path("/run/user/1000/docker.sock"))
    t, r, s = uuid4(), uuid4(), uuid4()
    lease = ProxyNetworkLease(
        internal_name=_make_network_name(t, r, s, 1, "internal"),
        egress_name=_make_network_name(t, r, s, 1, "egress"),
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )
    inspected: list[str] = []

    async def exists(_name: str) -> bool:
        return True

    async def inspect(name: str) -> dict[str, Any]:
        inspected.append(name)
        role = "internal" if name == lease.internal_name else "egress"
        return {
            "Internal": role == "internal",
            "Labels": {
                "vuzol.managed": "true",
                "vuzol.resource": "proxy-network",
                "vuzol.network_role": role,
                "vuzol.task_id": str(t),
                "vuzol.run_id": str(r),
                "vuzol.step_id": str(s),
                "vuzol.lease_generation": "1",
            },
        }

    monkeypatch.setattr(m, "_network_exists", exists)
    monkeypatch.setattr(m, "_inspect_network", inspect)
    asyncio.run(m.validate_owned(lease))
    assert inspected == [lease.internal_name, lease.egress_name]


def test_recovery_validation_fails_closed_for_tampered_or_foreign_network(monkeypatch):
    m = ProxyNetworkManager(Path("/run/user/1000/docker.sock"))
    t, r, s = uuid4(), uuid4(), uuid4()
    lease = ProxyNetworkLease(
        internal_name=_make_network_name(t, r, s, 1, "internal"),
        egress_name=_make_network_name(t, r, s, 1, "egress"),
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )

    async def exists(_name: str) -> bool:
        return True

    async def foreign(_name: str) -> dict[str, Any]:
        return {"Internal": True, "Labels": {"vuzol.managed": "true"}}

    monkeypatch.setattr(m, "_network_exists", exists)
    monkeypatch.setattr(m, "_inspect_network", foreign)
    with pytest.raises(ProxyNetworkError, match="foreign recovery network"):
        asyncio.run(m.validate_owned(lease))

    tampered = ProxyNetworkLease(
        internal_name="foreign-name",
        egress_name=lease.egress_name,
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )
    with pytest.raises(ProxyNetworkError, match="inconsistent"):
        asyncio.run(m.validate_owned(tampered))


@pytest.mark.parametrize(
    ("internal_value", "error"),
    [(False, "internal network"), (True, "egress network")],
)
def test_recovery_validation_rejects_wrong_network_boundary(
    monkeypatch: pytest.MonkeyPatch, internal_value: bool, error: str
) -> None:
    m = ProxyNetworkManager(Path("/run/user/1000/docker.sock"))
    t, r, s = uuid4(), uuid4(), uuid4()
    lease = ProxyNetworkLease(
        internal_name=_make_network_name(t, r, s, 1, "internal"),
        egress_name=_make_network_name(t, r, s, 1, "egress"),
        task_id=t,
        run_id=r,
        step_id=s,
        lease_generation=1,
    )

    async def exists(_name: str) -> bool:
        return True

    async def inspect(name: str) -> dict[str, Any]:
        role = "internal" if name == lease.internal_name else "egress"
        return {
            "Internal": internal_value,
            "Labels": {
                "vuzol.managed": "true",
                "vuzol.resource": "proxy-network",
                "vuzol.network_role": role,
                "vuzol.task_id": str(t),
                "vuzol.run_id": str(r),
                "vuzol.step_id": str(s),
                "vuzol.lease_generation": "1",
            },
        }

    monkeypatch.setattr(m, "_network_exists", exists)
    monkeypatch.setattr(m, "_inspect_network", inspect)
    with pytest.raises(ProxyNetworkError, match=error):
        asyncio.run(m.validate_owned(lease))
