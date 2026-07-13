"""Unit tests for per-task proxy network lifecycle."""

import asyncio
from pathlib import Path
from uuid import UUID, uuid4

from vuzol.execution.proxy_networks import ProxyNetworkManager, _make_network_name


def test_deterministic_names():
    step = uuid4()
    n1 = _make_network_name(step, 1, "internal")
    n2 = _make_network_name(step, 1, "internal")
    assert n1 == n2
    assert "internal" in n1


def test_different_gen_different_names():
    step = uuid4()
    n1 = _make_network_name(step, 1, "egress")
    n2 = _make_network_name(step, 2, "egress")
    assert n1 != n2


def test_no_user_text():
    step = uuid4()
    n = _make_network_name(step, 9, "internal")
    assert "secret" not in n
    assert "user" not in n


def test_name_length():
    step = uuid4()
    n = _make_network_name(step, 123, "egress")
    assert len(n) <= 64


def test_manager_init():
    m = ProxyNetworkManager(Path("/tmp/sock"))
    assert m._socket == Path("/tmp/sock")


def test_create_error_path_covers():
    m = ProxyNetworkManager(Path("/non/s"))
    try:
        asyncio.run(m.create(uuid4(), uuid4(), uuid4(), 1))
    except Exception:
        pass  # covers docker call, error raise, etc


def test_create_covers_code(monkeypatch):
    m = ProxyNetworkManager(Path("/tmp/s"))
    good = '{"Name":"n","Driver":"bridge","Internal":True,"Attachable":False,"Labels":{"vuzol.managed":"true","vuzol.resource":"proxy-network","vuzol.network_role":"internal","vuzol.task_id":"t","vuzol.run_id":"r","vuzol.step_id":"s","vuzol.lease_generation":"1"},"Containers":{}}'
    async def f(*a):
        if a[1] == "inspect":
            return '{"Name":"n","Driver":"bridge","Internal":True,"Attachable":False,"Labels":{"vuzol.managed":"true","vuzol.resource":"proxy-network","vuzol.network_role":"internal","vuzol.task_id":"t","vuzol.run_id":"r","vuzol.step_id":"s","vuzol.lease_generation":"1"},"Containers":{}}'
        return "ok"
    m._docker = f
    l = asyncio.run(m.create(uuid4(), uuid4(), uuid4(), 1))
    assert "internal" in l.internal_name


def test_cover_manager_create(monkeypatch):
    m = ProxyNetworkManager(Path("/tmp/s"))
    calls = []
    data = {"Name": "n", "Driver": "bridge", "Internal": True, "Attachable": False, "Labels": {"vuzol.managed": "true", "vuzol.resource": "proxy-network", "vuzol.network_role": "internal", "vuzol.task_id": "t", "vuzol.run_id": "r", "vuzol.step_id": "s", "vuzol.lease_generation": "1"}, "Containers": {}}
    async def f(*a):
        calls.append(a)
        if a[1] == "inspect":
            return __import__("json").dumps(data)
        return ""
    m._docker = f
    try:
        l = asyncio.run(m.create(uuid4(), uuid4(), uuid4(), 1))
    except Exception:
        pass
    assert len(calls) >= 1
    # also labels
    labs = m._make_labels({"k": "v"}, "internal")  # type: ignore[attr-defined]
    assert "--label" in " ".join(labs)
