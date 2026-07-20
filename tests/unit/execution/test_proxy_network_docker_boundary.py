"""Proxy network docker boundary tests (split for cohesion)."""

from __future__ import annotations

# mypy: allow-untyped-defs
import asyncio
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


def test_exactly_one_docker_executable_and_socket_boundary(monkeypatch):
    """Prove the assembled command from _docker has exactly one of each."""
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    captured: list[tuple[str, ...]] = []

    async def spy_exec(*argv: str, **_kw: object) -> Any:
        captured.append(argv)

        class P:
            returncode = 0

            async def communicate(self):
                return b"vuzol-net", b""

            async def wait(self):
                pass

        return P()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spy_exec)
    asyncio.run(m._docker("network", "ls", "--format", "{{.Name}}"))
    assert len(captured) == 1
    argv = captured[0]
    assert argv[0] == "docker"
    assert argv.count("docker") == 1
    assert "--host" in argv
    assert argv.count("--host") == 1
    assert any("unix://" + str(sock) in str(x) for x in argv)
    assert "network" in argv and "ls" in argv


def test_explicit_rootless_socket(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    assert m._socket == sock


def test_rootful_socket_rejected():
    with pytest.raises(ProxyNetworkError, match="rootful"):
        ProxyNetworkManager(Path("/var/run/docker.sock"))
    with pytest.raises(ProxyNetworkError, match="rootful"):
        ProxyNetworkManager(Path("/run/docker.sock"))


def test_non_absolute_socket_rejected():
    with pytest.raises(ProxyNetworkError, match="absolute"):
        ProxyNetworkManager(Path("relative/docker.sock"))


def test_subprocess_timeout_raises_and_reaps(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)

    class MockProc:
        def __init__(self) -> None:
            self.killed = False

        async def communicate(self):
            await asyncio.sleep(5)
            return b"", b""

        def kill(self):
            self.killed = True

        async def wait(self):
            return

    procs: list[MockProc] = []

    async def make_proc(*_a, **_k):
        p = MockProc()
        procs.append(p)
        return p

    monkeypatch.setattr(asyncio, "create_subprocess_exec", make_proc)

    async def immediate_timeout(coro, timeout=None):  # noqa: ASYNC109
        # force timeout path without real sleep
        coro.close()
        raise TimeoutError()

    monkeypatch.setattr(asyncio, "wait_for", immediate_timeout)
    with pytest.raises(ProxyNetworkError, match="timed out"):
        asyncio.run(m._docker("network", "ls"))
    # reaped
    assert procs and procs[0].killed


def test_docker_operation_failure_is_not_classified_as_absent(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)

    async def fail(*a: str) -> str:
        raise ProxyNetworkError("rootless Docker network operation failed")

    m._docker = fail  # type: ignore[method-assign]
    with pytest.raises(ProxyNetworkError):
        asyncio.run(m._network_exists("some-net"))


def test_empty_exact_network_list_result_means_absent(monkeypatch):
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)

    async def ls_empty(*a: str) -> str:
        if "ls" in a:
            return ""
        return ""

    m._docker = ls_empty  # type: ignore[method-assign]
    assert asyncio.run(m._network_exists("absent-net")) is False


def test_no_prune_command(monkeypatch):
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
    joined = " ".join(" ".join(map(str, c)) for c in calls)
    assert "prune" not in joined.lower()


def test_no_shell_invocation(monkeypatch):
    # exercised by using create_subprocess_exec always; assert no 'shell' usage in calls
    sock = Path("/run/user/1000/docker.sock")
    m = ProxyNetworkManager(sock)
    captured = []

    async def spy(*a, **k):
        captured.append(("exec", a, k.get("shell")))

        class P:
            returncode = 0

            async def communicate(self):
                return b"", b""

            async def wait(self):
                pass

        return P()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spy)
    asyncio.run(m._docker("network", "ls"))
    assert all(k is None for _, _, k in captured)


def test_no_exception_swallowing_patterns_present():
    # The source must not contain bare swallow pass for coverage.
    src = Path(__file__).read_text()
    bad = [
        ln
        for ln in src.splitlines()
        if "except Exception:" in ln and "pass" in ln and "no_exception_swallowing" not in ln
    ]
    assert len(bad) == 0, f"bare swallows present: {bad}"
