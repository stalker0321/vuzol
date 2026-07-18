"""Proxy service validation tests (split for cohesion)."""

from __future__ import annotations

from ._test_proxy_service_helpers import *


@pytest.mark.anyio
async def test_wait_ready_is_bounded_and_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    calls = 0

    async def fail(*_args: str, **_kwargs: object) -> str:
        nonlocal calls
        calls += 1
        raise ProxyServiceError("not ready")

    async def immediate(_delay: float) -> None:
        return None

    monkeypatch.setattr(manager, "_docker", fail)
    monkeypatch.setattr(asyncio, "sleep", immediate)
    with pytest.raises(ProxyServiceError, match="readiness timed out"):
        await manager._wait_ready("proxy")
    assert calls == 50


@pytest.mark.anyio
async def test_validate_container_accepts_exact_runtime_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    name = _make_proxy_name(*identity)
    policy_path = tmp_path / "policy.json"
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
    )

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(name, policy_path, identity, running=True)

    monkeypatch.setattr(manager, "_inspect_container", inspect)
    await manager._validate_container(name, networks, policy_path, *identity, running=True)


@pytest.mark.parametrize(
    "defect",
    [
        "networks",
        "alias",
        "running",
        "identity",
        "labels",
        "capabilities",
        "privileges",
        "limits",
        "ports",
        "mounts",
    ],
)
@pytest.mark.anyio
async def test_validate_container_rejects_each_hardening_defect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, defect: str
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    name = _make_proxy_name(*identity)
    policy_path = tmp_path / "policy.json"
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
    )
    data = _inspect(name, policy_path, identity, running=True)
    if defect == "networks":
        data["NetworkSettings"]["Networks"].pop("vuzol-egress")
    elif defect == "alias":
        data["NetworkSettings"]["Networks"]["vuzol-internal"]["Aliases"] = [name]
    elif defect == "running":
        data["State"]["Running"] = False
    elif defect == "identity":
        data["Config"]["User"] = "0:0"
    elif defect == "labels":
        data["Config"]["Labels"] = {"foreign": "true"}
    elif defect == "capabilities":
        data["HostConfig"]["CapDrop"] = []
    elif defect == "privileges":
        data["HostConfig"]["SecurityOpt"] = []
    elif defect == "limits":
        data["HostConfig"]["Memory"] = 0
    elif defect == "ports":
        data["HostConfig"]["PortBindings"] = {"8888/tcp": [{"HostPort": "8888"}]}
    elif defect == "mounts":
        data["Mounts"] = []

    async def inspect(_name: str) -> dict[str, Any]:
        return data

    monkeypatch.setattr(manager, "_inspect_container", inspect)
    with pytest.raises(ProxyServiceError):
        await manager._validate_container(name, networks, policy_path, *identity, running=True)


@pytest.mark.anyio
async def test_container_lookup_and_inspect_fail_closed_on_ambiguous_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())

    async def docker(*args: str, **_kwargs: object) -> str:
        if args[0] == "ps":
            return "expected\nforeign\n"
        return "[]"

    monkeypatch.setattr(manager, "_docker", docker)
    with pytest.raises(ProxyServiceError, match="ambiguous"):
        await manager._container_exists("expected")
    with pytest.raises(ProxyServiceError, match="malformed"):
        await manager._inspect_container("expected")
