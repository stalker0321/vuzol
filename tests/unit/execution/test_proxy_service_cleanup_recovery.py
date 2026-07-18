"""Proxy service cleanup recovery tests (split for cohesion)."""

from __future__ import annotations

from ._test_proxy_service_helpers import *


@pytest.mark.anyio
async def test_wait_until_dead_rejects_tampered_lease_and_accepts_absence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
    )
    lease = ProxyServiceLease(
        container_name="tampered",
        networks=networks,
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
        policy_hash="a" * 64,
    )
    with pytest.raises(ProxyServiceError, match="inconsistent"):
        await manager.wait_until_dead(lease)
    valid = lease.__class__(**{**lease.__dict__, "container_name": _make_proxy_name(*identity)})

    async def absent(_name: str) -> bool:
        return False

    monkeypatch.setattr(manager, "_container_exists", absent)
    await manager.wait_until_dead(valid)


@pytest.mark.anyio
async def test_recovery_refuses_foreign_sandbox_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    name = f"vuzol-{str(identity[2])[:12]}-{identity[3]}"
    policy_path = tmp_path / "policy.json"
    existence = iter((True,))

    async def exists(seen: str) -> bool:
        assert seen == name
        return next(existence)

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(name, policy_path, identity, running=True, labels={"foreign": "true"})

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    with pytest.raises(ProxyServiceError, match="foreign sandbox"):
        await manager._remove_owned_sandbox(*identity)
    assert _sandbox_ownership_labels(*identity)["vuzol.resource"] == "sandbox-container"


@pytest.mark.anyio
async def test_recovery_validation_accepts_only_exact_container_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    sandbox_name = f"vuzol-{str(identity[2])[:12]}-{identity[3]}"
    proxy_name = _make_proxy_name(*identity)
    manifest = ProxyRecoveryManifest(
        directory=manager._runtime_directory(*identity),
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
        policy_hash="a" * 64,
    )
    inspected: list[str] = []

    async def exists(name: str) -> bool:
        return name in {sandbox_name, proxy_name}

    async def inspect(name: str) -> dict[str, Any]:
        inspected.append(name)
        labels = (
            _sandbox_ownership_labels(*identity)
            if name == sandbox_name
            else _ownership_labels(*identity)
        )
        return _inspect(name, tmp_path / "policy.json", identity, running=True, labels=labels)

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_inspect_container", inspect)

    await manager.validate_recovery_resources(manifest)

    assert inspected == [sandbox_name, proxy_name]


@pytest.mark.anyio
async def test_recovery_stops_and_removes_only_owned_sandbox(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    name = f"vuzol-{str(identity[2])[:12]}-{identity[3]}"
    policy_path = tmp_path / "policy.json"
    existence = iter((True, True, False))
    calls: list[tuple[str, ...]] = []

    async def exists(seen: str) -> bool:
        assert seen == name
        return next(existence)

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(
            name,
            policy_path,
            identity,
            running=False,
            labels=_sandbox_ownership_labels(*identity),
        )

    async def docker(*args: str, **_kwargs: object) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    monkeypatch.setattr(manager, "_docker", docker)
    await manager._remove_owned_sandbox(*identity)
    assert calls == [("rm", "-f", name)]


@pytest.mark.anyio
async def test_recovery_stop_handles_sandbox_auto_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    name = f"vuzol-{str(identity[2])[:12]}-{identity[3]}"
    existence = iter((True, False, False))
    calls: list[tuple[str, ...]] = []

    async def exists(_name: str) -> bool:
        return next(existence)

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(
            name,
            tmp_path / "policy.json",
            identity,
            running=True,
            labels=_sandbox_ownership_labels(*identity),
        )

    async def docker(*args: str, **_kwargs: object) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    monkeypatch.setattr(manager, "_docker", docker)
    await manager._remove_owned_sandbox(*identity)
    assert calls == [("stop", "--time", "5", name)]


@pytest.mark.anyio
async def test_cleanup_removes_container_networks_and_exact_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()

    async def exists_create(_name: str) -> bool:
        return False

    async def docker(*_args: str, **_kwargs: object) -> str:
        return ""

    async def no_op(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(manager, "_container_exists", exists_create)
    monkeypatch.setattr(manager, "_docker", docker)
    monkeypatch.setattr(manager, "_validate_container", no_op)
    monkeypatch.setattr(manager, "_wait_ready", no_op)
    lease = await manager.create(*identity, (_target(),))
    runtime_dir = manager._runtime_directory(*identity)

    removed: list[str] = []

    async def remove(name: str, *_args: object) -> None:
        removed.append(name)

    monkeypatch.setattr(manager, "_remove_owned_container", remove)
    await manager.cleanup(lease)
    assert removed == [lease.container_name]
    assert networks.cleaned == [lease.networks]
    assert not runtime_dir.exists()


@pytest.mark.anyio
async def test_cleanup_rejects_tampered_lease_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()
    lease = ProxyServiceLease(
        container_name="foreign",
        networks=await networks.create(*identity),
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
        policy_hash="0" * 64,
    )
    remove = monkeypatch.setattr(manager, "_remove_owned_container", pytest.fail)
    assert remove is None
    with pytest.raises(ProxyServiceError, match="inconsistent"):
        await manager.cleanup(lease)
    assert networks.cleaned == []


@pytest.mark.anyio
async def test_remove_owned_container_refuses_foreign_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()
    name = _make_proxy_name(*identity)
    policy_path = manager._runtime_directory(*identity) / "policy.json"

    async def exists(_name: str) -> bool:
        return True

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(name, policy_path, identity, running=True, labels={"foreign": "true"})

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    with pytest.raises(ProxyServiceError, match="foreign"):
        await manager._remove_owned_container(name, *identity)


@pytest.mark.anyio
async def test_remove_owned_running_container_stops_then_removes_exactly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    name = _make_proxy_name(*identity)
    policy_path = tmp_path / "policy.json"
    existence = iter((True, False))
    calls: list[tuple[str, ...]] = []

    async def exists(_name: str) -> bool:
        return next(existence)

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(name, policy_path, identity, running=True)

    async def docker(*args: str, **_kwargs: object) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    monkeypatch.setattr(manager, "_docker", docker)
    await manager._remove_owned_container(name, *identity)
    assert calls == [("stop", "--time", "5", name), ("rm", name)]


@pytest.mark.anyio
async def test_wait_until_dead_checks_ownership_and_running_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    name = _make_proxy_name(*identity)
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
    )
    lease = ProxyServiceLease(
        container_name=name,
        networks=networks,
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
        policy_hash="a" * 64,
    )
    states = iter((True, False))

    async def exists(_name: str) -> bool:
        return True

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(
            name,
            tmp_path / "policy.json",
            identity,
            running=next(states),
        )

    async def immediate(_delay: float) -> None:
        return None

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    monkeypatch.setattr(asyncio, "sleep", immediate)
    await manager.wait_until_dead(lease)


def test_cleanup_rejects_modified_policy_file(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    directory = manager._runtime_directory(*identity)
    path = directory / "policy.json"
    content = _render_policy((_target(),))
    manager._write_private_policy(directory, path, content)
    path.chmod(0o600)
    with pytest.raises(ProxyServiceError, match="ambiguous"):
        manager._remove_private_policy(directory, path, expected_hash=hashlib_sha256(content))
    path.write_bytes(b"modified")
    path.chmod(0o444)
    with pytest.raises(ProxyServiceError, match="modified"):
        manager._remove_private_policy(directory, path, expected_hash=hashlib_sha256(content))
