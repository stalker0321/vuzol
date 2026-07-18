"""Proxy service reconciliation tests (split for cohesion)."""

from __future__ import annotations

from ._test_proxy_service_helpers import (
    STATE_FILE,
    FakeNetworks,
    Path,
    ProxyNetworkLease,
    ProxyServiceError,
    _identity,
    _make_proxy_name,
    _manager,
    _render_policy,
    _target,
    hashlib_sha256,
    pytest,
)


@pytest.mark.anyio
async def test_startup_reconciliation_uses_manifest_and_exact_cleanup_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()
    directory = manager._runtime_directory(*identity)
    policy_path = directory / "policy.json"
    policy = _render_policy((_target(),))
    policy_hash = hashlib_sha256(policy)
    manager._write_private_policy(directory, policy_path, policy)
    manager._write_state(directory, *identity, policy_hash)
    events: list[str] = []

    async def remove_sandbox(*seen: object) -> None:
        assert seen == identity
        events.append("sandbox")

    async def remove_proxy(name: str, *seen: object) -> None:
        assert name == _make_proxy_name(*identity)
        assert seen == identity
        events.append("proxy")

    async def container_absent(_name: str) -> bool:
        return False

    async def cleanup(lease: ProxyNetworkLease) -> None:
        assert lease.task_id == identity[0]
        assert lease.run_id == identity[1]
        assert lease.step_id == identity[2]
        assert lease.lease_generation == identity[3]
        assert lease.internal_name.endswith("-internal")
        assert lease.egress_name.endswith("-egress")
        events.append("networks")

    monkeypatch.setattr(manager, "_remove_owned_sandbox", remove_sandbox)
    monkeypatch.setattr(manager, "_remove_owned_container", remove_proxy)
    monkeypatch.setattr(manager, "_container_exists", container_absent)
    networks.cleanup = cleanup  # type: ignore[method-assign]
    manifests = manager.recovery_manifests()
    assert len(manifests) == 1
    await manager.cleanup_recovery_manifest(manifests[0])
    assert events == ["sandbox", "proxy", "networks"]
    assert not directory.exists()


@pytest.mark.anyio
async def test_startup_reconciliation_refuses_ambiguous_or_foreign_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    directory = manager._runtime_directory(*identity)
    directory.mkdir(parents=True, mode=0o700)
    manager._runtime_root.chmod(0o700)
    state_path = directory / STATE_FILE
    state_path.write_text("{}")
    state_path.chmod(0o600)
    monkeypatch.setattr(manager, "_remove_owned_sandbox", pytest.fail)
    with pytest.raises(ProxyServiceError, match="malformed"):
        manager.recovery_manifests()


@pytest.mark.anyio
async def test_startup_reconciliation_is_empty_and_refuses_untracked_entries(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    assert manager.recovery_manifests() == ()
    manager._runtime_root.mkdir(mode=0o700)
    (manager._runtime_root / "untracked").write_text("foreign")
    with pytest.raises(ProxyServiceError, match="ambiguous entry"):
        manager.recovery_manifests()


@pytest.mark.anyio
async def test_startup_reconciliation_sanitizes_missing_manifest(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    manager._runtime_root.mkdir(mode=0o700)
    (manager._runtime_root / "owned-looking-directory").mkdir(mode=0o700)
    with pytest.raises(ProxyServiceError, match="manifest is unavailable"):
        manager.recovery_manifests()


@pytest.mark.anyio
async def test_startup_reconciliation_binds_manifest_to_directory(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    directory = manager._runtime_root / "wrong-directory"
    directory.mkdir(parents=True, mode=0o700)
    manager._runtime_root.chmod(0o700)
    manager._write_state(directory, *identity, "a" * 64)
    with pytest.raises(ProxyServiceError, match="path is inconsistent"):
        manager.recovery_manifests()
