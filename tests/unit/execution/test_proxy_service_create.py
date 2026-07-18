"""Proxy service create tests (split for cohesion)."""

from __future__ import annotations

from ._test_proxy_service_helpers import *


def test_proxy_identity_and_policy_are_deterministic_and_secret_free() -> None:
    identity = _identity()
    assert _make_proxy_name(*identity) == _make_proxy_name(*identity)
    assert _make_proxy_name(*identity).endswith("-proxy")
    rendered = _render_policy((_target("uploads.openai.com"), _target()))
    body = json.loads(rendered)
    assert body["targets"] == [
        {"hostname": "api.openai.com", "port": 443},
        {"hostname": "uploads.openai.com", "port": 443},
    ]
    assert b"purpose" not in rendered
    assert b"secret" not in rendered
    assert rendered == _render_policy((_target(), _target("uploads.openai.com")))
    with pytest.raises(ProxyServiceError):
        _render_policy(())
    with pytest.raises(ProxyServiceError):
        _make_proxy_name(identity[0], identity[1], identity[2], 0)
    with pytest.raises(ProxyServiceError, match="malformed"):
        _dict({"State": []}, "State", "proxy")


@pytest.mark.anyio
async def test_create_rejects_empty_targets_before_files_or_docker(tmp_path: Path) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    with pytest.raises(ProxyServiceError, match="non-empty"):
        await manager.create(*_identity(), ())
    assert networks.created == []


def test_proxy_manager_rejects_rootful_relative_and_unpinned_inputs(tmp_path: Path) -> None:
    networks = FakeNetworks()
    with pytest.raises(ProxyServiceError, match="rootless"):
        ProxyServiceManager(Path("/run/docker.sock"), tmp_path, IMAGE)
    with pytest.raises(ProxyServiceError, match="rootless"):
        ProxyServiceManager(Path("relative.sock"), tmp_path, IMAGE)
    with pytest.raises(ProxyServiceError, match="absolute"):
        ProxyServiceManager(SOCKET, Path("relative"), IMAGE)
    with pytest.raises(ProxyServiceError, match="digest"):
        ProxyServiceManager(SOCKET, tmp_path, "vuzol-proxy:latest")
    assert networks.created == []


@pytest.mark.anyio
async def test_create_is_transactional_hardened_and_writes_private_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()
    calls: list[tuple[str, ...]] = []

    async def exists(_name: str) -> bool:
        return False

    async def docker(*args: str, **_kwargs: object) -> str:
        calls.append(args)
        return ""

    name = _make_proxy_name(*identity)
    policy_path = manager._runtime_directory(*identity) / "policy.json"
    inspected_states = iter((False, True))

    async def inspect(_name: str) -> dict[str, Any]:
        return _inspect(name, policy_path, identity, running=next(inspected_states))

    async def ready(_name: str) -> None:
        return None

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_docker", docker)
    monkeypatch.setattr(manager, "_inspect_container", inspect)
    monkeypatch.setattr(manager, "_wait_ready", ready)

    lease = await manager.create(*identity, (_target(),))
    assert lease.container_name == _make_proxy_name(*identity)
    assert lease.proxy_url == "http://vuzol-proxy:8888"
    assert networks.created == [identity]
    create = next(call for call in calls if call[0] == "create")
    rendered = " ".join(create)
    assert "--network vuzol-egress" in rendered
    assert "--read-only" in create
    assert "--user 10002:10002" in rendered
    assert "--cap-drop ALL" in rendered
    assert "no-new-privileges:true" in create
    assert "--memory 67108864" in rendered
    assert "--memory-swap 67108864" in rendered
    assert "--cpus 0.25" in rendered
    assert "--pids-limit 32" in rendered
    assert "--publish" not in create and "-p" not in create
    assert "docker.sock" not in rendered
    assert "prune" not in rendered
    connect = next(call for call in calls if call[:2] == ("network", "connect"))
    assert connect == (
        "network",
        "connect",
        "--alias",
        PROXY_ALIAS,
        "vuzol-internal",
        lease.container_name,
    )
    assert calls[-1] == ("start", lease.container_name)
    label_values = {create[index + 1] for index, item in enumerate(create) if item == "--label"}
    assert label_values == {f"{key}={value}" for key, value in _ownership_labels(*identity).items()}
    runtime_dir = manager._runtime_directory(*identity)
    policy_path = runtime_dir / "policy.json"
    assert stat.S_IMODE(runtime_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(policy_path.stat().st_mode) == 0o444
    state_path = runtime_dir / STATE_FILE
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    state = json.loads(state_path.read_text())
    assert state == {
        "lease_generation": identity[3],
        "policy_hash": lease.policy_hash,
        "run_id": str(identity[1]),
        "step_id": str(identity[2]),
        "task_id": str(identity[0]),
        "version": 1,
    }
    assert hashlib_sha256(policy_path.read_bytes()) == lease.policy_hash


@pytest.mark.anyio
async def test_create_failure_rolls_back_networks_and_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()

    async def exists(_name: str) -> bool:
        return False

    async def fail(*_args: str, **_kwargs: object) -> str:
        raise ProxyServiceError("create failed")

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_docker", fail)
    with pytest.raises(ProxyServiceError, match="create failed"):
        await manager.create(*identity, (_target(),))
    assert len(networks.cleaned) == 1
    runtime_dir = manager._runtime_directory(*identity)
    assert not runtime_dir.exists()


@pytest.mark.anyio
async def test_create_reports_incomplete_rollback_without_hiding_primary_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()

    async def exists(_name: str) -> bool:
        return False

    async def create_failure(*_args: str, **_kwargs: object) -> str:
        raise ProxyServiceError("primary create failure")

    async def cleanup_failure(_lease: ProxyNetworkLease) -> None:
        raise ProxyServiceError("cleanup failure")

    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_docker", create_failure)
    networks.cleanup = cleanup_failure  # type: ignore[assignment]
    with pytest.raises(ProxyServiceError, match="rollback was incomplete") as raised:
        await manager.create(*identity, (_target(),))
    assert isinstance(raised.value.__cause__, ProxyServiceError)
    assert str(raised.value.__cause__) == "primary create failure"


@pytest.mark.anyio
async def test_create_rejects_container_collision_and_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()

    async def present(_name: str) -> bool:
        return True

    monkeypatch.setattr(manager, "_container_exists", present)
    with pytest.raises(ProxyServiceError, match="collision"):
        await manager.create(*identity, (_target(),))
    assert len(networks.cleaned) == 1
    assert not manager._runtime_directory(*identity).exists()


@pytest.mark.anyio
async def test_post_create_failure_removes_owned_container_before_networks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    networks = FakeNetworks()
    manager = _manager(tmp_path, networks)
    identity = _identity()
    events: list[str] = []

    async def exists(_name: str) -> bool:
        return False

    async def docker(*args: str, **_kwargs: object) -> str:
        if args[:2] == ("network", "connect"):
            raise ProxyServiceError("connect failed")
        return ""

    async def remove(*_args: object) -> None:
        events.append("container")

    original_cleanup = networks.cleanup

    async def cleanup(lease: ProxyNetworkLease) -> None:
        events.append("networks")
        await original_cleanup(lease)

    networks.cleanup = cleanup  # type: ignore[method-assign]
    monkeypatch.setattr(manager, "_container_exists", exists)
    monkeypatch.setattr(manager, "_docker", docker)
    monkeypatch.setattr(manager, "_remove_owned_container", remove)
    with pytest.raises(ProxyServiceError, match="connect failed"):
        await manager.create(*identity, (_target(),))
    assert events == ["container", "networks"]


def test_runtime_root_must_remain_private(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    manager._runtime_root.mkdir(mode=0o700)
    manager._runtime_root.chmod(0o755)
    with pytest.raises(ProxyServiceError, match="private real directory"):
        manager._validate_runtime_root()


@pytest.mark.anyio
async def test_docker_boundary_uses_one_explicit_host_and_sanitizes_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    executable = tmp_path / "docker"
    executable.write_text("#!/bin/sh\nprintf safe-output\n")
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path))
    assert await manager._docker("ps", "-a") == "safe-output"

    executable.write_text("#!/bin/sh\necho sensitive >&2\nexit 2\n")
    with pytest.raises(ProxyServiceError, match="operation failed") as raised:
        await manager._docker("inspect", "resource")
    assert "sensitive" not in str(raised.value)
