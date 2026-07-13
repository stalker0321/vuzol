import asyncio
import json
import stat
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_networks import ProxyNetworkLease
from vuzol.execution.proxy_service import (
    POLICY_CONTAINER_PATH,
    PROXY_ALIAS,
    STATE_FILE,
    ProxyServiceError,
    ProxyServiceLease,
    ProxyServiceManager,
    _dict,
    _make_proxy_name,
    _ownership_labels,
    _render_policy,
    _sandbox_ownership_labels,
)

IMAGE = f"vuzol-proxy@sha256:{'a' * 64}"
SOCKET = Path("/run/user/994/docker.sock")


class FakeNetworks:
    def __init__(self) -> None:
        self.created: list[tuple[UUID, UUID, UUID, int]] = []
        self.cleaned: list[ProxyNetworkLease] = []

    async def create(
        self, task_id: UUID, run_id: UUID, step_id: UUID, generation: int
    ) -> ProxyNetworkLease:
        self.created.append((task_id, run_id, step_id, generation))
        return ProxyNetworkLease(
            internal_name="vuzol-internal",
            egress_name="vuzol-egress",
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            lease_generation=generation,
        )

    async def cleanup(self, lease: ProxyNetworkLease) -> None:
        self.cleaned.append(lease)


def _target(hostname: str = "api.openai.com") -> AllowedConnectTarget:
    return AllowedConnectTarget(hostname=hostname, port=443, purpose="runtime API")


def _manager(tmp_path: Path, networks: FakeNetworks) -> ProxyServiceManager:
    return ProxyServiceManager(
        SOCKET,
        tmp_path / "proxy-runtime",
        IMAGE,
        networks=networks,  # type: ignore[arg-type]
    )


def _identity() -> tuple[UUID, UUID, UUID, int]:
    return uuid4(), uuid4(), uuid4(), 3


def _inspect(
    name: str,
    policy_path: Path,
    identity: tuple[UUID, UUID, UUID, int],
    *,
    running: bool,
    labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    task_id, run_id, step_id, generation = identity
    return {
        "Name": f"/{name}",
        "Config": {
            "Image": IMAGE,
            "User": "10002:10002",
            "Labels": labels or _ownership_labels(task_id, run_id, step_id, generation),
            "ExposedPorts": None,
        },
        "HostConfig": {
            "ReadonlyRootfs": True,
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges:true"],
            "Memory": 67_108_864,
            "MemorySwap": 67_108_864,
            "NanoCpus": 250_000_000,
            "PidsLimit": 32,
            "PortBindings": {},
        },
        "State": {"Running": running},
        "NetworkSettings": {
            "Networks": {
                "vuzol-egress": {"Aliases": [name]},
                "vuzol-internal": {"Aliases": [name, PROXY_ALIAS]},
            }
        },
        "Mounts": [
            {
                "Type": "bind",
                "Source": str(policy_path),
                "Destination": POLICY_CONTAINER_PATH,
                "RW": False,
            }
        ],
    }


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
    networks.cleanup = cleanup  # type: ignore[method-assign]
    assert await manager.reconcile_startup() == 1
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
        await manager.reconcile_startup()


@pytest.mark.anyio
async def test_startup_reconciliation_is_empty_and_refuses_untracked_entries(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    assert await manager.reconcile_startup() == 0
    manager._runtime_root.mkdir(mode=0o700)
    (manager._runtime_root / "untracked").write_text("foreign")
    with pytest.raises(ProxyServiceError, match="ambiguous entry"):
        await manager.reconcile_startup()


@pytest.mark.anyio
async def test_startup_reconciliation_sanitizes_missing_manifest(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    manager._runtime_root.mkdir(mode=0o700)
    (manager._runtime_root / "owned-looking-directory").mkdir(mode=0o700)
    with pytest.raises(ProxyServiceError, match="manifest is unavailable"):
        await manager.reconcile_startup()


@pytest.mark.anyio
async def test_startup_reconciliation_binds_manifest_to_directory(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    identity = _identity()
    directory = manager._runtime_root / "wrong-directory"
    directory.mkdir(parents=True, mode=0o700)
    manager._runtime_root.chmod(0o700)
    manager._write_state(directory, *identity, "a" * 64)
    with pytest.raises(ProxyServiceError, match="path is inconsistent"):
        await manager.reconcile_startup()


def test_runtime_root_must_remain_private(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeNetworks())
    manager._runtime_root.mkdir(mode=0o700)
    manager._runtime_root.chmod(0o755)
    with pytest.raises(ProxyServiceError, match="private real directory"):
        manager._validate_runtime_root()


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


def hashlib_sha256(content: bytes) -> str:
    import hashlib

    return hashlib.sha256(content).hexdigest()
