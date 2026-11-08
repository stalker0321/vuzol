"""Sandboxes on a node's Docker: mounts become staged volumes, only read-only ones."""

from __future__ import annotations

import pytest

from vuzol.config.settings import ExecutionSettings
from vuzol.execution.node import NodeDockerRuntime, NodeProxyServiceManager
from vuzol.execution.sandbox import SandboxError

from ._execution_helpers import MountMode, Path, docker_run_argv, envelope


def _read_only(tmp_path: Path):
    configured = envelope(tmp_path)
    mounts = tuple(
        mount.model_copy(update={"mode": MountMode.READ_ONLY})
        for mount in configured.sandbox.mounts
    )
    return configured.model_copy(
        update={"sandbox": configured.sandbox.model_copy(update={"mounts": mounts})}
    )


def test_volumes_replace_bind_mounts_by_index(tmp_path: Path) -> None:
    configured = _read_only(tmp_path)
    argv = docker_run_argv(
        tmp_path / "node.sock",
        "task",
        configured,
        volumes={0: ("task-m0", None), 1: ("task-m1", "auth.json")},
    )
    mounts = [argv[index + 1] for index, value in enumerate(argv) if value == "--mount"]
    assert mounts == [
        "type=volume,src=task-m0,dst=/workspace,readonly",
        "type=volume,src=task-m1,dst=/artifacts,volume-subpath=auth.json,readonly",
    ]
    assert not any("type=bind" in mount for mount in mounts)


def test_without_volumes_mounts_stay_binds(tmp_path: Path) -> None:
    argv = docker_run_argv(tmp_path / "docker.sock", "task", envelope(tmp_path))
    assert all(
        argv[index + 1].startswith("type=bind,")
        for index, value in enumerate(argv)
        if value == "--mount"
    )


@pytest.mark.anyio
async def test_a_node_refuses_writable_mounts_before_touching_docker(tmp_path: Path) -> None:
    runtime = NodeDockerRuntime("gcp", tmp_path / "node.sock", tmp_path / "local.sock")
    with pytest.raises(SandboxError, match="read-only mounts only"):
        await runtime.run(envelope(tmp_path), cancellation=None)  # type: ignore[arg-type]


def test_node_proxy_mounts_its_policy_from_a_volume(tmp_path: Path) -> None:
    proxy = NodeProxyServiceManager(
        tmp_path / "node.sock",
        tmp_path / "local.sock",
        tmp_path / "proxy",
        "sha256:" + "c" * 64,
        helper_image="sandbox@sha256:" + "d" * 64,
    )
    policy = tmp_path / "proxy" / "x" / "policy.json"
    mount = proxy._policy_mount("vuzol-proxy-1", policy)
    assert mount == (
        "type=volume,src=vuzol-proxy-1-policy,dst=/etc/vuzol-proxy/policy.json,"
        "volume-subpath=policy.json,readonly"
    )
    assert proxy._policy_mount_matches(
        {"Type": "volume", "Name": "vuzol-proxy-1-policy"}, "vuzol-proxy-1", policy
    )
    assert not proxy._policy_mount_matches(
        {"Type": "bind", "Source": str(policy)}, "vuzol-proxy-1", policy
    )


def test_no_nodes_by_default_and_ids_are_unique() -> None:
    assert ExecutionSettings().nodes == ()
    node = {"id": "gcp", "docker_socket": "/run/user/994/vuzol-node-gcp.sock"}
    assert ExecutionSettings(nodes=[node]).nodes[0].model_call_parallelism == 1
    with pytest.raises(ValueError, match="unique"):
        ExecutionSettings(nodes=[node, node])
    with pytest.raises(ValueError, match="absolute"):
        ExecutionSettings(nodes=[{"id": "gcp", "docker_socket": "node.sock"}])
