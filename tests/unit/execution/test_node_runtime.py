"""Sandboxes on a node's Docker: mounts become staged volumes, only read-only ones."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from vuzol.config.settings import ExecutionSettings
from vuzol.execution.node import NodeDockerRuntime, NodeProxyServiceManager
from vuzol.execution.sandbox import RootlessDockerRuntime, SandboxError, SandboxInterrupted
from vuzol.providers.ports import CodexProcessResult
from vuzol.workflows.ports import CancellationContext

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
@pytest.mark.parametrize("exit_code", [0, 1, 124])
async def test_node_copies_writes_only_after_normal_exit(tmp_path, monkeypatch, exit_code):
    configured = envelope(tmp_path)
    runtime = NodeDockerRuntime("gcp", tmp_path / "node.sock", tmp_path / "local.sock")
    runtime._volumes = AsyncMock()
    runtime._volumes.copy.return_value = None
    result = CodexProcessResult(exit_code, "", "", 1)
    monkeypatch.setattr(RootlessDockerRuntime, "run", AsyncMock(return_value=result))
    assert await runtime.run(configured, CancellationContext()) == result
    assert runtime._volumes.copy_back.await_count == 2
    assert runtime._volumes.remove.await_count == 2


@pytest.mark.anyio
@pytest.mark.parametrize(
    "failure", [SandboxError("lost node"), SandboxInterrupted(CodexProcessResult(124, "", "", 1))]
)
async def test_node_never_copies_back_interrupted_runs(tmp_path, monkeypatch, failure):
    configured = envelope(tmp_path)
    runtime = NodeDockerRuntime("gcp", tmp_path / "node.sock", tmp_path / "local.sock")
    runtime._volumes = AsyncMock()
    runtime._volumes.copy.return_value = None
    monkeypatch.setattr(RootlessDockerRuntime, "run", AsyncMock(side_effect=failure))
    with pytest.raises(SandboxError):
        await runtime.run(configured, CancellationContext())
    runtime._volumes.copy_back.assert_not_awaited()
    assert runtime._volumes.remove.await_count == 2


@pytest.mark.anyio
async def test_copy_back_failure_never_returns_provider_result(tmp_path, monkeypatch):
    configured = envelope(tmp_path)
    runtime = NodeDockerRuntime("gcp", tmp_path / "node.sock", tmp_path / "local.sock")
    runtime._volumes = AsyncMock()
    runtime._volumes.copy.return_value = None
    runtime._volumes.copy_back.side_effect = SandboxError("copy-back failed")
    monkeypatch.setattr(
        RootlessDockerRuntime,
        "run",
        AsyncMock(return_value=CodexProcessResult(0, "success", "", 1)),
    )
    with pytest.raises(SandboxError, match="copy-back"):
        await runtime.run(configured, CancellationContext())
    assert runtime._volumes.remove.await_count == 2


def test_nested_mounts_remain_separate_volumes(tmp_path):
    configured = _read_only(tmp_path)
    outer, metadata = configured.sandbox.mounts
    outer = outer.model_copy(update={"mode": MountMode.READ_WRITE})
    metadata = metadata.model_copy(update={"target": Path("/workspace/.git")})
    configured = configured.model_copy(
        update={"sandbox": configured.sandbox.model_copy(update={"mounts": (outer, metadata)})}
    )
    argv = docker_run_argv(
        tmp_path / "node.sock",
        "task",
        configured,
        volumes={0: ("task-m0", None), 1: ("task-m1", None)},
    )
    mounts = [argv[i + 1] for i, value in enumerate(argv) if value == "--mount"]
    assert mounts == [
        "type=volume,src=task-m0,dst=/workspace",
        "type=volume,src=task-m1,dst=/workspace/.git,readonly",
    ]


def test_node_copy_back_settings():
    node = ExecutionSettings(nodes=[{"id": "gcp", "docker_socket": "/run/node.sock"}]).nodes[0]
    assert node.max_copy_back_bytes == 2 * 1024**3
    assert node.provider_parallelism == 0
    with pytest.raises(ValueError):
        type(node)(id="gcp", docker_socket="/run/node.sock", max_copy_back_bytes=0)
    with pytest.raises(ValueError):
        type(node)(id="gcp", docker_socket="/run/node.sock", provider_parallelism=-1)


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


@pytest.mark.anyio
async def test_cancelled_context_skips_copy_back(tmp_path, monkeypatch):
    configured = envelope(tmp_path)
    runtime = NodeDockerRuntime("gcp", tmp_path / "node.sock", tmp_path / "local.sock")
    runtime._volumes = AsyncMock()
    runtime._volumes.copy.return_value = None
    monkeypatch.setattr(
        RootlessDockerRuntime, "run", AsyncMock(return_value=CodexProcessResult(0, "", "", 1))
    )
    cancellation = CancellationContext()
    cancellation.request()
    with pytest.raises(SandboxError, match="cancelled"):
        await runtime.run(configured, cancellation)
    runtime._volumes.copy_back.assert_not_awaited()


@pytest.fixture
def local_tar_docker(monkeypatch, tmp_path):
    """Replace Docker with host tar/sh on fixture directories, never a daemon."""
    import asyncio
    import re
    import shlex

    from vuzol.execution import node

    real_spawn = asyncio.create_subprocess_exec
    volume = tmp_path / "volume"
    volume.mkdir()
    calls = []

    async def spawn(*argv, **kwargs):
        calls.append(argv)
        mounts = {}
        for index, arg in enumerate(argv):
            if arg == "--mount":
                parts = dict(p.split("=", 1) for p in argv[index + 1].split(",") if "=" in p)
                mounts[parts["dst"]] = str(volume) if parts["type"] == "volume" else parts["src"]
        index = argv.index("--entrypoint")
        command = [argv[index + 1], *argv[index + 3 :]]
        if command[0] == "sh":
            command[2] = re.sub(r"/original|/w\b", lambda m: shlex.quote(mounts[m[0]]), command[2])
        else:
            command = [mounts.get(arg, arg) for arg in command]
        return await real_spawn(*command, **kwargs)

    monkeypatch.setattr(node.asyncio, "create_subprocess_exec", spawn)
    return volume, calls


@pytest.mark.anyio
async def test_copy_back_replaces_tree_preserves_nested_metadata_and_deletions(
    tmp_path,
    local_tar_docker,
):
    from vuzol.execution.node import NodeVolumes

    volume, calls = local_tar_docker
    source = tmp_path / "worktree"
    source.mkdir()
    (source / "deleted").write_text("old")
    (source / ".git").write_text("gitdir: trusted")
    (volume / "new").write_text("new")
    (volume / "new").chmod(0o751)
    (volume / ".git").write_text("untrusted outer metadata")
    await NodeVolumes(tmp_path / "local.sock", tmp_path / "node.sock").copy_back(
        source,
        "volume",
        "image",
        protected=(Path(".git"),),
        max_bytes=100_000,
        cancellation=CancellationContext(),
    )
    assert sorted(p.name for p in source.iterdir()) == [".git", "new"]
    assert (source / ".git").read_text() == "gitdir: trusted"
    assert (source / "new").stat().st_mode & 0o777 == 0o751
    assert not list(tmp_path.glob(".worktree-*"))
    pack, unpack = calls[:2]
    assert pack[2] == f"unix://{tmp_path / 'node.sock'}"
    assert "--read-only" in pack and "none" in pack and "0:0" in pack
    assert unpack[2] == f"unix://{tmp_path / 'local.sock'}"
    assert all(cap in unpack for cap in ("CHOWN", "FOWNER", "DAC_OVERRIDE"))
    assert "--numeric-owner" in unpack and "--exclude=./.git" in unpack


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["cap", "cancelled", "lost", "timeout", "unpack"])
async def test_copy_back_failure_leaves_original_tree_untouched(
    tmp_path,
    local_tar_docker,
    monkeypatch,
    failure,
):
    from vuzol.execution import node

    volume, _calls = local_tar_docker
    source = tmp_path / "worktree"
    source.mkdir()
    (source / "old").write_text("keep")
    (volume / "new").write_bytes(b"x" * 100_000)
    cancellation = CancellationContext()
    if failure == "cancelled":
        cancellation.request()
    if failure == "timeout":
        monkeypatch.setattr(node, "COPY_BACK_TIMEOUT_SECONDS", 0)
    if failure == "lost":
        # A failed pack can still have sent a valid partial archive.
        spawn = node.asyncio.create_subprocess_exec

        async def failed_pack(*args, **kwargs):
            process = await spawn(*args, **kwargs)
            if args[2] == f"unix://{tmp_path / 'node.sock'}":
                process.wait = AsyncMock(return_value=1)
            return process

        monkeypatch.setattr(node.asyncio, "create_subprocess_exec", failed_pack)
    if failure == "unpack":
        # Extraction failure must not authorize the swap either.
        spawn = node.asyncio.create_subprocess_exec

        async def failed_unpack(*args, **kwargs):
            process = await spawn(*args, **kwargs)
            if "-xpf" in args:
                process.wait = AsyncMock(return_value=1)
            return process

        monkeypatch.setattr(node.asyncio, "create_subprocess_exec", failed_unpack)
    with pytest.raises(SandboxError):
        await node.NodeVolumes(tmp_path / "local.sock", tmp_path / "node.sock").copy_back(
            source,
            "volume",
            "image",
            protected=(),
            max_bytes=1 if failure == "cap" else 200_000,
            cancellation=cancellation,
        )
    assert [p.name for p in source.iterdir()] == ["old"]
    assert (source / "old").read_text() == "keep"


@pytest.mark.anyio
async def test_coding_runtime_refuses_extra_writable_mounts_before_docker(tmp_path):
    runtime = NodeDockerRuntime(
        "gcp",
        tmp_path / "node.sock",
        tmp_path / "local.sock",
        writable_worktree_only=True,
    )
    runtime._volumes = AsyncMock()
    with pytest.raises(SandboxError, match="only the task worktree"):
        await runtime.run(envelope(tmp_path), CancellationContext())
    runtime._volumes.create.assert_not_awaited()


@pytest.mark.anyio
async def test_node_worker_waits_for_socket_before_opening_database():
    from unittest.mock import MagicMock

    from vuzol.config import Settings
    from vuzol.workflows.worker import RoutedWorkflowWorker

    factory = MagicMock(side_effect=AssertionError("must not touch database"))
    worker = RoutedWorkflowWorker(
        Settings(),
        factory,
        registries=MagicMock(),
        owner="node",
        handlers={},
        profile_ids=frozenset({"pi"}),
        ready=lambda: False,
    )
    assert await worker.process_one() is False
    factory.assert_not_called()


@pytest.mark.parametrize("networked", [False, True])
def test_node_provider_composition_uses_pi_transport_and_extra_lanes(
    tmp_path, monkeypatch, networked
):
    from unittest.mock import MagicMock

    from tests.unit.projects.test_model_matrix import configured
    from vuzol.cli import executor
    from vuzol.config import Settings
    from vuzol.execution import codex

    settings = Settings(
        execution={
            "nodes": [
                {
                    "id": "gcp",
                    "docker_socket": str(tmp_path / "node.sock"),
                    "provider_parallelism": 2,
                    "model_call_parallelism": 0,
                    "max_copy_back_bytes": 1024,
                },
                {"id": "idle", "docker_socket": str(tmp_path / "idle.sock")},
            ]
        }
    )
    registries = MagicMock()
    registries.profiles.items.return_value = (
        configured("pi"),
        configured("codex", profile_id="oauth"),
    )
    registries.sandboxes.items.return_value = ()
    if networked:
        settings = settings.model_copy(
            update={
                "execution": settings.execution.model_copy(
                    update={"proxy_image": "sha256:" + "c" * 64}
                )
            }
        )
        registries.sandboxes.items.return_value = (
            MagicMock(enabled=True, image="sandbox@sha256:" + "d" * 64),
        )
    registry = MagicMock()
    monkeypatch.setattr(executor, "AdapterRegistry", MagicMock(return_value=registry))
    adapter = MagicMock()
    monkeypatch.setattr(executor, "cli_adapter", adapter)
    handler = MagicMock()
    monkeypatch.setattr(executor, "ProviderStepHandler", handler)
    envelope_factory = MagicMock()
    monkeypatch.setattr(executor, "ExecutionEnvelopeFactory", envelope_factory)
    groups = executor._node_providers(
        settings,
        registries,
        MagicMock(),
        MagicMock(),
        None,
        artifacts=MagicMock(),
        worktrees=MagicMock(),
        worktree_access=MagicMock(),
        owner="test",
    )
    assert len(groups) == 1
    node, worker, _reaper = groups[0]
    assert node.id == "gcp"
    assert worker._profile_ids == frozenset({"selected"})
    assert set(worker._handlers) == {"execute_code"}
    assert worker._ready() is False
    assert adapter.call_count == 1 and adapter.call_args.args[0].provider == "pi"
    transport = adapter.call_args.args[1]
    assert isinstance(transport, codex.SandboxCodexTransport)
    if networked:
        assert isinstance(transport._proxy, NodeProxyServiceManager)
    else:
        assert transport._proxy is None
    assert isinstance(transport._runtime, NodeDockerRuntime)
    assert transport._runtime._max_copy_back_bytes == 1024
    assert transport._runtime._writable_worktree_only is True
    assert envelope_factory.call_args.kwargs["node"] == "gcp"
    chain = executor.ExecutorChain(MagicMock(), MagicMock(), node_providers=(worker, worker))
    assert chain.lanes().count(worker) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["active", "expired", "terminal", "foreign", "lost"])
async def test_node_reaper_uses_existing_lease_authority(tmp_path, monkeypatch, state):
    from datetime import UTC, datetime, timedelta
    from unittest.mock import MagicMock

    from vuzol.execution import reconciliation
    from vuzol.storage.models import SupervisedProcess
    from vuzol.storage.types import ProcessStatus, RunStatus, StepStatus

    configured = envelope(tmp_path)
    process = SupervisedProcess(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        worktree_id=configured.worktree_id,
        lease_generation=configured.lease_generation,
        status=ProcessStatus.RUNNING,
        runtime_metadata={
            "node": "other" if state == "foreign" else "gcp",
            "runtime_envelope": configured.model_dump(mode="json", exclude={"stdin"}),
        },
    )
    session = AsyncMock()
    session.get.return_value = process
    factory = MagicMock()
    factory.begin.return_value.__aenter__.return_value = session
    now = datetime.now(UTC)
    authority = reconciliation.DurableLeaseState(
        identity_consistent=True,
        run_status=RunStatus.RUNNING,
        step_status=StepStatus.COMPLETED if state == "terminal" else StepStatus.RUNNING,
        current_generation=1,
        lease_owner="executor",
        heartbeat_at=now,
        lease_expires_at=now + timedelta(seconds=60 if state == "active" else -60),
    )
    monkeypatch.setattr(
        reconciliation.ProxyStartupReconciler,
        "_read_state_for_update",
        AsyncMock(return_value=authority),
    )
    runtime = AsyncMock()
    if state == "lost":
        runtime.cleanup.side_effect = SandboxError("node unreachable")
    reaper = reconciliation.NodeStepReconciler(factory, runtime, None, node="gcp", owner="test")
    reaped = await reaper._reap(configured.step_id)
    assert reaped is (state in {"expired", "terminal"})
    assert (process.reaped_at is not None) is reaped
    if state in {"active", "foreign"}:
        runtime.cleanup.assert_not_awaited()
    else:
        runtime.cleanup.assert_awaited_once_with(configured.model_copy(update={"stdin": ""}))


@pytest.mark.anyio
async def test_cancelled_helper_kills_and_joins_docker_client(tmp_path, monkeypatch):
    import asyncio
    from unittest.mock import MagicMock

    from vuzol.execution import node

    started = asyncio.Event()
    process = MagicMock()
    process.wait = AsyncMock(return_value=0)

    async def communicate(_stdin):
        started.set()
        await asyncio.Event().wait()

    process.communicate = communicate
    monkeypatch.setattr(node.asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    task = asyncio.create_task(node._docker(tmp_path / "local.sock", "run"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    process.kill.assert_called_once()
    process.wait.assert_awaited_once()


@pytest.mark.anyio
async def test_copy_back_preparation_failure_is_a_sandbox_error(tmp_path):
    from vuzol.execution.node import NodeVolumes

    with pytest.raises(SandboxError, match="copy-back"):
        await NodeVolumes(tmp_path / "local.sock", tmp_path / "node.sock").copy_back(
            tmp_path / "missing",
            "volume",
            "image",
            protected=(),
            max_bytes=1,
            cancellation=CancellationContext(),
        )


@pytest.mark.anyio
async def test_staging_and_copy_back_preserve_worktree_access_acls(tmp_path, local_tar_docker):
    import asyncio
    import os
    import subprocess

    from vuzol.execution.node import NodeVolumes

    volume, _calls = local_tar_docker
    source = tmp_path / "worktree"
    source.mkdir()
    target = source / "editable"
    target.write_text("edit me")
    await asyncio.to_thread(
        subprocess.run, ["/usr/bin/setfacl", "-m", f"u:{os.getuid()}:rw-", str(target)], check=True
    )
    acl = os.getxattr(target, "system.posix_acl_access")
    await asyncio.to_thread(
        subprocess.run,
        ["/usr/bin/setfacl", "-m", f"d:u:{os.getuid()}:rwx", str(source)],
        check=True,
    )
    default_acl = os.getxattr(source, "system.posix_acl_default")
    volumes = NodeVolumes(tmp_path / "local.sock", tmp_path / "node.sock")
    await volumes.copy(source, "volume", "image")
    assert os.getxattr(volume / "editable", "system.posix_acl_access") == acl
    assert os.getxattr(volume, "system.posix_acl_default") == default_acl
    await volumes.copy_back(
        source,
        "volume",
        "image",
        protected=(),
        max_bytes=100_000,
        cancellation=CancellationContext(),
    )
    assert os.getxattr(target, "system.posix_acl_access") == acl
    assert os.getxattr(source, "system.posix_acl_default") == default_acl


@pytest.mark.anyio
async def test_proxy_recovery_removes_only_its_sandbox_volumes(tmp_path, monkeypatch):
    from vuzol.execution.proxy_service import ProxyServiceManager

    configured = envelope(tmp_path)
    proxy = NodeProxyServiceManager(
        tmp_path / "node.sock",
        tmp_path / "local.sock",
        tmp_path / "proxy",
        "sha256:" + "c" * 64,
        helper_image="sandbox@sha256:" + "d" * 64,
    )
    proxy._volumes = AsyncMock()
    monkeypatch.setattr(ProxyServiceManager, "_remove_owned_sandbox", AsyncMock())
    await proxy._remove_owned_sandbox(
        configured.task_id,
        configured.run_id,
        configured.step_id,
        configured.lease_generation,
    )
    proxy._volumes.remove_owned.assert_awaited_once_with(configured.ownership_labels)


@pytest.mark.anyio
async def test_nested_metadata_is_staged_separately_and_excluded_from_copy_back(
    tmp_path, monkeypatch
):
    configured = envelope(tmp_path)
    outer, metadata = configured.sandbox.mounts
    metadata = metadata.model_copy(
        update={"target": Path("/workspace/.git"), "mode": MountMode.READ_ONLY}
    )
    configured = configured.model_copy(
        update={"sandbox": configured.sandbox.model_copy(update={"mounts": (outer, metadata)})}
    )
    runtime = NodeDockerRuntime("gcp", tmp_path / "node.sock", tmp_path / "local.sock")
    runtime._volumes = AsyncMock()
    runtime._volumes.copy.return_value = None

    async def run(_runtime, envelope, _cancellation):
        argv = runtime._run_argv(envelope.container_name, envelope)
        assert f"type=volume,src={envelope.container_name}-m0,dst=/workspace" in argv
        assert f"type=volume,src={envelope.container_name}-m1,dst=/workspace/.git,readonly" in argv
        return CodexProcessResult(0, "", "", 1)

    monkeypatch.setattr(RootlessDockerRuntime, "run", run)
    cancellation = CancellationContext()
    await runtime.run(configured, cancellation)
    assert runtime._volumes.copy.await_count == 2
    runtime._volumes.copy_back.assert_awaited_once_with(
        outer.source,
        f"{configured.container_name}-m0",
        configured.sandbox.image,
        protected=(Path(".git"),),
        max_bytes=2 * 1024**3,
        cancellation=cancellation,
    )


@pytest.mark.anyio
async def test_cancelled_staging_joins_both_helpers(tmp_path, monkeypatch):
    import asyncio
    from unittest.mock import MagicMock

    from vuzol.execution import node

    source = tmp_path / "source"
    source.mkdir()
    started = asyncio.Event()

    async def wait():
        started.set()
        await asyncio.Event().wait()

    processes = [MagicMock(wait=AsyncMock(side_effect=wait)) for _ in range(2)]
    spawn = AsyncMock(side_effect=processes)
    monkeypatch.setattr(node.asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(
        node.NodeVolumes(tmp_path / "local.sock", tmp_path / "node.sock").copy(
            source, "volume", "image"
        )
    )
    await started.wait()
    for process in processes:
        process.wait.side_effect = None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for process in processes:
        process.kill.assert_called_once()
        assert process.wait.await_count == 2


@pytest.mark.anyio
async def test_task_cancellation_during_copy_back_keeps_original(
    tmp_path, local_tar_docker, monkeypatch
):
    import asyncio

    from vuzol.execution import node

    volume, _calls = local_tar_docker
    source = tmp_path / "worktree"
    source.mkdir()
    (source / "old").write_text("keep")
    (volume / "new").write_text("remote")
    streaming = asyncio.Event()
    spawn = node.asyncio.create_subprocess_exec

    async def pause(*args, **kwargs):
        process = await spawn(*args, **kwargs)
        if args[2] == f"unix://{tmp_path / 'node.sock'}":

            async def read(_size):
                streaming.set()
                await asyncio.Event().wait()

            process.stdout.read = read
        return process

    monkeypatch.setattr(node.asyncio, "create_subprocess_exec", pause)
    task = asyncio.create_task(
        node.NodeVolumes(tmp_path / "local.sock", tmp_path / "node.sock").copy_back(
            source,
            "volume",
            "image",
            protected=(),
            max_bytes=100_000,
            cancellation=CancellationContext(),
        )
    )
    await streaming.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert [p.name for p in source.iterdir()] == ["old"]
    assert not list(tmp_path.glob(".worktree-*"))


@pytest.mark.anyio
async def test_failed_swap_restores_original(tmp_path, local_tar_docker, monkeypatch):
    from vuzol.execution.node import NodeVolumes

    volume, _calls = local_tar_docker
    source = tmp_path / "worktree"
    source.mkdir()
    (source / "old").write_text("keep")
    (volume / "new").write_text("remote")
    rename = Path.rename

    def fail_replacement(path, destination):
        if path.name.startswith(".worktree-node-"):
            raise OSError("rename failed")
        return rename(path, destination)

    monkeypatch.setattr(Path, "rename", fail_replacement)
    with pytest.raises(SandboxError, match="copy-back"):
        await NodeVolumes(tmp_path / "local.sock", tmp_path / "node.sock").copy_back(
            source,
            "volume",
            "image",
            protected=(),
            max_bytes=100_000,
            cancellation=CancellationContext(),
        )
    assert [p.name for p in source.iterdir()] == ["old"]
    assert not list(tmp_path.glob(".worktree-*"))
