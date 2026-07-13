"""Real migrated-rootless acceptance for the production proxy service manager."""

import asyncio
import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.codex import SandboxCodexTransport
from vuzol.execution.domain import ProcessEnvelope, SandboxSpec
from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_service import ProxyServiceManager, _make_proxy_name
from vuzol.execution.sandbox import RootlessDockerRuntime, SandboxError
from vuzol.workflows.ports import CancellationContext

pytestmark = pytest.mark.docker


def _required_path(name: str) -> Path:
    value = os.environ.get(name)
    if value is None:
        pytest.skip(f"{name} is required for explicit rootless proxy acceptance")
    return Path(value)


def _seccomp_profile() -> tuple[Path, str]:
    path = _required_path("VUZOL_SANDBOX_SECCOMP_PROFILE")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _docker(socket: Path, *args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "--host", f"unix://{socket}", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def test_real_proxy_service_lifecycle_and_route_isolation() -> None:
    socket = _required_path("VUZOL_ROOTLESS_DOCKER_SOCKET")
    runtime_root = _required_path("VUZOL_PROXY_RUNTIME_ROOT")
    image = os.environ.get("VUZOL_PROXY_IMAGE")
    sandbox_image = os.environ.get("VUZOL_SANDBOX_TEST_IMAGE", "vuzol-sandbox:local")
    if image is None:
        pytest.skip("VUZOL_PROXY_IMAGE is required for explicit rootless proxy acceptance")
    assert socket.stat().st_uid == os.geteuid(), "test must run as the rootless daemon identity"

    manager = ProxyServiceManager(socket, runtime_root, image)
    task_id, run_id, step_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    lease = asyncio.run(
        manager.create(
            task_id,
            run_id,
            step_id,
            1,
            (AllowedConnectTarget(hostname="api.openai.com", port=443, purpose="acceptance"),),
        )
    )
    try:
        inspected = _docker(socket, "inspect", lease.container_name)
        assert inspected.returncode == 0, inspected.stderr
        data: dict[str, Any] = json.loads(inspected.stdout)[0]
        attached = data["NetworkSettings"]["Networks"]
        assert set(attached) == {lease.networks.internal_name, lease.networks.egress_name}
        assert data["HostConfig"]["PortBindings"] == {}
        assert data["Config"].get("ExposedPorts") is None

        connect_script = """
const net=require('net'), tls=require('tls');
const raw=net.connect(8888,'vuzol-proxy');
let head=Buffer.alloc(0); const timer=setTimeout(()=>process.exit(10),15000);
raw.on('connect',()=>raw.write(
  'CONNECT api.openai.com:443 HTTP/1.1\\r\\nHost: api.openai.com:443\\r\\n\\r\\n'));
raw.on('data',function first(chunk){
  head=Buffer.concat([head,chunk]); const end=head.indexOf('\\r\\n\\r\\n'); if(end<0)return;
  raw.removeListener('data',first);
  if(!head.subarray(0,end).toString().includes('200'))process.exit(11);
  const secure=tls.connect({socket:raw,servername:'api.openai.com'},()=>{
    secure.write('HEAD / HTTP/1.1\\r\\nHost: api.openai.com\\r\\nConnection: close\\r\\n\\r\\n');
  });
  secure.once('data',data=>{
    clearTimeout(timer); process.exit(data.toString().startsWith('HTTP/')?0:12)
  });
  secure.on('error',()=>process.exit(13));
});
raw.on('error',()=>process.exit(14));
"""
        proxied = _docker(
            socket,
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            lease.networks.internal_name,
            sandbox_image,
            "node",
            "-e",
            connect_script,
        )
        assert proxied.returncode == 0, proxied.stderr

        direct_script = """
const net=require('net'); let connected=false;
const s=net.connect(443,'1.1.1.1',()=>{connected=true; process.exit(20)});
s.on('error',()=>process.exit(0));
setTimeout(()=>process.exit(connected?21:0),3000);
"""
        direct = _docker(
            socket,
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            lease.networks.internal_name,
            sandbox_image,
            "node",
            "-e",
            direct_script,
        )
        assert direct.returncode == 0, "internal-only client unexpectedly had direct egress"
    finally:
        asyncio.run(manager.cleanup(lease))

    assert _docker(socket, "inspect", lease.container_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.internal_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.egress_name).returncode != 0
    assert not runtime_root.exists() or not any(runtime_root.iterdir())


def test_real_proxy_exact_recovery_cleanup_removes_crash_leftovers() -> None:
    socket = _required_path("VUZOL_ROOTLESS_DOCKER_SOCKET")
    runtime_root = _required_path("VUZOL_PROXY_RUNTIME_ROOT")
    image = os.environ.get("VUZOL_PROXY_IMAGE")
    if image is None:
        pytest.skip("VUZOL_PROXY_IMAGE is required for explicit rootless proxy acceptance")
    assert socket.stat().st_uid == os.geteuid(), "test must run as the rootless daemon identity"

    manager = ProxyServiceManager(socket, runtime_root, image)
    task_id, run_id, step_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    lease = asyncio.run(
        manager.create(
            task_id,
            run_id,
            step_id,
            1,
            (AllowedConnectTarget(hostname="api.openai.com", port=443, purpose="acceptance"),),
        )
    )
    # Deliberately do not call lease cleanup: this is the state left by a
    # killed executor. A fresh production manager must recover it from disk.
    restarted = ProxyServiceManager(socket, runtime_root, image)
    manifest = restarted.recovery_manifests()[0]
    asyncio.run(restarted.validate_recovery_resources(manifest))
    asyncio.run(restarted.cleanup_recovery_manifest(manifest))
    assert restarted.recovery_manifests() == ()
    assert _docker(socket, "inspect", lease.container_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.internal_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.egress_name).returncode != 0
    assert not runtime_root.exists() or not any(runtime_root.iterdir())


def test_real_production_sandbox_negative_network_matrix() -> None:
    socket = _required_path("VUZOL_ROOTLESS_DOCKER_SOCKET")
    runtime_root = _required_path("VUZOL_PROXY_RUNTIME_ROOT")
    image = os.environ.get("VUZOL_PROXY_IMAGE")
    sandbox_tag = os.environ.get("VUZOL_SANDBOX_TEST_IMAGE", "vuzol-sandbox:local")
    if image is None:
        pytest.skip("VUZOL_PROXY_IMAGE is required for explicit rootless proxy acceptance")
    sandbox_inspect = _docker(socket, "image", "inspect", sandbox_tag)
    assert sandbox_inspect.returncode == 0, sandbox_inspect.stderr
    sandbox_id = json.loads(sandbox_inspect.stdout)[0]["Id"]
    sandbox_image = f"vuzol-sandbox@{sandbox_id}"
    seccomp_profile, seccomp_digest = _seccomp_profile()

    manager = ProxyServiceManager(socket, runtime_root, image)
    identity = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), 1
    lease = asyncio.run(
        manager.create(
            *identity,
            (AllowedConnectTarget(hostname="api.openai.com", port=443, purpose="acceptance"),),
        )
    )
    network_inspect = _docker(socket, "network", "inspect", lease.networks.internal_name)
    assert network_inspect.returncode == 0, network_inspect.stderr
    internal_gateway = json.loads(network_inspect.stdout)[0]["IPAM"]["Config"][0]["Gateway"]
    matrix_script = r"""
const net = require('net');
const dns = require('dns');
const expected = 'http://vuzol-proxy:8888';
for (const key of ['HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy']) {
  if (process.env[key] !== expected) throw new Error('bad proxy variable '+key);
}
for (const key of ['ALL_PROXY','NO_PROXY','all_proxy','no_proxy']) {
  if (process.env[key] !== '') throw new Error('uncleared bypass variable '+key);
}
function denied(authority) { return new Promise((resolve,reject) => {
  const socket = net.connect(8888, 'vuzol-proxy'); let response='';
  const timer=setTimeout(()=>{
    socket.destroy();reject(new Error('proxy denial timeout '+authority))
  },4000);
  socket.on('connect',()=>socket.write(
    `CONNECT ${authority} HTTP/1.1\r\nHost: ${authority}\r\n\r\n`));
  socket.on('data',chunk=>{response+=chunk; if(response.includes('\r\n\r\n')) {
    clearTimeout(timer); socket.destroy();
    response.startsWith('HTTP/1.1 403') ? resolve() : reject(new Error('not denied '+authority));
  }});
  socket.on('error',reject);
}); }
function noDirect(host, port=443) { return new Promise((resolve,reject) => {
  const socket=net.connect({host,port});
  const timer=setTimeout(()=>{socket.destroy();resolve()},1500);
  socket.on('connect',()=>{
    clearTimeout(timer);socket.destroy();reject(new Error('direct route '+host))
  });
  socket.on('error',()=>{clearTimeout(timer);resolve()});
}); }
function noAlternateDns() { return new Promise((resolve,reject) => {
  const resolver=new dns.Resolver(); resolver.setServers(['8.8.8.8']);
  const timer=setTimeout(()=>{resolver.cancel();resolve()},2000);
  resolver.resolve4('example.com',(error)=>{clearTimeout(timer);
    error ? resolve() : reject(new Error('alternate DNS succeeded'))});
}); }
(async()=>{
  for (const target of [
    'example.com:443','1.1.1.1:443','[2606:4700:4700::1111]:443',
    '127.0.0.1:443','[::1]:443','10.0.0.1:443','172.16.0.1:443',
    '192.168.0.1:443','169.254.169.254:443','224.0.0.1:443',
    '0.0.0.0:443','host.docker.internal:443','api.openai.com:80'
  ]) await denied(target);
  // This is the second CONNECT a client would issue after an approved HTTPS
  // response redirects it to an unapproved origin.
  await denied('redirect.invalid:443');
  for (const host of ['1.1.1.1','2606:4700:4700::1111','169.254.169.254']) await noDirect(host);
  await noDirect(process.env.TEST_INTERNAL_GATEWAY, 2375);
  await noDirect('host.docker.internal', 443);
  await noAlternateDns();
  console.log('NEGATIVE_MATRIX_OK');
})().catch(error=>{console.error(error.message);process.exit(1)});
"""
    spec = SandboxSpec(
        image=sandbox_image,
        uid=10001,
        gid=10001,
        seccomp_profile=seccomp_profile,
        seccomp_profile_sha256=seccomp_digest,
        working_directory=Path("/workspace"),
        mounts=(),
        cpu_count=0.25,
        memory_bytes=67_108_864,
        pids_limit=32,
        tmpfs_bytes=16_777_216,
        open_files_limit=256,
        output_bytes=32_768,
        timeout_seconds=30,
        stop_grace_seconds=2,
        network_disabled=False,
        proxy_network=lease.networks.internal_name,
        https_proxy_url=lease.proxy_url,
        environment={"TEST_INTERNAL_GATEWAY": internal_gateway},
    )
    envelope = ProcessEnvelope(
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        worktree_id=uuid.uuid4(),
        profile_id="acceptance",
        provider_attempt=1,
        lease_generation=identity[3],
        argv=("node", "-e", matrix_script),
        stdin="",
        sandbox=spec,
    )
    try:
        result = asyncio.run(RootlessDockerRuntime(socket).run(envelope, CancellationContext()))
        assert result.exit_code == 0, result.stderr
        assert result.stdout.strip() == "NEGATIVE_MATRIX_OK"
    finally:
        asyncio.run(manager.cleanup(lease))

    assert _docker(socket, "inspect", lease.container_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.internal_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.egress_name).returncode != 0
    assert not runtime_root.exists() or not any(runtime_root.iterdir())


@pytest.mark.parametrize("termination", ["cancellation", "proxy_outage"])
def test_real_controlled_egress_abnormal_exit_cleans_every_resource(termination: str) -> None:
    socket = _required_path("VUZOL_ROOTLESS_DOCKER_SOCKET")
    runtime_root = _required_path("VUZOL_PROXY_RUNTIME_ROOT")
    image = os.environ.get("VUZOL_PROXY_IMAGE")
    sandbox_tag = os.environ.get("VUZOL_SANDBOX_TEST_IMAGE", "vuzol-sandbox:local")
    if image is None:
        pytest.skip("VUZOL_PROXY_IMAGE is required for explicit rootless proxy acceptance")
    sandbox_inspect = _docker(socket, "image", "inspect", sandbox_tag)
    assert sandbox_inspect.returncode == 0, sandbox_inspect.stderr
    sandbox_id = json.loads(sandbox_inspect.stdout)[0]["Id"]
    sandbox_image = f"vuzol-sandbox@{sandbox_id}"
    seccomp_profile, seccomp_digest = _seccomp_profile()
    identity = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), 1
    process_id = uuid.uuid4()
    invocation = MagicMock(
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        lease_generation=identity[3],
    )
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(
        return_value=(
            AllowedConnectTarget(hostname="api.openai.com", port=443, purpose="acceptance"),
        )
    )

    async def build(
        _invocation: object, *, proxy_network: str, https_proxy_url: str
    ) -> tuple[ProcessEnvelope, uuid.UUID]:
        spec = SandboxSpec(
            image=sandbox_image,
            uid=10001,
            gid=10001,
            seccomp_profile=seccomp_profile,
            seccomp_profile_sha256=seccomp_digest,
            working_directory=Path("/workspace"),
            mounts=(),
            cpu_count=0.25,
            memory_bytes=67_108_864,
            pids_limit=32,
            tmpfs_bytes=16_777_216,
            open_files_limit=256,
            output_bytes=4096,
            timeout_seconds=30,
            stop_grace_seconds=1,
            network_disabled=False,
            proxy_network=proxy_network,
            https_proxy_url=https_proxy_url,
            environment={},
        )
        return (
            ProcessEnvelope(
                task_id=identity[0],
                run_id=identity[1],
                step_id=identity[2],
                worktree_id=uuid.uuid4(),
                profile_id="acceptance",
                provider_attempt=1,
                lease_generation=identity[3],
                argv=("node", "-e", "setInterval(()=>{},1000)"),
                stdin="",
                sandbox=spec,
            ),
            process_id,
        )

    envelopes.build = AsyncMock(side_effect=build)
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    manager = ProxyServiceManager(socket, runtime_root, image)
    transport = SandboxCodexTransport(
        RootlessDockerRuntime(socket), envelopes, MagicMock(), manager
    )
    cancellation = CancellationContext()
    sandbox_name = f"vuzol-{str(identity[2])[:12]}-{identity[3]}"

    async def scenario() -> None:
        task = asyncio.create_task(transport.run(invocation, cancellation))
        for _attempt in range(300):
            if task.done():
                await task
            inspected = await asyncio.to_thread(_docker, socket, "inspect", sandbox_name)
            if inspected.returncode == 0:
                break
            await asyncio.sleep(0.1)
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            pytest.fail("production sandbox did not start before cancellation deadline")
        if termination == "cancellation":
            cancellation.request()
            with pytest.raises(SandboxError, match="cancelled"):
                await task
        else:
            stopped = await asyncio.to_thread(_docker, socket, "stop", _make_proxy_name(*identity))
            assert stopped.returncode == 0, stopped.stderr
            with pytest.raises(RuntimeError, match="proxy exited"):
                await task

    asyncio.run(scenario())
    envelopes.fail_unknown.assert_awaited_once_with(process_id)
    assert _docker(socket, "inspect", sandbox_name).returncode != 0
    managed_containers = _docker(
        socket, "ps", "-a", "--filter", "label=vuzol.managed=true", "--format", "{{.Names}}"
    )
    managed_networks = _docker(
        socket,
        "network",
        "ls",
        "--filter",
        "label=vuzol.managed=true",
        "--format",
        "{{.Name}}",
    )
    assert managed_containers.returncode == 0 and not managed_containers.stdout.strip()
    assert managed_networks.returncode == 0 and not managed_networks.stdout.strip()
    assert not runtime_root.exists() or not any(runtime_root.iterdir())
