"""Exercise Node's actual fetch through the production controlled-egress runtime."""

import asyncio
import json
import os
import uuid
from pathlib import Path

import pytest

from vuzol.execution.domain import ProcessEnvelope, SandboxSpec
from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_service import ProxyServiceManager
from vuzol.execution.sandbox import RootlessDockerRuntime
from vuzol.workflows.ports import CancellationContext

from .test_proxy_service import _docker, _required_path, _seccomp_profile

pytestmark = pytest.mark.docker


def test_node_fetch_uses_controlled_proxy_without_direct_egress() -> None:
    socket = _required_path("VUZOL_ROOTLESS_DOCKER_SOCKET")
    runtime_root = _required_path("VUZOL_PROXY_RUNTIME_ROOT")
    proxy_image = os.environ.get("VUZOL_PROXY_IMAGE")
    sandbox_image = os.environ.get("VUZOL_SANDBOX_TEST_IMAGE")
    if proxy_image is None or sandbox_image is None:
        pytest.skip("explicit pinned proxy and sandbox images are required")
    assert socket.stat().st_uid == os.geteuid(), "run as the rootless daemon identity"
    seccomp_profile, seccomp_digest = _seccomp_profile()
    manager = ProxyServiceManager(socket, runtime_root, proxy_image)
    identity = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), 1
    lease = asyncio.run(
        manager.create(
            *identity,
            (AllowedConnectTarget(hostname="opencode.ai", port=443, purpose="acceptance"),),
        )
    )
    script = r"""
const net = require('node:net');
async function deniedFetch() {
  try {
    await fetch('https://example.com/', {signal: AbortSignal.timeout(5000)});
    throw new Error('unapproved origin reached');
  } catch (error) {
    if (error.message === 'unapproved origin reached') throw error;
    if (error.name === 'TimeoutError') throw new Error('proxy did not promptly deny origin');
  }
}
function deniedDirect() { return new Promise((resolve, reject) => {
  const socket = net.connect(443, '1.1.1.1');
  const timer = setTimeout(() => { socket.destroy(); resolve(); }, 2000);
  socket.on('connect', () => {
    clearTimeout(timer); socket.destroy(); reject(new Error('direct egress available'));
  });
  socket.on('error', () => { clearTimeout(timer); resolve(); });
}); }
(async () => {
  if (process.env.NODE_USE_ENV_PROXY !== '1') throw new Error('Node proxy opt-in missing');
  // Public metadata only: no credentials, inference, token use or provider charges.
  const response = await fetch('https://opencode.ai/inference/go/v1/models', {
    signal: AbortSignal.timeout(15000)
  });
  if (!response.ok) throw new Error('provider metadata HTTP ' + response.status);
  const body = await response.json();
  if (!Array.isArray(body.data) || body.data.length === 0) {
    throw new Error('provider model metadata missing');
  }
  await deniedFetch();
  await deniedDirect();
  console.log('NODE_CONTROLLED_PROXY_OK');
})().catch(error => {
  console.error(error.message, error.cause?.code || '', error.cause?.message || '');
  process.exit(1);
});
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
        memory_bytes=134_217_728,
        pids_limit=64,
        tmpfs_bytes=16_777_216,
        open_files_limit=256,
        output_bytes=4096,
        timeout_seconds=30,
        stop_grace_seconds=1,
        network_disabled=False,
        proxy_network=lease.networks.internal_name,
        https_proxy_url=lease.proxy_url,
        environment={},
    )
    envelope = ProcessEnvelope(
        task_id=identity[0],
        run_id=identity[1],
        step_id=identity[2],
        worktree_id=uuid.uuid4(),
        profile_id="acceptance",
        provider_attempt=1,
        lease_generation=identity[3],
        argv=("node", "-e", script),
        stdin="",
        sandbox=spec,
    )
    try:
        inspected = _docker(socket, "network", "inspect", lease.networks.internal_name)
        assert inspected.returncode == 0, inspected.stderr
        assert json.loads(inspected.stdout)[0]["Internal"] is True
        result = asyncio.run(RootlessDockerRuntime(socket).run(envelope, CancellationContext()))
        assert result.exit_code == 0, result.stderr
        assert result.stdout.strip() == "NODE_CONTROLLED_PROXY_OK"
    finally:
        asyncio.run(manager.cleanup(lease))
    assert _docker(socket, "inspect", lease.container_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.internal_name).returncode != 0
    assert _docker(socket, "network", "inspect", lease.networks.egress_name).returncode != 0
