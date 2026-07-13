"""Real migrated-rootless acceptance for the production proxy service manager."""

import asyncio
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_service import ProxyServiceManager

pytestmark = pytest.mark.docker


def _required_path(name: str) -> Path:
    value = os.environ.get(name)
    if value is None:
        pytest.skip(f"{name} is required for explicit rootless proxy acceptance")
    return Path(value)


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
