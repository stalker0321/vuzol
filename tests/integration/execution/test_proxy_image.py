"""Authoritative Docker smoke tests for the CONNECT-only proxy image."""

import json
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.docker


def _docker(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


@pytest.fixture(scope="module")
def proxy_image() -> Iterator[str]:
    tag = f"vuzol-connect-proxy-smoke-{uuid.uuid4().hex[:12]}"
    build = _docker("build", "--pull=false", "-t", tag, "-f", "Dockerfile.proxy", ".", timeout=300)
    assert build.returncode == 0, build.stderr
    try:
        yield tag
    finally:
        remove = _docker("image", "rm", tag)
        assert remove.returncode == 0, remove.stderr
        assert _docker("image", "inspect", tag).returncode != 0


def _policy(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "targets": [{"hostname": "api.openai.com", "port": 443}],
                "connect_timeout_seconds": 5,
                "idle_timeout_seconds": 30,
                "tunnel_timeout_seconds": 300,
                "maximum_bytes_per_direction": 67_108_864,
            }
        )
    )
    # The containing directory is 0700. The bind-mounted policy itself is
    # world-readable only inside the container user namespace and has no secrets.
    path.chmod(0o444)


def _hardened_run_args(name: str, policy: Path) -> list[str]:
    return [
        "run",
        "-d",
        "--name",
        name,
        "--network",
        "none",
        "--read-only",
        "--user",
        "10002:10002",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--memory",
        "67108864",
        "--memory-swap",
        "67108864",
        "--cpus",
        "0.25",
        "--pids-limit",
        "32",
        "--ulimit",
        "nofile=1024:1024",
        "--mount",
        f"type=bind,src={policy.resolve()},dst=/etc/vuzol-proxy/policy.json,readonly",
    ]


def test_connect_proxy_image_starts_and_enforces_hardening(proxy_image: str) -> None:
    name = f"vuzol-connect-proxy-{uuid.uuid4().hex[:12]}"
    with tempfile.TemporaryDirectory(prefix="vuzol-connect-policy-") as directory:
        policy = Path(directory) / "policy.json"
        _policy(policy)
        run = _docker(*_hardened_run_args(name, policy), proxy_image)
        assert run.returncode == 0, run.stderr
        try:
            inspect_result = _docker("inspect", name)
            assert inspect_result.returncode == 0, inspect_result.stderr
            data: dict[str, Any] = json.loads(inspect_result.stdout)[0]
            host = data["HostConfig"]
            config = data["Config"]
            assert data["State"]["Running"] is True
            assert config["User"] == "10002:10002"
            assert host["ReadonlyRootfs"] is True
            assert host["CapDrop"] == ["ALL"]
            assert "no-new-privileges:true" in host["SecurityOpt"]
            assert host["Memory"] == 67_108_864 and host["MemorySwap"] == 67_108_864
            assert host["NanoCpus"] == 250_000_000
            assert host["PidsLimit"] == 32
            assert host["NetworkMode"] == "none"
            assert config.get("ExposedPorts") is None
            assert host["PortBindings"] == {}
            assert len(data["Mounts"]) == 1
            mount = data["Mounts"][0]
            assert mount["Destination"] == "/etc/vuzol-proxy/policy.json"
            assert mount["RW"] is False

            for _attempt in range(50):
                health = _docker(
                    "exec",
                    name,
                    "python",
                    "-c",
                    "import socket; s=socket.create_connection(('127.0.0.1',8888)); "
                    "s.sendall(b'GET /healthz HTTP/1.1\\r\\nHost: proxy\\r\\n\\r\\n'); "
                    "assert b'204 No Content' in s.recv(1024)",
                )
                if health.returncode == 0:
                    break
                time.sleep(0.1)
            assert health.returncode == 0, health.stderr
        finally:
            remove = _docker("rm", "-f", name)
            assert remove.returncode == 0, remove.stderr
            assert _docker("inspect", name).returncode != 0


def test_connect_proxy_image_fails_closed_without_policy(proxy_image: str) -> None:
    result = _docker("run", "--rm", "--network", "none", proxy_image)
    assert result.returncode != 0
    assert "policy" in (result.stdout + result.stderr).lower()


def test_connect_proxy_image_fails_closed_with_malformed_policy(proxy_image: str) -> None:
    with tempfile.TemporaryDirectory(prefix="vuzol-connect-policy-") as directory:
        policy = Path(directory) / "policy.json"
        policy.write_text('{"version":1,"targets":[]}')
        policy.chmod(0o444)
        result = _docker(
            "run",
            "--rm",
            "--network",
            "none",
            "--mount",
            f"type=bind,src={policy.resolve()},dst=/etc/vuzol-proxy/policy.json,readonly",
            proxy_image,
        )
        assert result.returncode != 0
        assert "policy" in (result.stdout + result.stderr).lower()
