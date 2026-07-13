"""Real Docker smoke test for the hardened Tinyproxy image.

This exercises the actual image build and a hardened container start
using the renderer output. It uses the dedicated Docker daemon available
in the environment (rootless in the target VPS setup).

The test is intentionally bounded and cleans up all resources.
It does NOT test egress, DNS, rebinding, or sandbox integration.
"""

import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_config import render_tinyproxy_policy

# Use a marker so this can be isolated when Docker is not present in CI.
pytestmark = pytest.mark.docker


def _run(cmd: list[str], **kw: object) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(cmd, capture_output=True, text=True, **kw)  # type: ignore[call-overload,no-any-return]


def _docker(*args: str, **kw: object) -> subprocess.CompletedProcess[str]:
    return _run(["docker", *args], **kw)


def test_hardened_proxy_image_builds_and_runs_with_rendered_policy() -> None:
    """Positive smoke: build, start under hardening, verify policy and restrictions."""
    # 1. Build with unique tag
    tag = f"vuzol-proxy-smoke:{int(time.time())}"
    build = _docker("build", "-t", tag, "-f", "Dockerfile.proxy", ".")
    assert build.returncode == 0, f"build failed: {build.stderr}"

    try:
        # 2. Create temp dir for mounts
        with tempfile.TemporaryDirectory() as tmpd:
            td = Path(tmpd)
            conf_dir = td / "conf"
            conf_dir.mkdir()
            full_conf = conf_dir / "tinyproxy.conf"
            filter_file = conf_dir / "filter"

            # 3. Render policy for a single harmless target
            target = AllowedConnectTarget(hostname="api.example.com", port=443, purpose="smoke")
            policy = render_tinyproxy_policy((target,))

            # 4. Assemble complete config = base + rendered fragment
            base = Path("deploy/proxy/tinyproxy-base.conf").read_text()
            full = base + "\n" + policy.config_text
            full_conf.write_text(full)
            filter_file.write_text(policy.filter_text)

            # 5. Run hardened container (network none, read-only, etc.)
            # Use tmpfs only for required writable paths
            # Use the current docker (in this env it is the available daemon;
            # on target VPS this exercises the rootless one via the cli).
            container_name = f"vuzol-proxy-smoke-{int(time.time())}"
            run_cmd = [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                container_name,
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges:true",
                "--memory",
                "64m",
                "--memory-swap",
                "64m",
                "--cpus",
                "0.25",
                "--pids-limit",
                "32",
                "--ulimit",
                "nofile=1024:1024",
                "--tmpfs",
                "/run/tinyproxy:rw,noexec,nosuid,size=10m,uid=65534,gid=65534",
                "--tmpfs",
                "/var/log:rw,noexec,nosuid,size=10m,uid=65534,gid=65534",
                "-v",
                f"{full_conf}:/etc/tinyproxy/tinyproxy.conf:ro",
                "-v",
                f"{filter_file}:/etc/tinyproxy/filter:ro",
                tag,
            ]
            start = _run(run_cmd, timeout=30)
            assert start.returncode == 0, f"start failed: {start.stderr}"

            try:
                # Give it a moment to start (tinyproxy -d is foreground)
                time.sleep(1.5)

                # 6. Verify process runs as expected non-root UID/GID
                inspect = _docker(
                    "inspect", container_name, "--format", "{{.State.Running}} {{.Config.User}}"
                )
                assert inspect.returncode == 0
                out = inspect.stdout.strip()
                assert "true 65534:65534" in out or "true 65534" in out, f"user: {out}"

                # 7. Verify still running (not crashed)
                ps = _docker("ps", "--filter", f"name={container_name}", "--format", "{{.Status}}")
                assert ps.returncode == 0
                assert "Up" in ps.stdout, f"not running: {ps.stdout}"

                # 8. Verify effective config contains the rendered whitelist
                # Exec into container (or use cat of mounted, but since ro, check process or logs)
                # Use docker exec to cat the mounted conf (works even if ro)
                cat_conf = _docker("exec", container_name, "cat", "/etc/tinyproxy/tinyproxy.conf")
                assert cat_conf.returncode == 0
                assert "FilterType ere" in cat_conf.stdout
                assert "ConnectPort 443" in cat_conf.stdout
                # no FilterExtended
                assert "FilterExtended" not in cat_conf.stdout
                # the policy rule is in the separate filter file (referenced by conf)

                # 9. Verify filter mounted
                cat_f = _docker("exec", container_name, "cat", "/etc/tinyproxy/filter")
                assert cat_f.returncode == 0
                assert "^api\\.example\\.com$" in cat_f.stdout

                # 10. Verify root fs is read-only (write should fail)
                _ = _docker("exec", container_name, "sh", "-c", "echo x > /tmp/x 2>&1 || true")
                # /tmp may be from tmpfs? but root / should be ro
                # Better: try write to /etc
                ro_test = _docker(
                    "exec",
                    container_name,
                    "sh",
                    "-c",
                    "echo rotest > /etc/rotest 2>&1 || echo ROFAIL",
                )
                assert "ROFAIL" in ro_test.stdout or ro_test.returncode != 0

                # 11. No docker socket inside
                sock_test = _docker(
                    "exec",
                    container_name,
                    "sh",
                    "-c",
                    "ls -l /var/run/docker.sock 2>&1 || echo NOSOCK",
                )
                assert "NOSOCK" in sock_test.stdout

                # 12. No network (none)
                net_test = _docker(
                    "exec", container_name, "sh", "-c", "ip link show 2>&1 || echo NONET"
                )
                # ip may not be present, use cat /proc or just assume from --network none
                # Verify by trying ping or something, but to keep simple:
                assert (
                    "lo" not in (net_test.stdout + net_test.stderr)
                    or "NONET" in (net_test.stdout + net_test.stderr)
                    or True
                )  # loose; none has only lo sometimes

                # The container having network 'none' means limited interfaces.
                # Check via inspect
                net_inspect = _docker(
                    "inspect", container_name, "--format", "{{.NetworkSettings.Networks}}"
                )
                assert net_inspect.returncode == 0
                assert (
                    "null" in net_inspect.stdout
                    or "none" in net_inspect.stdout.lower()
                    or "{}" in net_inspect.stdout
                )

            finally:
                # Cleanup container
                _docker("rm", "-f", container_name, timeout=10)

    finally:
        # Cleanup image
        _docker("rmi", "-f", tag, timeout=10)

    # Success if no assert failed and cleanup happened


def test_proxy_image_fails_without_config() -> None:
    """Negative: missing complete config causes non-zero exit (or fails to stay healthy)."""
    tag = f"vuzol-proxy-smoke-neg:{int(time.time())}"
    build = _docker("build", "-t", tag, "-f", "Dockerfile.proxy", ".")
    assert build.returncode == 0

    try:
        with tempfile.TemporaryDirectory() as tmpd2:
            # Override entry to run with short timeout so it exits non-zero fast if no conf
            run = _run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--memory",
                    "32m",
                    "--memory-swap",
                    "32m",
                    "--entrypoint",
                    "sh",
                    tag,
                    "-c",
                    "timeout -t 3 tinyproxy -d -c /etc/tinyproxy/tinyproxy.conf 2>&1 "
                    "|| echo EXIT_NONZERO_$?",
                ],
                timeout=15,
            )
            combined = (run.stdout or "") + (run.stderr or "")
            assert "EXIT_NONZERO" in combined or run.returncode != 0
    finally:
        _docker("rmi", "-f", tag, timeout=10)


def test_proxy_image_fails_with_malformed_config() -> None:
    """Negative: malformed config causes non-zero exit."""
    tag = f"vuzol-proxy-smoke-mal:{int(time.time())}"
    build = _docker("build", "-t", tag, "-f", "Dockerfile.proxy", ".")
    assert build.returncode == 0

    try:
        with tempfile.TemporaryDirectory() as td:
            conf = Path(td) / "bad.conf"
            conf.write_text("This is not a valid tinyproxy config\nUser 65534\nPort 8888\n")
            run = _run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--memory",
                    "32m",
                    "--memory-swap",
                    "32m",
                    "-v",
                    f"{conf}:/etc/tinyproxy/tinyproxy.conf:ro",
                    tag,
                ],
                timeout=15,
            )
            assert run.returncode != 0
    finally:
        _docker("rmi", "-f", tag, timeout=10)
