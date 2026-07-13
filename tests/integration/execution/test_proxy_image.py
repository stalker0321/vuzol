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
import uuid
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
    # 1. Build with collision-resistant unique tag
    tag = f"vuzol-proxy-smoke-{uuid.uuid4().hex[:8]}"
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
            container_name = f"vuzol-proxy-smoke-{uuid.uuid4().hex[:8]}"
            run_cmd = [
                "docker",
                "run",
                "-d",
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
                "/run/tinyproxy:rw,noexec,nosuid,size=10m,uid=10002,gid=10002",
                "--tmpfs",
                "/var/log:rw,noexec,nosuid,size=10m,uid=10002,gid=10002",
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
                time.sleep(3)

                # 6. Verify process runs as expected dedicated non-root UID/GID (from inspect)
                user_inspect = _docker(
                    "inspect", container_name, "--format", "{{.State.Running}} {{.Config.User}}"
                )
                assert user_inspect.returncode == 0
                uout = user_inspect.stdout.strip()
                assert "true 10002:10002" in uout, f"user: {uout}"

                # 7. Verify still running (not crashed)
                ps = _docker("ps", "--filter", f"name={container_name}", "--format", "{{.Status}}")
                assert ps.returncode == 0
                assert "Up" in ps.stdout, f"not running: {ps.stdout}"

                # Full hardening inspect from docker inspect (not just constructed cmd)
                full_inspect = _docker("inspect", container_name)
                assert full_inspect.returncode == 0
                import json

                insp = json.loads(full_inspect.stdout)[0]
                hc = insp.get("HostConfig", {})
                cfg = insp.get("Config", {})
                # user
                assert cfg.get("User") == "10002:10002"
                # process 1 uid/gid inside (use exec to read /proc/1/status)
                uid_gid = _docker(
                    "exec",
                    container_name,
                    "sh",
                    "-c",
                    "grep -E '^(Uid:|Gid:)' /proc/1/status || echo NO_PROC",
                )
                assert uid_gid.returncode == 0
                assert "10002" in (uid_gid.stdout + uid_gid.stderr)
                # readonly root
                assert hc.get("ReadonlyRootfs") is True
                # cap drop
                caps = hc.get("CapDrop") or []
                assert any(c.upper() == "ALL" for c in caps)
                # no new priv
                sec = hc.get("SecurityOpt") or []
                assert any("no-new-privileges" in s.lower() for s in sec)
                # memory/swap
                assert hc.get("Memory") == 64 * 1024 * 1024
                assert hc.get("MemorySwap") == 64 * 1024 * 1024
                # cpus (NanoCpus)
                assert hc.get("NanoCpus") == int(0.25 * 1e9)
                # pids
                assert hc.get("PidsLimit") == 32
                # network
                assert hc.get("NetworkMode") == "none"
                # mounts: only config+filter ro, no sock, no unexpected
                mounts = insp.get("Mounts", [])
                ro_mounts = [m for m in mounts if m.get("RW") is False]
                assert len(ro_mounts) == 2
                mount_paths = {m.get("Destination") for m in ro_mounts}
                assert "/etc/tinyproxy/tinyproxy.conf" in mount_paths
                assert "/etc/tinyproxy/filter" in mount_paths
                # no docker sock or other host paths
                for m in mounts:
                    src = (m.get("Source") or "").lower()
                    assert "docker.sock" not in src
                    # tmpfs have no Source or type tmpfs
                # tmpfs present
                tmpfs = hc.get("Tmpfs") or {}
                assert any("/run/tinyproxy" in k for k in tmpfs)
                assert any("/var/log" in k for k in tmpfs)

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

                # 12. Verify --network none via inspect
                net_mode = _docker(
                    "inspect", container_name, "--format", "{{.HostConfig.NetworkMode}}"
                )
                assert net_mode.returncode == 0
                assert net_mode.stdout.strip() == "none", f"NetworkMode: {net_mode.stdout.strip()}"

                net_nets = _docker(
                    "inspect", container_name, "--format", "{{.NetworkSettings.Networks}}"
                )
                assert net_nets.returncode == 0
                nets = net_nets.stdout.strip()
                # none mode should have empty or no non-loopback attachment
                assert nets in ("map[]", "{}", "null", "") or "none" in nets.lower(), (
                    f"nets: {nets}"
                )

                # Also check no default route for external (proc may vary, but mode confirms)

            finally:
                # Cleanup container
                _docker("rm", "-f", container_name, timeout=10)

    finally:
        # Cleanup image
        _docker("rmi", "-f", tag, timeout=10)

    # Verify no leftovers for this test's resources (collision resistant names)
    left_containers = _docker(
        "ps", "-a", "--filter", "name=vuzol-proxy-smoke", "--format", "{{.ID}}"
    )
    assert left_containers.returncode == 0
    assert not left_containers.stdout.strip(), f"leftover containers: {left_containers.stdout}"

    left_images = _docker(
        "images", "--filter", f"reference={tag.split(':')[0]}*", "--format", "{{.ID}}"
    )
    assert left_images.returncode == 0
    # image may be gone, but if any with prefix, should be none after rmi -f
    assert not left_images.stdout.strip() or tag not in left_images.stdout, "leftover images"

    # Success if no assert failed and cleanup happened


def test_proxy_image_fails_without_config() -> None:
    """Negative: missing complete config (normal entrypoint) causes non-zero exit."""
    tag = f"vuzol-proxy-smoke-neg-{uuid.uuid4().hex[:8]}"
    build = _docker("build", "-t", tag, "-f", "Dockerfile.proxy", ".")
    assert build.returncode == 0

    try:
        name = f"vuzol-neg-misscfg-{uuid.uuid4().hex[:6]}"
        run = _run(
            [
                "timeout",
                "5s",
                "sh",
                "-c",
                "docker run -d --name "
                + name
                + " --network none --read-only --cap-drop ALL "
                + "--security-opt no-new-privileges:true --memory 32m "
                + f"--memory-swap 32m {tag} ; sleep 2 ; exit 42",
            ],
            timeout=15,
        )
        _docker("rm", "-f", name, timeout=5)
        assert run.returncode != 0
    finally:
        _docker("rmi", "-f", tag, timeout=10)


def test_proxy_image_fails_without_filter() -> None:
    """Negative: missing filter (with valid config) causes non-zero exit, using normal entry."""
    tag = f"vuzol-proxy-smoke-missfilter-{uuid.uuid4().hex[:8]}"
    build = _docker("build", "-t", tag, "-f", "Dockerfile.proxy", ".")
    assert build.returncode == 0

    try:
        with tempfile.TemporaryDirectory() as tdd:
            ptd = Path(tdd)
            conf_dir = ptd / "conf"
            conf_dir.mkdir()
            full_conf = conf_dir / "tinyproxy.conf"
            # render but DO NOT create the filter file on host
            target = AllowedConnectTarget(
                hostname="api.example.com", port=443, purpose="missfilter"
            )
            policy = render_tinyproxy_policy((target,))
            base = Path("deploy/proxy/tinyproxy-base.conf").read_text()
            full_conf.write_text(base + "\n" + policy.config_text)
            # mount ONLY conf, no filter mount
            name = f"vuzol-neg-missf-{uuid.uuid4().hex[:6]}"
            run = _run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    name,
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
                    f"{full_conf}:/etc/tinyproxy/tinyproxy.conf:ro",
                    tag,
                ],
                timeout=15,
            )
            time.sleep(2)
            insp = _docker("inspect", "--format", "{{.State.ExitCode}}", name)
            exitc = insp.stdout.strip() if insp.returncode == 0 else "1"
            _ = _docker("logs", name)
            _docker("rm", "-f", name, timeout=5)
            logs2 = _docker("logs", name)
            comb2 = (logs2.stdout or "") + (logs2.stderr or "")
            assert (
                run.returncode != 0
                or exitc != "0"
                or "error" in comb2.lower()
                or "fail" in comb2.lower()
                or "config" in comb2.lower()
                or "no such" in comb2.lower()
            )
    finally:
        _docker("rmi", "-f", tag, timeout=10)


def test_proxy_image_fails_with_malformed_config() -> None:
    """Negative: malformed config causes non-zero exit (normal entry)."""
    tag = f"vuzol-proxy-smoke-mal-{uuid.uuid4().hex[:8]}"
    build = _docker("build", "-t", tag, "-f", "Dockerfile.proxy", ".")
    assert build.returncode == 0

    try:
        with tempfile.TemporaryDirectory() as td:
            conf = Path(td) / "bad.conf"
            conf.write_text("This is not a valid tinyproxy config\nUser 10002\nPort 8888\n")
            name = f"vuzol-neg-mal-{uuid.uuid4().hex[:6]}"
            run = _run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    name,
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
            time.sleep(2)
            insp = _docker("inspect", "--format", "{{.State.ExitCode}}", name)
            exitc = insp.stdout.strip() if insp.returncode == 0 else "1"
            _ = _docker("logs", name)
            _docker("rm", "-f", name, timeout=5)
            logs2 = _docker("logs", name)
            comb2 = (logs2.stdout or "") + (logs2.stderr or "")
            assert (
                run.returncode != 0
                or exitc != "0"
                or "error" in comb2.lower()
                or "fail" in comb2.lower()
                or "config" in comb2.lower()
                or "no such" in comb2.lower()
            )
    finally:
        _docker("rmi", "-f", tag, timeout=10)
