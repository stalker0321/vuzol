"""Native Codex bootstrap and credential boundaries, without model/network calls."""

import json
import os
import subprocess

import pytest

from vuzol.providers.codex import canonical_codex_argv

pytestmark = pytest.mark.docker


@pytest.mark.parametrize("read_only", [True, False])
def test_native_codex_helper_starts_without_exposing_account_state(read_only: bool) -> None:
    socket = os.getenv("VUZOL_ROOTLESS_DOCKER_SOCKET")
    image = os.getenv("VUZOL_CODEX_SANDBOX_IMAGE")
    seccomp = os.getenv("VUZOL_CODEX_SANDBOX_SECCOMP")
    if not socket or not image or not seccomp:
        pytest.skip("native Codex sandbox image/socket/seccomp must be explicitly configured")
    argv = canonical_codex_argv(read_only=read_only)
    config = next(value for value in argv if value.startswith("permissions."))
    profile = "vuzol-reader" if read_only else "vuzol-executor"
    script = r"""
const fs = require('node:fs');
const cp = require('node:child_process');
fs.mkdirSync('/codex-home/sessions');
for (const name of ['auth.json','sessions/history.jsonl','other-secret']) {
    fs.writeFileSync('/codex-home/' + name, 'SYNTHETIC_SECRET');
}
fs.writeFileSync('/codex-home/config.toml', '# SYNTHETIC_SECRET\n');
fs.writeFileSync('/workspace/input.txt', 'INPUT_OK');
fs.symlinkSync('/codex-home/auth.json', '/workspace/account-link');
const readOnly = process.argv[3] === 'true';
const checks = [
    'set -eu',
    'test "$(cat /workspace/input.txt)" = INPUT_OK',
    ...['/codex-home/auth.json','/codex-home/config.toml',
        '/codex-home/sessions/history.jsonl','/codex-home/other-secret',
        '/workspace/account-link'].map(path =>
            'if cat ' + path + ' >/dev/null 2>&1; then exit 41; fi'),
    readOnly
        ? 'if touch /workspace/forbidden 2>/dev/null; then exit 42; fi'
        : 'touch /workspace/allowed',
    'echo BOUNDARIES_OK',
].join('; ');
const result = cp.spawnSync('codex', ['sandbox', '--permission-profile', process.argv[1],
    '-c', process.argv[2], '--', '/bin/sh', '-c', checks], {encoding:'utf8'});
console.log(JSON.stringify({exit: result.status, stdout: result.stdout, stderr: result.stderr}));
"""
    result = subprocess.run(
        [
            "docker",
            "--host",
            f"unix://{socket}",
            "run",
            "--rm",
            "--pull=never",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt=no-new-privileges",
            "--security-opt",
            f"seccomp={seccomp}",
            "--user",
            "10001:10001",
            "--memory",
            "512m",
            "--cpus",
            "1",
            "--pids-limit",
            "128",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,noexec,mode=1777",  # noqa: S108 - container tmpfs
            "--tmpfs",
            "/codex-home:rw,nosuid,nodev,mode=700,uid=10001,gid=10001",
            "--tmpfs",
            "/workspace:rw,nosuid,nodev,mode=755,uid=10001,gid=10001",
            "--env",
            "CODEX_HOME=/codex-home",
            "--env",
            "HOME=/tmp/home",
            "--workdir",
            "/workspace",
            "--entrypoint",
            "/usr/local/bin/node",
            image,
            "-e",
            script,
            profile,
            config,
            str(read_only).lower(),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    native = json.loads(result.stdout)
    assert native["exit"] == 0, native["stderr"]
    assert native["stdout"] == "BOUNDARIES_OK\n"
