#!/usr/bin/env python3
"""Fail-closed, no-provider readiness check for the deliberately narrow Vuzol MVP."""

from __future__ import annotations

import argparse
import hashlib
import pwd
import subprocess
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEPLOYED = Path("/opt/vuzol")
REGISTRY = Path("/etc/vuzol/executor-registries.toml")
MIRROR = Path("/srv/vuzol/repositories/vuzol")
SOCKET = Path("/run/user/994/docker.sock")
GATES = ("format-check", "lint", "type-check", "security", "test")
VALIDATION_ENVIRONMENT = {
    "CI": "1",
    "COVERAGE_FILE": "/tmp/.coverage",  # noqa: S108 - container-only tmpfs
    "GIT_CONFIG_COUNT": "1",
    "GIT_CONFIG_KEY_0": "safe.directory",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_VALUE_0": "/workspace",
    "GIT_TERMINAL_PROMPT": "0",
    "HOME": "/tmp/home",  # noqa: S108 - container-only tmpfs
    "MYPY_CACHE_DIR": "/tmp/mypy-cache",  # noqa: S108 - container-only tmpfs
    "PATH": "/opt/vuzol-validation/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONPATH": "/workspace/src",
    "RUFF_CACHE_DIR": "/tmp/ruff-cache",  # noqa: S108 - container-only tmpfs
    "UV_CACHE_DIR": "/tmp/uv-cache",  # noqa: S108 - container-only tmpfs
    "UV_NO_SYNC": "1",
    "UV_OFFLINE": "1",
    "UV_PROJECT_ENVIRONMENT": "/opt/vuzol-validation",
    "VIRTUAL_ENV": "/opt/vuzol-validation",
}


class MvpCheckError(RuntimeError):
    pass


def _run(argv: tuple[str, ...], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(  # noqa: S603 - caller supplies finite operational argv
        argv, cwd=cwd, check=False, capture_output=True, text=True
    )
    if completed.returncode:
        stdout_tail = completed.stdout.strip().splitlines()[-8:]
        stderr_tail = completed.stderr.strip().splitlines()[-8:]
        detail = stdout_tail + stderr_tail
        tail = " | ".join(detail) if detail else "no detail"
        raise MvpCheckError(f"command failed ({argv[0]}): {tail}")
    return completed.stdout.strip()


def _git(repository: Path, *argv: str) -> str:
    return _run(("git", "-c", f"safe.directory={repository}", "-C", str(repository), *argv))


def _service_snapshot() -> tuple[int, int]:
    output = _run(
        (
            "systemctl",
            "show",
            "vuzol-executor.service",
            "--property=ActiveState,SubState,MainPID,NRestarts",
        )
    )
    values = dict(line.split("=", 1) for line in output.splitlines())
    if values.get("ActiveState") != "active" or values.get("SubState") != "running":
        raise MvpCheckError("vuzol-executor is not active/running")
    pid = int(values.get("MainPID", "0"))
    if pid < 1:
        raise MvpCheckError("vuzol-executor has no live PID")
    return pid, int(values.get("NRestarts", "0"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _is_executor_dockerd(process: Path, executor_uid: int) -> bool:
    try:
        return (
            process.name.isdigit()
            and process.stat().st_uid == executor_uid
            and "dockerd" in (process / "comm").read_text(errors="ignore")
        )
    except FileNotFoundError:
        # Processes may exit between /proc enumeration and inspection.
        return False


def _mapped_identity(container_id: int) -> int:
    executor = pwd.getpwnam("vuzol-executor")
    dockerd = next(
        (
            process
            for process in Path("/proc").iterdir()
            if _is_executor_dockerd(process, executor.pw_uid)
        ),
        None,
    )
    if dockerd is None:
        raise MvpCheckError("rootless dockerd process was not found")
    for line in (dockerd / "uid_map").read_text().splitlines():
        inside, outside, length = (int(value) for value in line.split())
        if inside <= container_id < inside + length:
            return outside + container_id - inside
    raise MvpCheckError("validation sandbox UID is absent from the rootless mapping")


def _docker(*argv: str) -> str:
    return _run(
        (
            "sudo",
            "-n",
            "-u",
            "vuzol-executor",
            "env",
            f"DOCKER_HOST=unix://{SOCKET}",
            "XDG_RUNTIME_DIR=/run/user/994",
            "docker",
            *argv,
        )
    )


def _registry() -> dict[str, object]:
    checked = ROOT / "deploy/registries.executor.toml"
    production_hash = _run(("sudo", "-n", "sha256sum", str(REGISTRY))).split()[0]
    if _sha256(checked) != production_hash:
        raise MvpCheckError("production registry differs from the reviewed registry")
    return tomllib.loads(checked.read_text())


def _configured_image(document: dict[str, object], profile_id: str) -> str:
    sandboxes = document.get("sandboxes")
    if not isinstance(sandboxes, list):
        raise MvpCheckError("registry has no sandbox list")
    for item in sandboxes:
        if isinstance(item, dict) and item.get("id") == profile_id:
            image = item.get("image")
            if isinstance(image, str) and "@sha256:" in image:
                return image
    raise MvpCheckError(f"sandbox image is unavailable: {profile_id}")


def _require_provider_profiles(document: dict[str, object]) -> None:
    profiles = document.get("profiles")
    if not isinstance(profiles, list):
        raise MvpCheckError("registry has no provider profile list")
    matches = [
        item
        for item in profiles
        if isinstance(item, dict) and item.get("id") == "codex-subscription-prod"
    ]
    if len(matches) != 1 or matches[0].get("enabled") is not True:
        raise MvpCheckError("codex-subscription-prod is not uniquely enabled")
    planners = [
        item
        for item in profiles
        if isinstance(item, dict) and item.get("id") == "openai-planner-prod"
    ]
    if len(planners) != 1:
        raise MvpCheckError("openai-planner-prod is not uniquely configured")
    planner = planners[0]
    expected = {
        "provider": "openai-compatible",
        "model": "gpt-5-nano-2025-08-07",
        "launch_mode": "api",
        "roles": ["planner"],
        "output_limit": 1_000,
        "enabled": True,
    }
    if any(planner.get(key) != value for key, value in expected.items()):
        raise MvpCheckError("openai-planner-prod does not match the bounded production policy")


def _validation_gates(image: str) -> None:
    mapped_uid = _mapped_identity(10001)
    executor_uid = pwd.getpwnam("vuzol-executor").pw_uid
    with tempfile.TemporaryDirectory(prefix="vuzol-mvp-check-") as temporary:
        temporary_root = Path(temporary)
        checkout = temporary_root / "repository"
        _run(("git", "clone", "--quiet", "--no-hardlinks", str(ROOT), str(checkout)))
        _run(("sudo", "-n", "setfacl", "-m", f"u:{executor_uid}:x", str(temporary_root)))
        _run(("sudo", "-n", "setfacl", "-m", f"u:{mapped_uid}:x", str(temporary_root)))
        _run(("sudo", "-n", "setfacl", "-R", "-m", f"u:{mapped_uid}:rwX", str(checkout)))
        _run(("sudo", "-n", "setfacl", "-R", "-m", f"d:u:{mapped_uid}:rwX", str(checkout)))
        try:
            tool_commands = (
                ("/usr/bin/make", "--version"),
                ("python", "--version"),
                ("uv", "--version"),
            )
            for command in tool_commands:
                _docker("run", "--rm", "--network", "none", image, *command)
            for gate in GATES:
                arguments = [
                    "run",
                    "--rm",
                    "--pull",
                    "never",
                    "--network",
                    "none",
                    "--read-only",
                    "--user",
                    "10001:10001",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--security-opt",
                    "seccomp=/etc/vuzol/sandbox-seccomp.json",
                    "--memory",
                    "1073741824",
                    "--memory-swap",
                    "1073741824",
                    "--cpus",
                    "1.0",
                    "--pids-limit",
                    "128",
                    "--ulimit",
                    "nofile=1024:1024",
                    "--tmpfs",
                    "/tmp:rw,nosuid,nodev,noexec,size=134217728",  # noqa: S108
                    "--workdir",
                    "/workspace",
                    "--mount",
                    f"type=bind,src={checkout},dst=/workspace",
                    "--mount",
                    f"type=bind,src={checkout / '.git'},dst=/workspace/.git,readonly",
                ]
                for key, value in sorted(VALIDATION_ENVIRONMENT.items()):
                    arguments.extend(("--env", f"{key}={value}"))
                arguments.extend(
                    (
                        image,
                        "/usr/bin/make",
                        gate,
                    )
                )
                _docker(*arguments)
            if _git(checkout, "status", "--short"):
                raise MvpCheckError("validation gates modified the disposable checkout")
            if (checkout / ".venv").exists():
                raise MvpCheckError("validation gates created a worktree .venv")
        finally:
            _run(("sudo", "-n", "setfacl", "-R", "-x", f"u:{mapped_uid}", str(checkout)))
            _run(("sudo", "-n", "setfacl", "-R", "-k", str(checkout)))
            _run(("sudo", "-n", "setfacl", "-x", f"u:{mapped_uid}", str(temporary_root)))
            _run(("sudo", "-n", "setfacl", "-x", f"u:{executor_uid}", str(temporary_root)))


def _durable_state() -> None:
    sql = """
SELECT violation_type, object_id, run_id, run_status
FROM (
  SELECT 'qualification_reserved_budget' AS violation_type,
         r.id::text AS object_id, x.id::text AS run_id, x.status::text AS run_status
  FROM provider_budget_reservations r JOIN runs x ON x.id=r.run_id
  WHERE r.status='reserved' AND x.selected_route->>'experiment_id' LIKE '%qual%'
  UNION ALL
  SELECT 'terminal_reserved_budget', r.id::text, x.id::text, x.status::text
  FROM provider_budget_reservations r JOIN runs x ON x.id=r.run_id
  WHERE r.status='reserved' AND x.status IN ('completed', 'failed', 'cancelled', 'blocked')
  UNION ALL
  SELECT 'terminal_active_worktree', w.id::text, x.id::text, x.status::text
  FROM worktrees w JOIN runs x ON x.id=w.run_id
  WHERE w.delivery_state='active'
    AND x.status IN ('completed', 'failed', 'cancelled', 'blocked')
) AS violations
ORDER BY violation_type, object_id
LIMIT 10;
"""
    output = _run(
        (
            "docker",
            "exec",
            "vuzol-postgres-1",
            "psql",
            "-U",
            "vuzol",
            "-d",
            "vuzol",
            "-X",
            "-A",
            "-t",
            "-F",
            ",",
            "-c",
            sql,
        )
    )
    if violations := output.strip():
        bounded = "; ".join(violations.splitlines())
        raise MvpCheckError(f"durable state invariant violated: {bounded}")


def _proxy_runtime_is_empty() -> bool:
    """Inspect the protected RuntimeDirectory without weakening its mode."""
    output = _run(
        (
            "sudo",
            "-n",
            "find",
            "/run/vuzol/proxy",
            "-mindepth",
            "1",
            "-maxdepth",
            "1",
            "-print",
            "-quit",
        )
    )
    return not output


def check(expected_sha: str) -> dict[str, object]:
    if _git(ROOT, "status", "--short"):
        raise MvpCheckError("public checkout is dirty")
    if _git(ROOT, "rev-parse", "HEAD") != expected_sha:
        raise MvpCheckError("public checkout SHA differs from the expected SHA")
    if _git(ROOT, "rev-parse", "origin/main") != expected_sha:
        raise MvpCheckError("origin branch differs from the expected SHA")
    if _git(DEPLOYED, "status", "--short") or _git(DEPLOYED, "rev-parse", "HEAD") != expected_sha:
        raise MvpCheckError("deployed checkout is dirty or at the wrong SHA")
    if _git(MIRROR, "rev-parse", "refs/heads/main") != expected_sha:
        raise MvpCheckError("managed source mirror base ref differs from the deployed SHA")
    pid_before, restarts_before = _service_snapshot()
    runtime = Path("/run/vuzol/proxy")
    if not runtime.is_dir() or runtime.stat().st_mode & 0o777 != 0o700:
        raise MvpCheckError("systemd-managed proxy runtime directory is unavailable")
    document = _registry()
    _require_provider_profiles(document)
    provider_image = _configured_image(document, "project-default")
    validation_image = _configured_image(document, "vuzol-validation")
    _docker("image", "inspect", provider_image)
    _docker("image", "inspect", validation_image)
    if _docker("ps", "-q", "--filter", "label=vuzol.managed=true"):
        raise MvpCheckError("stale managed container exists")
    if _docker("network", "ls", "-q", "--filter", "label=vuzol.managed=true"):
        raise MvpCheckError("stale managed network exists")
    mapped_uid = _mapped_identity(10001)
    acl = _run(("sudo", "-n", "getfacl", "-R", "-cp", "/srv/vuzol/worktrees"))
    if f"user:{mapped_uid}:" in acl:
        raise MvpCheckError("stale sandbox ACL exists")
    if not _proxy_runtime_is_empty():
        raise MvpCheckError("stale proxy runtime entry exists")
    _durable_state()
    _validation_gates(validation_image)
    pid_after, restarts_after = _service_snapshot()
    if (pid_after, restarts_after) != (pid_before, restarts_before):
        raise MvpCheckError("executor PID or restart count changed during readiness checking")
    return {
        "schema_version": "vuzol-mvp-check.v1",
        "sha": expected_sha,
        "executor_pid": pid_after,
        "executor_restarts": restarts_after,
        "provider_image": provider_image,
        "validation_image": validation_image,
        "gates": list(GATES),
        "status": "ready",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-sha", required=True)
    args = parser.parse_args()
    try:
        result = check(args.expected_sha)
    except MvpCheckError as error:
        print(f"MVP_CHECK_FAILED: {error}")
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
