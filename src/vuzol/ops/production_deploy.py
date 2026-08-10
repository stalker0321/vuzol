"""Fail-closed, serialized production deployment with code rollback."""

from __future__ import annotations

import fcntl
import os
import re
import shlex
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

FULL_SHA = re.compile(r"[0-9a-f]{40}")
INTERPRETER_CONTAINER = "vuzol-interpreter-1"
IMAGE_SHA_LABEL = "dev.hryshyn.vuzol.git-sha"
DEFAULT_SERVICES = (
    "vuzol-applier.service",
    "vuzol-executor.service",
    "vuzol-project-provisioner.service",
    "vuzol-static-publisher-worker.service",
    "vuzol-telegram-delivery.service",
    "vuzol-telegram.service",
    "vuzol-worker.service",
)


class DeploymentError(RuntimeError):
    """Deployment did not reach a verified, internally consistent release."""


Runner = Callable[[Sequence[str], Path | None, Mapping[str, str] | None], str]


@dataclass(frozen=True, slots=True)
class DeploymentConfig:
    source: Path
    deployed: Path = Path("/opt/vuzol")
    runtime_env: Path = Path("/home/vodkolyan/.config/vuzol/runtime.env")
    service_env: Path = Path("/etc/vuzol/executor.env")
    uv: Path = Path("/home/vodkolyan/.local/bin/uv")
    lock_file: Path = Path("/run/lock/vuzol-deploy.lock")
    services: tuple[str, ...] = DEFAULT_SERVICES


@dataclass(frozen=True, slots=True)
class DeploymentResult:
    previous_sha: str
    deployed_sha: str
    rolled_back: bool = False


def run_command(
    argv: Sequence[str], cwd: Path | None = None, env: Mapping[str, str] | None = None
) -> str:
    completed = subprocess.run(  # noqa: S603 - finite argv assembled from validated config
        tuple(argv),
        cwd=cwd,
        env=None if env is None else dict(env),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        tail = " | ".join(detail[-8:]) if detail else "no command detail"
        raise DeploymentError(f"command failed ({argv[0]}): {tail}")
    return completed.stdout.strip()


def load_environment(path: Path) -> dict[str, str]:
    """Load the deliberately simple systemd EnvironmentFile syntax without logging values."""

    values = dict(os.environ)
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise DeploymentError(f"invalid environment assignment in {path}")
        key, raw_value = line.split("=", 1)
        if not key or not key.replace("_", "A").isalnum():
            raise DeploymentError(f"invalid environment key in {path}")
        parsed = shlex.split(raw_value, comments=False, posix=True)
        if len(parsed) > 1:
            raise DeploymentError(f"invalid environment value in {path}")
        values[key] = "" if not parsed else parsed[0]
    return values


def require_full_sha(value: str) -> str:
    normalized = value.strip().lower()
    if FULL_SHA.fullmatch(normalized) is None:
        raise DeploymentError("target must be a full 40-character Git SHA")
    return normalized


class ProductionDeployer:
    def __init__(self, config: DeploymentConfig, runner: Runner = run_command) -> None:
        self._config = config
        self._run = runner

    def deploy(self, target_sha: str) -> DeploymentResult:
        target = require_full_sha(target_sha)
        self._config.lock_file.parent.mkdir(parents=True, exist_ok=True)
        with self._config.lock_file.open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise DeploymentError("another production deployment is active") from error
            return self._deploy_locked(target)

    def _deploy_locked(self, target: str) -> DeploymentResult:
        self._preflight(target)
        previous = self._git(self._config.deployed, "rev-parse", "HEAD")
        if previous == target:
            self._attest(target)
            return DeploymentResult(previous_sha=previous, deployed_sha=target)
        changed = False
        try:
            self._install_release(target)
            changed = True
            self._attest(target)
        except Exception as deploy_error:
            if not changed and self._git(self._config.deployed, "rev-parse", "HEAD") != target:
                raise
            try:
                self._install_release(previous, fetch=False, migrate=False)
                self._attest(previous, verify_migrations=False)
            except Exception as rollback_error:
                raise DeploymentError(
                    f"deployment failed and rollback failed: {deploy_error}; {rollback_error}"
                ) from rollback_error
            raise DeploymentError(
                f"deployment failed; code rolled back to {previous}"
            ) from deploy_error
        return DeploymentResult(previous_sha=previous, deployed_sha=target)

    def _preflight(self, target: str) -> None:
        for path, label in (
            (self._config.source, "operator checkout"),
            (self._config.deployed, "production checkout"),
        ):
            if not path.is_dir():
                raise DeploymentError(f"{label} is missing")
            if self._git(path, "status", "--porcelain"):
                raise DeploymentError(f"{label} is dirty")
        resolved = self._git(self._config.source, "rev-parse", f"{target}^{{commit}}")
        if resolved != target:
            raise DeploymentError("target commit is unavailable in the operator checkout")
        for required in (self._config.runtime_env, self._config.service_env, self._config.uv):
            if not required.exists():
                raise DeploymentError(f"required deployment input is missing: {required}")

    def _install_release(self, target: str, *, fetch: bool = True, migrate: bool = True) -> None:
        if fetch:
            self._run(
                (
                    "git",
                    "-C",
                    str(self._config.deployed),
                    "fetch",
                    str(self._config.source),
                    "main",
                ),
                None,
                None,
            )
        self._git(self._config.deployed, "checkout", "--detach", target)
        self._run(
            (
                str(self._config.uv),
                "sync",
                "--project",
                str(self._config.deployed),
                "--frozen",
                "--no-dev",
            ),
            None,
            None,
        )
        if migrate:
            self._run(
                (str(self._config.deployed / ".venv/bin/alembic"), "upgrade", "head"),
                self._config.deployed,
                load_environment(self._config.service_env),
            )
        compose = self._compose_environment(target)
        self._run(self._compose("build", "interpreter"), None, compose)
        self._run(self._compose("up", "-d", "--no-deps", "interpreter"), None, compose)
        self._run(("systemctl", "restart", *self._config.services), None, None)

    def _attest(self, expected: str, *, verify_migrations: bool = True) -> None:
        if self._git(self._config.deployed, "rev-parse", "HEAD") != expected:
            raise DeploymentError("production checkout SHA mismatch")
        image_sha = self._run(
            (
                "docker",
                "inspect",
                INTERPRETER_CONTAINER,
                "--format",
                f'{{{{index .Config.Labels "{IMAGE_SHA_LABEL}"}}}}',
            ),
            None,
            None,
        )
        if image_sha != expected:
            raise DeploymentError("interpreter image SHA mismatch")
        states = self._run(("systemctl", "is-active", *self._config.services), None, None)
        if states.splitlines() != ["active"] * len(self._config.services):
            raise DeploymentError("one or more Vuzol services are not active")
        container_state = self._run(
            ("docker", "inspect", INTERPRETER_CONTAINER, "--format", "{{.State.Status}}"),
            None,
            None,
        )
        if container_state != "running":
            raise DeploymentError("interpreter container is not running")
        if verify_migrations:
            migration_environment = load_environment(self._config.service_env)
            alembic = str(self._config.deployed / ".venv/bin/alembic")
            heads = self._run((alembic, "heads"), self._config.deployed, migration_environment)
            current = self._run((alembic, "current"), self._config.deployed, migration_environment)
            if _alembic_revisions(current) != _alembic_revisions(heads):
                raise DeploymentError("database migration revision is not at head")

    def _compose(self, *args: str) -> tuple[str, ...]:
        return (
            "docker",
            "compose",
            "--project-directory",
            str(self._config.deployed),
            "--env-file",
            str(self._config.runtime_env),
            "--profile",
            "interpretation",
            *args,
        )

    def _compose_environment(self, sha: str) -> dict[str, str]:
        return {**os.environ, "VUZOL_BUILD_GIT_SHA": sha}

    def _git(self, repository: Path, *args: str) -> str:
        return self._run(("git", "-C", str(repository), *args), None, None)


def _alembic_revisions(output: str) -> frozenset[str]:
    return frozenset(
        line.split()[0]
        for line in output.splitlines()
        if line and not line.startswith(("INFO", "WARNING"))
    )
