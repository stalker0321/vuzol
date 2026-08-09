from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from vuzol.ops.production_deploy import (
    DeploymentConfig,
    DeploymentError,
    ProductionDeployer,
    load_environment,
    require_full_sha,
)

OLD = "1" * 40
NEW = "2" * 40


class FakeRunner:
    def __init__(self, source: Path, deployed: Path, *, fail_restart_once: bool = False) -> None:
        self.source = source
        self.deployed = deployed
        self.current = OLD
        self.image = OLD
        self.fail_restart_once = fail_restart_once
        self.calls: list[tuple[str, ...]] = []

    def __call__(
        self,
        argv: Sequence[str],
        cwd: Path | None,
        env: Mapping[str, str] | None,
    ) -> str:
        del cwd
        command = tuple(argv)
        self.calls.append(command)
        if command[:3] == ("git", "-C", str(self.source)):
            if command[3:5] == ("status", "--porcelain"):
                return ""
            if command[3] == "rev-parse" and command[4].endswith("^{commit}"):
                return command[4][:-9]
        if command[:3] == ("git", "-C", str(self.deployed)):
            operation = command[3:]
            if operation == ("status", "--porcelain"):
                return ""
            if operation == ("rev-parse", "HEAD"):
                return self.current
            if operation[:2] == ("checkout", "--detach"):
                self.current = operation[2]
                return ""
            if operation[0] == "fetch":
                return ""
        if command[:3] == ("docker", "compose", "--project-directory"):
            if "build" in command:
                assert env is not None
                self.image = env["VUZOL_BUILD_GIT_SHA"]
            return ""
        if command[:2] == ("systemctl", "restart"):
            if self.fail_restart_once:
                self.fail_restart_once = False
                raise DeploymentError("injected restart failure")
            return ""
        if command[:2] == ("systemctl", "is-active"):
            return "\n".join("active" for _ in command[2:])
        if command[:2] == ("docker", "inspect"):
            return self.image if "Labels" in command[-1] else "running"
        return ""


def config(tmp_path: Path) -> DeploymentConfig:
    source = tmp_path / "source"
    deployed = tmp_path / "deployed"
    source.mkdir()
    deployed.mkdir()
    runtime_env = tmp_path / "runtime.env"
    service_env = tmp_path / "service.env"
    uv = tmp_path / "uv"
    runtime_env.write_text("RUNTIME=yes\n")
    service_env.write_text("VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_DATABASE_DSN\n")
    uv.write_text("")
    return DeploymentConfig(
        source=source,
        deployed=deployed,
        runtime_env=runtime_env,
        service_env=service_env,
        uv=uv,
        lock_file=tmp_path / "deploy.lock",
        services=("vuzol-a.service", "vuzol-b.service"),
    )


def test_deploy_attests_checkout_image_and_services(tmp_path: Path) -> None:
    deployment = config(tmp_path)
    runner = FakeRunner(deployment.source, deployment.deployed)

    result = ProductionDeployer(deployment, runner).deploy(NEW)

    assert result.previous_sha == OLD
    assert result.deployed_sha == NEW
    assert runner.current == NEW and runner.image == NEW
    assert any(call[:2] == ("systemctl", "restart") for call in runner.calls)


def test_failed_activation_rolls_code_and_image_back(tmp_path: Path) -> None:
    deployment = config(tmp_path)
    runner = FakeRunner(deployment.source, deployment.deployed, fail_restart_once=True)

    with pytest.raises(DeploymentError, match=f"code rolled back to {OLD}"):
        ProductionDeployer(deployment, runner).deploy(NEW)

    assert runner.current == OLD and runner.image == OLD


def test_rejects_short_sha_and_dirty_checkout(tmp_path: Path) -> None:
    with pytest.raises(DeploymentError, match="full 40-character"):
        require_full_sha("abc123")
    deployment = config(tmp_path)
    runner = FakeRunner(deployment.source, deployment.deployed)

    def dirty(argv: Sequence[str], cwd: Path | None, env: Mapping[str, str] | None) -> str:
        if tuple(argv)[-2:] == ("status", "--porcelain"):
            return " M file"
        return runner(argv, cwd, env)

    with pytest.raises(DeploymentError, match="operator checkout is dirty"):
        ProductionDeployer(deployment, dirty).deploy(NEW)


def test_environment_parser_preserves_secrets_without_expansion(tmp_path: Path) -> None:
    environment = tmp_path / "service.env"
    environment.write_text("# comment\nPLAIN=value\nQUOTED='value with spaces'\nEMPTY=\n")

    loaded = load_environment(environment)

    assert loaded["PLAIN"] == "value"
    assert loaded["QUOTED"] == "value with spaces"
    assert loaded["EMPTY"] == ""


def test_same_release_is_attested_without_reinstallation(tmp_path: Path) -> None:
    deployment = config(tmp_path)
    runner = FakeRunner(deployment.source, deployment.deployed)

    result = ProductionDeployer(deployment, runner).deploy(OLD)

    assert result.previous_sha == result.deployed_sha == OLD
    assert not any("checkout" in call for call in runner.calls)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("BROKEN", "invalid environment assignment"),
        ("BAD-KEY=value", "invalid environment key"),
        ("KEY=two words", "invalid environment value"),
    ],
)
def test_environment_parser_fails_closed(tmp_path: Path, content: str, message: str) -> None:
    environment = tmp_path / "broken.env"
    environment.write_text(content)

    with pytest.raises(DeploymentError, match=message):
        load_environment(environment)


def test_attestation_rejects_image_service_and_container_mismatch(tmp_path: Path) -> None:
    deployment = config(tmp_path)
    runner = FakeRunner(deployment.source, deployment.deployed)
    deployer = ProductionDeployer(deployment, runner)
    runner.image = NEW
    with pytest.raises(DeploymentError, match="image SHA mismatch"):
        deployer._attest(OLD)

    runner.image = OLD

    def inactive(argv: Sequence[str], cwd: Path | None, env: Mapping[str, str] | None) -> str:
        if tuple(argv)[:2] == ("systemctl", "is-active"):
            return "active\nfailed"
        return runner(argv, cwd, env)

    with pytest.raises(DeploymentError, match="services are not active"):
        ProductionDeployer(deployment, inactive)._attest(OLD)

    def stopped(argv: Sequence[str], cwd: Path | None, env: Mapping[str, str] | None) -> str:
        command = tuple(argv)
        if command[:2] == ("docker", "inspect") and "Labels" not in command[-1]:
            return "exited"
        return runner(argv, cwd, env)

    with pytest.raises(DeploymentError, match="container is not running"):
        ProductionDeployer(deployment, stopped)._attest(OLD)
