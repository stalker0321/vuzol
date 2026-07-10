import os
import subprocess
import sys
from pathlib import Path


def test_app_entrypoint_fails_clearly_on_invalid_configuration() -> None:
    environment = os.environ | {"VUZOL_PORT": "invalid"}

    result = subprocess.run(
        [sys.executable, "-m", "vuzol.cli.app"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode != 0
    assert "validation error for Settings" in result.stderr
    assert "VUZOL_PORT" not in result.stderr


def test_worker_entrypoint_fails_clearly_on_invalid_configuration() -> None:
    environment = os.environ | {"VUZOL_WORKER_POLL_INTERVAL_SECONDS": "0"}

    result = subprocess.run(
        [sys.executable, "-m", "vuzol.cli.worker"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode != 0
    assert "validation error for Settings" in result.stderr
    assert "VUZOL_WORKER_POLL_INTERVAL_SECONDS" not in result.stderr


def test_app_entrypoint_fails_before_server_on_invalid_registry(tmp_path: Path) -> None:
    registry = tmp_path / "invalid.toml"
    registry.write_text("[[profiles]\n")
    environment = os.environ | {"VUZOL_REGISTRY_FILE": str(registry)}

    result = subprocess.run(
        [sys.executable, "-m", "vuzol.cli.app"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode != 0
    assert "ConfigurationLoadError" in result.stderr
    assert "invalid registry file" in result.stderr
    assert "Uvicorn running" not in result.stderr
