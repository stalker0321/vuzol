import os
import subprocess
import sys


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
