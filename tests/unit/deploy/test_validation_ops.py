"""Validation ops tests (split for cohesion)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from coverage.results import should_fail_under

ROOT = Path(__file__).parents[3]


def test_platform_suite_keeps_temporary_coverage_floor() -> None:
    """Vuzol keeps a temporary 90% floor until P0/P1 automation replaces it."""
    configuration = (ROOT / "pyproject.toml").read_text()
    assert configuration.count("--cov-fail-under=90") == 1
    makefile = (ROOT / "Makefile").read_text()
    assert "coverage report --precision=6 --fail-under=90" in makefile


def test_coverage_precision_rejects_unrounded_below_threshold() -> None:
    """Behavioral floor: precision=6 must fail values that round to 90 at precision=0."""

    assert should_fail_under(89.998225, 90.0, 6)
    assert not should_fail_under(90.0, 90.0, 6)
    configuration = (ROOT / "pyproject.toml").read_text()
    assert "precision = 2" in configuration
    assert configuration.count("--cov-fail-under=90") == 1
    makefile = (ROOT / "Makefile").read_text()
    assert "coverage report --precision=6 --fail-under=90" in makefile


def _isolated_coverage_env(coverage_file: Path) -> dict[str, str]:
    """Keep the nested pytest run from writing into the outer coverage data file."""

    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("COV_CORE") and key != "COVERAGE_FILE"
    }
    env["COVERAGE_FILE"] = str(coverage_file)
    return env


def test_pytest_failure_and_below_threshold_are_nonzero(tmp_path: Path) -> None:
    """Subprocess pytest must return non-zero for failures and coverage miss."""

    root = tmp_path
    (root / "sample.py").write_text("def covered():\n    return 1\n\ndef missed():\n    return 2\n")
    (root / "test_sample.py").write_text(
        "import sample\n\ndef test_covered():\n    assert sample.covered() == 1\n"
    )
    config = root / "pyproject.toml"
    config.write_text(
        "[tool.pytest.ini_options]\naddopts='--cov=sample --cov-fail-under=90'\n"
        "[tool.coverage.report]\nprecision=2\n"
    )
    below = subprocess.run(
        (sys.executable, "-m", "pytest", "-q", "-c", str(config)),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env=_isolated_coverage_env(root / ".coverage"),
    )
    assert below.returncode != 0
    assert "fail-under=90" in below.stdout or "FAILED" in below.stdout or below.returncode != 0
    (root / "test_sample.py").write_text("def test_failure():\n    assert False\n")
    failed = subprocess.run(
        (sys.executable, "-m", "pytest", "-q", "-c", str(config), "--no-cov"),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert failed.returncode != 0


def test_validation_wrapper_returns_the_real_pytest_status() -> None:
    """Offline validation must return pytest's exit code, not hide failures."""
    content = (ROOT / "deploy/validation/run_tests.py").read_text()
    assert "return completed.returncode" in content
    assert "| tee" not in content
    assert "shell=True" not in content
