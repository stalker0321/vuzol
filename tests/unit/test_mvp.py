import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest
from coverage.results import should_fail_under

ROOT = Path(__file__).parents[2]


def _module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_coverage_precision_rejects_unrounded_below_threshold() -> None:
    assert should_fail_under(89.91, 90.0, 2)
    assert not should_fail_under(90.00, 90.0, 2)
    configuration = (ROOT / "pyproject.toml").read_text()
    assert "precision = 2" in configuration
    assert configuration.count("--cov-fail-under=90") == 1


def test_pytest_failure_and_below_threshold_are_nonzero(tmp_path: Path) -> None:
    (tmp_path / "sample.py").write_text(
        "def covered():\n    return 1\n\ndef missed():\n    return 2\n"
    )
    (tmp_path / "test_sample.py").write_text(
        "import sample\n\ndef test_covered():\n    assert sample.covered() == 1\n"
    )
    config = tmp_path / "pyproject.toml"
    config.write_text(
        "[tool.pytest.ini_options]\naddopts='--cov=sample --cov-fail-under=90'\n"
        "[tool.coverage.report]\nprecision=2\n"
    )
    below = subprocess.run(
        (sys.executable, "-m", "pytest", "-q", "-c", str(config)),
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )
    assert below.returncode != 0
    assert "fail-under=90" in below.stdout
    (tmp_path / "test_sample.py").write_text("def test_failure():\n    assert False\n")
    failed = subprocess.run(
        (sys.executable, "-m", "pytest", "-q", "-c", str(config), "--no-cov"),
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )
    assert failed.returncode != 0


def test_validation_wrapper_returns_the_real_pytest_status() -> None:
    content = (ROOT / "deploy/validation/run_tests.py").read_text()
    assert "return completed.returncode" in content
    assert "| tee" not in content
    assert "shell=True" not in content


def test_canary_uses_running_service_without_stop_or_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _module("mvp_canary", ROOT / "deploy/mvp/canary.py")
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"experiment_id": "mvp-canary-test"}))
    calls: list[tuple[str, ...]] = []

    def fake_run(argv: tuple[str, ...]) -> str:
        calls.append(argv)
        if argv[0] == "systemctl":
            return "ActiveState=active\nSubState=running\nMainPID=123\nNRestarts=0\n"
        if argv[1] == "seed":
            return '{"run_uuid":"run"}'
        return '{"runs":[{"status":"completed"}]}'

    monkeypatch.setattr(module, "_run", fake_run)
    result = module.run(request, timeout_seconds=1)
    flattened = " ".join(part for call in calls for part in call)
    assert result["excluded_from_worker_quality"] is True
    assert " stop " not in f" {flattened} "
    assert " restart " not in f" {flattened} "
    assert (
        calls.count(
            (
                "systemctl",
                "show",
                "vuzol-executor.service",
                "--property=ActiveState,SubState,MainPID,NRestarts",
            )
        )
        == 2
    )
