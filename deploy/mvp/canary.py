#!/usr/bin/env python3
"""Seed and observe one marked canary on the continuously running production executor."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

TERMINAL = {"blocked", "cancelled", "completed", "failed"}


def _experiment_cli() -> Path:
    executable = Path(sys.executable).with_name("vuzol-experiment")
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError(
            f"certification companion executable is absent or not executable: {executable}"
        )
    return executable


def _run(argv: tuple[str, ...]) -> str:
    completed = subprocess.run(  # noqa: S603 - finite CLI commands only
        argv, check=False, capture_output=True, text=True
    )
    if completed.returncode:
        raise RuntimeError((completed.stderr or completed.stdout).strip())
    return completed.stdout


def _service() -> tuple[int, int]:
    output = _run(
        (
            "/usr/bin/systemctl",
            "show",
            "vuzol-executor.service",
            "--property=ActiveState,SubState,MainPID,NRestarts",
        )
    )
    values = dict(line.split("=", 1) for line in output.splitlines())
    if values.get("ActiveState") != "active" or values.get("SubState") != "running":
        raise RuntimeError("production executor is not active/running")
    return int(values["MainPID"]), int(values["NRestarts"])


def run(request: Path, *, timeout_seconds: int) -> dict[str, object]:
    request_body = json.loads(request.read_text())
    experiment_id = request_body["experiment_id"]
    if "qual" not in experiment_id and "canary" not in experiment_id:
        raise RuntimeError("canary experiment ID must be explicitly marked")
    experiment_cli = _experiment_cli()
    before = _service()
    seeded = json.loads(_run((str(experiment_cli), "seed", str(request))))
    deadline = time.monotonic() + timeout_seconds
    inspected: dict[str, object] = {}
    while time.monotonic() < deadline:
        inspected = json.loads(_run((str(experiment_cli), "inspect", experiment_id)))
        candidate_runs = inspected.get("runs", [])
        runs = candidate_runs if isinstance(candidate_runs, list) else []
        if runs and all(isinstance(run, dict) and run.get("status") in TERMINAL for run in runs):
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("canary did not reach a terminal durable state")
    after = _service()
    if after != before:
        raise RuntimeError("production executor PID or restart count changed")
    return {
        "schema_version": "vuzol-mvp-canary.v1",
        "excluded_from_worker_quality": True,
        "executor_pid": after[0],
        "executor_restarts": after[1],
        "experiment_executable": str(experiment_cli),
        "seed": seeded,
        "inspect": inspected,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("request", type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args()
    print(json.dumps(run(args.request, timeout_seconds=args.timeout_seconds), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
