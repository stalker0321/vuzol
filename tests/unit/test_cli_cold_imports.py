"""Each console entrypoint must load without pytest's pre-imported modules."""

import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINTS = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]


@pytest.mark.parametrize("entrypoint", ENTRYPOINTS.values(), ids=ENTRYPOINTS.keys())
def test_entrypoint_imports_in_a_fresh_process(entrypoint: str) -> None:
    module, name = entrypoint.split(":")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import importlib, sys; "
            "assert callable(getattr(importlib.import_module(sys.argv[1]), sys.argv[2]))",
            module,
            name,
        ],
        cwd=ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(ROOT / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
