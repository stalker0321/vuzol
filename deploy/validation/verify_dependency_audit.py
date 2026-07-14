#!/usr/bin/env python3
"""Verify a build-time dependency audit against the mounted lock inputs."""

import hashlib
import json
from pathlib import Path
from typing import Any

ATTESTATION_ROOT = Path("/opt/vuzol-validation-audit")
INPUTS = (Path("pyproject.toml"), Path("uv.lock"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    expected_lines = (ATTESTATION_ROOT / "inputs.sha256").read_text().splitlines()
    expected: dict[str, str] = {}
    for line in expected_lines:
        digest, separator, name = line.partition("  ")
        if separator != "  " or name not in {path.name for path in INPUTS}:
            raise RuntimeError("dependency-audit input attestation is malformed")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise RuntimeError("dependency-audit input digest is malformed")
        expected[name] = digest
    if set(expected) != {path.name for path in INPUTS}:
        raise RuntimeError("dependency-audit input attestation is incomplete")
    for path in INPUTS:
        if _sha256(path) != expected[path.name]:
            raise RuntimeError(f"dependency lock input differs from validation image: {path.name}")

    report: dict[str, Any] = json.loads((ATTESTATION_ROOT / "pip-audit.json").read_text())
    dependencies = report.get("dependencies")
    if not isinstance(dependencies, list) or not dependencies:
        raise RuntimeError("dependency-audit report contains no dependencies")
    vulnerable = [
        item.get("name", "unknown")
        for item in dependencies
        if not isinstance(item, dict) or item.get("vulns") != []
    ]
    if vulnerable:
        raise RuntimeError("dependency-audit report contains vulnerabilities")
    print(f"Offline dependency audit verified {len(dependencies)} locked packages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
