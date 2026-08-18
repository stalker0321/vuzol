"""Deterministic host capability preflight for project environment contracts."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class CapabilityState(StrEnum):
    READY = "ready"
    NEEDS_APPROVAL = "needs_approval"
    NEEDS_SETUP = "needs_setup"


@dataclass(frozen=True, slots=True)
class CapabilityCheck:
    key: str
    label: str
    state: CapabilityState
    detail: str


_EXECUTABLES = {
    "node-runtime": "node",
    "python-runtime": "python3",
    "git": "git",
}


def preflight_capabilities(
    contract: dict[str, object],
    *,
    which: Callable[[str], str | None] = shutil.which,
    managed_toolchain_root: Path | None = None,
) -> tuple[CapabilityCheck, ...]:
    """Classify every declared requirement without mutating the host."""

    raw = contract.get("capabilities")
    if not isinstance(raw, dict):
        return ()
    checks: list[CapabilityCheck] = []
    for key in sorted(raw):
        requirement = raw[key]
        if not isinstance(key, str) or not isinstance(requirement, dict):
            continue
        label = str(requirement.get("label") or key)
        provisioning = requirement.get("provisioning")
        if provisioning == "approval_required":
            checks.append(
                CapabilityCheck(
                    key, label, CapabilityState.NEEDS_APPROVAL, "host change requires approval"
                )
            )
            continue
        if provisioning == "external_setup":
            checks.append(
                CapabilityCheck(
                    key, label, CapabilityState.NEEDS_SETUP, "external configuration is required"
                )
            )
            continue
        if key == "android-sdk":
            state = (
                CapabilityState.READY
                if _managed_capability_ready(key, managed_toolchain_root)
                else CapabilityState.NEEDS_SETUP
            )
            detail = (
                "managed Android toolchain is available"
                if state is CapabilityState.READY
                else "managed Android toolchain is not installed"
            )
            checks.append(CapabilityCheck(key, label, state, detail))
            continue
        executable = _EXECUTABLES.get(key)
        if executable is None:
            checks.append(
                CapabilityCheck(
                    key, label, CapabilityState.NEEDS_SETUP, "no trusted adapter is registered"
                )
            )
        elif which(executable) is None:
            checks.append(
                CapabilityCheck(
                    key, label, CapabilityState.NEEDS_SETUP, f"{executable} is not installed"
                )
            )
        else:
            checks.append(
                CapabilityCheck(key, label, CapabilityState.READY, f"{executable} is available")
            )
    return tuple(checks)


def _managed_capability_ready(key: str, root: Path | None) -> bool:
    if key != "android-sdk" or root is None:
        return False
    target = root / "android-sdk"
    required = (
        target / "android-sdk/platform-tools/adb",
        target / "jdk/bin/java",
        target / "gradle/bin/gradle",
    )
    return all(
        path.is_file() and not path.is_symlink() and path.stat().st_mode & 0o111
        for path in required
    )


def blocking_capabilities(checks: tuple[CapabilityCheck, ...]) -> tuple[CapabilityCheck, ...]:
    return tuple(check for check in checks if check.state is not CapabilityState.READY)
