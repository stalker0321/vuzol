"""Deterministic host capability preflight for project environment contracts."""

from __future__ import annotations

import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from vuzol.projects.descriptors import host_executables
from vuzol.projects.toolchains import load_installed_toolchain
from vuzol.security.confined_paths import executable_within_roots


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


_EXECUTABLES = host_executables()


def preflight_capabilities(
    contract: dict[str, object],
    *,
    which: Callable[[str], str | None] = shutil.which,
    managed_toolchain_root: Path | None = None,
    installation_status: Mapping[str, str] | None = None,
    approved_roots: tuple[Path, ...] | None = None,
) -> tuple[CapabilityCheck, ...]:
    """Classify every declared requirement without mutating the host.

    ``installation_status`` carries the persisted probe state (stale/failed
    installations are excluded). Discovery never installs and never grants
    permissions.
    """

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
        status = None if installation_status is None else installation_status.get(key)
        if status in {"stale", "failed"}:
            checks.append(
                CapabilityCheck(
                    key, label, CapabilityState.NEEDS_SETUP, f"installation is {status}"
                )
            )
            continue
        executable = _EXECUTABLES.get(key)
        installed = (
            None
            if managed_toolchain_root is None
            else load_installed_toolchain(managed_toolchain_root, key)
        )
        if installed is not None:
            checks.append(
                CapabilityCheck(
                    key,
                    label,
                    CapabilityState.READY,
                    f"managed {key} {installed.version} is available",
                )
            )
        elif executable is None:
            checks.append(
                CapabilityCheck(
                    key, label, CapabilityState.NEEDS_SETUP, "managed toolchain is not installed"
                )
            )
        else:
            resolved = which(executable)
            if resolved is None:
                checks.append(
                    CapabilityCheck(
                        key, label, CapabilityState.NEEDS_SETUP, f"{executable} is not installed"
                    )
                )
            elif approved_roots is not None and not executable_within_roots(
                Path(resolved), approved_roots
            ):
                checks.append(
                    CapabilityCheck(
                        key,
                        label,
                        CapabilityState.NEEDS_SETUP,
                        f"{executable} is not readable in the confined environment",
                    )
                )
            else:
                checks.append(
                    CapabilityCheck(key, label, CapabilityState.READY, f"{executable} is available")
                )
    return tuple(checks)


def blocking_capabilities(checks: tuple[CapabilityCheck, ...]) -> tuple[CapabilityCheck, ...]:
    return tuple(check for check in checks if check.state is not CapabilityState.READY)
