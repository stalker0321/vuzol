"""Disk free-space gate for new heavy work (S10-3a).

Default-off: ``min_free_bytes=0`` preserves historical behavior. When enabled,
HEAVY queue claims are skipped without leasing (no attempt/budget burn). A last
moment re-check before worktree preparation returns a retryable outcome if space
dropped after claim (residual TOCTOU; attempt is refunded for this category).

Control-plane, light, recovery, retention, and finalization paths must not call
this gate to block safety/cleanup actions.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from vuzol.config.settings import Settings

DISK_PRESSURE_CATEGORY = "disk_pressure"
DISK_PRESSURE_SUMMARY = "insufficient free disk for heavy work"


class FreeSpaceProbe(Protocol):
    """Injectable free-space measurement (production uses statvfs)."""

    def free_bytes(self, path: Path) -> int:
        """Return free bytes available to non-root on the filesystem of ``path``."""


class StatVfsProbe:
    """Default probe: ``os.statvfs`` free blocks for non-privileged callers."""

    def free_bytes(self, path: Path) -> int:
        status = os.statvfs(path)
        return int(status.f_bavail) * int(status.f_frsize)


@dataclass(frozen=True, slots=True)
class DiskPressureAssessment:
    """Bounded assessment — never includes absolute paths (log-safe)."""

    allowed: bool
    reason: str
    required_bytes: int = 0
    free_bytes: int | None = None

    @property
    def blocked(self) -> bool:
        return not self.allowed


def measurement_paths(settings: Settings) -> tuple[Path, ...]:
    """Paths whose free space is measured.

    Explicit ``disk_pressure.paths`` when set; otherwise worktree + artifact roots
    (where heavy setup allocates).
    """

    configured = settings.disk_pressure.paths
    if configured:
        return configured
    return (settings.worktree_root, settings.artifact_root)


def assess_disk_pressure(
    settings: Settings,
    *,
    probe: FreeSpaceProbe | None = None,
) -> DiskPressureAssessment:
    """Return whether new heavy work may start.

    - ``min_free_bytes <= 0``: disabled, always allowed.
    - Free space must be **>=** threshold on every measured path (equal allowed).
    - Probe ``OSError``: fail-closed (not allowed) for new heavy work only.
    """

    required = int(settings.disk_pressure.min_free_bytes)
    if required <= 0:
        return DiskPressureAssessment(allowed=True, reason="disabled", required_bytes=0)

    active = probe if probe is not None else StatVfsProbe()
    lowest: int | None = None
    for path in measurement_paths(settings):
        try:
            free = active.free_bytes(path)
        except OSError:
            return DiskPressureAssessment(
                allowed=False,
                reason="probe_error",
                required_bytes=required,
            )
        lowest = free if lowest is None else min(lowest, free)
        if free < required:
            return DiskPressureAssessment(
                allowed=False,
                reason="low",
                required_bytes=required,
                free_bytes=free,
            )
    return DiskPressureAssessment(
        allowed=True,
        reason="ok",
        required_bytes=required,
        free_bytes=lowest,
    )


def heavy_work_allowed(
    settings: Settings,
    *,
    probe: FreeSpaceProbe | None = None,
) -> bool:
    """True when HEAVY claims / worktree setup may proceed."""

    return assess_disk_pressure(settings, probe=probe).allowed
