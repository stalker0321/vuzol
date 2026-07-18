"""Disk free-space gate for new heavy work (S10-3a).

Default-off: ``min_free_bytes=0`` preserves historical behavior. When enabled,
HEAVY queue claims are skipped without leasing (no attempt/budget burn). A last
moment re-check before worktree preparation returns a retryable outcome if space
dropped after claim (residual TOCTOU; attempt is refunded for this category).

Claim callers **must** pass ``Settings`` when evaluating HEAVY work. Missing
settings fail closed for HEAVY (wiring mistake must not silently disable a
configured-or-unknown gate). Explicit ``min_free_bytes=0`` remains the
compatible off switch when settings are provided.

Control-plane, light, recovery, retention, and finalization paths must not call
this gate to block safety/cleanup actions.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from vuzol.config.settings import Settings
from vuzol.observability import get_logger

DISK_PRESSURE_CATEGORY = "disk_pressure"
DISK_PRESSURE_SUMMARY = "insufficient free disk for heavy work"
# Claim loops may poll often; log at most once per interval per process.
_CLAIM_DEFER_LOG_INTERVAL_SECONDS = 60.0
_last_claim_defer_log_monotonic: float = 0.0
_LOGGER = get_logger(__name__)


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


def assess_heavy_claim_gate(
    settings: Settings | None,
    *,
    probe: FreeSpaceProbe | None = None,
) -> DiskPressureAssessment:
    """Gate for HEAVY claim paths.

    - ``settings is None`` → fail closed (``missing_settings``): production claim
      paths must pass settings; omitting them must not silently allow HEAVY work.
    - ``settings`` present → :func:`assess_disk_pressure` (zero min_free still
      disables the free-space check).
    """

    if settings is None:
        return DiskPressureAssessment(
            allowed=False,
            reason="missing_settings",
            required_bytes=0,
        )
    return assess_disk_pressure(settings, probe=probe)


def report_claim_disk_pressure_deferred(
    assessment: DiskPressureAssessment,
    *,
    force: bool = False,
    log_interval_seconds: float = _CLAIM_DEFER_LOG_INTERVAL_SECONDS,
) -> bool:
    """Emit bounded claim-time observability for deferred HEAVY claims.

    Logs at most once per ``log_interval_seconds`` (process-local) unless
    ``force=True``. Payload is path-free (reason, free/required bytes only).

    Returns True when a log line was emitted.
    """

    global _last_claim_defer_log_monotonic
    if assessment.allowed:
        return False
    now = time.monotonic()
    if (
        not force
        and _last_claim_defer_log_monotonic > 0.0
        and (now - _last_claim_defer_log_monotonic) < log_interval_seconds
    ):
        return False
    _last_claim_defer_log_monotonic = now
    _LOGGER.warning(
        "heavy work claim deferred due to disk pressure",
        extra={
            "event": "ops.disk_pressure.deferred",
            "source": "claim",
            "reason": assessment.reason,
            "required_bytes": assessment.required_bytes,
            "free_bytes": assessment.free_bytes,
        },
    )
    return True


def reset_claim_defer_log_state_for_tests() -> None:
    """Test helper: clear rate-limit state between cases."""

    global _last_claim_defer_log_monotonic
    _last_claim_defer_log_monotonic = 0.0
