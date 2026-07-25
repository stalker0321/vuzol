"""Unit tests for S10-3a disk low-watermark assessment and settings."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.config.settings import DiskPressureSettings, Settings
from vuzol.ops.disk_pressure import (
    DISK_PRESSURE_CATEGORY,
    DiskPressureAssessment,
    assess_disk_pressure,
    assess_heavy_claim_gate,
    heavy_work_allowed,
    measurement_paths,
    report_claim_disk_pressure_deferred,
    reset_claim_defer_log_state_for_tests,
)


class _FixedProbe:
    def __init__(self, mapping: dict[Path, int] | Exception) -> None:
        self._mapping = mapping

    def free_bytes(self, path: Path) -> int:
        if isinstance(self._mapping, Exception):
            raise self._mapping
        if path not in self._mapping:
            raise OSError(f"unmapped path {path}")
        return self._mapping[path]


def _settings(
    tmp_path: Path,
    *,
    min_free_bytes: int = 0,
    paths: tuple[Path, ...] = (),
) -> Settings:
    return Settings(
        environment="test",
        repository_root=tmp_path / "repositories",
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
        disk_pressure=DiskPressureSettings(
            min_free_bytes=min_free_bytes,
            paths=paths,
        ),
    )


def test_zero_min_free_disables_gate(tmp_path: Path) -> None:
    settings = _settings(tmp_path, min_free_bytes=0)
    probe = _FixedProbe({settings.worktree_root: 0, settings.artifact_root: 0})
    result = assess_disk_pressure(settings, probe=probe)
    assert result.allowed is True
    assert result.reason == "disabled"
    assert heavy_work_allowed(settings, probe=probe) is True


def test_above_threshold_allows(tmp_path: Path) -> None:
    settings = _settings(tmp_path, min_free_bytes=1_000)
    probe = _FixedProbe({settings.worktree_root: 5_000, settings.artifact_root: 2_000})
    result = assess_disk_pressure(settings, probe=probe)
    assert result.allowed is True
    assert result.reason == "ok"
    assert result.free_bytes == 2_000


def test_equal_threshold_allows(tmp_path: Path) -> None:
    settings = _settings(tmp_path, min_free_bytes=1_000)
    probe = _FixedProbe({settings.worktree_root: 1_000, settings.artifact_root: 1_000})
    result = assess_disk_pressure(settings, probe=probe)
    assert result.allowed is True
    assert result.free_bytes == 1_000


def test_below_threshold_blocks(tmp_path: Path) -> None:
    settings = _settings(tmp_path, min_free_bytes=1_000)
    probe = _FixedProbe({settings.worktree_root: 999, settings.artifact_root: 50_000})
    result = assess_disk_pressure(settings, probe=probe)
    assert result.blocked is True
    assert result.reason == "low"
    assert result.free_bytes == 999
    assert result.required_bytes == 1_000
    assert heavy_work_allowed(settings, probe=probe) is False


def test_probe_error_fail_closed(tmp_path: Path) -> None:
    settings = _settings(tmp_path, min_free_bytes=100)
    probe = _FixedProbe(OSError("statvfs failed"))
    result = assess_disk_pressure(settings, probe=probe)
    assert result.allowed is False
    assert result.reason == "probe_error"


def test_explicit_paths_override_defaults(tmp_path: Path) -> None:
    custom = tmp_path / "volume"
    custom.mkdir()
    settings = _settings(tmp_path, min_free_bytes=500, paths=(custom,))
    assert measurement_paths(settings) == (custom,)
    probe = _FixedProbe({custom: 100})
    assert assess_disk_pressure(settings, probe=probe).blocked is True


def test_settings_reject_relative_paths() -> None:
    with pytest.raises(ValidationError, match="absolute"):
        DiskPressureSettings(min_free_bytes=1, paths=(Path("relative"),))


def test_settings_reject_negative_min_free() -> None:
    with pytest.raises(ValidationError):
        DiskPressureSettings(min_free_bytes=-1)


def test_default_settings_compatible() -> None:
    # Defaults must not change historical free-for-all heavy work.
    pressure = DiskPressureSettings()
    assert pressure.min_free_bytes == 0
    assert pressure.paths == ()


def test_category_constant_stable() -> None:
    assert DISK_PRESSURE_CATEGORY == "disk_pressure"


def test_missing_settings_fail_closed_for_claim_gate() -> None:
    result = assess_heavy_claim_gate(None)
    assert result.blocked is True
    assert result.reason == "missing_settings"


def test_claim_defer_log_rate_limited() -> None:
    """Return value encodes rate-limit; avoids caplog interaction with suite logging config."""

    reset_claim_defer_log_state_for_tests()
    assessment = DiskPressureAssessment(
        allowed=False, reason="low", required_bytes=1000, free_bytes=10
    )
    assert report_claim_disk_pressure_deferred(assessment, log_interval_seconds=60.0) is True
    assert report_claim_disk_pressure_deferred(assessment, log_interval_seconds=60.0) is False
    assert (
        report_claim_disk_pressure_deferred(assessment, force=True, log_interval_seconds=60.0)
        is True
    )
    assert (
        report_claim_disk_pressure_deferred(
            DiskPressureAssessment(allowed=True, reason="ok"), force=True
        )
        is False
    )
