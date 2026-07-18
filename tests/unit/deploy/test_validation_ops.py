"""Validation ops tests (split for cohesion)."""

from __future__ import annotations

from ._test_mvp_helpers import (
    ROOT,
)


def test_platform_suite_keeps_temporary_coverage_floor() -> None:
    """Vuzol keeps a temporary 90% floor until P0/P1 automation replaces it."""
    configuration = (ROOT / "pyproject.toml").read_text()
    assert configuration.count("--cov-fail-under=90") == 1
    makefile = (ROOT / "Makefile").read_text()
    assert "coverage report --precision=6 --fail-under=90" in makefile


def test_validation_wrapper_returns_the_real_pytest_status() -> None:
    """Offline validation must return pytest's exit code, not hide failures."""
    content = (ROOT / "deploy/validation/run_tests.py").read_text()
    assert "return completed.returncode" in content
    assert "| tee" not in content
    assert "shell=True" not in content


def test_validation_clone_uses_the_verified_operator_checkout() -> None:
    content = (ROOT / "deploy/mvp/check.py").read_text()
    assert '"--no-hardlinks", str(ROOT), str(checkout)' in content
    assert '"--no-hardlinks", str(DEPLOYED), str(checkout)' not in content
    assert 'f"u:{executor_uid}:x", str(temporary_root)' in content
    assert 'f"u:{mapped_uid}:x", str(temporary_root)' in content
    assert '"-x", f"u:{mapped_uid}", str(temporary_root)' in content
    assert '"-x", f"u:{executor_uid}", str(temporary_root)' in content
    assert '"--read-only"' in content
    assert '"seccomp=/etc/vuzol/sandbox-seccomp.json"' in content
    assert "dst=/workspace/.git,readonly" in content
    assert '"UV_NO_SYNC": "1"' in content
    assert '"UV_OFFLINE": "1"' in content
    assert '"UV_CACHE_DIR": "/tmp/uv-cache"' in content
