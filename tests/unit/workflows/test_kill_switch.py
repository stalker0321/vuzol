"""D6 Q2 kill switch: flag defaults and env wiring (unit)."""

from __future__ import annotations

from _pytest.monkeypatch import MonkeyPatch

from vuzol.config import Settings


def test_dispatch_freeze_defaults_off() -> None:
    assert Settings(environment="test").workflow.dispatch_freeze is False


def test_dispatch_freeze_env_enables(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("VUZOL_WORKFLOW__DISPATCH_FREEZE", "true")
    assert Settings(environment="test").workflow.dispatch_freeze is True
