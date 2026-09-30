"""D6 Q5: remote execution is explicitly out of scope (lead decision)."""

from __future__ import annotations

import pytest

from vuzol.config import Settings


@pytest.mark.skip(reason="D6 Q5: remote execution is explicitly out of scope; no flag, no calls")
def test_remote_execution_not_verified() -> None:
    raise AssertionError("unreachable: remote is out of scope for D6")


def test_remote_has_no_flag_and_no_production_callers() -> None:
    from pathlib import Path

    settings_fields = set(Settings.model_fields)
    assert not any("remote" in name for name in settings_fields)
    root = Path(__file__).resolve().parents[2] / "src" / "vuzol"
    callers: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for name in ("claim_node_step", "claim_slot", "pull_artifact"):
            if name in text and "def " + name not in text and "test" not in str(path):
                callers.append(f"{path.relative_to(root)}:{name}")
    # Definitions and the slot/node primitives themselves are allowed; only
    # production call sites would count, and there are none.
    production = [
        entry
        for entry in callers
        if not entry.startswith(
            ("projects/node_claim.py", "storage/slots.py", "execution/remote.py")
        )
    ]
    assert production == []
