"""J3 INTAKE shadow integration: advisory event, zero transitions, ledger cost."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from tests.integration.providers._test_routing_helpers import (
    NormalizedUsage,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]

_HASH = "a" * 64


@dataclass(frozen=True, slots=True)
class _Result:
    usage: NormalizedUsage | None
    provider_request_id: str | None


def _valid_output() -> dict[str, Any]:
    return {
        "schema": "decision.v3",
        "decision_kind": "intake",
        "state_revision": 0,
        "effect": "execute_request",
        "relation": "new",
        "target_id": None,
        "support_refs": ["turn:abc"],
        "coverage": "complete",
        "abstain": False,
        "reason_code": "clear_match",
        "input_fingerprint": _HASH,
    }
