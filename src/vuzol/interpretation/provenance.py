"""Provenance helpers (D4 W2).

Plan items are marked ``derived`` with source refs to the originating turn
and D1 spec revision. The original turn stays reachable for planner, worker
and reviewer through refs — never replaced by generated text.

Storage: ``TaskSpecRevision.source_turn_id`` (D1) carries the turn link;
``PlanRevision.immutable_body`` carries per-item ``derived``/``source_*``
markers via ``canonical_plan_body`` (no migration). Legacy rows without
provenance read as ``derived=False`` / ``None`` refs and never break.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any


def plan_item_provenance(
    *,
    derived: bool = True,
    source_turn_id: uuid.UUID | str | None = None,
    source_spec_revision: str | None = None,
) -> dict[str, Any]:
    return {
        "derived": derived,
        "source_turn_id": None if source_turn_id is None else str(source_turn_id),
        "source_spec_revision": source_spec_revision,
    }


def task_source_refs(task: object) -> dict[str, Any]:
    """Refs a planner/worker/reviewer can follow to the original turn."""

    return {
        "source_turn_id": getattr(task, "source_turn_id", None),
        "spec_revision": getattr(task, "spec_revision", None),
        "original_text": getattr(task, "original_text", None),
    }


def coerce_legacy_plan_item(item: Mapping[str, Any]) -> dict[str, Any]:
    """Fill missing provenance on legacy plan items without backfill."""

    coerced = dict(item)
    coerced.setdefault("derived", False)
    coerced.setdefault("source_turn_id", None)
    coerced.setdefault("source_spec_revision", None)
    return coerced
