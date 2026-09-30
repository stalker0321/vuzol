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


def provenance_reference(task: object) -> str:
    """Opaque worker/planner ref carrying the source turn link (D4 REDO).

    Read by the provider-step path (handlers.py) alongside ``original_input``:
    the text stays available and the turn identity travels in the reference.
    No consumer parses it beyond carrying it through; legacy tasks without a
    turn keep the previous ``task:{id}:original`` shape.
    """

    task_id = getattr(task, "id", None)
    source_turn_id = getattr(task, "source_turn_id", None)
    base = f"task:{task_id}:original"
    if source_turn_id is None:
        return base
    return f"{base}:turn:{source_turn_id}"


def extract_plan_item_source(
    body: Mapping[str, Any], ordinal: int
) -> tuple[uuid.UUID | None, str | None]:
    """Read system-stamped provenance for one materialized item (D4 REDO).

    Returns ``(source_turn_id, source_spec_revision)`` or ``(None, None)``
    for legacy items without markers. Never raises: malformed refs fail
    closed to unknown provenance instead of breaking materialization.
    """

    raw_items = body.get("items")
    if not isinstance(raw_items, list):
        return None, None
    for raw in raw_items:
        if not isinstance(raw, dict) or raw.get("ordinal") != ordinal:
            continue
        if raw.get("derived") is not True:
            return None, None
        ref = raw.get("source_turn_ref")
        try:
            turn_id = uuid.UUID(str(ref)) if ref is not None else None
        except (ValueError, AttributeError, TypeError):
            return None, None
        spec_revision = raw.get("source_spec_revision")
        if spec_revision is not None and not isinstance(spec_revision, str):
            return None, None
        return turn_id, spec_revision
    return None, None
