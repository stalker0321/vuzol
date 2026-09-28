"""Joint ledger+harness export (WP13).

Combines the harness aggregate (telemetry durations/outcomes) with the WP01
ledger projections (``usage_totals_by_purpose``, ``usage_retry_subtotal``).
Double-count defense: ledger projections are two views of the same rows
("same rows / never an addend") — the export keeps them separate and never
sums retry rows into purpose totals or ledger costs into harness estimates.
Measured totals and unavailable counts stay split; pricing revisions gate
comparability.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any

EXPORT_SCHEMA = "experiment-joint-export.v1"


def joint_export(
    harness_summary: dict[str, Any],
    purpose_totals: Sequence[tuple[str | None, Decimal, int]],
    retry_subtotal: tuple[Decimal, int],
    *,
    experiment_id: str,
) -> dict[str, Any]:
    """Merge harness and ledger views without double counting."""

    purpose_rows = [
        {"purpose": purpose, "cost_units": str(cost), "invocations": count}
        for purpose, cost, count in purpose_totals
    ]
    purpose_cost = sum((cost for _, cost, _ in purpose_totals), Decimal("0"))
    purpose_invocations = sum(count for _, _, count in purpose_totals)
    retry_cost, retry_invocations = retry_subtotal
    ledger = {
        "by_purpose": purpose_rows,
        "purpose_cost_units": str(purpose_cost),
        "purpose_invocations": purpose_invocations,
        "retry_subtotal": {
            "cost_units": str(retry_cost),
            "invocations": retry_invocations,
            "note": "retry/repair projection of the same rows, never an addend",
        },
    }
    double_count_check = {
        "retry_not_added_to_purpose": True,
        "ledger_not_added_to_harness_estimate": True,
        "retry_invocations_le_purpose_invocations": retry_invocations <= purpose_invocations,
    }
    if not double_count_check["retry_invocations_le_purpose_invocations"]:
        raise ValueError("retry subtotal exceeds purpose totals: ledger views inconsistent")
    return {
        "schema_version": EXPORT_SCHEMA,
        "experiment_id": experiment_id,
        "harness": harness_summary,
        "ledger": ledger,
        "double_count_check": double_count_check,
    }


def pricing_comparable(pricing_revisions: Sequence[str | None]) -> dict[str, object]:
    """Pricing-version-aware gate: mixed revisions forbid cash comparison."""

    revisions = sorted({revision or "unknown" for revision in pricing_revisions})
    comparable = len(revisions) == 1 and revisions != ["unknown"]
    return {
        "comparable": comparable,
        "revisions": revisions,
        "note": (
            "same pricing revision required for cash comparison"
            if comparable
            else "pricing revisions differ or unknown: report bounded estimates only"
        ),
    }
