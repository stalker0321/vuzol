"""Versioned L0/L1/L2/L3 review policy (WP07, wired in D0).

Risk only escalates, never downgrades: the policy adds depth on top of the
existing high-risk gates in ``review/handler.py``.  It never removes the
mandatory independent review for HIGH/PRIVILEGED results.

Levels:

- L0 — mechanical only. LOW risk, docs-only or tiny changes.
- L1 — mechanical + focused pattern review. D0 has no operator switch for
  ``l1_enabled``; the parameter exists only as an explicit escalation path
  (disabled L1 escalates to L2, never down to L0). No promise of an operator
  setting is made.
- L2 — bounded model review per partition (default for MEDIUM and up).
- L3 — bounded model review per partition + cross-partition assessment
  (HIGH/PRIVILEGED and large diffs).

Coverage (D0 ADR): L2 and L3 require an independent model call; L0 and L1
are mechanical only. MEDIUM maps to L2, so a medium handler invokes the
required reviewer. ``should_skip_rereview`` is documented as unused by the
dispatch path (kept as a pure helper with unit coverage).

Jev is not a dependency of review and must stay unconnected.
"""

from __future__ import annotations

import re
from enum import StrEnum

from vuzol.execution.scaffold import path_is_docs_only
from vuzol.storage.types import RiskLevel

REVIEW_POLICY_REVISION = "review-policy.v1"


class IndependentReviewError(Exception):
    """Independent review could not complete safely."""


class ReviewLevel(StrEnum):
    L0 = "L0"
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"


class FileClass(StrEnum):
    DOCS = "docs"
    CODE = "code"
    GENERATED = "generated"
    LOCKFILE = "lockfile"
    PRIVILEGED = "privileged"


_PRIVILEGED_PATH_PARTS = frozenset(
    {"ansible", "deploy", "deployment", "helm", "infra", "k8s", "systemd", "terraform"}
)

_LOCKFILE_NAMES = frozenset(
    {
        "package-lock.json",
        "pnpm-lock.yaml",
        "poetry.lock",
        "requirements.txt",
        "uv.lock",
        "cargo.lock",
        "go.lock",
        "gemfile.lock",
    }
)

_GENERATED_SUFFIXES = (
    ".min.js",
    ".min.css",
    ".bundle.js",
    ".pb.go",
    ".generated.py",
    "_generated.py",
    ".g.dart",
)

_GENERATED_DIR_PARTS = frozenset({"dist", "build", "out", "__pycache__", ".next", "coverage"})

_LEVEL_ORDER = {
    ReviewLevel.L0: 0,
    ReviewLevel.L1: 1,
    ReviewLevel.L2: 2,
    ReviewLevel.L3: 3,
}


def classify_file(path: str) -> FileClass:
    """Deterministically classify one changed path (no I/O)."""

    normalized = path.lower()
    parts = {part for part in re.split(r"[/._-]+", normalized) if part}
    filename = normalized.rsplit("/", 1)[-1]
    if parts & _PRIVILEGED_PATH_PARTS:
        return FileClass.PRIVILEGED
    if filename in _LOCKFILE_NAMES:
        return FileClass.LOCKFILE
    if filename.endswith(_GENERATED_SUFFIXES) or any(
        part in _GENERATED_DIR_PARTS for part in normalized.split("/")
    ):
        return FileClass.GENERATED
    if path_is_docs_only(path):
        return FileClass.DOCS
    return FileClass.CODE


def level_for(risk: RiskLevel, file_class: FileClass, *, l1_enabled: bool = True) -> ReviewLevel:
    """Map (risk, file-class) to a review level. Never downgrades risk."""

    if risk is RiskLevel.PRIVILEGED:
        return ReviewLevel.L3
    if risk is RiskLevel.HIGH:
        # HIGH minimum is L2; privileged files escalate to L3.
        return ReviewLevel.L3 if file_class is FileClass.PRIVILEGED else ReviewLevel.L2
    if risk is RiskLevel.MEDIUM:
        return ReviewLevel.L2
    # LOW risk: depth comes from the file class only.
    if file_class is FileClass.PRIVILEGED:
        return ReviewLevel.L3
    if file_class in {FileClass.LOCKFILE, FileClass.GENERATED}:
        return ReviewLevel.L2
    if file_class is FileClass.DOCS:
        return ReviewLevel.L0
    level = ReviewLevel.L1
    if level is ReviewLevel.L1 and not l1_enabled:
        return ReviewLevel.L2
    return level


def resolve_review_plan(
    risk: RiskLevel,
    changed_files: tuple[str, ...],
    *,
    l1_enabled: bool = True,
) -> dict[str, object]:
    """Resolve the overall level as the max over files (escalation only)."""

    levels = tuple(
        level_for(risk, classify_file(path), l1_enabled=l1_enabled) for path in changed_files
    )
    overall = max(levels, key=lambda level: _LEVEL_ORDER[level], default=ReviewLevel.L0)
    return {
        "policy_revision": REVIEW_POLICY_REVISION,
        "level": overall.value,
        "l1_enabled": l1_enabled,
        "file_classes": {path: classify_file(path).value for path in changed_files},
    }


def should_skip_rereview(
    *,
    previous_policy_revision: str,
    previous_base_commit: str,
    previous_result_commit: str,
    previous_diff_hash: str | None,
    base_commit: str,
    result_commit: str,
    diff_hash: str | None,
    policy_revision: str = REVIEW_POLICY_REVISION,
) -> bool:
    """No re-review for an unchanged candidate under the same policy.

    Any change of base/result/diff hash or of the policy revision requires a
    fresh review. A changed hash invalidates the previous verdict.

    D0 note: the dispatch path does not call this helper; every review step
    performs a fresh review. It is kept as a pure, unit-tested predicate for
    future admission use.
    """

    return (
        previous_policy_revision == policy_revision
        and previous_base_commit == base_commit
        and previous_result_commit == result_commit
        and (previous_diff_hash or "") == (diff_hash or "")
    )


def requires_independent(level: ReviewLevel) -> bool:
    """D0 coverage floor: L2/L3 require an independent model call."""

    return level in {ReviewLevel.L2, ReviewLevel.L3}
