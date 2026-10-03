"""Decision-opportunity corpus with pre-decision snapshots and human labels (J5).

Separate from the bench ``corpus.py``: the unit of observation is one decision
opportunity recorded *before* the outcome (current turn, available sources,
coverage and allowed runtime options). Labels are human, not runtime output.

Temporal-leakage guard: opportunities that share a ``group_id`` (same
conversation/project/time cluster, or synthetic paraphrases of one case) must
stay in a single split; paraphrases are never independent observations.
"""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import Field

from vuzol.experiments.domain import FrozenModel, stable_json_hash

DECISION_CORPUS_SCHEMA = "decision-corpus.v3"


class DecisionCorpusError(ValueError):
    """The corpus is malformed or leaks across splits."""


class DecisionFamily(StrEnum):
    INTENT = "intent"
    REFERENCES = "references"
    PENDING_DIALOGUE = "pending_dialogue"
    COMPOUND = "compound"
    WORK_SHAPE = "work_shape"
    CONTEXT_DRIFT = "context_drift"
    ADVERSARIAL = "adversarial"
    RECOVERY_REVIEW = "recovery_review"


class DecisionSplit(StrEnum):
    DEV = "dev"
    CALIBRATION = "calibration"
    HELD_OUT = "held_out"


class DecisionLabel(FrozenModel):
    """Human label for one opportunity. Runtime output is never the label."""

    effect: str | None = None
    relation: str | None = None
    target_ref: str | None = None
    abstain_allowed: bool = False
    required_sources: tuple[str, ...] = ()
    danger: str = Field(default="low", pattern=r"^(low|medium|high)$")


class DecisionOpportunity(FrozenModel):
    opportunity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,99}$")
    group_id: str = Field(min_length=1, max_length=100)
    family: DecisionFamily
    split: DecisionSplit
    current_turn: str = Field(min_length=1, max_length=4_000)
    snapshot_ref: str = Field(min_length=1, max_length=200)
    allowed_effects: tuple[str, ...] = ()
    allowed_refs: tuple[str, ...] = ()
    label: DecisionLabel
    # A fixed recorded model response for deterministic replay (optional).
    recorded_response: dict[str, Any] | None = None


class DecisionCorpus(FrozenModel):
    schema_version: str = DECISION_CORPUS_SCHEMA
    corpus_revision: str = Field(min_length=1, max_length=100)
    opportunities: tuple[DecisionOpportunity, ...] = Field(min_length=1, max_length=2_000)

    @property
    def content_hash(self) -> str:
        return stable_json_hash(self)

    def by_family(self, family: DecisionFamily) -> tuple[DecisionOpportunity, ...]:
        return tuple(item for item in self.opportunities if item.family is family)

    def by_split(self, split: DecisionSplit) -> tuple[DecisionOpportunity, ...]:
        return tuple(item for item in self.opportunities if item.split is split)

    def families(self) -> tuple[DecisionFamily, ...]:
        present = {item.family for item in self.opportunities}
        return tuple(family for family in DecisionFamily if family in present)


def validate_no_temporal_leakage(corpus: DecisionCorpus) -> None:
    """Every group lives in exactly one split; paraphrases never straddle."""

    seen: dict[str, DecisionSplit] = {}
    ids: set[str] = set()
    for opportunity in corpus.opportunities:
        if opportunity.opportunity_id in ids:
            raise DecisionCorpusError(f"duplicate opportunity id: {opportunity.opportunity_id}")
        ids.add(opportunity.opportunity_id)
        existing = seen.get(opportunity.group_id)
        if existing is not None and existing is not opportunity.split:
            raise DecisionCorpusError(
                f"temporal leakage: group {opportunity.group_id} spans {existing.value} "
                f"and {opportunity.split.value}"
            )
        seen[opportunity.group_id] = opportunity.split


def load_decision_corpus(path: Path) -> DecisionCorpus:
    corpus = DecisionCorpus.model_validate_json(path.read_text(encoding="utf-8"))
    validate_no_temporal_leakage(corpus)
    return corpus


def dump_decision_corpus(corpus: DecisionCorpus, path: Path) -> str:
    path.write_text(
        json.dumps(corpus.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return corpus.content_hash
