"""Versioned experiment corpus (WP13, EXPERIMENTS.md §2).

A corpus is a fixed, hash-pinned set of tasks with strata (pilot table §2),
dev/calibration/held-out splits, a smoke-8 subset for harness repair, and a
Jev negative set. Live benchmark is forbidden; CI runs only fixture corpora.
"""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import Field, model_validator

from vuzol.experiments.domain import FrozenModel, stable_json_hash

CORPUS_SCHEMA = "experiment-corpus.v1"
CORPUS_REVISION = "corpus.v1"


class CorpusStratum(StrEnum):
    ISOLATED_CODING = "isolated_coding"
    INTEGRATION_CODING = "integration_coding"
    RESEARCH = "research"
    DATA_FILES = "data_files"
    CAPABILITY_REUSE = "capability_reuse"
    LONG_HORIZON = "long_horizon"
    JEV_NEGATIVE = "jev_negative"


class CorpusSplit(StrEnum):
    DEV = "dev"
    CALIBRATION = "calibration"
    HELD_OUT = "held_out"


class CorpusTask(FrozenModel):
    task_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    family: str = Field(min_length=1, max_length=100)
    stratum: CorpusStratum
    split: CorpusSplit
    smoke: bool = False
    goal: str = Field(min_length=1, max_length=4_000)
    acceptance: tuple[str, ...] = Field(min_length=1, max_length=30)


class CorpusManifest(FrozenModel):
    schema_version: str = CORPUS_SCHEMA
    corpus_revision: str = Field(min_length=1, max_length=100)
    tasks: tuple[CorpusTask, ...] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_unique_tasks(self) -> Self:
        task_ids = [task.task_id for task in self.tasks]
        if len(set(task_ids)) != len(task_ids):
            raise ValueError("corpus task IDs must be unique")
        return self

    @property
    def content_hash(self) -> str:
        return stable_json_hash(self)

    def smoke_tasks(self) -> tuple[CorpusTask, ...]:
        return tuple(task for task in self.tasks if task.smoke)

    def split_tasks(self, split: CorpusSplit) -> tuple[CorpusTask, ...]:
        return tuple(task for task in self.tasks if task.split is split)


def load_corpus_manifest(path: Path) -> CorpusManifest:
    """Load and validate a versioned corpus fixture (deterministic, no live)."""

    return CorpusManifest.model_validate_json(path.read_text(encoding="utf-8"))


def dump_corpus_manifest(manifest: CorpusManifest, path: Path) -> str:
    path.write_text(
        json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest.content_hash
