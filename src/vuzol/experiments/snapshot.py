"""Frozen policy snapshot for paired experiment runs (WP13, EXPERIMENTS.md §3).

Before a cohort starts, profiles, prices, prompts, tool versions, env and
cache policy are pinned with the policy/configuration revision. The snapshot
is immutable (frozen model + content hash); any drift invalidates comparison.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field

from vuzol.experiments.domain import FrozenModel, stable_json_hash

SNAPSHOT_SCHEMA = "experiment-policy-snapshot.v1"


class SnapshotProfile(FrozenModel):
    profile_id: str = Field(min_length=1, max_length=100)
    provider: str = Field(min_length=1, max_length=100)
    model: str = Field(min_length=1, max_length=200)
    roles: tuple[str, ...] = ()


class SnapshotPricing(FrozenModel):
    pricing_revision: str = Field(min_length=1, max_length=100)
    input_per_million: Decimal | None = Field(default=None, ge=0)
    output_per_million: Decimal | None = Field(default=None, ge=0)
    configured_cost_per_call: Decimal | None = Field(default=None, ge=0)
    unknown: bool = False


class PolicySnapshot(FrozenModel):
    schema_version: str = SNAPSHOT_SCHEMA
    snapshot_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    policy_revision: str = Field(min_length=1, max_length=200)
    configuration_revision: str = Field(min_length=1, max_length=200)
    profiles: tuple[SnapshotProfile, ...] = Field(min_length=1)
    pricing: tuple[SnapshotPricing, ...] = Field(default=())
    prompt_versions: dict[str, str] = Field(default_factory=dict)
    tool_versions: dict[str, str] = Field(default_factory=dict)
    environment: dict[str, str] = Field(default_factory=dict)
    cache_policy: str = Field(default="", max_length=1_000)
    created_at: datetime

    @property
    def snapshot_hash(self) -> str:
        return stable_json_hash(self)

    def pricing_for(self, pricing_revision: str) -> SnapshotPricing | None:
        for entry in self.pricing:
            if entry.pricing_revision == pricing_revision:
                return entry
        return None
