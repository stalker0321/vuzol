"""Versioned context manifest and entries for provider requests (WP02).

The manifest records what was selected for a consumer, the exact content hash of
each resolved predecessor artifact, estimated size, freshness and truncation
flags. It is a derived provenance object; PostgreSQL remains the source of truth
for bindings and the artifact store for bytes.
"""

from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field

CONTEXT_MANIFEST_SCHEMA = "context-manifest.v1"


class FrozenContextModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ContextEntry(FrozenContextModel):
    binding_id: uuid.UUID
    slot: str = Field(min_length=1, max_length=100)
    source: str = Field(min_length=1, max_length=100)
    reference: str = Field(min_length=1, max_length=500)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_name: str = Field(min_length=1, max_length=100)
    schema_version: str = Field(min_length=1, max_length=100)
    byte_count: int = Field(ge=0)
    estimated_tokens: int = Field(ge=0)
    truncated: bool = False
    freshness: str = Field(default="fresh", pattern=r"^(fresh|stale|unknown)$")


class ContextManifest(FrozenContextModel):
    schema_version: str = CONTEXT_MANIFEST_SCHEMA
    role: str = Field(min_length=1, max_length=50)
    entries: tuple[ContextEntry, ...] = ()
    excluded: tuple[str, ...] = ()
    incomplete: bool = False

    @property
    def total_bytes(self) -> int:
        return sum(entry.byte_count for entry in self.entries)

    @property
    def estimated_tokens(self) -> int:
        return sum(entry.estimated_tokens for entry in self.entries)

    @property
    def content_hashes(self) -> tuple[str, ...]:
        return tuple(entry.content_hash for entry in self.entries)
