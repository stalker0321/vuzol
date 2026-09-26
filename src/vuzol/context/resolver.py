"""Resolve persisted input bindings into provider context (WP02).

Required bindings fail closed: a missing, unresolved, wrong-hash, foreign-scope
or expired required artifact stops the consumer before the provider is called.
Optional bindings may be dropped (and are recorded in ``excluded``).
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.context.models import ContextEntry, ContextManifest
from vuzol.execution.artifacts import ArtifactError, ArtifactStore
from vuzol.providers.domain import ContextItem
from vuzol.storage.models import Artifact, InputBinding, Task

CONTEXT_ITEM_MAX_CHARS = 20_000
CONTEXT_ITEM_MAX_COUNT = 50
RESEARCH_RESULT_SCHEMA = "research-result"
RESEARCH_RESULT_SCHEMA_VERSION = "research-result.v1"


class BindingError(RuntimeError):
    """A required input binding could not be resolved safely."""

    def __init__(self, category: str, message: str | None = None) -> None:
        self.category = category
        super().__init__(message or category)


@dataclass(frozen=True, slots=True)
class ResolvedBinding:
    binding_id: uuid.UUID
    slot: str
    source: str
    reference: str
    content: bytes
    content_hash: str
    schema_name: str
    schema_version: str
    freshness: str
    required: bool


@dataclass(frozen=True, slots=True)
class ResolvedContext:
    bindings: tuple[ResolvedBinding, ...]
    excluded: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.bindings


def estimate_tokens(content: bytes) -> int:
    return max(1, (len(content) + 3) // 4)


async def resolve_context(
    session: AsyncSession,
    artifacts: ArtifactStore | None,
    *,
    consumer_step_id: uuid.UUID,
    project_id: str | None,
) -> ResolvedContext:
    rows = tuple(
        (
            await session.scalars(
                select(InputBinding)
                .where(InputBinding.consumer_step_id == consumer_step_id)
                .order_by(InputBinding.slot, InputBinding.created_at, InputBinding.id)
            )
        ).all()
    )
    if not rows:
        return ResolvedContext(bindings=())
    resolved: list[ResolvedBinding] = []
    excluded: list[str] = []
    for row in rows:
        try:
            resolved.append(await _resolve_one(session, artifacts, row, project_id))
        except BindingError:
            if row.required:
                raise
            excluded.append(row.slot)
    return ResolvedContext(bindings=tuple(resolved), excluded=tuple(excluded))


async def _resolve_one(
    session: AsyncSession,
    artifacts: ArtifactStore | None,
    binding: InputBinding,
    project_id: str | None,
) -> ResolvedBinding:
    if (
        binding.status != "resolved"
        or binding.artifact_id is None
        or binding.content_hash is None
    ):
        raise BindingError("binding_unresolved", f"binding {binding.id} is not resolved")
    artifact = await session.get(Artifact, binding.artifact_id)
    if artifact is None:
        raise BindingError("artifact_missing", f"artifact for binding {binding.id} is missing")
    if artifact.content_hash != binding.content_hash:
        raise BindingError("hash_mismatch", f"binding {binding.id} content hash drifted")
    if (
        binding.scope_project_id is not None
        and project_id is not None
        and binding.scope_project_id != project_id
    ):
        raise BindingError("foreign_scope", f"binding {binding.id} belongs to another scope")
    artifact_task = (
        await session.get(Task, artifact.task_id)
        if artifact.task_id is not None and project_id is not None
        else None
    )
    if artifact_task is not None and artifact_task.project_id != project_id:
        raise BindingError("foreign_scope", "artifact project does not match consumer")
    if artifacts is None:
        raise BindingError("artifact_store_unavailable", "artifact bytes cannot be read")
    try:
        content = artifacts.read(artifact.content_uri)
    except ArtifactError as error:
        raise BindingError("artifact_missing", str(error)) from error
    if hashlib.sha256(content).hexdigest() != binding.content_hash:
        raise BindingError("hash_mismatch", f"binding {binding.id} bytes do not match its hash")
    freshness = "fresh"
    if binding.freshness_max_age_seconds is not None and artifact.created_at is not None:
        age = (datetime.now(UTC) - artifact.created_at).total_seconds()
        if age > binding.freshness_max_age_seconds:
            if binding.required:
                raise BindingError("expired", f"binding {binding.id} is expired")
            freshness = "stale"
    return ResolvedBinding(
        binding_id=binding.id,
        slot=binding.slot,
        source=binding.schema_name,
        reference=f"binding:{binding.id}",
        content=content,
        content_hash=binding.content_hash,
        schema_name=binding.schema_name,
        schema_version=binding.schema_version,
        freshness=freshness,
        required=binding.required,
    )


def pack_context(
    resolved: ResolvedContext, *, role: str
) -> tuple[ContextManifest, tuple[ContextItem, ...]]:
    """Chunk resolved bindings into bounded ContextItems plus a manifest.

    All resolved content is preserved; exceeding the provider context item cap
    fails closed instead of silently truncating mandatory constraints.
    """

    entries: list[ContextEntry] = []
    items: list[ContextItem] = []
    for binding in resolved.bindings:
        text = binding.content.decode("utf-8", "replace")
        chunks = tuple(
            text[offset : offset + CONTEXT_ITEM_MAX_CHARS]
            for offset in range(0, len(text), CONTEXT_ITEM_MAX_CHARS)
        ) or ("",)
        if len(items) + len(chunks) > CONTEXT_ITEM_MAX_COUNT:
            raise BindingError(
                "context_incomplete",
                "resolved context exceeds the provider context item limit",
            )
        for index, chunk in enumerate(chunks, start=1):
            items.append(
                ContextItem(
                    source=binding.source[:100],
                    reference=f"{binding.reference}:part-{index}-of-{len(chunks)}"[:500],
                    content=chunk,
                    content_hash=binding.content_hash,
                )
            )
        entries.append(
            ContextEntry(
                binding_id=binding.binding_id,
                slot=binding.slot,
                source=binding.source[:100],
                reference=binding.reference[:500],
                content_hash=binding.content_hash,
                schema_name=binding.schema_name,
                schema_version=binding.schema_version,
                byte_count=len(binding.content),
                estimated_tokens=estimate_tokens(binding.content),
                truncated=len(chunks) > 1,
                freshness=binding.freshness,
            )
        )
    manifest = ContextManifest(
        role=role,
        entries=tuple(entries),
        excluded=resolved.excluded,
    )
    return manifest, tuple(items)


def estimate_context_tokens(resolved: ResolvedContext) -> int:
    return sum(estimate_tokens(binding.content) for binding in resolved.bindings)
