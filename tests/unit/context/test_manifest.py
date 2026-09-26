"""Unit tests for the versioned context manifest packing rules."""

from __future__ import annotations

import hashlib
import uuid

import pytest

from vuzol.context.resolver import (
    BindingError,
    ResolvedBinding,
    ResolvedContext,
    estimate_tokens,
    pack_context,
)


def _binding(content: bytes, *, slot: str = "predecessor_result") -> ResolvedBinding:
    return ResolvedBinding(
        binding_id=uuid.uuid4(),
        slot=slot,
        source="research-result",
        reference="binding:test",
        content=content,
        content_hash=hashlib.sha256(content).hexdigest(),
        schema_name="research-result",
        schema_version="research-result.v1",
        freshness="fresh",
        required=True,
    )


def test_pack_context_builds_versioned_manifest() -> None:
    content = b"research marker: synthesize me"
    manifest, items = pack_context(
        ResolvedContext(bindings=(_binding(content),)), role="summarizer"
    )

    assert manifest.schema_version == "context-manifest.v1"
    assert manifest.role == "summarizer"
    assert len(manifest.entries) == 1
    entry = manifest.entries[0]
    assert entry.content_hash == hashlib.sha256(content).hexdigest()
    assert entry.estimated_tokens == estimate_tokens(content)
    assert entry.schema_version == "research-result.v1"
    assert items[0].content == content.decode()
    assert items[0].content_hash == entry.content_hash
    assert not entry.truncated


def test_pack_context_fails_closed_when_item_limit_exceeded() -> None:
    bindings = tuple(_binding(b"x", slot=f"slot-{index}") for index in range(51))
    with pytest.raises(BindingError) as error:
        pack_context(ResolvedContext(bindings=bindings), role="summarizer")
    assert error.value.category == "context_incomplete"


def test_pack_context_preserves_large_content_by_chunking() -> None:
    content = ("ы" * 45_000).encode()
    manifest, items = pack_context(
        ResolvedContext(bindings=(_binding(content),)), role="summarizer"
    )

    assert len(items) == 3
    assert "".join(item.content for item in items) == content.decode()
    assert manifest.entries[0].truncated is True
