"""Versioned context assembly for provider requests (WP02).

Public surface:

- ``ContextManifest`` / ``ContextEntry``: versioned provenance of what a consumer
  received.
- ``resolve_context`` / ``pack_context``: resolve persisted ``InputBinding`` rows
  into bounded ``ContextItem`` content, fail-closed for required bindings.

Compatibility: the legacy path (no binding rows) still builds context as before;
``ContextItem`` remains the provider request contract.
"""

from vuzol.context.models import (
    CONTEXT_MANIFEST_SCHEMA,
    ContextEntry,
    ContextManifest,
)
from vuzol.context.resolver import (
    RESEARCH_RESULT_SCHEMA,
    RESEARCH_RESULT_SCHEMA_VERSION,
    BindingError,
    ResolvedBinding,
    ResolvedContext,
    estimate_context_tokens,
    estimate_tokens,
    pack_context,
    resolve_context,
)

__all__ = [
    "CONTEXT_MANIFEST_SCHEMA",
    "RESEARCH_RESULT_SCHEMA",
    "RESEARCH_RESULT_SCHEMA_VERSION",
    "BindingError",
    "ContextEntry",
    "ContextManifest",
    "ResolvedBinding",
    "ResolvedContext",
    "estimate_context_tokens",
    "estimate_tokens",
    "pack_context",
    "resolve_context",
]
