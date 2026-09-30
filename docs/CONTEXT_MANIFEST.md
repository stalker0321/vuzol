# Context manifest and input bindings (WP02)

Status: implemented for the first vertical slice `research_execute → synthesize`.
Legacy behavior is preserved: a consumer with no binding rows builds context
exactly as before.

## Records

- **InputBinding** (`input_bindings`): one explicit, hash-pinned predecessor
  output for a consumer step. Columns: `consumer_step_id`, `producer_step_id`,
  `artifact_id`, `slot`, `schema_name`, `schema_version`, `content_hash`,
  `scope_project_id`, `access_scope`, `required`, `status`
  (`pending|resolved`), `freshness_max_age_seconds`, `created_at`, `resolved_at`.
  Unique per `(consumer_step_id, slot)`.
- **Artifact**: the predecessor bytes are persisted through the existing
  `ArtifactStore` (`content_hash` = sha256, `content_uri` = `artifact:<path>`).

The versioned JSON contract remains
`docs/schemas/input-binding.v1.schema.json` (InputBinding v1, frozen copy of WP00). The DB row is
the implementation; `consumer.item_id` maps to `consumer_step_id` and
`scope.project_id` to `scope_project_id`.

## Manifest (`context-manifest.v1`)

`vuzol.context.models.ContextManifest` records what the consumer actually
received: `role`, one `ContextEntry` per resolved binding (`binding_id`, `slot`,
`source`, `reference`, `content_hash`, `schema_name`, `schema_version`,
`byte_count`, `estimated_tokens`, `truncated`, `freshness`), plus `excluded`
slots and an `incomplete` flag. It is derived provenance; PostgreSQL owns the
binding rows and the artifact store owns bytes (`docs/contracts/ADR-A01.md`).

## Resolution and fail-closed rules

`vuzol.context.resolver.resolve_context(session, artifacts, consumer_step_id,
project_id)`:

1. `status != resolved` / missing `artifact_id` / missing `content_hash`
   → `binding_unresolved`.
2. Artifact row missing → `artifact_missing`.
3. `artifact.content_hash != binding.content_hash` → `hash_mismatch`.
4. `binding.scope_project_id` or the artifact's task project differs from the
   consumer project → `foreign_scope`.
5. Artifact bytes unreadable from the managed root → `artifact_missing`.
6. Re-hashed bytes differ from `content_hash` → `hash_mismatch`.
7. `freshness_max_age_seconds` exceeded → required: `expired`; optional:
   included with `freshness="stale"`.

A **required** failure raises `BindingError`; the handler turns it into a
pre-provider failure category `context_binding_<category>` and releases the
reservation, so the provider is never called. **Optional** failures are dropped
and recorded in `manifest.excluded`.

`pack_context` chunks each binding into bounded `ContextItem`s
(≤20 000 chars, ≤50 items total). Exceeding the item limit raises
`context_incomplete` rather than silently truncating mandatory constraints.
Large content is preserved across chunks (`truncated=true` in the manifest
entry is a size flag, not a data loss flag).

## Compatibility with `ContextItem`

`ContextItem` is unchanged (`source`, `reference`, `content`, `content_hash`;
`ProviderRequest.context` ≤ 50). The resolver emits `ContextItem`s so every
existing adapter keeps working:

- legacy sources (`workflow_plan_result`, `system_skill`, `system_repair`) still
  build `ContextItem`s in `_build_request` for `execute_code`/`execute_agent`;
- the new path appends binding-derived `ContextItem`s for `synthesize`;
- a consumer without binding rows sees an empty resolved context and therefore
  the previous behavior (no-op for research/synthesize).

`prepare_context` remains a no-op for existing coding workflows; WP02 adds a new
binding-driven path and does not rewrite the old one. Old runs keep their pinned
workflow/policy revisions.

## Estimate before reservation (E21)

The producer writes `context_estimate_tokens` into the consumer step payload
when it creates the binding. Routing's `_estimated_input_tokens` now sums the
original text, the task draft, that declared context estimate, the output JSON
schema and the policy/prompt revisions, so the reservation is not systematically
undersized. The reservation is still created before the bytes are resolved
(manifest-first estimate); `_build_request` performs the DB/file resolution and
validates hashes.

## Producer

`ProviderStepHandler._persist_input_bindings` runs after a successful
`research_execute`: it persists the result as a `research_result` artifact
(`research-provider-result.v1` legacy provider text, without a verified label),
finds the downstream `synthesize` step via its
`dependency_metadata.predecessor_ordinals`, and records one required, resolved
`InputBinding`. It is idempotent per `(consumer_step_id, slot)`.

## Migration / backfill

Revision `b3f7a2c91e04` (parent `c1a9f0b7d234`), single linear head. It only
creates `input_bindings`; there is no data backfill (existing runs have no rows
and keep legacy behavior). `downgrade()` drops the table. Deploy requires the
usual `migration_preflight` head sync.

## Accounting

The producer persists bindings inside the same transaction as the WP01 usage
reconciliation, and the consumer's resolution failure releases its reservation
through the existing pre-provider unwind. Budget/reservation semantics are
unchanged.
