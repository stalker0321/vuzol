# Research provenance (WP06)

Verifiable research artifact + reuse in the next task. One retrieval
integration (variant B, approved HTTP adapter), claim/source report, explicit
synthesis binding. Without retrieval there is no verified research.

## 1. research-result.v1

Contract: `docs/schemas/research-result.v1.schema.json` (+ `docs/schemas/research-result.v1.example.json`, frozen WP00/WP06 copies).
Payload shape:

- `sources[]`: `source_id`, `uri`, `retriever` (`approved-http` |
  `local-docs-fixture`), `retrieved_at`, `content_hash` (sha256 of the exact
  bytes the positions point into), `scope`, `freshness_class` (`fresh` |
  `stale`), optional `quote`.
- `claims[]`: `claim_id`, `statement`, `support` (`supported` | `unsupported` |
  `conflicting`), `citations[]` of (`source_id`, `position`).
- Rules (enforced by `src/vuzol/research/report.py:validate_report`,
  fail-closed): supported requires ≥1 citation; conflicting requires ≥2;
  unknown `source_id` / empty position rejected; zero sources invalid
  (`no_retrieval_no_verified_research`); unsupported is marked, never silent.

## 2. Retrieval (variant B)

`src/vuzol/research/retrieval.py`: `ApprovedHttpRetrieval` behind an
allowlist with bounded redirects/bytes/time (`RetrievalBounds`); live fetch
requires explicit `allow_live=True`, otherwise `live_forbidden`. CI uses
`FixtureRetrieval` over frozen fixtures — deterministic, no network.
Retrieved bytes are opaque data (injection stays inert, never executed).
Capability selection comes from the WP03 registry:
`descriptor_for_capability("web_research")` → `web-research` (ACTION,
read_only); unmapped capabilities have no selection.

## 3. Claim/source report + synthesis binding + D0 guard

`src/vuzol/research/synthesize.py`: `sources_from_retrieved` lifts transport
sources (hash/scope/retriever/freshness), `bind_report` builds + validates,
`build_synthesis_context` renders the explicit block with `[verified]` /
`[unsupported]` / `[conflicting]` markers, per-citation hashes and `(stale)`
flags. Invalid reports never produce a block (`SynthesisError`).
D0 split: the provider-step text wrapper is `research-provider-result.v1`
(`src/vuzol/providers/handlers.py:_research_result_bytes`), read by legacy
readers without a verified label; `research-result.v1` is reserved for
`sources[]/claims[]`. The synthesize consumer validates claimed
source-report bytes fail-closed (`research/report.py:validate_source_report_bytes`
via `jsonschema.Draft202012Validator` + `validate_report`) before any provider
spend — mismatched schema/shape raises `BindingError(source_report_schema_mismatch)`
as a pre-provider failure (reservation released). Retrieval is not connected to
`research_execute` in D0 (D3 scope): only the contract + fail-closed consumer.
Persistence reuses the WP02 path unchanged: `Artifact(research_result,
application/json)` + `InputBinding(slot=predecessor_result,
schema=research-provider-result.v1 for legacy provider text,
schema=research-result.v1 for verified source reports, required, hash-pinned)` → `resolve_context`
(hash-drift / foreign-scope / freshness enforced, fail-closed).

## 4. Freshness policy

Per source class, enforced at resolve time (`context/resolver.py`):
`fresh` (within `freshness_max_age_seconds`), `stale` (exceeded, only for
non-required bindings — visible, not verified), `expired` (exceeded on a
required binding — consumer stops). Report-level `freshness_class` mirrors
the resolve outcome so stale citations stay visible in the synthesis block.

## 5. End-to-end example (question → report → consumer)

```python
retrieved = FixtureRetrieval(fixtures={"fixture://adapter.md": b"..."}).fetch(
    "fixture://adapter.md", now="2026-09-27T10:00:00Z")
(source,) = sources_from_retrieved((retrieved,), retriever="local-docs-fixture", scope="vuzol")
report = bind_report(research_id=..., question="Which adapter backs CI?",
    sources=(source,),
    claims=(Claim(claim_id="c1", statement="CI is fixture-based.",
                  support="supported", citations=(("s1", "para 1"),)),),
    created_at="2026-09-27T10:10:00Z")
payload = canonical_json(report)  # sha256-pinned
artifact = await store.persist(session, ..., content=payload, ...)
binding = InputBinding(consumer_step_id=synthesize.id, artifact_id=artifact.id,
    schema_name="research-result", schema_version="research-result.v1",
    content_hash=artifact.content_hash, required=True, status="resolved", ...)
resolved = await resolve_context(session, store, consumer_step_id=..., project_id="vuzol")
block = build_synthesis_context(report)  # citations + markers for the summarizer
```

Covered on postgres by
`tests/integration/providers/test_research_provenance.py` (flow + stale).
Old `research.v1` without bindings keeps the legacy no-op path
(`docs/CONTEXT_MANIFEST.md`); a follow-up task reuses any valid source
artifact via its `content_hash`.

## 6. Boundaries

No Drive auth, no crawler, no giant RAG, no repo-wide embeddings. Forbidden
in CI: live web, unallowlisted hosts, unbounded fetch. Approvals,
permissions, secret scope, budget semantics, review minimum unchanged.
