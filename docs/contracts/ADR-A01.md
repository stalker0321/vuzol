# ADR-A01 — Execution ownership, record identity and version semantics

> Status: frozen, base 9be9054. Immutable frozen copy (WP00 baseline) — do not edit semantics.

- **Status:** Proposed (WP00 contracts baseline). Not yet accepted.
- **Scope:** Horizon / Attempt / Effect / InputBinding / PermissionEnvelope.
- **Base commit:** `79d7326e3e490647cb8a0e50c2c25ae23daa6409` (audit commit = current `main`).
- **Relation to existing decisions:** complements ADR-0001, ADR-0007, ADR-0008 and invariants 1, 3, 4, 10, 16, 18; it does **not** supersede, rename or weaken any accepted ADR. SQL table/column names are migration details, not part of this contract.

## Context

WP01+ must not invent incompatible state models. The code already has `Task`, `Run`, `Step`, `WorkPackage`, `PlanRevision`, `Approval`, `Artifact`, `UsageRecord` and `ProviderBudgetReservation` (`src/vuzol/storage/models.py`), with `version`, `lease_generation`, `attempt_count` and `content_hash` fields. The report §14/§27 (D01, D04, D06, D10) asks for one authoritative ownership model, immutable attempt/effect identity, and a clear split between ownership state and content truth. Today there is no append-only Attempt ledger and no persisted Effect intent; `Step.attempt_count` is a counter, not a lineage, and unknown effects are represented by a boolean (`Step.unknown_effects`) rather than a durable operation key.

## Decision

### A01.1 Single writer per record

The **runtime** (control plane) is the only author of Horizon / Task / Run / Step / Attempt / Effect *state* in PostgreSQL. Other actors produce observations or artifacts, never authoritative state:

| Actor | May author | Must not author |
|---|---|---|
| runtime (control loop) | state, status, transitions, references, Event + outbox | content bytes |
| executor / worker | observation, output artifact (via ArtifactStore) | step/run/task status |
| compiler | frozen plan snapshot after policy validation | budget, permissions |
| validator | evidence records | business-state transitions |
| applier | the apply effect under a runtime transition | approval status decisions |
| UI / Telegram / CLI | commands from a principal | canonical state |

A commit is one DB transaction that mutates state **and** writes the corresponding `Event` and any required outbox row (invariant 1, ADR-0001, ADR-0007). No chat, prompt or model memory becomes a second state store.

### A01.2 Source-of-truth split

| Fact | Authority | Notes |
|---|---|---|
| Ownership, status, references, grants, budget | PostgreSQL | canonical; reconstructable projections (Telegram) are derived |
| Repository content | Git commit/ref + hash | `repository_revision`, `result_commit`, `integration_head_commit` |
| Immutable bytes | ArtifactStore `content_hash` (sha256) | DB stores metadata + reference only |
| Model/session history | none | derived, never authoritative |

### A01.3 Record ownership, identity, version semantics

| Record | Owner | Identity | Version / fence semantics | Storage |
|---|---|---|---|---|
| **Horizon** | runtime | `horizon_id` (UUID), stable across the horizon | `goal_revision` = monotonic semantic revision of goal+acceptance; `version` = optimistic-concurrency row version; `status` follows the lifecycle below | PostgreSQL. v1 = additive evolution of `WorkPackage`; the SQL table is not renamed. Goal stays a field, not a separate aggregate. |
| **Attempt** | runtime | `attempt_id` (UUID); unique `(step_id, attempt_no)` | Immutable after close. Retry/repair/takeover = **new** row with `parent_attempt_id` lineage. `attempt_no` = work lineage; `lease.generation` = fencing token; `provider_attempt` = provider retry counter — three different things, never conflated. `purpose`/`attempt_kind` immutable. | PostgreSQL append-only + optional usage ref |
| **Effect** | applier (effectful adapter) under a runtime transition | `effect_id` (UUID); `operation_key` stable and unique, created **before** launch | `operation_key` immutable; retry of the same effect reuses it; a new business effect gets a new key. Launch carries `lease_generation`; late/fenced generations cannot settle the effect. | PostgreSQL intent + receipt; external receipt as artifact ref |
| **InputBinding** | runtime (compiler records intent; runtime resolves) | `binding_id` (UUID); logical key `(consumer.item_id, artifact_id, content_hash, schema_version)` | Immutable once recorded. `content_hash` pins exact bytes; `schema_version` pins shape. Missing / wrong-hash / foreign-scope / expired **required** binding stops the consumer. A semantic output change requires a new schema version. | PostgreSQL ref; bytes live in ArtifactStore |
| **PermissionEnvelope** | issuer = human principal or policy, **never the model** | `envelope_id` (UUID) + `envelope_hash` (sha256 over canonical form) | Immutable; any material change → new envelope, new hash, invalidated prior approval binding (invariant 35). `revocation_version` monotonic. | PostgreSQL/policy authoritative; hash referenced by Horizon and Effect |

Schemas: `schemas/horizon.schema.json`, `schemas/attempt.schema.json`, `schemas/effect.schema.json`, `schemas/input-binding.schema.json`, `schemas/permission-envelope.schema.json`.

### A01.4 Identity vs version vs fence (explicit)

- **identity** — stable UUID (`*_id`).
- **content identity** — sha256 (`content_hash`, `diff_hash`, `envelope_hash`, `payload_hash`).
- **lineage** — `parent_*_id` links (attempt lineage, plan revision parent).
- **fence** — `lease_generation` (monotonic, checked on every settle).
- **optimistic concurrency** — row `version` (compare-and-swap on update).
- **semantic revision** — `goal_revision`, `pricing_revision`, `policy_revision`, `configuration_revision`, `schema_version`.

These are orthogonal. `retry` must not increment a fence, and a fence must not create a new business effect.

### A01.5 Horizon lifecycle

`draft → ready → running ↔ evaluating → succeeded`, with alternatives `waiting_resource`, `waiting_approval`, `waiting_input`, `paused`, `failed`, `cancelled`. An empty work-item queue is **not** success; success requires an acceptance artifact bound to the current `goal_revision`. Partial results are retained but not marked as full success.

### A01.6 Effect recovery contract

Order: (1) in a short transaction validate state revision, lease generation, grant, descriptor and budget, then persist Attempt + Effect intent and the stable `operation_key`; (2) run the effect **outside** the DB transaction; (3) persist receipt/artifact and settle under the fence. Crash between (2) and (3) → reconcile by `operation_key` / external receipt; if the result cannot be proven → `uncertain`, and blind retry is forbidden. DB fencing does not physically stop a zombie; non-fenceable providers require an idempotency key or single-active-operation + reconciliation.

### A01.7 Compatibility with existing runs

- Additions are **additive nullable** columns/tables; one side remains authoritative, no uncontrolled dual-write (report §23).
- An old run stays pinned to its workflow/policy/configuration revision across restart; a new flow is opt-in behind a flag.
- Legacy rows are backfilled with provenance `legacy`/`unknown`, never with fabricated history.
- Existing `Step.attempt_count`, `Task.budget_epoch`, `Approval`, `Artifact`, `WorkPackage` semantics are preserved; new Attempt/Effect rows wrap them instead of replacing them.

## Consequences

- Runtime becomes the single reconciliation point; executors are observation sources.
- WP01 (Attempt/ledger), WP05 (Effect/reconciliation) and WP08 (Horizon runtime) get a frozen I/O contract.
- Migration cost: additive tables + nullable refs, one migration owner.
- Retrieval of "who changed what" is answered by `Event` + `Attempt` + `Effect`, not by logs.

## Invariants preserved

1, 3, 4, 10, 16, 18; ADR-0001, ADR-0007, ADR-0008. No existing ADR is changed.

## Not decided here (follow-ups, do not implement under WP00)

- Physical SQL table/column naming and whether `WorkPackage` is eventually renamed.
- A separate `Goal` aggregate (deferred until multiple horizons per goal).
- Second execution node transport and Node model.
- The exact `PricingRevision` schema belongs to ADR-A02.
