# Step 03 PostgreSQL Persistence Implementation Plan

## Status

Planning pass complete. No Step 03 runtime code or migration has been applied yet.

Typed database pool, timeout, and migration-lock settings are implemented as the first prerequisite. Storage runtime code and migrations remain not started.

## Technology decision

- SQLAlchemy 2 typed declarative models and explicit async sessions;
- psycopg 3 binary distribution for PostgreSQL access;
- Alembic with an async migration environment;
- PostgreSQL 16 in development and integration tests;
- no SQLite compatibility layer.

The current workload is small, but the control plane and future Telegram/workflow workers are asynchronous. One async persistence contract avoids maintaining parallel sync and async repositories. SQLAlchemy stays inside `vuzol.storage`; domain and policy modules receive repository protocols or a unit of work.

## Package layout

```text
src/vuzol/storage/
  database.py          engine/session composition
  types.py             constrained database enums and shared types
  models/              SQLAlchemy table mappings by domain
  repositories/        repository implementations
  transitions.py       legal transition tables and atomic transition service
  unit_of_work.py      transaction ownership
  leasing.py           fenced step/outbox lease operations
  errors.py            provider-neutral storage errors
alembic/
  env.py
  versions/0001_mvp_persistence_foundation.py
tests/integration/storage/
```

## Secret and engine boundary

`Settings.database_dsn_reference` remains a reference. The storage composition factory resolves it only for consumer scope `system:database`, creates the engine, and immediately discards the plaintext value. The runtime configuration model, logs, exceptions, revisions, and engine representation must not expose it.

Engine defaults:

- `pool_pre_ping=True`;
- bounded pool size and timeout from typed settings;
- PostgreSQL statement and lock timeouts for migrations and worker claims;
- UTC database timestamps using `now()`;
- explicit transaction ownership; no application-wide ambient session.

## Identifier and timestamp policy

- application-generated UUIDv7-compatible UUID values when library support is stable, otherwise UUID4 for MVP;
- Telegram IDs remain signed `BIGINT`;
- all timestamps are `TIMESTAMPTZ` and database-generated where ordering or expiry matters;
- external idempotency keys and content hashes use bounded text with unique constraints;
- mutable rows carry an integer version for compare-and-set updates where needed.

## Tables

### Canonical workflow

`tasks`

- source user/chat/topic, project, original text, transcript and voice reference;
- current TaskDraft JSONB plus schema/prompt/interpreter revisions;
- status, risk, task type, parent task, timestamps.

`runs`

- task, workflow name/version, route, budget mode, status and failure summary;
- configuration, policy, prompt and repository revisions;
- start/end timestamps.

`steps`

- run, ordinal/dependency metadata, type, status, executor profile;
- typed envelope JSONB payload/result with stable searchable columns outside JSONB;
- retry/idempotency classes, attempts, `available_at`, timeout;
- lease owner, expiry, heartbeat and monotonically increasing generation;
- unknown-effects marker, failure category and timestamps.

`events`

- append-only entity/actor/event data, previous/new state, correlation IDs and payload;
- no update or cascade-delete path from canonical entities.

### External delivery and Telegram

`external_inbox`

- source, consumer, external event ID, payload hash/reference, outcome and linked entity;
- unique `(source, consumer, external_event_id)`.

`transactional_outbox`

- destination, operation, entity/event link, projection revision, idempotency key;
- payload/artifact reference, attempts, availability, categorized/ambiguous error;
- fenced lease fields and delivered/retention timestamps.

`topic_mappings` and `telegram_message_links`

- unique topic `(chat_id, message_thread_id)`;
- unique Telegram message `(chat_id, message_id)` with thread and entity role;
- deletion of a projection never deletes canonical workflow rows.

### Decisions and evidence

- `approvals`: step, immutable action-envelope hash, token hash, target, status, actor and database expiry;
- `artifacts`: entity links, content address, size/hash/media type, sensitivity, visibility and retention;
- `usage_records`: provider/profile/model, entity links, token/cost/quota/duration/request identity/outcome;
- `interpretations` and `clarification_decisions`;
- `validation_results`;
- `routing_decisions` and `profile_health_observations`;
- `configuration_revisions` containing normalized non-secret content or an artifact reference.

### Execution resources

- `worktrees`: task/run, project, source remote, base commit, branch, path, ownership, delivery state and timestamps;
- `supervised_processes`: step, command envelope hash, cwd, pid/container identity, lifecycle, exit/signal and stdout/stderr artifacts.

## Constraints and indexes

- database check constraints for every status, risk, retry and idempotency vocabulary;
- unique stable profile/configuration IDs where persisted;
- partial queue indexes on queued steps/outbox items ordered by priority and availability;
- lease-expiry indexes for active leased rows;
- task/run/status and event/entity/time indexes;
- artifact content hash and retention indexes;
- approval token hash unique, action-envelope hash indexed;
- foreign keys default to `RESTRICT`; only projection/ephemeral child records may cascade after explicit review.

## Transactions and repositories

The unit of work owns one `AsyncSession` and never leaks it to application modules. Initial interfaces:

- task, run, step and event repositories;
- inbox/outbox repositories;
- topic and Telegram-link repositories;
- approval, artifact and usage repositories;
- interpretation, validation, routing/configuration and execution-resource repositories.

Required atomic services:

1. create task plus original-input event;
2. validate transition, update entity and append event;
3. deduplicate inbox input plus create canonical intake/outbox work;
4. consume approval plus transition exact step and create outbox item;
5. claim/heartbeat/complete step with owner and fencing generation compare-and-set.

Repository methods return domain-neutral records or typed DTOs, not live ORM objects tied to a closed session.

## Leasing protocol

Claim uses `SELECT ... FOR UPDATE SKIP LOCKED`, database `now()`, capability predicates and deterministic ordering. In the same transaction it increments `lease_generation`, assigns owner/expiry and returns both identifiers.

Heartbeat, completion and retry scheduling require `(id, lease_owner, lease_generation)` and affected-row count exactly one. An old generation cannot create successor steps, publish a final artifact or complete outbox delivery. Unknown-effect expiry is blocked rather than requeued.

## State transitions

Transition vocabularies are Python enums mirrored by database checks. Legal task/run/step transition maps live in one storage-adjacent service. Each transition locks or compare-and-sets the current row and appends an event in the same transaction. Tests cover every legal edge and representative illegal edges; application logs are not audit state.

## Migration policy

The initial migration creates the complete MVP persistence foundation rather than a partial subset. Alembic configuration includes:

- one migration advisory lock;
- lock and statement timeouts;
- explicit upgrade command and schema-head startup check;
- downgrade only while lossless; destructive rollback uses restore/roll-forward documentation;
- migration tests from an empty PostgreSQL database and from the previous tagged schema;
- no automatic concurrent migration from every app/worker process.

## Test topology

Compose adds PostgreSQL 16 with a health check and named development volume. Integration tests use a separate disposable database, run Alembic to head, and isolate test data by database/schema recreation. CI and local commands fail rather than fall back to SQLite.

Mandatory tests:

- empty upgrade and repeated head check;
- concurrent startup migration lock;
- original input and interpretation retention;
- transition/event atomicity and rollback on event failure;
- two workers cannot claim one step;
- stale fencing generation cannot heartbeat or commit;
- safe expired lease is discoverable; unknown-effect lease is not blindly queued;
- inbox duplicate creates canonical state once;
- state change and outbox insert commit or roll back together;
- single-use/expired approval under concurrency;
- Telegram projection deletion preserves canonical task;
- artifacts and usage remain traceable;
- DSN and secret values do not appear in logs, reprs or errors.

## Implementation sequence

1. Add dependencies, typed database settings, PostgreSQL Compose test service and Alembic environment.
2. Implement enums, all table mappings, constraints and initial migration.
3. Add engine/session/unit-of-work composition with scoped DSN resolution.
4. Implement canonical repositories and transition/event transaction.
5. Implement inbox/outbox and fenced leasing operations.
6. Implement remaining evidence/resource repositories.
7. Run real PostgreSQL migration, concurrency and recovery tests.
8. Perform an independent schema/index/delete-policy review before marking Step 03 complete.

## Explicit deferrals

- no workflow scheduler or worker dispatch behavior from Step 06;
- no Telegram API behavior from Step 04;
- no provider health polling or routing scoring from Step 07;
- no retention deletion job from Step 10;
- no SQLite test adapter.
