# Step 03 — PostgreSQL Persistence Model

## Goal

Create the canonical persistent model for tasks, runs, steps, events, Telegram mappings, approvals, artifacts, profiles, and usage.

## Deliverable

Versioned database migrations and repository interfaces supporting all later workflow behavior without treating Telegram or process memory as state.

## Applicable architecture decisions

- ADR-0001 — PostgreSQL Is the Canonical Source of Truth.
- ADR-0007 — Transactional Delivery and Fenced Leases.

## Core entities

### Task

Represents user intent across one or more attempts.

Minimum fields:

- ID;
- user ID;
- source chat and topic;
- project ID when applicable;
- original text;
- transcript and source voice reference when applicable;
- current normalized TaskDraft;
- current status;
- risk;
- task type;
- created, updated, completed timestamps;
- parent task or continuation reference when needed.

### Run

Represents one execution attempt or resumed workflow.

Minimum fields:

- ID;
- task ID;
- workflow type and version;
- status;
- selected route;
- budget mode;
- start and end timestamps;
- failure category and summary.

### Step

Represents one persisted unit of work.

Minimum fields:

- ID;
- run ID;
- ordinal or dependency metadata;
- step type;
- status;
- executor profile;
- payload;
- result;
- attempt count;
- idempotency classification;
- lease owner and expiry;
- heartbeat;
- timeout;
- timestamps;
- failure and unknown-effects fields.
- lease generation fencing token;
- external idempotency key when applicable.

### Event

Append-only state and audit history:

- entity type and ID;
- event type;
- actor;
- previous and new state when relevant;
- correlation IDs;
- structured payload;
- timestamp.

### Telegram message link

Maps Telegram messages to system entities:

- chat ID;
- message thread ID;
- message ID;
- task, run, step, or approval reference;
- message role;
- created timestamp.

### External inbox

Deduplicates inputs before business-state changes:

- source and consumer identity;
- external update or event ID;
- payload hash and retained raw-payload reference where permitted;
- received and processed timestamps;
- processing outcome and linked entity;
- unique constraint on source, consumer, and external ID.

### Transactional outbox

Represents durable external delivery:

- destination and operation type;
- linked task, run, step, event, or projection;
- desired projection revision;
- idempotency key;
- payload or artifact reference;
- status, attempts, and `available_at`;
- last ambiguous or categorized error;
- lease owner, expiry, and generation;
- created, delivered, and retention timestamps.

### Topic mapping

Persistent topic-to-scope binding.

### Approval

- step ID;
- requested action;
- normalized target;
- human-readable summary;
- one-time token hash;
- status;
- requested and decided timestamps;
- deciding user;
- expiry.

### Artifact

- ID;
- task/run/step references;
- type;
- content-addressed path or URI;
- size;
- hash;
- media type;
- sensitivity and visibility classification;
- retention deadline;
- metadata.

### Usage record

- provider;
- profile;
- model;
- task/run/step IDs;
- input, output, and cached tokens when known;
- cost or quota units when known;
- duration;
- provider request ID;
- outcome.

### Additional operational records

Persist provider profiles and health observations, routing decisions and alternatives, interpretation revisions, clarification decisions, validation results, worktree identity and base commit, supervised process metadata, and configuration or policy revision references. Stable searchable fields remain relational even when provider-neutral envelopes use JSONB.

## State integrity

Use explicit enums or constrained text values.

State changes must occur through repository or service methods that:

1. validate allowed transition;
2. update the row;
3. append an event in the same transaction.

Do not rely on application logging as the audit trail.

Approval consumption, step transition, and creation of any resulting outbox item occur atomically. Database time is authoritative for lease and approval expiry.

## Queue and leases

The `steps` table will later support worker leasing using PostgreSQL row locking.

Prepare fields and indexes for queries equivalent to:

```sql
SELECT ...
FROM steps
WHERE status = 'queued'
  AND available_at <= now()
ORDER BY priority, created_at
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

Do not implement full workflow behavior yet, but prove lease-safe repository operations.

Lease acquisition increments a fencing generation. Heartbeat, result commit, retry scheduling, and outbox delivery completion use compare-and-set checks against the current owner and generation. Expiry alone never authorizes a stale worker to write a result.

## JSON use

JSONB is appropriate for provider-neutral payload and result envelopes, but stable searchable concepts belong in normal columns.

Do not store the entire system as opaque JSON blobs.

## Migrations

Use one migration tool and document:

- upgrade;
- downgrade policy;
- generating a migration;
- testing migrations;
- backup requirement before destructive production migration.

Production migration policy also defines advisory locking, lock and statement timeouts, application/schema compatibility during deploy, and roll-forward or restore behavior when downgrade is unsafe. Application startup must not race multiple automatic migration attempts.

## Repository interfaces

Implement explicit repositories or units of work for:

- tasks;
- runs;
- steps;
- events;
- topic mappings;
- Telegram message links;
- approvals;
- artifacts;
- usage.
- external inbox and transactional outbox;
- interpretations and clarifications;
- profiles, health observations, and routing decisions;
- validation results;
- worktrees and supervised processes;
- configuration and policy revisions.

Keep SQL or ORM details inside storage modules.

## Tests

Required:

- clean database upgrades to latest schema;
- migrations run twice safely where applicable;
- concurrent application startup does not race migrations;
- a representative previous application version either works with the deployment migration sequence or is explicitly prevented from starting;
- task creation persists original input;
- legal state transition succeeds and appends an event atomically;
- illegal transition is rejected;
- concurrent workers cannot lease the same step;
- a stale worker cannot heartbeat or commit after lease generation changes;
- expired lease can be identified;
- approval token cannot be reused;
- Telegram message links resolve to the correct task;
- deleting a Telegram projection does not delete canonical task state;
- usage and artifact rows remain traceable to the task.
- duplicate inbox input creates business state once;
- state change and outbox creation commit or roll back together;
- an outbox item can be retried without duplicating canonical state;

Use a real PostgreSQL test database for concurrency and locking tests. SQLite is not a valid substitute for these tests.

## Forbidden implementations

- state stored only in Telegram messages;
- task status inferred from the latest log line;
- application-wide direct database session access;
- unbounded JSON blobs without stable columns;
- destructive cascade deletes that erase audit history;
- retrying unknown-effect steps automatically;
- best-effort external notification without a persisted outbox item;
- lease acceptance based only on worker-provided time;
- using SQLite behavior to validate PostgreSQL leasing.

## Acceptance criteria

- migrations create the complete MVP persistence foundation;
- task and step transitions are atomic with events;
- message-to-task affinity can be resolved from persisted mappings;
- the schema supports leasing, approvals, artifacts, and usage;
- the schema supports inbox deduplication, durable outbox delivery, validation, routing audit, and execution-resource recovery;
- concurrency tests prove one-step-one-lease behavior;
- database repositories contain no Telegram or provider business logic.

## Completion report

Report:

- entity relationship summary;
- migration tool and commands;
- transition rules implemented;
- concurrency test method;
- indexes added;
- retention and deletion assumptions;
- unresolved schema risks.
