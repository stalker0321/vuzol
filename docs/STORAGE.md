# PostgreSQL Storage

PostgreSQL is the canonical state store. Telegram, worker memory, logs, worktrees, and artifacts are projections or external resources, never task-state authorities.

## Stack

- PostgreSQL 16.13;
- SQLAlchemy 2 typed async mappings;
- psycopg 3 async driver;
- Alembic transactional migrations with a PostgreSQL advisory lock.

SQLite is not supported for migration, locking, lease, or concurrency tests.

## Local commands

```bash
make db-up
make db-migrate
make db-current
make test-postgres
make db-down
```

The committed credentials are local-development defaults only. Production supplies `VUZOL_DATABASE_DSN_REFERENCE` and its referenced environment or mounted-file secret. The storage factory resolves only scope `system:database`; plaintext DSNs are not retained in runtime configuration or logs.

## Schema

The initial migration creates 20 application tables:

- canonical workflow: `tasks`, `runs`, `steps`, `events`;
- delivery: `external_inbox`, `transactional_outbox`, `topic_mappings`, `telegram_message_links`;
- decisions/evidence: `approvals`, `artifacts`, `usage_records`, `interpretations`, `clarification_decisions`, `validation_results`, `routing_decisions`, `profile_health_observations`, `configuration_revisions`, `provider_profiles`;
- execution resources: `worktrees`, `supervised_processes`.

Stable searchable concepts are columns; provider-neutral envelopes use JSONB. All timestamps are timezone-aware. Telegram IDs use signed `BIGINT`. All 27 foreign keys use `RESTRICT`; deleting a Telegram projection cannot cascade into canonical state.

## Transactions

`UnitOfWork` owns one async session and transaction. Application modules use repositories and detached records, not ambient sessions or live ORM objects.

Atomic operations include:

- task state transition plus append-only event;
- inbox deduplication;
- canonical state plus outbox insert;
- single-use approval consumption;
- step and outbox lease claim/completion.

Step and outbox claims use `FOR UPDATE SKIP LOCKED`, PostgreSQL `now()`, and monotonically increasing fencing generations. Heartbeat or completion with a stale owner/generation affects zero rows and raises `LeaseLost`.

## Migrations

App and worker startup never run migrations. Operators run Alembic explicitly. Migration acquisition, DDL, and release are separated so the session-level advisory lock cannot accidentally roll back transactional DDL.

The initial migration supports lossless downgrade for development verification. Future destructive migrations require backup and roll-forward/restore planning. `alembic check` must report no model drift before completion.

## Verified behavior

- clean upgrade and repeated upgrade to head;
- lossless downgrade and re-upgrade;
- two concurrent migration processes serialize under the advisory lock;
- two workers cannot claim the same step or outbox item;
- stale fencing generations cannot heartbeat or commit;
- illegal transitions and failed units of work roll back;
- approval tokens are single-use;
- inbox duplicates produce one record;
- projection deletion preserves canonical tasks;
- artifacts and usage remain traceable.
- Step 05 persists original intake, private attachment references, raw transcripts, immutable
  schema-versioned interpretations, and the current TaskDraft projection without replacing input.
