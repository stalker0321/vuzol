# ADR-0001 — PostgreSQL Is the Canonical Source of Truth

## Status

Accepted

## Decision

Use PostgreSQL for canonical task, run, step, event, approval, topic-mapping, artifact-metadata, and usage state.

Telegram and in-memory processes are projections or workers, not state authorities.

## Reason

The system requires concurrent workers, leases, restart recovery, atomic state transitions, audit history, and future full-text search. PostgreSQL provides these without adding Redis or a separate workflow database.

## Consequences

- state transitions and events should be committed atomically;
- PostgreSQL-specific concurrency tests are required;
- Telegram status can be rebuilt;
- SQLite is not used to validate production lease behavior;
- later `pgvector` can be considered without adding a separate database.
