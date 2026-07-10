# ADR-0007 — Transactional Delivery and Fenced Leases

## Status

Accepted

## Decision

Use a PostgreSQL-backed inbox for deduplicating external input, a transactional outbox for durable external delivery, and monotonically increasing fencing tokens for step leases.

Business-state changes and creation of the corresponding outbox item occur in one database transaction. External APIs are called only by delivery workers after commit. Delivery is at least once; consumers and projections must therefore be idempotent.

A worker may commit a step result only while it owns the current lease generation. Lease time is based on PostgreSQL time. Idempotency keys are used for external operations when the provider supports them.

## Reason

Telegram, provider, and worker failures can occur after either side has completed an operation but before the caller receives confirmation. Database transactions cannot atomically include these external systems. Inbox/outbox records make incomplete delivery observable and recoverable, while fencing tokens prevent an expired worker from committing stale results after another worker has acquired the step.

## Consequences

- Telegram updates are uniquely identified by bot identity and update ID;
- Telegram projections and other durable notifications are delivered from an outbox;
- an ambiguous external result is reconciled or blocked rather than blindly repeated;
- outbox retention and dead-letter handling are operational concerns;
- lease acquisition increments a generation checked by heartbeats and result commits;
- killing or timing out a process does not by itself prove that no side effect occurred.
