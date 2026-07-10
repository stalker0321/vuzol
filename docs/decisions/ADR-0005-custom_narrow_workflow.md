# ADR-0005 — Use a Narrow Custom Workflow Runtime for MVP

## Status

Accepted

## Decision

Implement a small explicit state machine and PostgreSQL-backed step queue rather than adopting Temporal, DBOS, LangGraph, or another general workflow platform in the MVP.

## Reason

The MVP needs a limited set of known workflows, persisted steps, leases, retries, approvals, and recovery. A general workflow platform adds concepts and operations before the real workload is known.

## Consequences

- workflow templates are explicit and code-defined;
- arbitrary model-generated graphs are not supported;
- interfaces must remain narrow enough to permit later replacement;
- adoption of a durable runtime is reconsidered only after measured complexity or reliability pain.
