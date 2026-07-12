# Changelog

This file records completed implementation changes, not plans or speculative ideas.

## Unreleased

- added ADR-0007 for transactional inbox/outbox delivery and fenced leases;
- added ADR-0008 for untrusted repository, attachment, retrieved-content, and model-output boundaries;
- expanded MVP persistence, Telegram delivery, workflow recovery, sandbox, approvals, budgets, Git result lifecycle, backup, and acceptance contracts;
- added explicit ADR references to implementation steps and supply-chain checks to repository foundation.
- clarified one-time documentation bootstrap into the project repository and SSH remote setup without copying credentials.
- completed Step 01 repository foundation with Python 3.12, `uv`, typed settings, app and worker entry points, JSON logging, health endpoints, tests, security gates, and non-root Docker packaging.
- completed Step 02 configuration and registries with typed hard limits, TOML project/profile/topic models, immutable registries, scoped secret resolution, stable revisions, reload compatibility checks, and startup validation.
- added typed bounded PostgreSQL pool, timeout, and migration-lock settings as a Step 03 prerequisite.
- completed Step 03 with the canonical PostgreSQL schema, Alembic migrations, async repositories and unit of work, atomic audit transitions, inbox/outbox, approval consumption, fenced leasing, and real concurrency tests.
- completed Step 04 with authorized forum ingress, ID-based topic routing, task affinity and clarification, attachment intake policy, idempotent controls, transactional Telegram delivery, reconstructable revision-safe status cards, and a `python-telegram-bot` long-polling adapter.
- completed Step 04.1 with a dedicated Telegram delivery runtime, destination-filtered fenced outbox leases, canonical-state status and clarification dispatch, bounded retry/dead-letter/ambiguous outcomes, and an optional single-bot Compose profile.
- completed Step 05 with strict provider-neutral TaskDraft interpretation, durable attachment and voice transcription flow, replaceable OpenAI-compatible and fake adapters, repair/fallback policy, escaped semantic clarification, and a 45-fixture safety-gated evaluation harness.
- completed Step 06 with closed versioned workflow templates, fully materialized persisted graphs,
  interpretation disposition, audited task/run/step state machines, fenced handler outcomes,
  transactional queue/profile concurrency, durable controls, retry and expiry recovery policy, and
  drain-aware worker shutdown.
- completed Step 07 with provider-neutral model contracts, deterministic capability/health/budget
  routing, atomic routed claims and conservative budget reservations, decimal usage reconciliation,
  revision-scoped profile health, bounded fallbacks, an OpenAI-compatible model-only adapter, and
  structurally isolated Codex CLI profiles whose production execution remains gated on Step 08.

- Initial architecture and MVP implementation documentation created.
