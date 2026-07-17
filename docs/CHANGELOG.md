# Changelog

This file records completed implementation changes, not plans or speculative ideas.

## Unreleased

- enabled **independent model review** for high/privileged coding results: after mechanical
  gates, a model-only reviewer/planner profile returns a structured pass/warn/block verdict
  over a truncated read-only diff bundle (no sandbox, no host secrets beyond the API credential).

## 2026-07-16 — Step 09 validation, mechanical review, and approvals

- added the production **coding.v1** post-execute chain: deterministic **validate** (Git facts,
  trusted gates, host-owned result commit), optional **mechanical review** for medium risk, and
  exact-result **approve_result** with a dedicated privileged applier;
- high/privileged risk still fails closed until an independent model reviewer is enabled;
- bound Approve/Redo/Reject cards to an immutable action envelope and the global «Апрувы» topic,
  while project status stays in the project topic and completion reports post to «История»;
- added the single editable «Статус проектов» dashboard (active tasks, model identity, subscription
  limit bars) refreshed via outbox without spam;
- codified forum pin policy: История → Статус проектов → Апрувы → Новый проект;
- stabilized registry content revisions across process starts; apply no longer fails on unrelated
  profile/model-label registry drift when delivery policy and repository identity still match;
- fixed dual dashboard outbox enqueue in one transaction (approval cards no longer roll back on
  `uq_outbox_idempotency`); recovery refunds unused LEASED claim attempts; review is retryable;
- local apply CAS-advances the target branch even when it is cleanly checked out on the managed
  primary tree (freshly provisioned projects);
- granted the mapped sandbox identity temporary writable ACL access for every regular coding run,
  not only the experimental worker-capsule path;
- completed and production-qualified the Step 08 execution boundary with a dedicated rootless
  Docker daemon, task-specific standalone Git worktrees, pinned sandbox and seccomp identities,
  controlled proxy egress, supervised processes, bounded artifacts, cleanup/reconciliation, and
  live Codex acceptance;
- added isolated Grok subscription transports and a bounded Step 09A experiment runner; Grok trust
  promotion remains experimental and incomplete;
- retained the bounded `/sol` intake as one explicit coding entry; general NL coding intake follows
  the same coding.v1 chain when interpretation classifies the task as coding.

## 2026-07-15 — Telegram forum workspace routing

- Declared stable display names for global and per-project forum topics and synchronized configured
  mappings at bot startup.
- Routed retained-result decisions into a dedicated global approvals topic while preserving the
  project status projection and removing approval controls after a persisted decision.
- Added explicit text/voice project intake, persisted bounded provisioning, initial Git repository
  and Telegram-topic creation, validated dynamic registry overlays, and fail-closed handling for an
  unknown non-idempotent topic-creation outcome.
- Added the persistent production workflow-worker unit required to move interpreted intake into
  dispatch and provisioning after host restarts.

## Earlier foundation

- added adaptive project-topic routing and a full read-only architecture-agent workflow with a
  bounded GPT-5 nano planning step, isolated repository inspection, and Telegram result delivery;
- added the narrow retained-result decision loop precursors and fail-closed MVP readiness checks;
- closed a sub-threshold coverage rounding escape with a separate six-decimal raw coverage check;
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
- implemented the Step 08 code foundation: per-task Git worktrees, fail-closed rootless Docker preflight, non-root/read-only sandbox argv, supervised process records, content-hashed and redacted artifacts, typed no-shell Git, fenced Codex transport, dedicated executor worker, heavy queue isolation, path containment, retain/cleanup, schema, configuration, and tests.
- completed Step 06 with closed versioned workflow templates, fully materialized persisted graphs,
  interpretation disposition, audited task/run/step state machines, fenced handler outcomes,
  transactional queue/profile concurrency, durable controls, retry and expiry recovery policy, and
  drain-aware worker shutdown.
- completed Step 07 with provider-neutral model contracts, deterministic capability/health/budget
  routing, atomic routed claims and conservative budget reservations, decimal usage reconciliation,
  revision-scoped profile health, bounded fallbacks, an OpenAI-compatible model-only adapter, and
  structurally isolated Codex CLI profiles that were subsequently activated behind the completed
  Step 08 execution boundary.

- Initial architecture and MVP implementation documentation created.
