# Current Project State

Current phase: MVP implementation  
Current step: Step 04.1 complete
Status: ready for Step 05

## Completed

- project purpose and architecture direction defined;
- Telegram forum workspace model defined;
- semantic interpreter selected instead of keyword classification;
- task-state and policy boundaries defined;
- MVP, V2, V3, and explicit non-goals separated;
- detailed MVP implementation plan prepared.
- specification hardening completed for transactional delivery, fenced leases, untrusted execution, approval binding, hard budgets, Git delivery, and disaster recovery.
- repository documentation imported and made canonical;
- Python 3.12 package initialized with `uv` and a committed lockfile;
- independent app and worker entry points implemented;
- typed settings, structured JSON logging, and liveness/readiness endpoints implemented;
- strict Ruff, mypy, pytest coverage, dependency-audit, and secret-scan gates implemented;
- non-root Docker image and Compose app/worker topology verified;
- GitHub SSH `origin` and tracking `main` branch configured.
- typed application limits, retention, concurrency, authorization scope, and secret references implemented;
- strict TOML models and loaders for projects, provider profiles, and Telegram topics implemented;
- immutable project, profile, and topic registries with normalized paths and cross-reference validation implemented;
- consumer-scoped environment and file secret resolution implemented without global secret materialization;
- deterministic non-secret configuration revisions and security-sensitive snapshot compatibility checks implemented;
- invalid registry files and missing required secrets verified to stop app and worker startup.
- Step 03 prerequisite added: bounded database pool, statement/lock timeout, and migration advisory-lock settings.
- PostgreSQL 16 Compose topology and isolated test database added;
- complete 20-table MVP persistence schema and Alembic migration implemented;
- async engine, scoped DSN resolution, unit of work, and explicit repositories implemented;
- atomic task transitions/events, inbox/outbox, single-use approvals, and projection-safe deletion implemented;
- step and outbox `SKIP LOCKED` leasing with fencing generations implemented;
- clean/repeated/concurrent migration and real PostgreSQL concurrency tests verified.
- authorized Telegram forum ingress with ID-based topic routing and transactional deduplication;
- reply and single-active-task affinity with explicit ambiguity instead of recency guessing;
- persisted attachment metadata and bounded pre-download safety policy;
- idempotent callback actions that enqueue workflow controls without executing them inline;
- reconstructable, escaped status cards with monotonic revisions and edit rate limiting;
- `python-telegram-bot` 22 long-polling adapter behind provider-neutral DTOs and client protocol;
- ambiguous Telegram sends quarantined from automatic retry pending reconciliation.
- dedicated fenced Telegram outbox delivery runtime with destination-filtered claiming;
- canonical-state acknowledgement/status dispatch, clarification rendering, and confirmed message
  link persistence;
- bounded transient retry, dead-letter, and ambiguous-outcome handling;
- optional Compose profile for separate ingress and delivery processes sharing one bot token.

## Next action

Start Step 05:

`docs/implementation/steps/05_voice_and_semantic_interpreter.md`

## Open decisions

- first semantic-interpreter provider;
- first transcription provider;
- initial numeric limits and targets: interpreter evaluation gates, task budgets, retention, shutdown deadline, RPO, and RTO.

These choices must not delay repository initialization. They should remain replaceable configuration decisions.

## Blockers

None.
