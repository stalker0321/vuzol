# Step 10 — Recovery, Operations, and MVP Hardening

## Goal

Make the complete vertical workflow reliable on the current VPS and ready for daily personal use.

## Deliverable

Restart recovery, resource limits, retention, backups, operational visibility, failure injection tests, and a demonstrated MVP acceptance run.

## Applicable architecture decisions

- ADR-0001 — PostgreSQL Is the Canonical Source of Truth.
- ADR-0002 — Use One Private Telegram Forum Group.
- ADR-0003 — Use a Model-Based Semantic Interpreter.
- ADR-0004 — Start as a Modular Monolith.
- ADR-0005 — Use a Narrow Custom Workflow Runtime for MVP.
- ADR-0006 — Per-Task Worktrees and Constrained Sandboxes.
- ADR-0007 — Transactional Delivery and Fenced Leases.
- ADR-0008 — Treat Repository Content and Model Output as Untrusted.

## Startup recovery

On application or worker startup:

- verify database connectivity and migrations;
- identify expired leases;
- classify interrupted steps;
- safely requeue read-only or isolated-retryable work;
- send unknown-effect work to blocked or validation-required state;
- restore pending approvals;
- resume pending inbox processing and outbox delivery;
- reject stale lease generations and reconcile supervised processes;
- rebuild missing Telegram status projections;
- mark unhealthy provider profiles without blocking unrelated work.

## Resource policy for current VPS

Assume:

- 2 vCPU;
- 4 GB RAM;
- 40 GB disk.

Required defaults:

- heavy concurrency: 1;
- light API-only concurrency: small and configurable;
- 2–4 GB swap recommended and documented;
- container memory and process limits;
- worker watchdog;
- disk-space threshold that pauses new heavy tasks;
- control-plane responsiveness protected from heavy work.

## Retention and cleanup

Define and implement configurable defaults for:

- completed worktrees: short retention;
- failed or blocked worktrees: longer retention;
- logs: rotation and maximum size;
- artifacts: retention by type;
- original voice files: privacy-aware retention;
- Docker images and caches: safe cleanup;
- database events and usage: longer retention;
- backups: separate retention.

Cleanup must be idempotent and must not remove active, pending-review, or unresolved resources.

Artifact cleanup reconciles database metadata with filesystem objects in both directions. It quarantines unexpected or orphaned files before deletion and never follows symlinks outside the artifact root. Per-task and global storage caps prevent a single task from exhausting disk.

## Backup

At minimum:

- scheduled PostgreSQL backup;
- artifact and configuration backup;
- restore documentation;
- backup success/failure event in `System`;
- no plaintext provider credentials in any backup; backup encryption is mandatory before off-host transfer.

Define and document:

- recovery point objective and recovery time objective;
- encrypted off-host backup destination and retention;
- separation and recovery of encryption keys;
- a consistency procedure for PostgreSQL, configuration revisions, and content-addressed artifacts;
- application and schema versions required for restore;
- behavior when artifacts are missing but metadata exists, and vice versa.

A restore test is required. A backup that has never been restored is not accepted as verified.

Every restore drill uses an isolated location, verifies hashes and referential integrity, starts the compatible application version, and records measured recovery point and recovery time. Restore testing must not overwrite production state.

## Observability

Persist or expose:

- active tasks by state;
- queued and leased steps;
- provider health;
- quota and rate-limit states;
- worker heartbeat;
- task and step duration;
- retries and failure categories;
- inbox age, outbox age, delivery attempts, and dead-letter count;
- token and usage records;
- disk and memory pressure;
- cleanup and backup status.

A web dashboard is not required. Telegram `System`, structured logs, and database queries are sufficient for MVP.

## Redaction

Before persistence or Telegram output:

- redact configured secret patterns;
- avoid full prompts in default logs;
- store raw provider results only behind explicit retention policy;
- prevent stack traces from exposing tokens;
- limit voice and personal-task retention;
- classify artifacts before Telegram or backup export and block destinations outside their visibility policy.

## Failure injection

Demonstrate at least:

- application restart during waiting state;
- worker crash during safe model call;
- worker crash after project-local file change;
- lost Telegram API response;
- duplicate Telegram update;
- provider timeout;
- provider quota exhaustion;
- expired approval;
- disk low-watermark;
- invalid project validation command;
- database reconnect after brief outage;
- stale worker result after lease reassignment;
- graceful shutdown with an active child process;
- outbox crash before and after the external API call;
- symlink escape and blocked egress attempt;
- concurrent budget reservation at the configured cap;
- target branch drift after approval;

## End-to-end acceptance scenarios

### Scenario A — Simple project task

User sends a text request in a project topic.

Expected:

- project inferred from topic;
- TaskDraft persisted;
- worktree created;
- compatible executor selected;
- changes made;
- tests run;
- diff returned;
- no LLM review if low risk.

### Scenario B — Ambiguous continuation

Two tasks are active and user says “do the second option.”

Expected:

- no automatic attachment;
- clarification with task choices;
- selected answer continues the correct task.

### Scenario C — Voice infrastructure inspection

User sends a voice message asking to inspect memory use but not kill anything.

Expected:

- transcript retained;
- read-only infrastructure workflow;
- no destructive command;
- result returned with no approval required.

### Scenario D — Privileged action

Executor proposes firewall or SSH change.

Expected:

- runtime risk escalation;
- precise approval card;
- changed command invalidates old approval;
- no execution before approval.

### Scenario E — Restart

Worker or app is stopped during a safe or isolated step.

Expected:

- state survives;
- safe work resumes;
- unknown effects do not replay blindly;
- Telegram projection returns.

## Documentation and operator commands

Document:

- startup and shutdown;
- migration;
- health check;
- viewing active tasks;
- pausing all heavy work;
- rotating a credential;
- disabling a profile;
- cleanup;
- backup;
- restore;
- recovering a blocked worktree;
- inspecting and replaying or abandoning dead-letter outbox items;
- rotating backup encryption keys and running an isolated restore drill;
- adding a project topic;
- adding a second VPS worker later.

## Forbidden implementations

- declaring success after only a happy-path demo;
- unbounded logs or artifacts;
- cleanup that ignores active state;
- backup without restore test;
- backup stored only on the same VPS;
- unencrypted off-host backup or encryption key stored only with its ciphertext;
- exposing secrets in Telegram system messages;
- blocking Telegram responsiveness behind heavy worker load;
- automatically retrying unknown external side effects;
- introducing a dashboard, Redis, vector DB, or workflow platform during hardening.

## Acceptance criteria

- all MVP completion gates in `00_MVP_PLAN.md` pass;
- restart and failure-injection tests pass;
- operational commands are documented;
- disk, memory, and concurrency limits are enforced;
- backup and restore are verified;
- measured RPO and RTO meet documented targets;
- inbox, outbox, fencing, hard budgets, and graceful shutdown pass failure injection;
- no known high-severity security issue remains;
- `CURRENT.md` marks MVP complete only after the final review pass.

## Completion report

Report:

- final architecture and process layout;
- resource measurements on the VPS;
- recovery matrix;
- failure-injection results;
- backup and restore proof;
- retention values;
- unresolved limitations;
- recommended first V2 priorities based on measured pain.
