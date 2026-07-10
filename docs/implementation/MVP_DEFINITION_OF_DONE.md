# MVP Definition of Done

The MVP is not complete merely because the bot can call a model.

It is complete only when all items below are demonstrated.

## Input and context

- allowlisted Telegram text requests work;
- voice requests are transcribed;
- original text, voice reference, and raw transcript are retained;
- topic IDs provide stable scope;
- replies map to persisted tasks;
- ambiguous continuation asks for clarification;
- duplicate Telegram input creates canonical intake state once;
- attachments are size-limited, quarantined, and treated as untrusted.

## Interpretation and routing

- semantic interpretation returns a validated TaskDraft;
- the interpreter does not choose credentials;
- Python policy enforces project capability boundaries;
- a compatible healthy profile is selected;
- no-compatible-profile state is handled without data loss;
- budget mode never weakens security;
- hard token, cost, attempt, fallback, and duration limits are enforced;
- interpreter safety thresholds gate automatic execution.

## Persistence and workflow

- tasks, runs, steps, events, approvals, artifacts, and usage are persisted;
- legal state transitions are enforced;
- leases prevent duplicate execution;
- pause, resume, cancel, and retry are durable;
- restart recovery works;
- unknown side effects are not blindly retried;
- stale lease generations cannot heartbeat or commit results;
- inbox processing and transactional outbox delivery recover after restart;
- each run records the configuration, policy, prompt, workflow, and repository revisions needed for audit.

## Execution

- coding uses a per-task worktree;
- primary checkout remains unchanged;
- sandbox is non-root;
- Docker socket is absent;
- unrelated repositories and secrets are inaccessible;
- one-heavy-task limit is enforced;
- network egress is deny-by-default and purpose-scoped;
- symlink, path traversal, metadata endpoint, and secret-exfiltration tests pass;
- the final Git delivery state and immutable patch or commit are recorded.

## Validation, review, and approval

- project validators run and persist results;
- failed validation blocks completion;
- low-risk work avoids unnecessary review;
- high-risk work receives independent review;
- privileged action requires exact single-use approval;
- changed command invalidates prior approval;
- changed target state, diff, environment reference, sandbox, or policy invalidates prior approval;
- natural-language input cannot consume a privileged approval;
- approval consumption and executable transition are atomic.

## Telegram UX

- task status is reconstructable from PostgreSQL;
- duplicate updates and callbacks are safe;
- progress messages are rate-limited;
- final result includes summary, validations, diff, and artifacts;
- system failures appear without leaking secrets;
- Telegram output uses transactional delivery and stale projection revisions cannot overwrite current state;
- oversized output is escaped and delivered as bounded messages or artifacts.

## Operations

- logs and artifacts have retention;
- cleanup is safe and idempotent;
- disk pressure blocks new heavy work before failure;
- backup succeeds;
- restore has been tested;
- provider credentials can be rotated;
- an unhealthy profile can be disabled;
- operator documentation exists;
- graceful shutdown and child-process recovery are tested;
- off-host encrypted backup and isolated restore meet documented RPO and RTO;
- orphan artifact reconciliation and global storage caps are enforced;
- dependency locking, secret scanning, and dependency audit checks pass.

## Final evidence

A final review report must include:

- end-to-end scenario results;
- failure-injection results;
- test commands and outputs;
- current VPS resource measurements;
- known limitations;
- recommended V2 priorities based on actual usage.
