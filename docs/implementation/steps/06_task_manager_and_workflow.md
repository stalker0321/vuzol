# Step 06 — Task Manager and Workflow Engine

## Goal

Implement the deterministic workflow manager that advances persisted tasks and steps, leases work to workers, handles interruptions, and survives process restarts.

## Deliverable

A small custom state machine and PostgreSQL-backed queue. It must be intentionally narrower than a general workflow platform.

## Applicable architecture decisions

- ADR-0001 — PostgreSQL Is the Canonical Source of Truth.
- ADR-0005 — Use a Narrow Custom Workflow Runtime for MVP.
- ADR-0007 — Transactional Delivery and Fenced Leases.

## Workflow responsibilities

The task manager:

- creates runs from interpreted tasks;
- selects a workflow template;
- creates persisted steps;
- advances allowed state transitions;
- queues executable steps;
- leases steps to workers;
- processes heartbeats and timeouts;
- handles pause, resume, cancel, retry, blocked, and awaiting-user states;
- records all transitions as events;
- rebuilds pending work after restart.

It does not interpret natural language or perform provider calls directly.

## Initial workflow templates

Support at least:

### Simple model task

```text
interpret
→ execute_model
→ format_result
→ complete
```

### Coding task

```text
interpret
→ optional_plan
→ prepare_context
→ prepare_worktree
→ execute_code
→ validate
→ optional_review
→ await_apply_or_complete
```

### Research task

```text
interpret
→ optional_clarification
→ research_execute
→ synthesize
→ complete
```

### Infrastructure task

```text
interpret
→ inspect
→ plan
→ approval
→ privileged_execute
→ validate
→ complete_or_block
```

Keep templates explicit. Do not implement arbitrary model-generated workflow graphs in the MVP.

## Step semantics

Every step type defines:

- input schema;
- output schema;
- required capability;
- retry class;
- timeout;
- idempotency class;
- allowed predecessor states;
- success and failure transitions.

Suggested idempotency classes:

- read_only;
- idempotent;
- isolated_retryable;
- non_idempotent;
- unknown_effects_possible.

## Leasing

Workers claim steps transactionally.

Requirements:

- `FOR UPDATE SKIP LOCKED` or equivalent;
- lease owner;
- lease expiry;
- heartbeat;
- maximum attempts;
- `available_at` for backoff;
- worker capability match;
- profile concurrency enforcement.

A worker losing its lease must stop or discard late results.

Every lease acquisition increments a fencing generation. Heartbeats and final result commits use PostgreSQL time and compare the worker ID plus generation. A stale worker is not allowed to create later workflow steps, publish artifacts as final, or mark external delivery successful. The process supervisor receives lease-loss cancellation and records whether termination was confirmed.

## Retry policy

Examples:

- model timeout before response: retryable;
- invalid structured output: one repair, then fallback;
- read-only shell command: retryable;
- code execution inside disposable worktree: usually isolated-retryable;
- external side effect with unknown completion: not automatically retryable;
- failed tests: workflow decision, not infrastructure retry.

## Pause and cancel

Pause:

- stops dispatch of future steps;
- does not kill an atomic operation unless the executor supports safe interruption;
- records requested and effective pause state.

Cancel:

- prevents future steps;
- requests process termination;
- marks incomplete side effects for validation;
- cleans isolated resources only when safe.

## Recovery

On startup:

1. find expired leases;
2. classify each step by idempotency and available evidence;
3. requeue safe steps;
4. send unknown-effect steps to validation or blocked state;
5. rebuild Telegram projections asynchronously.

Do not replay the entire task from the beginning.

## Graceful shutdown

On `SIGTERM` or planned restart, a process:

1. stops acquiring new work;
2. keeps the control plane responsive during the drain window;
3. requests safe cancellation of supervised child processes that cannot finish;
4. continues heartbeat only while it still supervises the operation;
5. commits completed results with the current fencing generation;
6. leaves unconfirmed effects for normal recovery classification;
7. exits within a configured shutdown deadline.

## Scheduler classes

On the current VPS:

- control queue — always responsive;
- light queue — interpreter, summaries, API-only work; small concurrency;
- heavy queue — Codex, builds, tests; concurrency 1;
- privileged queue — concurrency 1 and approval required.

## Tests

Required:

- legal workflows reach completion;
- illegal transition rejected;
- two workers cannot execute one step;
- heartbeat extends lease;
- expired safe step requeues;
- expired unknown-effect step blocks;
- pause prevents new dispatch;
- resume continues from persisted state;
- cancel does not claim rollback;
- retry backoff works;
- late worker result after lease loss is rejected;
- stale lease generation cannot heartbeat, create successor steps, or publish a final result;
- planned shutdown stops leasing and either drains or records uncertainty for child processes;
- process restart simulation resumes correctly;
- Telegram unavailability does not block internal state progress.

## Forbidden implementations

- one long-lived coroutine as the only task state;
- model-generated arbitrary state transitions;
- blind retry of every exception;
- treating failed validation as an infrastructure retry;
- storing only “current task status” without step history;
- killing arbitrary processes without recording outcome uncertainty;
- requiring a general workflow framework for the MVP;
- accepting lease timestamps supplied by a worker as authoritative.

## Acceptance criteria

- task workflows are durable and explicit;
- worker leasing is concurrency-safe;
- pause, resume, cancel, retry, and awaiting-user behavior are persisted;
- restart recovery is demonstrated in tests;
- unknown effects never trigger blind automatic replay;
- current VPS concurrency limits are enforced.

## Completion report

Report:

- workflow templates;
- state and step transition tables;
- lease and heartbeat settings;
- retry classes;
- recovery decisions;
- load or concurrency tests;
- unresolved idempotency risks.
