# MVP Implementation Plan

## MVP objective

Deliver a reliable end-to-end workflow:

```text
Telegram request
→ interpretation
→ persisted task
→ policy and routing
→ isolated execution
→ deterministic validation
→ optional approval or review
→ Telegram result
→ restart recovery
```

The plan is ordered so that every step produces a testable, runnable increment. Later steps must not force replacement of earlier core interfaces.

## Status legend

- `not started`
- `planning`
- `in progress`
- `blocked`
- `review`
- `complete`

## Step map

| Step | Title | Status | Depends on | Main deliverable |
|---|---|---|---|---|
| 01 | Repository foundation | complete | — | Runnable typed Python application with quality gates |
| 02 | Configuration and registries | complete | 01 | Validated configuration, secrets boundaries, project/provider registries |
| 03 | PostgreSQL persistence model | complete | 01–02 | Migrations and repositories for tasks, steps, events, topics, profiles |
| 04 | Telegram forum workspace | complete | 02–03 | Authorized forum-group ingress and reconstructable projections |
| 05 | Voice and semantic interpretation | not started | 02–04 | Transcription, TaskDraft schema, cheap model interpreter, clarification flow |
| 06 | Task manager and workflow engine | not started | 03–05 | Persisted state machine, queue, leases, retries, pause/resume/cancel |
| 07 | Provider adapters and routing | not started | 02, 05–06 | Replaceable model adapters, profile health, capability-based policy routing |
| 08 | Worktrees and execution sandbox | not started | 06–07 | Isolated coding executor with process supervision and artifacts |
| 09 | Validation, review, and approvals | not started | 04, 06–08 | Deterministic validators, scoped review, action-specific approvals |
| 10 | Recovery, operations, and MVP hardening | not started | all previous | Reboot recovery, cleanup, backups, observability, end-to-end acceptance |

## Cross-step rules

1. Each step must leave the application runnable.
2. Database changes require migrations.
3. New provider-specific behavior must stay behind an adapter.
4. New external side effects require persisted steps and explicit retry semantics.
5. Tests must accompany behavior, not follow in an unspecified future step.
6. `docs/CURRENT.md` and `docs/CHANGELOG.md` are updated only after validations pass.
7. An agent must not mark a step complete when a mandatory acceptance criterion is unresolved.
8. If implementation reveals a contradiction in the specification, stop and report it before choosing a new architecture.
9. Cross-boundary delivery must follow ADR-0007; direct best-effort notification from a business transaction is not sufficient.
10. Execution of repository or model-proposed content must follow ADR-0008 and treat that content as untrusted.

## Suggested execution strategy

For Steps 01, 02, 04, and simple portions of 07, planning and implementation may be combined.

Use separate planning, implementation, and review passes for:

- Step 03 database model;
- Step 05 semantic interpretation contract;
- Step 06 state machine and leasing;
- Step 08 process isolation;
- Step 09 approvals and runtime risk;
- Step 10 restart recovery.

## MVP completion gate

The MVP is complete only when all of the following are demonstrated:

- an allowlisted user sends a text or voice request in a mapped project topic;
- the original request and interpretation are persisted;
- ambiguity causes a clarification rather than incorrect execution;
- an executor is selected from capability and health data;
- a task-specific worktree is created;
- execution occurs without host-wide write access;
- tests or configured validators run and are persisted;
- high-risk actions require a single-use approval;
- a stopped application can resume a safe in-progress workflow;
- unknown side effects are not blindly retried;
- Telegram status can be rebuilt from database state;
- completed work returns a summary, diff, and artifacts;
- cleanup prevents unbounded worktree, log, and artifact growth;
- duplicate input creates canonical state once and transactional outbox delivery recovers after failure;
- stale lease generations cannot commit results after reassignment;
- untrusted content cannot escape filesystem or network policy boundaries;
- hard cost, token, attempt, duration, and storage limits stop new work predictably;
- approval is bound to the full immutable action envelope and current target state;
- the Git result has an explicit delivery state and immutable patch or commit identity;
- encrypted off-host backup and isolated restore meet measured RPO and RTO.
