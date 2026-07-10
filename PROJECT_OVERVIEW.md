# Project Overview — Personal Agent Task System

## What we are building

A personal task execution system running on a VPS and controlled primarily through a private Telegram forum group.

The system accepts natural-language and voice requests, turns them into persisted tasks, selects a suitable model or executor, performs the work in a controlled environment, validates the result, and reports progress and artifacts back through Telegram.

The goal is not to create an autonomous agent swarm. The goal is to create a reliable **task operating system** in which language models are replaceable reasoning and execution components.

## Responsibility model

```text
Telegram defines user context and scope
→ a low-cost model interprets intent
→ Python applies policy and manages state
→ a planner is used only when necessary
→ an executor performs the work
→ deterministic tools validate objective outcomes
→ an independent reviewer is used only when risk justifies it
→ PostgreSQL stores the source of truth
```

### Language models

Models are used for work that requires semantic understanding or reasoning:

- interpreting informal or transcribed requests;
- identifying goals, constraints, missing information, and required capabilities;
- planning ambiguous work;
- coding and research;
- targeted review.

### Python runtime

Python remains responsible for:

- task lifecycle and state transitions;
- permissions and approval gates;
- provider and account selection;
- queues, leases, retries, timeouts, pause, resume, and cancellation;
- budgets, quotas, health checks, and fallbacks;
- persistence and restart recovery;
- tool execution policy;
- Telegram presentation;
- validation orchestration.

A model may suggest risk or actions, but it may not authorize itself, select unrestricted credentials, or bypass policy.

### Deterministic tools

Tools verify facts that should not be delegated to model judgment:

- process exit codes;
- tests, lint, type checks, and builds;
- diffs and changed-file sets;
- schema validation;
- file and artifact existence;
- quota and worker health;
- Git state.

## Telegram workspace

The main interface is one private Telegram supergroup with forum topics.

Expected permanent topics:

- `Inbox` — requests without a known project;
- `Tasks` — compact active-task dashboard;
- `Approvals` — explicit confirmation requests;
- `Changelog` — human-readable completed changes;
- `System` — health, quota, backup, and failure events;
- one topic per active project;
- `Personal` — flights, comparisons, and non-project work;
- optionally `Research` — long investigations not tied to a repository.

A topic is mapped by immutable `chat_id + message_thread_id`, never by its mutable name.

Project topics provide routing context. Replies and task buttons provide task affinity. Telegram is a control plane and presentation layer, not the canonical state store.

## Request intake

Voice messages are a primary input method.

```text
Telegram update
→ authorization and topic resolution
→ transcription if required
→ semantic interpreter
→ validated TaskDraft
→ policy and routing
```

The semantic interpreter is a cheap, replaceable model. It returns structured fields such as:

- action: create task, continue task, answer, approve, cancel, or conversation;
- task type;
- project;
- goal and requested outcome;
- constraints and missing information;
- required capabilities;
- whether planning or clarification is required;
- suggested complexity and risk.

The original message or transcript is always retained. The structured interpretation supplements it and never replaces it.

Natural-language understanding must not depend on keyword lists. Rules are appropriate only for explicit commands, authorization, hard security policy, objective runtime signals, and state transitions.

## Task lifecycle

The task service is deterministic Python code backed by PostgreSQL.

Typical states:

```text
received
→ interpreted
→ context_prepared
→ planned
→ waiting_approval
→ executing
→ validating
→ reviewing
→ completed
```

Additional states include:

```text
awaiting_user
paused
retrying
quota_exhausted
blocked
failed
cancelled
rolled_back
```

Each side-effecting action is represented as a distinct persisted step. Approvals apply to one specific step. Cancellation stops future work and attempts to stop the current process; it does not imply automatic reversal of completed external side effects.

External delivery follows an inbox/outbox model. Inputs are deduplicated before changing canonical state, business transitions create durable outbox records transactionally, and delivery workers tolerate at-least-once execution. Step leases use fencing generations so a stale worker cannot commit a late result after reassignment.

## Routing

Routing is split into two layers:

1. **Semantic interpretation** identifies task meaning and required capabilities.
2. **Python policy** selects the actual workflow, model profile, account, permissions, budget, and review requirements.

Models and accounts are represented as profiles rather than persistent agent personalities. Profiles may include two isolated Codex accounts, MiMo, GLM through supported tools, generic API providers, and future remote workers.

Provider-specific behavior is hidden behind stable adapters.

## Execution and safety

Coding work uses a separate Git worktree per task. Execution should occur in a non-root Docker sandbox where practical.

Expected zones:

- project sandbox — read/write only to the task worktree and artifact directory;
- review sandbox — read-only access to requirements, diff, and validation results;
- privileged host executor — tightly constrained and approval-gated VPS operations.

The default sandbox must not receive the Docker socket, privileged mode, unrelated repositories, or all credentials.

Repository content, attachments, retrieved content, and model output are untrusted. Network access is deny-by-default and purpose-scoped. Host safety does not depend on a model following instructions or on perfectly parsing arbitrary shell syntax.

Risk is evaluated repeatedly at intake, planning, tool request, actual command or diff, and deployment.

Privileged approvals bind to the complete immutable action envelope, target state, and policy revision. Natural-language messages and model output cannot consume such approvals.

## Context and memory

The system must not repeatedly send large Markdown files or full Telegram history to every model.

The MVP context strategy uses:

- short stable system policy;
- original request and structured TaskDraft;
- current project manifest;
- explicit acceptance criteria;
- targeted files or excerpts;
- relevant previous-step output;
- limited task-specific conversation.

Initial retrieval uses Git file listing, `ripgrep`, targeted reads, and summaries cached by content hash. PostgreSQL full-text search, symbol indexes, embeddings, and richer memory are later additions only if measurements justify them.

## Validation and review

Objective validation is preferred to model opinion.

Review policy is risk-based:

- low risk — deterministic validation;
- medium risk — validation and scoped diff inspection;
- high risk — independent model review using requirements, diff, and validator output;
- privileged or destructive — review, rollback plan, and explicit user approval.

The reviewer does not receive the executor's hidden reasoning or unrestricted credentials.

## Initial deployment

Current VPS constraints:

- 2 vCPU;
- 4 GB RAM;
- 40 GB disk;
- one heavy execution slot;
- no local models;
- strict retention and cleanup.

The MVP should physically consist of:

1. one modular Python application;
2. one or more worker processes;
3. PostgreSQL.

Logical components remain separate in code, but are not separate microservices by default.

A future larger VPS can become an execution plane while the current VPS remains the control plane.

## MVP outcome

The MVP is complete when this flow is reliable:

```text
Telegram text or voice request
→ semantic interpretation
→ task persisted in PostgreSQL
→ policy selects an executor
→ task worktree and sandbox prepared
→ work executed
→ validations run
→ optional approval or review
→ result, diff, and artifacts returned through Telegram
→ workflow survives process and VPS restarts
```

The demonstrated flow must also tolerate duplicate delivery and stale workers, enforce hard resource and cost limits, record an explicit Git delivery state, and meet documented backup recovery objectives.

## Planned evolution

### Version 2

- multiple provider and account profiles;
- quota-aware routing and fallbacks;
- remote workers;
- PostgreSQL full-text search;
- project and directory summaries;
- scheduled tasks;
- routing and retrieval evaluations;
- richer Telegram task dashboards and optional temporary task topics.

### Version 3, only when measured need exists

- `pgvector`;
- advanced reranking;
- richer structured memory;
- data-driven routing optimization;
- formal durable workflow runtime;
- web observability dashboard;
- selective parallel task decomposition.

## Explicit non-goals

The initial system does not include:

- a permanent supervisor agent;
- agent swarms or agent-to-agent chat;
- keyword-based natural-language classification;
- one Telegram bot per model;
- a topic for every trivial task;
- full-topic history in every prompt;
- OpenClaw as the platform core;
- Temporal, Redis, graph databases, or a separate vector database;
- internal MCP/A2A networking;
- local LLMs on the current VPS;
- automatic privileged actions;
- automatic self-modification of routing or security policy;
- microservice decomposition for its own sake.
