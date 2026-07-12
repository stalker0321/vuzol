# Provider Routing

Vuzol keeps provider and account selection in deterministic Python policy. TaskDraft identifies
meaning and required capabilities but cannot select credentials or concrete accounts.

## Provider profiles

Provider profiles are configured in the TOML registry. Common fields include:

- `roles`: interpreter, planner, executor, reviewer, summarizer, or transcriber;
- `capabilities` and `supported_task_types`;
- `cost_class`: cheap, balanced, or strong;
- `routing_priority`, where lower values are preferred after policy filtering;
- context/output and concurrency limits;
- explicit `fallback_profile_ids`;
- conservative cost and quota accounting values.

API profiles require a credential-free HTTPS `api_base_url`. Credentials remain scoped references,
such as `env:VUZOL_OPENAI_EXECUTOR_API_KEY`; only the selected adapter resolves its reference.

CLI profiles require a unique `runtime_identity` and absolute `state_directory`. Enabled CLI
profiles cannot share or nest state directories. The example registry contains two disabled,
structurally isolated Codex profiles. Vuzol does not copy or inspect their authentication files.

## Routing order

The router rejects profiles that violate role, task type, capability, project, sandbox, health,
quota, context, output, hard-budget, or concurrency policy. Eligible profiles are ordered by:

1. a trusted explicit profile request, if still permitted;
2. an explicit fallback edge after a categorized failure;
3. budget-mode cost-class preference;
4. configured priority;
5. active leases and queue depth;
6. stable profile ID.

Every routed workflow call stores its decision, alternatives, bounded exclusion reasons, selected
profile, and policy revision. Routing, hard-budget reservation, profile assignment, and fenced step
claim commit atomically.

## Budgets and usage

Before a call, Vuzol reserves bounded input/output tokens and cost/quota units. Task, step, call, and
rolling daily limits are checked under a PostgreSQL advisory transaction lock. Concurrent calls
cannot both spend the same remaining budget.

Known provider usage reconciles the reservation exactly once. Missing pricing or usage retains the
conservative reservation; it is never treated as free. A timeout after sending a request also keeps
the conservative charge because the provider may have consumed quota. A reservation is released
only when no request was sent.

## Health and failure handling

Health observations are immutable and bound to the current configuration revision. Authentication
failure affects only one profile. Rate-limit and provider-unavailable cooldowns are explicit, and
quota remains `unknown` when a provider does not expose authoritative data.

Adapters normalize authentication, quota, rate-limit, timeout, unavailable, invalid-output,
cancelled, context-size, unsupported-capability, permanent-request, and unknown failures. Raw
provider response bodies and exceptions do not enter task state, events, Telegram, or logs.

## Current execution boundary

The worker can execute safe, model-only OpenAI-compatible steps such as simple answers, planning,
research synthesis, and summarization. Automatic workflow start remains disabled by default.

The Codex CLI contract and account-isolation validation exist, but no production Codex process
transport is registered. Repository editing, tools, worktrees, and real Codex execution remain
blocked until the Step 08 sandbox and supervised-process boundary are implemented.
