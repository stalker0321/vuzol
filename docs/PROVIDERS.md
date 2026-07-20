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

Project-scoped `/model` preferences (see [Telegram](TELEGRAM.md)) may pin the coding/agent CLI
executor for a project to a worker family. Auto mode leaves ordering unchanged. Pin mode applies
only to `execute_code` / `execute_agent` (never API/research executor steps). It supplies the
trusted profile, restricts eligibility and post-failure fallbacks to the same family (Codex stays
on Codex; Grok may fall across Grok profiles only), and attaches claim-time model/reasoning-effort
overrides even on same-family fallbacks. A stored pin that cannot resolve to an enabled profile
blocks the step (`project_pin_unresolved`) instead of degrading to unrestricted auto routing.
Routing decisions persist bounded pin inputs (worker, trusted profile, restrict set, revision).

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
Production planning uses a dedicated GPT-5 nano API profile with a bounded 1,000-token output;
empty or token-truncated planner output is rejected rather than completed, and a validated plan is
handed to downstream `execute_code` / `execute_agent` steps as bounded redacted context items.
Content-quality plan rejection is recorded as a provider failure observation (not a success) and
reconciles usage under the planner failure category, but it does **not** force cross-profile
fallback—the same planner may retry within attempt limits.

**Supported automatic plan consumers** are only provider-executed coding/agent steps
(`execute_code`, `execute_agent`). In `infrastructure.v1`, the `plan` step is **approval/human
context only**: general privileged automatic execution remains outside the supported boundary, so
plan text is not injected into a provider executor request for `privileged_execute`.

Repository analysis and architectural discussion remain full agent tasks rather than planner work.
Architecture tasks route to subscription agents through a dedicated read-only workflow. The
repository worktree and provider permissions are both read-only, no validation/apply approval is
created, and the agent's bounded textual result is returned to the project topic.

The dedicated executor also registers isolated Codex and Grok CLI transports behind the Step 08
worktree, supervised-process, rootless-sandbox, and controlled-egress boundary. Provider state,
worktrees, containers, proxies, credentials, and concurrency are separated by profile and task.
Provider output is typed and untrusted; the host-side finalizer independently measures scope and
Git facts, runs trusted gates in a separate pinned validation image, and creates a result commit
only after verification succeeds.

Production user intake exposes only the explicit bounded `/sol` Telegram path documented in
`TELEGRAM.md`. It fixes the profile to `codex-subscription-prod`, accepts one to ten contained
repository-relative paths, permits no model repair, and retains rather than integrates the result.
Grok execution and multi-worker modes remain experimental evidence, not an automatically trusted
production route. Merge, push, deployment, privileged execution, and automatic trust promotion
remain unavailable.
