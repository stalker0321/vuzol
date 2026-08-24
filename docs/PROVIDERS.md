# Provider Routing

Vuzol keeps provider and account selection in deterministic Python policy. TaskDraft identifies
meaning and required capabilities but cannot select credentials or concrete accounts.

## Provider profiles

Provider profiles are configured in the TOML registry. Common fields include:

- `roles`: interpreter, planner, executor, reviewer, summarizer, or transcriber;
- `capabilities` and `supported_task_types`;
- `cost_class`: cheap, balanced, or strong;
- `routing_priority`, where lower values are preferred after policy filtering;
- model-level ceilings: `context_limit`, `output_limit`, and `concurrency_limit`;
- explicit `fallback_profile_ids`;
- conservative cost and quota accounting values.

API profiles require a credential-free HTTPS `api_base_url`. Credentials remain scoped references,
such as `env:VUZOL_OPENAI_EXECUTOR_API_KEY`; only the selected adapter resolves its reference.

Profile limits are model ceilings, not role budgets. Routing rejects any request whose
`max_output_tokens` exceeds the profile `output_limit`. How much a role actually requests is a
separate, narrower budget from `HardLimits` (process settings, overridable via `VUZOL_LIMITS__*`),
which must fit under the ceiling. Example: the planner profile may allow 8,000 output tokens while
the planner itself budgets 3,000.

### Role map

Roles are declared per profile and selected either by environment pin (interpretation stages) or
by workflow step type (`src/vuzol/providers/routing.py`):

| Role | Responsibility | Selected by |
| --- | --- | --- |
| `transcriber` | Telegram voice/audio → raw transcript text | `VUZOL_INTERPRETATION__TRANSCRIPTION_PROFILE_ID` |
| `interpreter` | raw text plus bounded context → TaskDraft JSON | `VUZOL_INTERPRETATION__PROFILE_ID` |
| `planner` | task → bounded plan JSON consumed by coding steps | `plan` step |
| `executor` | coding/agent work in sandboxes (`execute_code`, `execute_agent`) and model-only steps (`execute_model`, `research_execute`) | corresponding steps |
| `reviewer` | independent read-only review of a finished result | `review` step |
| `summarizer` | result synthesis | `synthesize` step |

Transcription and interpretation are two independent stages with separate models, profiles, and
credentials. A voice message is transcribed first, then interpreted; changing, rotating, or
rate-limiting one stage never affects the other. Both stages run only inside the dedicated
interpretation runtime — they are not workflow steps. The same runtime drives project-discussion
responses through the interpreter profile.

### Where profiles live

There is no single registry file. Each runtime loads its own registry document plus the shared
append-only project overlay:

| Runtime | Registry source | Notes |
| --- | --- | --- |
| Worker, executor, applier | `/etc/vuzol/executor-registries.toml` | repo mirror: `deploy/registries.executor.toml` |
| Project provisioner | `/etc/vuzol/executor-registries.toml` | writes dynamic projects into the overlay |
| Telegram ingress and delivery | `/etc/vuzol/telegram-registries.toml` | topics and projects needed at the chat boundary |
| Interpretation runtime (container) | host registry mounted via `VUZOL_REGISTRY_FILE_HOST` | interpreter and transcriber profiles live here, pinned by `VUZOL_INTERPRETATION__PROFILE_ID` and `VUZOL_INTERPRETATION__TRANSCRIPTION_PROFILE_ID` |

Every service also appends `/var/lib/vuzol/registry/projects.json` (the provisioner-owned overlay,
see [Configuration](CONFIGURATION.md)) and resolves role budgets from process settings. Two
checked-in files pin production registry facts and must change together with any registry edit:
`deploy/mvp/check.py` and `tests/unit/deploy/test_provider_registry.py`.

### Role-scoped profiles

One account should not carry one limit for every role. A profile may declare
`base_profile_id` and inherit every unset field from the referenced profile in the same
registry file. The base carries the shared account facts (model, credential, routing,
context limit) and stays disabled; each role profile overrides only what differs:

```toml
[[profiles]]
id = "openrouter-base"
model = "deepseek/deepseek-v4-flash-0731"
api_base_url = "https://openrouter.ai/api/v1"
output_limit = 8000
roles = []
enabled = false

[[profiles]]
id = "openrouter-reviewer"
base_profile_id = "openrouter-base"
roles = ["reviewer"]
output_limit = 6000
enabled = true
```

This mechanism suits deployments that share one provider account across roles. The current
production registry instead gives every role its own account and credential reference.

API profiles may also carry role-scoped reasoning controls:

- `reasoning_enabled = false` explicitly disables thinking for providers that support
  the OpenRouter unified reasoning parameter (verified for the DeepSeek family);
- `max_reasoning_tokens` is only a soft upstream hint. Several providers ignore
  reasoning caps entirely and count reasoning against `max_tokens`, so `output_limit`
  must be sized for the worst case instead of relying on the cap;
- `model_reasoning_effort` maps onto the OpenRouter unified `reasoning.effort` hint for
  API profiles (CLI profiles forward it to the agent CLI instead). Like every reasoning
  control it is advisory; `output_limit` remains the enforced bound.

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
HTTP failures additionally emit a bounded `provider.http_failure` log event (status, category,
request id, secret-redacted body excerpt) so a misbehaving endpoint can be diagnosed from the
worker journal alone.

## Current execution boundary

The worker can execute safe, model-only OpenAI-compatible steps such as simple answers, planning,
research synthesis, and summarization. Automatic workflow start remains disabled by default.

The production executor registry (`deploy/registries.executor.toml`) currently routes:

| Role | Profile | Model | Credential | Output bound |
| --- | --- | --- | --- | --- |
| planner | `openrouter-deepseek-planner-prod` | DeepSeek via OpenRouter | `VUZOL_OPENROUTER_PLANNER_API_KEY` | `HardLimits.planner_output_tokens` (default 3,000, reasoning 1,800) under the profile `output_limit` of 8,000 |
| reviewer | `openrouter-mimo-reviewer-prod` | Xiaomi MiMo-V2.5 via OpenRouter, reasoning effort `low` | `VUZOL_OPENROUTER_REVIEWER_API_KEY` | derives its window from its own profile `output_limit` (8,000) |
| executor | `codex-subscription-prod`, `grok-subscription-a/b`, `tokenrouter-kimi-a` | subscription agent CLIs | subscription auth (no API keys) | sandbox-bound |
| interpreter / transcriber | configured in the interpreter registry, not this mirror | DeepSeek-based interpretation; OpenAI transcription API | their own credential references | their own profile limits |

Planning runs on the dedicated DeepSeek API profile with the planner budget from `HardLimits`;
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
