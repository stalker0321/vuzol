# Provider Routing

Vuzol keeps provider and account selection in deterministic Python policy. TaskDraft identifies
meaning and required capabilities but cannot select credentials or concrete accounts.

## Provider profiles

Provider profiles are configured in the TOML registry. Common fields include:

- `roles`: lead, interpreter, planner, executor, reviewer, summarizer, or transcriber;
- `capabilities` and `supported_task_types`;
- `cost_class`: cheap, balanced, or strong;
- `routing_priority`, where lower values are preferred after policy filtering;
- model-level ceilings: `context_limit`, `output_limit`, and `concurrency_limit`;
- explicit `fallback_profile_ids`;
- conservative cost and quota accounting values.

The subscription dashboard discovers enabled CLI profiles from this registry,
not a separate list of Codex/Grok names. Profiles sharing an explicit `account_id`
share one subscription row. `subscription_limits_visible=false` hides a retired
subscription from collection/display only; it does not disable execution or
invalidate pinned execution revisions. Hide all aliases to hide a shared account.
Missing/unsupported/stale measurements remain unknown, never synthetic 100%.
Currently numeric collection supports Codex and Grok, not Pi/OpenCode. Saved
observations older than 15 minutes and undated Grok log entries are not quota evidence.

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
| `lead` | goal → next bounded command batch | project `/model` choice, then `VUZOL_PROJECT_LEAD__PROFILE_ID` |
| `transcriber` | Telegram voice/audio → raw transcript text | `VUZOL_INTERPRETATION__TRANSCRIPTION_PROFILE_ID` |
| `interpreter` | raw text plus bounded context → TaskDraft JSON | `VUZOL_INTERPRETATION__PROFILE_ID` |
| `planner` | a goal → a plan revision the owner approves | the lead's `request_plan` (`planner_strong` slot) |
| `executor` | coding and agent work in sandboxes (`execute_code`, `execute_agent`) | those steps |
| `reviewer` | independent read-only review of a finished result and of a goal | `review` step, goal verification |
| `summarizer` | synthesis of legacy research runs | `synthesize` step of old workflows only |

Transcription and interpretation are two independent stages with separate models, profiles, and
credentials. A voice message is transcribed first, then interpreted; changing, rotating, or
rate-limiting one stage never affects the other. Both stages run only inside the dedicated
interpretation runtime — they are not workflow steps. The same runtime drives project-discussion
responses through the interpreter profile.

### Where profiles live

The production `jev-intake-prod` profile uses the native OpenRouter Decisions API,
pinned to `typesafe/jev-1.13`. Its credential is the scoped reference
`file:jev-openrouter.key`; keep the local 0600 file under `VUZOL_SECRET_FILE_ROOT`
and mount it read-only only into `vuzol-interpreter`. Set
`VUZOL_INTERPRETATION__PROFILE_ID=jev-intake-prod`, enable the `intake` kind and
set the operator-selected cohort via `VUZOL_JEV_ENABLED_KINDS` /
`VUZOL_JEV_CANARY_PERCENT`. Optional `VUZOL_INTERPRETATION__STRUCTURE_PROFILE_ID`
selects a generative planner for detailed plan payloads; it does not classify
messages in place of Jev. `VUZOL_INTERPRETATION__MANUAL_PROJECT_CREATION=true`
uses manual topic creation without invoking the provisioner.

See [ADR-0023](decisions/ADR-0023-live-jev-intake.md) for rollout and authority boundaries.

Every runtime, host units and containers alike, loads the same two documents: the registry
`/etc/vuzol/registries-t091.toml` (`VUZOL_REGISTRY_FILE`) and the account overlay
`/var/lib/vuzol/registry/account-profiles.toml` (`VUZOL_REGISTRY_OVERLAY_FILE`), merged before
validation. Containers bind-mount both at `/app/config/`. Neither file is in the repository: it is
public, and both carry host-specific facts. `config/registries.example.toml` shows the shape.

### Account profiles live only in the overlay

Account-bound CLI profiles belong in the overlay, never in the registry. The registry carries only
provider facts without an account binding: API profiles, prepared API workers, sandboxes,
projects, and topics. One account maps to one state directory, but may expose several model/role
profiles through an explicit shared `account_id`. There is no implicit default account or
cross-account fallback for explicit choices.

Because the overlay holds host-specific `state_directory` values, it is checked by a local script
next to it (`verify-account-profiles.py`), not by a unit test. The naming standard is
`codex-a/b/c`, `opencode-a/b/c`, and `tokenrouter-a`, with a model suffix only when a key is bound
to a single model (`tokenrouter-a-<model>`); `grok-subscription-a/b` and `pi-opencode-go-a/b/c`
already comply. Validate an edited overlay with the new code before restarting anything: a field
the schema no longer knows stops every service at startup.

### Role-scoped profiles

Role budgets and model ceilings are separate from an account's shared concurrency limit.
A profile may declare
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

Inheritance alone is not shared-account admission. Set the same explicit `account_id`
on profiles which use the same real account. Such profiles must agree on transport,
credential reference, API endpoint, network policy, runtime identity, state directory,
and concurrency limit. Model, roles, reasoning, and model ceilings may differ.

API profiles may also carry role-scoped reasoning controls:

- `reasoning_enabled = false` explicitly disables thinking for providers that support
  the OpenRouter unified reasoning parameter (verified for the DeepSeek family);
- `max_reasoning_tokens` is only a soft upstream hint. Several providers ignore
  reasoning caps entirely and count reasoning against `max_tokens`, so `output_limit`
  must be sized for the worst case instead of relying on the cap;
- `model_reasoning_effort` maps onto the OpenRouter unified `reasoning.effort` hint for
  API profiles (CLI profiles forward it to the agent CLI instead). Like every reasoning
  control it is advisory; `output_limit` remains the enforced bound.

CLI profiles require a unique `runtime_identity` and absolute `state_directory` unless
they explicitly share the same validated account binding. Nested state directories remain
forbidden. Profiles without `account_id` retain the original strict isolation. The example
registry contains two disabled,
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

Project-scoped `/model` preferences (see [Telegram](TELEGRAM.md)) now select profiles
for lead, goal verification, light/strong planning, light/strong review, and coding executor.
The picker is generated from enabled profiles and their permitted roles, not model-family
enums. It displays model, profile, account, and API/CLI transport. Owner authorization,
deduplication, and revision compare-and-set apply to every callback; old buttons cannot
overwrite a newer selection. "По умолчанию" removes the explicit choice.

Explicit choices never fall back to a different model or account. Unavailable, disabled,
or revised profiles fail closed with a visible unresolved-choice error. A choice pins the
profile's *assignment revision*: model, reasoning settings, account, endpoint, launch mode and
runtime identity/state directory (`vuzol.config.revision.assignment_revision`). Editing limits,
prices, routing priority, fallbacks, network policy, roles or capabilities does not invalidate
it; those are checked whenever a profile is chosen. Pins made with the older full revision are
still accepted and are rewritten to the assignment revision when the worker starts. Workflow run creation
freezes profile/model/reasoning/content revision; retries preserve it. Lead turns freeze both
lead and goal-reviewer assignments before inference. Changes affect new runs and new turns,
not already-started work. Changing `/model` does not unpause a goal or resume a waiting goal:
use its question/owner controls. A default reviewer is also pinned on its first review attempt.

`VUZOL_PROJECT_LEAD__INFERENCE_TIMEOUT_SECONDS` bounds each lead/goal-review call
(default 60 seconds, range 1–600), including CLI sandbox startup. It is provider-neutral;
token/money limits, lease renewal and stop fences remain independent. A timed-out call stays
unknown in the ledger; an explicit owner resume starts a new turn, not a replay of that call.

Pi leadership turns are snapshot-only structured inference: tools, extensions, skills and
local context discovery are disabled and the trusted system prompt explains the empty
workspace. The turn transport enforces that exact command. Worktree execution and
worktree review retain their separate write/read-only tool policies.

Legacy worker-family preferences remain readable for existing runs. A new executor choice
or reset clears that legacy preference so it cannot unexpectedly reappear. The executor slot
still concerns only sandbox coding/agent steps, not API/research executor steps. Other roles
(interpretation, transcription, synthesis) retain their existing configurable selection paths;
they are not added to this project picker.

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

Lead inference additionally records a stable phase ID, frozen input/profile, and normalized
provider receipt. A received phase can replay after restart without another call. A send
whose outcome is unknown is not automatically repeated. A late receipt corrects an orphan
estimate in place, retaining one usage row; partial usage does not invent a known price.
Authoritative pre-send failure releases the reservation, including an orphan estimate.

### Shared-account admission and recovery

Explicit accounts use PostgreSQL session advisory slot locks plus committed durable tickets,
acquired before calling any adapter. Use a direct/session-pooled PostgreSQL connection, not
transaction pooling. Limits cover all roles and profiles sharing the account, including API
and supervised CLI calls. A disconnect cannot erase a committed ticket and admit a second
call while the old launcher may still execute. CLI ownership rows carry the matching ticket.
Legacy isolated CLI profiles use a stable profile-scoped ticket when composed with the runtime
database, so lead/planner/reviewer/executor calls on that profile also share admission. This
does not combine different legacy accounts or invent shared credentials.

Normal completion releases a ticket only after confirmed runtime cleanup. Cancellation,
lost ownership, or unknown cleanup retains it. Startup/periodic reconciliation cleans exact
stale turn resources and can release tickets whose original launcher demonstrably drained.
Container absence alone is not proof that a partitioned launcher cannot resume.

`ProviderFailure.execution_completed` records transport completion independently of
`request_sent` and `usage`. A fully received API rejection with HTTP 400/401/403/404/413/422/429
releases the matching-token slot after the ownership checks; a fully read HTTP 200 response
that fails output validation also ends the invocation. No zero-cost usage is invented, and a
sent request remains sent. CLI adapters certify completion only after the supervised transport
returns a process result; the account wrapper still checks persisted runtime cleanup.
Usage alone (including a partial receipt) is never permission to release an account slot.
Transport timeouts/disconnects, interrupted bodies, HTTP 408/499/5xx, unrecognized rejections,
lost ownership, cancellation and unresolved launcher cleanup remain fail-closed. There is no
blanket error release or TTL. Rate-limit cooldown and authentication repair policies are
unchanged: admission being available does not itself schedule or authorize a retry.

Known limitation: a ticket left by an undrained/crashed launcher has no automatic expiry or
operator recovery command in this change. The account remains blocked rather than risking a
duplicate call. Do not repair it with ad-hoc SQL. A future recovery workflow must prove every
possible launcher stopped and exact runtime/proxy cleanup before matching-token release.

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

With `execution.enabled=true`, the host executor owns planning, independent review, and lead
turns for both API and CLI selections. The general worker omits these consumers, preventing
competing owners. Other API-only workflow roles remain on the worker. With execution disabled,
API planning/review/lead retain the worker path; selecting a CLI profile then fails visibly.
Automatic workflow start and project leadership remain disabled by default.

The host therefore needs access to credential references for any API planner/reviewer selected
there; a credential available only inside the worker container is insufficient. Verify scoped
secret access before rollout, without printing the secret or making a paid test call.
The worker and host must agree on `VUZOL_EXECUTION__ENABLED`: old deployments may deliberately
set it false inside the worker and true on the host. That mixed configuration must be changed
atomically with this release; otherwise both processes could claim planning/review/lead work.
This is a coordinated runtime rollout, not a safe partial image replacement.

Codex, Grok, Kimi, and Pi use the same role dispatch boundary. CLI planning and review use
read-only repository/git mounts and canonical inspection-only tool permissions. Lead and
goal-verification CLI calls use an empty read-only workspace and bounded captured evidence,
with real turn/invocation ownership rather than fabricated Task/Run/Step rows. Lease, goal
version, scope, delegation, and feature fences are rechecked before launch and during calls.

### Model calls for other services (`vuzol.model_calls`)

A container service (the assistant topic, the inbox) cannot run a CLI subscription account:
the account state lives only with the host executor. When such a service's profile is a CLI one,
it queues a row in `model_calls` with its prompt, input and output schema, then waits. Executor
lanes (`VUZOL_EXECUTION__MODEL_CALL_PARALLELISM`, default 2) lease the row and run the canonical
prompt-only command in an empty sandbox (`VUZOL_EXECUTION__MODEL_CALL_SANDBOX_PROFILE`, which must
proxy egress), with egress limited to the profile's own `runtime_network`. The lease is the
authority. It is rechecked before launch and watched while the process runs, and the sandbox
refuses any stdin but the frozen call. A failed call is settled, not retried, so the caller can
fall back to its next profile. The executor leases a row again only when the owner died. Leftover
sandboxes are reaped once no launcher holds them and the lease is gone. Without a sandbox profile
the lanes do not start, and a CLI profile there waits until the call's deadline.
A call may ask for the provider's own live web search. For Codex that is `web_search="live"`:
the search runs on OpenAI's side, so it needs no egress of its own, and the sandbox accepts the
flag only when the frozen call asked for it. The assistant asks for it, so it researches inside
one call instead of running our search rounds.

A call may also name remote MCP servers (streamable HTTP) that Codex calls itself, for
example `{"kiwi": "https://mcp.kiwi.com"}` for flight search. The executor runs only the
servers in its own `VUZOL_EXECUTION__MODEL_CALL_MCP_SERVERS`, with the same URL. Their hosts
are added to the sandbox's proxy allowlist for that call only. The assistant's list is
`assistant.mcp_servers`.

A lead or goal-review answer that cannot be parsed is redone, once per entry of
`VUZOL_PROJECT_LEAD__MODEL_RETRY_DELAYS_SECONDS` (default three: at once, then 30 s, then 120 s).
Only then is the owner asked to continue. From `LEAD_FALLBACK_AFTER` failed tries on (default 2),
the turn goes to the first usable `VUZOL_PROJECT_LEAD__LEAD_FALLBACK_PROFILE_IDS` profile.
The cheap lead stays the default; a bad answer costs a redo, not the owner's attention.

Lead turns run on `VUZOL_PROJECT_LEAD__LEAD_PARALLELISM` executor lanes (default 1). Each lane has
its own owner name, because a goal lease names the lane that holds it. A goal still has one lead
turn at a time: a lane that finds the lease held defers the wakeup.

### Current model choices

Model choices are installation configuration, never names in application logic. As of
November 2026 the deployment uses:

- **Project lead:** Muse Spark through Pi on OpenCode Go (`pi-opencode-go-c-muse`); an
  unreadable answer is redone, and later tries fall back to DeepSeek V4.1 Flash
  (`VUZOL_PROJECT_LEAD__LEAD_FALLBACK_PROFILE_IDS`).
- **Reviewer** (task and goal): MiMo V2.6 Flash through Pi.
- **Planner and coding executor:** Codex (`gpt-6.1-sol`) on a ChatGPT subscription, with Pi
  DeepSeek as the planner fallback.
- **Assistant:** Codex Sol through the model-call relay, with DeepSeek doing web research.
- **Inbox, monitor triage:** DeepSeek V4.1 Flash over the OpenCode Go API.
- **Voice:** OpenAI `gpt-4o-transcribe`.

A profile for another model of the same account inherits its binding:

```toml
[[profiles]]
id = "pi-opencode-go-c-muse"
base_profile_id = "pi-opencode-go-c"
model = "muse-spark-1.3-contributor"
model_reasoning_effort = "high"
routing_priority = 242
```

Select profiles per project with `/model`, or set installation defaults such as
`VUZOL_PROJECT_LEAD__PROFILE_ID` and `VUZOL_PROJECT_LEAD__REVIEWER_PROFILE_ID`. A default
does not override a project pin. Pi passes `model_reasoning_effort` as `--thinking`, and the
sandbox boundary rejects a different effort. Selecting another profile does not resume work,
replace the reviewer or rewrite old invocation receipts. Model selection grants neither goal
execution permission nor approval for publication.

Each Codex account needs its own provider state directory and `runtime_identity`; the main
host is the only refresh owner of its OAuth login (see the deployment guide).

### Planning and report tasks

Planning runs only through `leadership/planning.py` and the `planner_strong` slot.
Its result is a plan revision requiring owner approval. Per-task planner routing,
planner token settings and step-result handoff were removed in PR8. Workflow CLI
read-only calls now cover review; leadership planning uses the turn transport.

Repository analysis and architecture tasks use the fixed executor's read-only report path,
including validation, review and result approval. Subscription agents receive repository read
permissions; report approval does not apply a code change.

The dedicated executor also registers isolated Codex and Grok CLI transports behind the Step 08
worktree, supervised-process, rootless-sandbox, and controlled-egress boundary. Provider state,
worktrees, containers, proxies, credentials, and concurrency are separated by profile and task.
Provider output is typed and untrusted; result validation independently measures scope and Git
facts, runs trusted gates in a separate pinned validation image, and creates a result commit only
after verification succeeds.

Production coding work enters through the project lead: each approved plan item runs as a
`fixed_executor` workflow. Push, deployment, privileged execution, and automatic trust promotion
remain unavailable.
