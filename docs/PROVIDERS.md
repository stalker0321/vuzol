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

### Account profiles live only in the local overlay

Account-bound CLI profiles are local facts and never belong in the tracked registry. The tracked
`deploy/registries.executor.toml` carries only provider facts without an account
binding: the planner and reviewer API profiles, prepared API workers, sandboxes,
projects, and topics. Every account profile moves to the untracked overlay
selected by `VUZOL_REGISTRY_OVERLAY_FILE`; the production loader appends it to
the tracked registry before validation. One account maps to one state directory,
but may expose several model/role profiles through an explicit shared `account_id`.
There is no implicit default account or cross-account fallback for explicit choices.

The current local overlay is `/var/lib/vuzol/registry/account-profiles.toml`.
Because it holds host-specific `state_directory` values, it is checked by a
local script outside the repository (`verify-account-profiles.py` next to the
overlay), not by a checked-in unit test. The naming standard is `codex-a/b/c`, `opencode-a/b/c`, and
`tokenrouter-a`, with a model suffix only when a key is bound to a single model
(`tokenrouter-a-<model>`); `grok-subscription-a/b` and `pi-opencode-go-a/b/c`
already comply. `deploy/mvp/check.py` enforces that the tracked registry carries
no account-bound profile, so account profiles stay outside the MVP readiness
check by construction.

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
or revised profiles fail closed with a visible unresolved-choice error. Workflow run creation
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

### Selecting today's models (configuration, not application code)

The 2026-10-21 requested lead selection is **Muse Spark 1.3 Contributor / high**
through the existing Pi → OpenCode Go account C. Its native Pi 1.0.3 catalog ID
is `muse-spark-1.3-contributor`; the catalog maps `high` to the upstream `high`
effort. This is an installation/profile choice, not a model name in application logic.
The independent reviewer remains MiMo V2.6 Flash. Example overlay addition:

```toml
[[profiles]]
id = "pi-opencode-go-c-muse"
base_profile_id = "pi-opencode-go-c"
model = "muse-spark-1.3-contributor"
model_reasoning_effort = "high"
routing_priority = 242
```

Set the installation's `VUZOL_PROJECT_LEAD__PROFILE_ID` to that profile, and use
the existing model-matrix application API or `/model` to change any explicit
project lead selection. Changing a default alone does not override a project pin.
Pi now passes `model_reasoning_effort` as `--thinking`; both normal worktree and
snapshot transports enforce that exact choice. Missing or different effort is
not silently accepted by the sandbox boundary. Selecting another profile does
not resume work, replace the reviewer, or rewrite old invocation receipts.

The DeepSeek setup below records the preceding rollout, not a fixed dependency.

The requested choices are DeepSeek `deepseek-v4.1-flash` for lead and MiMo
`mimo-v2.6-flash` for review, both through Pi/OpenCode Go account C. These identifiers are
choices for the local overlay, not constants in lead/review policy, and are not an attestation
that production has been switched.

Keep `pi-opencode-go-c` as the DeepSeek profile, give it `account_id = "opencode-go-c"`.
Add another profile inheriting that binding, with a distinct ID and `model = "mimo-v2.6-flash"`.
Both can declare `roles = ["lead", "planner", "reviewer", "executor"]`: today's assignment
does not restrict which role either model may be selected for tomorrow. Per-role read-only
execution and independent-review checks still apply regardless of these capability flags.
Both share the same identity/state/network/concurrency. Pi's local authenticated provider
catalog must expose both model IDs; validate metadata without inference. Do not replace Pi
with a direct API endpoint merely because the account also offers one.

Then select the profiles through `/model`, or set installation defaults:

```text
VUZOL_PROJECT_LEAD__PROFILE_ID=pi-opencode-go-c
VUZOL_PROJECT_LEAD__REVIEWER_PROFILE_ID=<local-mimo-c-profile-id>
VUZOL_EXECUTION__REVIEWER_PROFILE_ID=<local-mimo-c-profile-id>
```

Keep `VUZOL_PROJECT_LEAD__ENABLED=false` while preparing the release. Test-enable it only
after migrations, host transport/secret preflight, and independent review; enable a managed
goal through its normal owner command. Disable new turns with the feature flag, and use
pause/cancel/revoke for an existing goal and its child work. Model selection grants neither
goal execution permission nor approval for publication/deployment.

### Model-assignment verification checkpoint (2026-10-21)

Implementation commit: `3e3bf05` on `agent/project-lead-v1` (baseline `44d527a`);
additional automatic-planner/cleanup assertions: `c38f999`.
No production deployment, migration, model invocation, Telegram mutation, or push was performed
for this change. Production was read-only attested on the existing T128 image; all three host
services remained active. Stop here for independent review before a coordinated rollout.

Verified contracts include role/profile selection and stale-button CAS; real host-entrypoint
dispatch to API and Pi with fake external I/O; immutable run/turn/reviewer pins; receipt replay
without another model call; crash between commands; pause/revoke cancellation; late receipt
accounting; shared-account admission, uncertain commit, disconnect, and cleanup recovery.
The installed Pi catalog was queried offline with placeholder credentials and network disabled;
it lists `opencode-go/mimo-v2.6-flash`. Authenticated account metadata (not inference) also listed
`deepseek-v4.1-flash` and `mimo-v2.6-flash` for account C. This does not attest the quality or
availability of a real generated response from either model.

The test PostgreSQL instance was an isolated container named
`vuzol-model-switch-test-20261020`, label `vuzol.disposable-test=model-switch-20261020`,
bound to loopback port 32774. Database/user were explicitly verified as
`vuzol_model_switch_test` / `vuzol_model_test`; production/shared/default DSNs were not used.
The existing disposable test venv was used, not the production venv. The integration fixture
terminates connections and truncates tables: serialize these commands and never substitute a
production DSN. Recreate an isolated instance before reusing commands after its removal.
After verification the named test container and its sole anonymous PostgreSQL volume were
removed. Test rows were disposable and are not backed up; fixtures recreate them, and the
representative trace is preserved below. No production database, container, or volume was removed.

Actual command environment (local disposable password, not a production credential):

```sh
cd /home/vodkolyan/vuzol-afterword-stabilization
export PATH=/tmp/vuzol-stage2.jzwfpD/venv/bin:$PATH
export PYTHONPATH=src:.
export PYTHONDONTWRITEBYTECODE=1
export VUZOL_TEST_DATABASE_DSN=postgresql://vuzol_model_test:disposable-local-only@127.0.0.1:32774/vuzol_model_switch_test # pragma: allowlist secret - disposable test credential
export VUZOL_MODEL_TEST_DSN=postgresql+psycopg://vuzol_model_test:disposable-local-only@127.0.0.1:32774/vuzol_model_switch_test # pragma: allowlist secret - disposable test credential
VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_MODEL_TEST_DSN alembic upgrade head
pytest --no-cov -q -rs tests/unit -m 'not docker' --tb=short
pytest --no-cov -q tests/integration -m 'not docker' --tb=short
mypy src/vuzol
git diff --check
```

Fresh migrations reached `d7a4f50e9404`; the c6/d7 pair was also downgraded/reapplied in
this disposable database during development. Do not infer safe production downgrade:
b5 explicitly refuses restoration of workflow NOT NULL fields once turn history exists.
A rollback must preserve compatible migration metadata and account/turn cleanup handling;
an old runtime cannot simply be pointed at the new head.

Final unit result: **3364 passed, 1 skipped** (remote execution is explicitly out of scope).
Type checking: **298 source files passed**. Changed Python files passed Ruff; the complete
source-tree Ruff invocation also reported a pre-existing `SIM105` in `execution/git.py`, which
was not changed. Secret scanning of the complete changed-file set passed after allowlisting
only literal migration identifiers and the explicitly disposable test DSNs, not production
credentials. Coverage collection was disabled in these
test commands; these are not coverage claims.

Full integration result: **898 passed, 1 skipped, 15 deselected** (`not docker`), 662.06 seconds.
The existing skipped test is `test_completed_plan_enqueues_system_trace`; its skip marker was
not changed. Docker-marked acceptance tests were excluded, not reported as passed.
This run started before the final auto-planner dispatch adjustment; the final routing/provider/
leadership paths were rerun separately afterward. The full run emitted 12 Telegram deprecation
warnings and one SQLAlchemy warning about a garbage-collected, non-checked-in connection. Its
allocation site was not established; it is not claimed fixed or suppressed. The warning appeared
during a Telegram projection test, which does not by itself establish the leaking caller.

After the final auto-planner adjustment, the following real-PostgreSQL group passed
**224 tests**, with SQLAlchemy warnings treated as errors and no reported warnings:

```sh
pytest --no-cov -q -rs tests/integration/providers tests/integration/leadership tests/integration/test_secret_scan.py --tb=short -W error::sqlalchemy.exc.SAWarning
git diff --name-only -z 44d527a -- '*.py' | xargs -0 ruff check
git diff --name-only -z 44d527a | xargs -0 detect-secrets-hook --baseline .secrets.baseline
```

An early development run also observed one non-reproduced shared-account turn ending with
an unknown receipt. A separate simultaneous receipt/watchdog failure race was reproduced and
fixed with deterministic fault tests, but it was not proven to be the original observation's
cause. Subsequent focused and full runs passed; repeated disconnect/cancellation soak testing
remains appropriate at independent review.

Rollout prerequisites include agreeing worker/host ownership flags, validating the local account
overlay with the new shared-account binding, scoped credentials for selected API roles, and a
schema-compatible rollback. In the inspected production configuration, enabled planning/review
profiles are CLI profiles; no extra API secret was copied or granted to the host. Adding an API
profile later still requires its scoped credential to be provisioned for its execution owner.

The original stage-2 goal/review/repair/restart scenario remains a real local Git/PostgreSQL test
with fake models and Telegram delivery, not a production scenario. Its request is explicitly a
marker-file outcome; the separate OAuth regression verifies that marker files cannot prove an
OAuth goal. Reproduce its trace after other integration tests finish:

```sh
pytest --no-cov -q -s tests/integration/leadership/test_managed_goal_flow.py::test_managed_goal_reviews_repairs_and_resumes_after_restart
```

Trace contract: owner `/lead` → package A → review rejection → repair → approved managed
integration commit (main unchanged) → new Python process and fresh database composition →
package B exactly once → independent criterion evidence → completed goal → meaningful fake
Telegram result/status. Turn-bound Pi transport and model switching are verified separately
through the actual executor entrypoint; the two tests are not presented as a paid DeepSeek/MiMo
end-to-end run.

Final trace plus model-matrix checks: **13 passed**, 22.78 seconds, with SQLAlchemy and
unraisable-exception warnings treated as errors (no warnings reported):

```sh
pytest --no-cov -q -s tests/integration/leadership/test_managed_goal_flow.py::test_managed_goal_reviews_repairs_and_resumes_after_restart tests/integration/providers/test_model_assignments.py --tb=short -W error::sqlalchemy.exc.SAWarning -W error::pytest.PytestUnraisableExceptionWarning
```

Captured disposable-scenario trace (2026-10-20 23:09 UTC / 2026-10-21 Amsterdam):

```text
package_a=5a1d8415-756c-4cbd-833f-59258739bfd5
head_a=59a4fcd81d6b53a58fb98917d743f928ff17377f
restart=separate Python process; fresh composition and DB engine
goal=55f66b00-90cc-4a2d-82b0-fb2d9b8333d8
branch=vuzol-managed/vuzol/847ef779c02d
package_b=3f89db67-30ac-4b35-a3ba-44d5d1638c55
lifecycle=completed
head_b=14dbe1f926358fce7bbdbe0b84115bd053384061
main=852d20746552746979014caf39b06e558de62361
```

### HTTP-account review follow-up (2026-10-21)

Baseline: clean `agent/project-lead-v1` at `24d0367`. Review input:
`/home/vodkolyan/vuzol-audit-2026-10-21/REVIEW_MODEL_ROLES.md` and its accompanying
`test_account_http_rejection.py`. P1 is fixed by the explicit completion-evidence contract
above, not by changing roles, setting `request_sent=False`, inventing usage, or expiring tickets.
No new migration is needed. Production, defaults, live accounts and services were not changed;
the existing rollout prerequisites and disabled-by-default lead setting still apply.

The original external probe produced **3 failures** and was left unchanged. Its injected
`httpx.AsyncClient` lacks `base_url`: with this installed httpx, the mock sees the request but
cookie/URL processing then raises `ValueError("unknown url type: '/chat/completions'")` before
the adapter receives the rejection. This is not itself proof of HTTP rejection classification.
The stock copy sets the client's base URL and additionally asserts the actual failure category;
all original ticket/second-HTTP-call assertions remain. With the old account-release condition
temporarily restored, this corrected test produced **7 assertion failures** for
429/401/400/403/404/413/422 (ticket retained, one HTTP call). With the fix, these pass.
The unmodified external script is not reported as passing.

Regression coverage includes real `AdapterRegistry` construction/secret resolution and a real
HTTP adapter with only `httpx.MockTransport` substituted: rejection → fresh registry with
corrected credentials/request → successful call by another role on the same account. Timeout,
disconnect, protocol error, HTTP 408/499/500/502/504 → durable ticket retained → a fresh registry
cannot send again. These are registry reconstruction tests, not a new OS process or a live
provider pilot. The real worker entrypoint verifies that a rejected lead call releases admission
while recording unknown, nonzero conservative cost and parking the goal for the owner.
CLI tests separately distinguish a returned process result from an uncertain transport exception;
token mismatch, lost ownership, cancellation and unresolved cleanup still prevent release.

Verification used only disposable container `vuzol-http-review-test-20261021`, label
`vuzol.disposable-test=http-review-20261021`, loopback port 32775. SQL verified database/user
as `vuzol_http_review_test` / `vuzol_http_test` before migrations reached `d7a4f50e9404`.
Use the disposable venv, not the production venv. Serialize PostgreSQL tests: the fixture
terminates connections and truncates its selected database. The DSNs below are recorded test
values, never production/shared defaults; recreate the isolated database before reusing them.

```sh
cd /home/vodkolyan/vuzol-afterword-stabilization
export PATH=/tmp/vuzol-stage2.jzwfpD/venv/bin:$PATH
export PYTHONPATH=src:.
export PYTHONDONTWRITEBYTECODE=1
export VUZOL_TEST_DATABASE_DSN=postgresql://vuzol_http_test:disposable-local-only@127.0.0.1:32775/vuzol_http_review_test # pragma: allowlist secret - disposable test credential
export VUZOL_HTTP_TEST_DSN=postgresql+psycopg://vuzol_http_test:disposable-local-only@127.0.0.1:32775/vuzol_http_review_test # pragma: allowlist secret - disposable test credential
VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_HTTP_TEST_DSN alembic upgrade head
pytest --no-cov -q -rs tests/unit --tb=short
pytest --no-cov -q tests/integration/providers/test_account_http_rejection.py tests/integration/leadership/test_runtime_provider_boundary.py --tb=short -W error::sqlalchemy.exc.SAWarning -W error::pytest.PytestUnraisableExceptionWarning
pytest --no-cov -q -rs tests/integration/providers tests/integration/leadership tests/integration/telegram/test_durable_ingress.py tests/integration/telegram/test_polling_commit.py tests/integration/workflows/test_production_consent.py tests/integration/test_secret_scan.py --tb=short -W error::sqlalchemy.exc.SAWarning -W error::pytest.PytestUnraisableExceptionWarning
mypy src/vuzol
git diff --check
```

Full unit result: **3391 passed, 1 skipped**, 35.71 seconds; the existing remote-execution
out-of-scope test remains skipped. Three dependency deprecation warnings (Starlette/PTB) were
reported, not suppressed. Focused HTTP/entrypoint check: **24 passed**, 14.20 seconds.
Final expanded integration group (the third pytest command): **271 passed**, 212.92 seconds,
no skips/deselections or warnings reported. This includes the final cross-role request checks.
Type checking: **298 source files passed**. These commands disable coverage collection;
there is no coverage claim. The full integration suite, paid/live providers, real Telegram
mutations and Docker-marked acceptance tests were not run for this follow-up. The pre-existing
full-suite connection warning described above is not claimed fixed by these changes.

All 12 changed Python files passed Ruff lint and format checks; changed-file secret scanning
and `git diff --check` passed. Reproduce lint/scanning against the pre-fix baseline:

```sh
git diff --name-only -z 24d0367 -- '*.py' | xargs -0 ruff check
git diff --name-only -z 24d0367 -- '*.py' | xargs -0 ruff format --check
git diff --name-only -z 24d0367 | xargs -0 detect-secrets-hook --baseline .secrets.baseline
```

After verification, the exact disposable container and its sole anonymous PostgreSQL volume
were removed and their absence checked. No production/shared data was deleted. Test rows are
not backed up; the fixtures recreate them in a new disposable database.

Remaining limit: genuinely unknown tickets still have no automatic expiry or operator recovery
command; no existing ticket is retroactively deleted by this fix. The goal can remain parked
after a rejection until normal owner controls are used. A bounded live pilot is still required
before claiming production readiness; this fix does not itself authorize or perform that pilot.

### Historical registry examples

Deployment preflight on 2026-10-21 found one remaining vendor-specific gate in
`build_bundle`: the installation-default reviewer still required Grok, despite generic
runtime dispatch. It now validates against the installed adapter catalog, retaining role,
coding task type, repository-read and CLI sandbox requirements. Regression checks cover all
four CLI transports and OpenAI-compatible API with execution both on and off; the corrected
test fixture reproduced 8 failures/2 passes before the fix. The resulting complete unit run
was **3404 passed, 1 skipped**, with three dependency deprecation warnings. This is separate
from the independent HTTP-fix acceptance above.

The checked-in executor registry (`deploy/registries.executor.toml`) and older local overlays
include the following examples; actual deployment defaults must be inspected locally:

| Role | Profile | Model | Credential | Output bound |
| --- | --- | --- | --- | --- |
| reviewer | `openrouter-mimo-reviewer-prod` | Xiaomi MiMo-V2.5 via OpenRouter, reasoning effort `low` | `VUZOL_OPENROUTER_REVIEWER_API_KEY` | derives its window from its own profile `output_limit` (8,000) |
| executor | account profiles (`codex-a`, `grok-subscription-a/b`, `tokenrouter-a`, `pi-opencode-go-a/b/c`) from the local overlay | subscription agent CLIs | subscription auth (no API keys) | sandbox-bound |
| interpreter / transcriber | configured in the interpreter registry, not this mirror | DeepSeek-based interpretation; OpenAI transcription API | their own credential references | their own profile limits |

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
Provider output is typed and untrusted; the host-side finalizer independently measures scope and
Git facts, runs trusted gates in a separate pinned validation image, and creates a result commit
only after verification succeeds.

Production user intake exposes only the explicit bounded `/sol` Telegram path documented in
`TELEGRAM.md`. It fixes the profile to `codex-a` (local overlay), accepts one to ten contained
repository-relative paths, permits no model repair, and retains rather than integrates the result.
Grok execution and multi-worker modes remain experimental evidence, not an automatically trusted
production route. Merge, push, deployment, privileged execution, and automatic trust promotion
remain unavailable.
