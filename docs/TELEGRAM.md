# Telegram Workspace

## Runtime boundary

The Telegram integration uses `python-telegram-bot` 22 in long-polling mode. Telegram library
objects remain in `vuzol.telegram.adapter`; ingress and control services consume frozen,
provider-neutral DTOs. A webhook can later feed the same services.

One bot identity is used for both processes. The bot token is resolved from
`telegram_bot_token_reference` only by Telegram ingress and Telegram delivery; no second bot or
token exists.
Allowed chat and user IDs are checked before message content is persisted or interpreted.

## Processes and delivery

`vuzol-telegram` owns long-polling ingress. `vuzol-telegram-delivery` is a separate long-running
consumer that owns only outbox rows whose destination is `telegram`; it never claims
`workflow_control`, `telegram_file`, or future provider destinations. Attachment download remains
deferred to Step 05.

Delivery claims use PostgreSQL `SKIP LOCKED`, lease owner, expiry, and a monotonically increasing
fencing generation. A crashed worker's expired item can be reclaimed, while stale generations
cannot complete it. API calls occur after claim commit. Confirmed message links and final delivery
state are persisted together after Telegram returns success.

Transient Telegram failures return to `pending` with bounded exponential backoff and become
`dead_letter` after the configured attempt limit. Permanent failures go directly to dead letter.
When Telegram may have accepted a send but did not return a message ID, the item becomes
`ambiguous` and is never automatically reclaimed or resent. Reconciliation is operational work,
not an in-memory restart flag.

## Ingress and affinity

Updates are keyed by bot identity and update ID in `external_inbox`. Inbox receipt, intake/task
creation, message links, and acknowledgement outbox records share one database transaction.
Topic scope is resolved from stable chat and thread IDs, never a display name.

Task affinity is resolved from a reply-linked task first, then exactly one active task in the
topic. Multiple active tasks produce a persisted clarification state. Step 05 will handle semantic
interpretation and explicit references.

Attachment metadata is validated before download. Counts, declared sizes, media types, unsafe
filenames, and archives are bounded or rejected. The Step 05 interpreter runtime owns durable
`telegram_file` download, private artifact persistence, and voice transcription; Telegram delivery
never claims those records.

## Forum workspace

The configured forum is the shared control plane for every project. Stable `chat_id` +
`message_thread_id` mappings live in the registry; **display names and pin intent are product
policy** in `vuzol.telegram.layout`, not ad-hoc labels of one live group. On Telegram ingress
startup, Vuzol upserts every configured mapping into PostgreSQL and synchronizes system topic
names through the Bot API. Routing never depends on a mutable display name.

### Canonical layout (product policy)

Permanently pinned system topics, top → bottom:

| Pin order | Display name        | Kind              | Role                                      |
| --------- | ------------------- | ----------------- | ----------------------------------------- |
| 1         | `История`           | `changelog`       | Append-only cross-project history         |
| 2         | `Статус проектов`   | `task_dashboard`  | Global project dashboard                  |
| 3         | `Апрувы`            | `approvals`       | Exact-result decisions across projects    |
| 4         | `Новый проект`      | `inbox`           | Project intake and environment creation   |

Additional workspace roles that are **not** part of the fixed pin stack:

- `Система` (`system`) for operational alerts and bounded orchestration traces;
- one `<project>` (`project`) topic per provisioned project.

### Orchestration traces

For production dogfood, every completed semantic-interpretation call and every terminal planner
attempt emits a durable one-shot message into `Система`. These are diagnostic projections from
PostgreSQL/outbox state, not best-effort process logs, so a Telegram or service restart does not
silently lose them.

The interpreter trace shows the task number and project, profile/model, prompt and schema versions,
input/output tokens, duration, repair use, the model-produced `TaskDraft`, and—when different—the
effective draft after deterministic policy enforcement. The planner trace shows attempt/status,
profile/model, measured tokens and reserved output limit, `finish_reason`, plain and structured
output, explicit warnings for token-limited or empty results, and whether a validated plan was
handed off. Empty or token-truncated planner output is not marked completed; it fails with a
retryable category when attempts remain. A completed, non-empty plan is attached as bounded,
redacted `ProviderRequest.context` items for `execute_code` / `execute_agent`. Workflows
materialized without a plan step still execute with empty planner context.
All provider/user text is HTML-escaped and bounded below the Telegram message limit; credentials,
provider request IDs, raw prompts, and execution artifacts are not included. These traces are
observational only: they do not change routing, validation, approval, or task status.

### Project status dashboard

The product always reuses the existing system topic kind `task_dashboard` (canonical name
`Статус проектов` from `vuzol.telegram.layout`). It does **not** create a parallel topic and does
not hard-code a chat or thread id: each control forum supplies its own mapping in the registry
(`kind = "task_dashboard"`), and delivery resolves that mapping the same way approvals resolve
`kind = "approvals"`.

That topic holds **one** reconstructable message with two sections:

1. every non-terminal task across all projects in the forum (project, public/local number,
   one-sentence goal, assigned model);
2. **subscription limits** (English UI) for every enabled CLI Codex/Grok profile: company, plan
   (Plus / Super), a monospace used/remaining bar, percent left, and reset time on the next line.
   Windows without data (for example a missing 5-hour quota) are omitted entirely.

Codex limits are read from ChatGPT usage (`wham/usage`) with the profile state `auth.json`.
Grok limits prefer the billing credits payload, with a fallback to the latest local billing log
under the profile state directory. Fetch failures are non-fatal and render as «недоступны».

The message is created once and then only edited; new status lines never spam additional messages.
Delivery uses role `project_status_dashboard` and content-hash revision coalescing.

`blocked` is a terminal unsuccessful outcome for user-facing projections and task affinity, even
though PostgreSQL keeps the distinct canonical status so an operator can understand unknown
effects and explicitly reopen a safe task later. Consequently failed and blocked tasks disappear
from this dashboard immediately.

### Project topic pin lifecycle

- When a project is provisioned, its topic is created and marked `pinned=true` so it joins the pin
  stack **after** the four fixed system topics (new projects append next).
- When work on a project is paused or finished (detailed lifecycle later), the topic is marked
  unpinned and leaves the pin stack; the topic itself remains for history.
- Registry field `pinned` on a topic is optional: system control kinds pin by layout when enabled;
  project topics pin only when explicitly set (provisioning sets true).

Forum-topic pin/reorder is currently available only on Telegram's MTProto client API, not on the
Bot API used by Vuzol. The product still records and synchronizes **desired** pin state; live pin
enforcement lands when bot methods exist. Display-name synchronization already enforces the
canonical Russian labels for system kinds.

Project topics use adaptive workflow selection. Implementation requests use the coding workflow;
repository-aware architecture and design discussions use the read-only architecture-agent
workflow and return their textual result in the task card.

### Project executor preference (`/model`)

In a project topic, bare `/model` opens an inline chooser for that project's default coding
executor. The preference is **project-scoped** and durable in PostgreSQL: it applies to future
executor steps for the project until changed again. It is not a per-task override.

Choices:

- **Routing (auto)** — restore deterministic capability/health/budget routing (default);
- **Sol / Terra / Luna** — pin the Codex CLI executor and select the product model variant, then
  choose reasoning effort (`low` / `medium` / `high` / `xhigh`);
- **Grok** — pin the first healthy Grok CLI executor profile (same-family fallbacks only).

Pinned selection uses the router's trusted-profile path and stores model/effort overrides on the
claimed step payload so the sandbox transport validates the exact command. Cross-family automatic
fallback (for example Sol → Grok) is disabled while a pin is active. Planner and reviewer roles
are unchanged by this preference.

`Новый проект` is an explicit provisioning boundary. An allowlisted text or voice message is
treated as the project's nature and goal. The interpreter proposes nine distinct display-name and
bounded repository-ID pairs. Telegram presents them as three rows of inline buttons plus `Другие
варианты`; no repository, registry entry, or project topic exists yet. Only the author of the intake
may select a current option. Selection persists the final identity and creates the provisioning
request. Regeneration invalidates the old revision, best-effort deletes its card, and sends a new
card after fresh options are persisted, so old buttons cannot provision anything even if Telegram
could not delete the old message.

The project provisioner then creates an initial Git repository and one project topic, validates and
atomically writes the dynamic registry overlay, reloads registry-caching services, and posts the
project description into the new topic. It never creates a remote, pushes, deploys, installs
dependencies, or executes user-supplied commands. An unknown Telegram topic-creation outcome blocks
for reconciliation instead of retrying.

An exact-result approval has its own message link in the global approvals topic. It does not replace
the task status message in the project topic. After a decision, Vuzol edits the approval card to
remove its buttons and show the persisted outcome, then separately refreshes the project card.

## Controls and projections

Callbacks resolve a persisted target, verify authorization and current existence, deduplicate by
callback identity, and enqueue a workflow-control outbox record. They do not perform transitions or
dangerous work in the Telegram handler.

After a task succeeds, its project-topic status card shows a bounded but detailed implementation
report, preferring the approved human summary or structured provider result, plus the independently
measured trusted gate names when available. It deliberately does not show source code, a commit
identity, or the diff. Failed and blocked terminal cards instead show an unsuccessful outcome, the
exact failed/blocked step, and its persisted safe failure summary or category. Both successful and
unsuccessful terminal outcomes are appended once to `История`; history keeps the requested task
separate from either the result or failure reason. Successful reports retain one factual outcome
and at most six implementation bullets; provider hand-off sections such as plans, file lists, run
instructions, and suggested next steps are omitted. Project cards and history identify the actual
execution worker model from `execute_code` / `execute_agent` rather than a planner or reviewer.

Approve, Redo, and Reject callbacks target the persisted approval ID, not a mutable task label. The
canonical approval envelope binds the target head, base and result commits, diff hash, gate
evidence, and configuration/policy revisions.

Approve queues the exact result for the separate `vuzol-applier` process. The applier revalidates
project policy and repository identity, fetches the retained commit locally, and advances the
configured branch with Git compare-and-swap. Target drift blocks the step; it never falls back to a
merge, push, or deployment. Redo rejects and closes the current result, then asks for a new bounded
`/sol` request with corrected instructions. Reject cancels the result without applying it.

Status cards are rebuilt from tasks, runs, steps, and events in PostgreSQL. External text is escaped
centrally for Telegram HTML and bounded to Telegram message limits. Each message link stores the
last applied projection revision; stale edits are ignored. Per-task edit reservations coalesce rapid
updates at the caller.

Telegram sends and edits are outbox delivery operations. A confirmed initial send creates its
message link. A lost response is marked `ambiguous` and is excluded from normal outbox claiming, so
it cannot create an unbounded resend loop. Step 10 supplies operational reconciliation for these
records.

## Verification

The suite uses a fake Telegram client and a real local PostgreSQL database. It covers authorization,
deduplication, topic routing, renamed-topic independence, reply affinity, ambiguity, attachment
policy, callback idempotency, projection reconstruction, escaping, stale revisions, API failure,
lost responses, and edit rate limiting. No live Telegram account is required.

## Configuration and startup

Required runtime values are:

- `VUZOL_REGISTRY_FILE` (Compose mounts `VUZOL_REGISTRY_FILE_HOST` at this path);
- `VUZOL_DATABASE_DSN_REFERENCE` and its referenced DSN secret;
- `VUZOL_TELEGRAM_BOT_TOKEN_REFERENCE` and its referenced single bot token;
- non-empty `VUZOL_ALLOWED_USER_IDS` and `VUZOL_ALLOWED_CHAT_IDS` for ingress.

Delivery polling, lease, attempt, and retry bounds use the
`VUZOL_TELEGRAM__DELIVERY_*` settings shown in `.env.example`. After migrations and local `.env`
configuration, run `docker compose --profile telegram up`.

## Manual live smoke test

1. Configure a test forum group/topic, allowlisted user/chat IDs, and the one bot token locally.
2. Run migrations and start the `telegram` Compose profile; confirm both Telegram services become
   ready without logging the token.
3. Send a task containing `<`, `>`, and `&`; confirm one escaped status card appears in the topic.
4. Restart delivery and confirm the delivered acknowledgement is not duplicated.
5. Continue the task and confirm its existing status card is edited rather than duplicated.
6. Create multiple active tasks, send an ambiguous continuation, and confirm the clarification
   lists candidates without associating the message with either task.
7. Temporarily break network access, restore it, and confirm bounded retry. Simulate an unknown
   send outcome only in a controlled environment and confirm the row remains `ambiguous` without
   automatic resend.

## Bounded coding dogfood

The first production coding slice is deliberately explicit. In a project topic configured with
`default_workflow = "adaptive_worker_trial"`, an allowlisted user may submit:

```text
/sol src/vuzol/example.py tests/unit/test_example.py
Implement the bounded task described here.
```

The first line is the complete allowed-file scope; one to ten contained repository-relative paths
are accepted. The remaining lines are the goal. Vuzol fixes the worker profile to
`codex-subscription-prod`, uses the current managed project revision, runs every trusted repository
gate, permits no automatic LLM repair, retains the result, and requests the exact-result Telegram
decision described above. An approved result may advance only the local managed branch; it is never
pushed or deployed. Ordinary messages and non-project topics do not enter this coding path.
