# Telegram Workspace

## Runtime boundary

The Telegram integration uses `python-telegram-bot` 22 in long-polling mode. Telegram library
objects remain in the adapter and durable transport inbox; business ingress and control services consume frozen,
provider-neutral DTOs. A webhook can later feed the same services.

One bot identity is used for both processes. The bot token is resolved from
`telegram_bot_token_reference` only by Telegram ingress and Telegram delivery; no second bot or
token exists.
Allowed chat and user IDs are checked before message content is persisted or interpreted.

## Processes and delivery

`vuzol-telegram` owns long-polling ingress. `vuzol-telegram-delivery` is a separate long-running
consumer that owns only outbox rows whose destination is `telegram` or `work_package_projection`;
it never claims `workflow_control`, `telegram_file`, or future provider destinations. Attachment
download remains deferred to Step 05.

Delivery claims use PostgreSQL `SKIP LOCKED`, lease owner, expiry, and a monotonically increasing
fencing generation. A crashed worker's expired item can be reclaimed, while stale generations
cannot complete it. API calls occur after claim commit. Confirmed message links and final delivery
state are persisted together after Telegram returns success.

Transient Telegram failures return to `pending` with bounded exponential backoff and become
`dead_letter` after the configured attempt limit. Permanent failures go directly to dead letter.
When Telegram may have accepted a send but did not return a message ID, the item becomes
`ambiguous` and is never automatically reclaimed or resent. Reconciliation is operational work,
not an in-memory restart flag.

## Project-lead cards

Each goal has one live status message, edited in place. It says what is happening
and whether the owner needs to act; profile, account and runtime details are omitted.
Questions and terminal outcomes get separate messages. Plan approval is the plan card
itself: full item summaries, short scope/checks, flagged risks, and **✅ Запускай**,
**✏️ Изменить**, **❌ Отменить**. The lead does not repeat the approval question.
Change prompts the owner to reply to that card; the lead receives the reply together
with the card's saved plan revision. Only the owner approval button authorizes the plan.

Other questions accept a Telegram reply to their message or a short inline choice.
The delivered message binds the answer to the exact question version, even after
pausing; old questions cannot answer newer ones. `/lead_answer <goal-id> <version> <answer>`
remains a fallback. Plain text without a reply in lead mode is an owner message.

These reads use the existing authorized `get_goal` boundary:

- `/lead_status`: refresh the live state card;
- `/lead_goal`: full original request, scope and acceptance criteria;
- `/lead_details`: goal, completed work, remaining work, blockers and owner actions;
- `/lead_debug`: IDs, versions, branch/commit, limits and raw criterion coverage.

Reads do not start/resume work or change the goal version. They remain available to
the owner when the lead feature is disabled. An allowed Telegram user is not
automatically the owner of every goal. Answers never approve a plan or resume a pause.
Owner-facing lead replies, questions and notices strip UUIDs and full 40/64-hex hashes;
identifiers remain available in debug output and server-side callback bindings.

`ГОТОВО` is green only for a completed goal whose current recorded result passes
the existing evidence assessment. A commit, a completed package or a historical
`completed` flag alone is insufficient. Active repair is not presented as a terminal
error. Full details are available separately instead of showing internal enums in
the ordinary state card. This projection does not introduce a new acceptance checker
or re-run live Git validation; it reads the canonical recorded evidence.

Cards use the existing safe CommonMark-to-Telegram-HTML renderer, not raw model HTML
or Rich Messages. A successful mutation already queues its state card atomically
with its receipt; Telegram ingress no longer queues a second success echo when
that notice addresses the calling topic. A goal without that notification origin
still gets a reply in the calling topic.
Long formatted notices reuse the durable reply-part chain: only the first part is
initially queued; a confirmed send admits the next part in the same fenced completion
transaction. Restart resumes the chain. An ambiguous send admits no next part and is
not automatically resent. Definite markup rejection uses the existing escaped plain
fallback. Existing literal notices remain literal.

Offline visual examples (working, ready, paused, question, error), with synthetic
data and the production renderer:

```bash
PYTHONPATH=src python tests/preview_lead_cards.py /tmp/vuzol-lead-cards
# Optional: --png uses an already-installed ImageMagick/Pango; installs nothing.
```

Open `index.html` locally. PNGs use host fonts, which may lack emoji glyphs; the
status text still identifies every state. These are rendered layout previews, not
screenshots from a live Telegram client, and generate no external messages.
With `project_lead.enabled=true`, plain project-topic text and `/lead <goal>` record the
same owner event. Without a live goal this creates a draft conversation, with no Task card.
The lead answers questions or requests a strong plan; the owner approves the plan card before
execution. Later messages join the live goal without implicitly changing scope or resuming it.
`/think` freezes read-only mode on new messages through the same lead turn. Mode switches need
`chat.enabled=true`; neither admission nor managed plan buttons require the legacy
`project_discussion_enabled` flag. This text-entry change requires no schema migration.

## Ingress and affinity

Polling first commits a raw receipt (`external_inbox.source=telegram_transport`) and a
`telegram_ingress:main` outbox job in one transaction, **before** acknowledging the update offset.
The chat/user allowlist is checked before storing a body; unauthorized identities leave only a
hash/rejection receipt and no replayable job. Authorized bodies are bounded to 1,000,000 serialized
bytes. Oversized updates retain a rejection/hash, not their body.
The raw JSON is stored as an escaped string, not decoded nested JSONB, so text containing
PostgreSQL-incompatible characters cannot make admission retry forever.

A process-owned consumer dispatches committed jobs to the existing ingress/control services.
Their independent business inbox receipt, intake/task creation, message links, and acknowledgement
outbox records still share one transaction. Restart before processing or after business commit but
before transport ack reuses those receipts; it does not create duplicate tasks.
Database failure during raw admission retains the old Telegram offset. Business-processing
failure does not stall polling: attempts have a 30-second cooperative timeout, a 60-second fenced
lease, at most three tries, and a 1/2-second retry delay. Other available updates can progress
between retries; strict per-chat ordering across failures is not promised.

Exhausted jobs remain in `dead_letter`, with an inbox failure receipt and a secret-safe log
containing the job ID and exception class, never the body or exception text. After diagnosing
and correcting the handler failure, inspect/replay only the selected job using the existing
operator tool (against an explicitly chosen database):

```bash
vuzol-outbox-replay --destination telegram_ingress:main --item-id UUID --json
vuzol-outbox-replay --destination telegram_ingress:main --item-id UUID --apply --json
```

The first command is read-only; `--apply` requeues with a fresh retry budget. Oversized/rejected
bodies cannot be recovered by replay: request a new valid message. Raw authorized updates are
private outbox content; do not expose them in logs or dashboards. This change does not add an
automatic retention/erasure policy for those bodies.
No new broker, migration, bot token, or project-lead flag is needed. The consumer starts/stops
with `vuzol-telegram`; outgoing delivery does not claim its destination.

Topic scope is resolved from stable chat and thread IDs, never a display name.

Task affinity is resolved from a reply-linked task first, then from exactly one `awaiting_user`
task in the topic awaiting a clarification answer; otherwise a new task is created. Step 05 will
handle semantic interpretation and explicit references.

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

Delivery is configurable with `VUZOL_TELEGRAM__ORCHESTRATION_TRACE_ENABLED` and deterministic
task-level sampling through `VUZOL_TELEGRAM__ORCHESTRATION_TRACE_SAMPLE_PERCENT` (`0`–`100`). The
same task always lands in the same sample bucket across processes and restarts, so its routine
interpreter and planner story is kept or suppressed together. The default remains `100` for
backward-compatible visibility. With
`VUZOL_TELEGRAM__ORCHESTRATION_TRACE_ALWAYS_INCLUDE_ANOMALIES=true` (the default), repair/policy
changes plus failed, empty, or token-truncated planner outcomes bypass routine sampling. Disabling
traces suppresses anomalies too. Sampling changes only Telegram projection: the source records and
outbox item remain durable.

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

1. every non-terminal task across all projects in the forum in a compact two-line row (project,
   public/local number and assigned model; then the one-sentence goal);
2. **subscription limits** for every enabled CLI Codex/Grok profile: company, plan, a monospace
   used/remaining bar, percent left, and compact reset time on the same line. Windows without data
   (for example a missing 5-hour quota) are omitted entirely.

Codex limits are read from ChatGPT usage (`wham/usage`) with the profile state `auth.json`.
Grok limits prefer the billing credits payload, with a fallback to the latest local billing log
under the profile state directory. Fetch failures are non-fatal and render as fixed, secret-safe
Russian guidance; retryable failures explicitly suggest `/update`, while authorization or profile
configuration failures do not pretend that a refresh can repair them.

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

### Project model assignments (`/model`)

In a project topic, `/model` opens the durable project role matrix. Choose a role, then
an enabled profile. The catalog displays provider/model, profile ID, account, and API/CLI
transport; no vendor is permanently assigned to a role. Available slots are lead, goal
verification, light/strong planner, light/strong reviewer, and coding executor. Light/strong
classification remains deterministic policy, not a model-selected privilege.

The menu is built from configured profiles and permitted roles. "По умолчанию" restores
the installation default/automatic policy for that slot. Model/reasoning variants are profile
configuration, not a hardcoded list of product names. Adding a new authenticated account or
new profile remains an operator configuration operation; choosing an existing one requires
no code edit, deployment, or restart.

Changes affect new workflow runs and new leadership turns. Already-created runs, retries,
and in-flight turns preserve their frozen assignment. Explicit selections never silently
fall back to another profile; a disabled/changed/unavailable selection blocks visibly.
The coding executor slot does not change research/API executor steps. Interpretation,
transcription, and synthesis retain their existing configuration paths.

Every callback uses the existing owner/project authorization, durable deduplication, and
preference revision check. Old buttons ask for a fresh `/model` menu. Selecting a model does
not approve effects, unpause a goal, or answer a pending question. Use normal goal controls
to continue waiting work. Legacy executor-family preferences remain compatible for old runs;
new executor selection/reset clears the old preference. See [Providers](PROVIDERS.md) for
pinning, shared-account limits, transport ownership, and recovery constraints.

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

`/import` in `Новый проект` starts a separate existing-repository intake. Vuzol asks for one public
`https://github.com/owner/repository` URL, derives the project identity from the repository name,
and persists the source URL before the privileged provisioner clones it. Git credentials, local
paths, URL credentials, non-HTTPS protocols, and non-GitHub hosts are rejected in this first bounded
version. The provisioner retains the remote default branch, infers a conservative existing test
command without installing dependencies, and enables static deployment only for an obvious static
repository. It then creates the project topic through the same durable provisioning path.

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
execution worker model from worker steps (`execute_model`, `execute_code`, `execute_agent`,
`research_execute`, `synthesize`) rather than a planner or reviewer.

Control buttons use Russian labels while keeping stable callback action tokens. The exact rendered
matrix is:

| Persisted state / projection condition | Labels shown | Callback actions | Target |
|---|---|---|---|
| Pending exact-result approval (takes precedence over task controls); capability/dependency provisioning approvals show only `Принять` / `Отклонить` | `Принять`, `Переделать`, `Отклонить` | `approve`, `redo`, `reject` | approval ID |
| Run is `created` and no pending result approval exists | `Старт` | `start` | task ID |
| Task is `received`, `context_prepared`, `planned`, `waiting_approval`, `executing`, `validating`, `reviewing`, or `retrying` | `Пауза`, `Отмена` | `pause`, `cancel` | task ID |
| Task is `paused` | `Продолжить`, `Отмена` | `resume`, `cancel` | task ID |
| Task is `interpreted`, `awaiting_user`, `quota_exhausted`, or terminal (`blocked`, `failed`, `cancelled`, `rolled_back`, `completed`) | no buttons; work-package items additionally render `Повторить` and `Изменить` on the status card for failed/blocked/quota-exhausted plans | — | — |
| Approval is no longer pending | no buttons; persisted decision is shown | — | — |

The stable legacy `retry` callback token is not generated by the normal Telegram UI, but work-package
item cards do render fenced item-level `Повторить` (retry) and `Изменить` buttons. A `retrying` task
therefore shows `Пауза` and `Отмена`.

Approve, Redo, and Reject callbacks target the persisted approval ID, not a mutable task label. The
canonical approval envelope binds the target head, base and result commits, diff hash, gate
evidence, and configuration/policy revisions.

Approve queues the exact result for the separate `vuzol-applier` process. The applier revalidates
project policy and repository identity, fetches the retained commit locally, and advances the
configured branch with Git compare-and-swap. Target drift blocks the step; it never falls back to a
merge, push, or deployment. Redo rejects and closes the current result, then asks the user to send
corrected instructions in the project topic; `/sol` remains an optional explicit route. Reject
cancels the result without applying it.

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
6. Create two active tasks where exactly one is `awaiting_user`, send a plain continuation, and
   confirm it lands on that task as a clarification answer; a standalone message with no
   `awaiting_user` task creates a new task.
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
`codex-a` (a local overlay account profile), uses the current managed project revision, runs every trusted repository
gate, permits no automatic LLM repair, retains the result, and requests the exact-result Telegram
decision described above. An approved result may advance only the local managed branch; it is never
pushed or deployed. Ordinary messages and non-project topics do not enter this coding path.
