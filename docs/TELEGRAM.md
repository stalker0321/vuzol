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

## Controls and projections

Callbacks resolve a persisted target, verify authorization and current existence, deduplicate by
callback identity, and enqueue a workflow-control outbox record. They do not perform transitions or
dangerous work in the Telegram handler.

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
gate, permits no LLM repair, retains the result, and never merges or deploys it. Ordinary messages
and non-project topics do not enter this coding path.
