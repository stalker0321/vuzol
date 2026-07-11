# Telegram Workspace

## Runtime boundary

The Telegram integration uses `python-telegram-bot` 22 in long-polling mode. Telegram library
objects remain in `vuzol.telegram.adapter`; ingress and control services consume frozen,
provider-neutral DTOs. A webhook can later feed the same services.

The bot token is resolved from `telegram_bot_token_reference` only for the Telegram process.
Allowed chat and user IDs are checked before message content is persisted or interpreted.

## Ingress and affinity

Updates are keyed by bot identity and update ID in `external_inbox`. Inbox receipt, intake/task
creation, message links, and acknowledgement outbox records share one database transaction.
Topic scope is resolved from stable chat and thread IDs, never a display name.

Task affinity is resolved from a reply-linked task first, then exactly one active task in the
topic. Multiple active tasks produce a persisted clarification state. Step 05 will handle semantic
interpretation and explicit references.

Attachment metadata is validated before download. Counts, declared sizes, media types, unsafe
filenames, and archives are bounded or rejected. Durable quarantine download and content scanning
are intentionally owned by Step 05 because voice/transcription introduces the first consumer.

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
