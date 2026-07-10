# Step 04 — Telegram Forum Workspace

## Goal

Connect a private Telegram forum group as the control and presentation interface while preserving PostgreSQL as the canonical state.

## Deliverable

An authorized bot that understands forum topic scope, records message mappings, creates task intake records, maintains task projections, and handles explicit control callbacks safely.

## Applicable architecture decisions

- ADR-0001 — PostgreSQL Is the Canonical Source of Truth.
- ADR-0002 — Use One Private Telegram Forum Group.
- ADR-0007 — Transactional Delivery and Fenced Leases.
- ADR-0008 — Treat Repository Content and Model Output as Untrusted.

## Ingress requirements

For every update:

1. validate allowed chat and user;
2. persist update identity for duplicate detection;
3. resolve `chat_id`, `message_thread_id`, reply target, and attachments;
4. resolve the configured topic scope;
5. map replies to an existing task when possible;
6. send the raw input to later interpretation;
7. acknowledge intake without claiming execution has succeeded.

Unknown chats and users are rejected without creating executable work.

Ingress uses the external inbox defined in ADR-0007. Authorization failure may be recorded as minimal security metadata but must not retain unauthorized message content by default. Update processing and creation of canonical intake state occur transactionally after deduplication.

## Attachments and input limits

Before download or interpretation, enforce configured limits for message length, attachment count, declared and actual size, media type, and download duration. Treat filenames, archives, documents, images, audio, and extracted text as untrusted input.

- store Telegram `file_id` and `file_unique_id` as source metadata, not as the only durable copy when later execution requires the file;
- stream downloads into a task-scoped quarantine or artifact location;
- reject path traversal, archive expansion beyond configured limits, and unsupported formats;
- do not execute attachment content during intake;
- apply privacy-aware retention to voice and personal files.

## Topic kinds

Support at least:

- inbox;
- task dashboard;
- approvals;
- changelog;
- system;
- project;
- personal;
- research.

Project topics may define a project ID and default capability boundary.

Topic names are display metadata only.

## Task affinity

Resolve continuation in this order:

1. callback or explicit task reference;
2. reply to a persisted task-linked message;
3. dedicated task-topic binding;
4. exactly one active task in the current project topic;
5. later semantic interpretation with limited context;
6. clarification.

Never silently attach an ambiguous message to the most recent task.

## Message projections

Persist links for:

- source request;
- task status card;
- approval card;
- final result;
- artifact or diff messages;
- system alerts.

If a Telegram message is edited or deleted, canonical task state remains unchanged.

All sends and edits are produced through the transactional outbox. A projection carries a monotonically increasing desired revision so an older delayed edit cannot overwrite newer task state. Telegram formatting is escaped centrally, messages are split or converted to artifacts at API limits, and a lost API response is reconciled before retry when possible.

## Status UX

Use one editable status card per active task where practical.

The status card should expose:

- task ID and short title;
- project or scope;
- current status and step;
- selected executor when available;
- elapsed time;
- latest meaningful event;
- buttons relevant to the current state.

Do not update it for every tool call. Rate-limit edits and coalesce rapid changes.

## Dashboard topics

`Tasks` contains a compact active-task dashboard rather than a high-volume event stream.

`Changelog` is append-only human-readable completion history, not raw logs.

`System` contains operational events such as unhealthy profiles, failed backup, and quota exhaustion.

## Callback safety

Callbacks for approval, pause, resume, cancel, or retry must:

- contain or resolve a persisted action ID;
- verify user authorization;
- verify current task or step state;
- be idempotent;
- never execute a dangerous action directly in the callback handler;
- enqueue a state transition or decision instead.

## Bot mode

Long polling is acceptable for initial development. Webhook deployment may be introduced later without changing the Telegram service interface.

## Tests

Required:

- unauthorized chat rejected;
- unauthorized user rejected;
- duplicate update handled once;
- project topic resolves by IDs;
- renamed topic does not affect mapping;
- reply to task-linked message resolves the task;
- two active tasks in one topic force clarification;
- status card can be reconstructed from database state;
- duplicate callback does not duplicate a transition;
- Telegram API failure does not corrupt workflow state;
- a lost send response cannot create an unbounded duplicate-message loop;
- an old projection revision cannot overwrite a newer status card;
- oversized, unsupported, and archive-expansion-limit attachments are rejected safely;
- Markdown or HTML control characters in model output cannot corrupt message formatting;
- message-edit rate limiting works.

Use adapter-level tests with a fake Telegram client. Do not require live Telegram for the full test suite.

## Forbidden implementations

- topic-name routing;
- storing workflow state only in message text;
- parsing status by reading Telegram history;
- executing a shell command inside a callback handler;
- calling Telegram directly from a canonical-state transaction instead of using the outbox;
- treating attachment filenames or extracted content as trusted;
- giving every message to a model before authorization;
- sending every internal event to Telegram;
- creating a dedicated topic for every trivial task in the MVP.

## Acceptance criteria

- an allowlisted user can create a raw intake task from a project topic;
- replies preserve task affinity;
- ambiguity is represented and can trigger clarification later;
- dashboard and status projections are reconstructable;
- duplicate updates and callbacks are safe;
- Telegram failures do not roll back canonical business state incorrectly.

## Completion report

Report:

- Telegram library and bot mode;
- update deduplication design;
- affinity algorithm;
- topic registry usage;
- callback idempotency;
- projection rebuild behavior;
- tests and limitations.
