# ADR-0002 — Use One Private Telegram Forum Group

## Status

Accepted

## Decision

Use one private Telegram supergroup with forum topics as the primary control interface.

Topics provide stable user-facing scopes such as project, personal, approvals, tasks, changelog, and system.

## Reason

Telegram is the most convenient daily interface for the user. Forum topics provide contextual separation without requiring one bot or one group per agent or project.

## Consequences

- topics are mapped by `chat_id + message_thread_id`, not name;
- project topics provide project context;
- replies and task buttons provide stronger task affinity;
- Telegram is not canonical state;
- a topic is not created for every trivial task;
- the bot must be allowlisted and appropriately privileged to manage topics.
