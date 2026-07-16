# ADR-0009 — Explicit Project Provisioning from the Inbox Topic

## Status

Accepted

## Decision

An allowlisted message in the dedicated `Новый проект` forum topic is an explicit request to
provision one project. Text and transcribed voice use the semantic interpreter to propose a bounded
set of nine ASCII project IDs and display names for the same persisted description. The initial
message does not choose the final identity and does not start provisioning. Telegram exposes the
options through revisioned inline callbacks. Only the intake author can choose a current option;
deterministic policy and a transaction-scoped collision lock validate that selection.

`Другие варианты` first advances the canonical revision and queues best-effort deletion of the old
card, then asks the semantic interpreter for nine fresh options. A callback from any older card is
rejected even when Telegram deletion fails, so regeneration cannot leave an actionable stale name.

PostgreSQL stores the canonical provisioning request and stage. A separate bounded privileged
worker creates an empty Git repository, creates one Telegram project topic, writes a validated
dynamic registry overlay, and restarts only processes that cache registry configuration. New
projects inherit the configured project template's sandbox, network, and Git delivery policy.

Telegram topic creation is non-idempotent. If its outcome is unknown, automatic retry is forbidden:
the request becomes blocked for manual reconciliation rather than risking a duplicate topic.

## Consequences

- ordinary messages cannot create unknown projects;
- the inbox topic is an explicit authority boundary, not name-based routing;
- model-proposed identities are inert until the intake author selects one;
- stale or non-author naming callbacks cannot start provisioning;
- no remote repository, push, deployment, dependency installation, or arbitrary command occurs;
- repository and registry paths are deterministic children of configured roots;
- a project becomes usable only after repository, topic, overlay, and process reload all succeed;
- provisioning can be reconstructed from PostgreSQL plus the registry overlay.
