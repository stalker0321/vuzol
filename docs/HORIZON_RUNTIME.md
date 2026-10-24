# Historical horizon storage

PR8 retired the horizon planner, settings flag and sequencer transitions. Planning
now belongs to the project lead and its `planner_strong` tool; plans are stored as
revisions and require owner approval. See [architecture invariants](ARCHITECTURE_INVARIANTS.md).

The historical `work_packages` columns (goal, criteria, lifetime budget, deadline,
phase, acceptance and integration refs), plan revisions, links, approvals, events
and usage receipts remain unchanged. Telegram still renders their historical
phases. `acceptance` remains registered because stored coding v4 runs need it;
shared evidence and apply gates are preserved.

Worker, executor and applier refuse startup if a non-terminal run needs a removed
handler, or a non-terminal package still pins `horizon-v1:enabled`. This release
uses a read-only startup guard, not a data migration: the supplied production
inventory contains no horizon or unmanaged packages. Other installations must
resolve those packages before upgrading; the guard never silently converts them.

Managed-goal work limits are authoritative. Provider admission includes settled
and outstanding receipts under the goal and all its historical package IDs;
retries and budget epochs do not refund spend.
