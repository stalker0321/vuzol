# Retired horizon runtime

PR8 retired the horizon planner, settings flag and sequencer transitions. Planning
now belongs to the project lead and its `planner_strong` tool; plans are stored as
revisions and require owner approval. See [architecture invariants](ARCHITECTURE_INVARIANTS.md).

The cleanup after it removed the rest: package goals and exit criteria, the
`acceptance` step and its evidence/waiver tables, criterion buttons on task cards and
the promotion gate before apply. Managed goals carry their own criteria; the lead
checks them through goal review and the owner accepts the goal from the final card.
Migration `b7d3e9f1c2a4` drops the empty columns and tables and refuses to run if any
of them still holds data.

Worker, executor and applier refuse startup if a non-terminal run needs a removed
handler. Cancel such runs before upgrading; the guard never converts them.

Managed-goal work limits are authoritative. Provider admission includes settled
and outstanding receipts under the goal and all its historical package IDs;
retries and budget epochs do not refund spend. The `horizon_id` column on usage
records and reservations is that accounting scope key.
